import { randomUUID } from 'node:crypto';
import { describe, it, expect, afterAll } from 'vitest';
import { watchPresence, type PresenceEvent } from '../../../src/presence/watch-presence.js';
import { PresenceStore } from '../../../src/presence/presence-store.js';
import type { DbResolver } from '../../../src/core/db-resolver.js';
import { ValueSignal } from '../../../src/common/signal.js';
import { startReplSet, stopReplSet, uniqueCollection } from '../../helpers/replset.js';
import { delay, trackNext, expectEvent, expectNoEvent, type Tracker } from '../../helpers/timing.js';

/**
 * CONTRACT: fm's presence gateway (see ./README.md).
 *
 * fm runs one MongoDB database per tenant. A presence pusher built on this
 * library therefore cannot use a bare `Db` — it must span every tenant
 * through a `DbResolver`, so the SAME userId in two different tenant
 * databases is tracked (and announced) completely independently, and a
 * server-shutdown sweep (`clearServer`) must reach every tenant database, not
 * just the default one. This pins that composition: `PresenceStore` /
 * `watchPresence` driven by a two-key `DbResolver`, mirroring the two-tenant
 * setup already exercised by `tests/presence/presence-store.test.ts`
 * ("PresenceStore across keys") and `tests/presence/watch-presence.test.ts`
 * ("watches every key and reports transitions per database").
 */

async function makeTwoKeyResolver(prefix: string): Promise<DbResolver> {
    const { client } = await startReplSet();
    const dbA = client.db(`${prefix}-a-${randomUUID().slice(0, 8)}`);
    const dbB = client.db(`${prefix}-b-${randomUUID().slice(0, 8)}`);
    return { keys: () => ['a', 'b'], dbFor: key => (key === 'a' ? dbA : dbB) };
}

async function makeTwoKeyStore(serverId: string = randomUUID()): Promise<PresenceStore> {
    const resolver = await makeTwoKeyResolver('presence-fm');
    const store = new PresenceStore({ db: resolver, serverId, collectionName: uniqueCollection('presence-fm') });
    await store.ensureIndexes();
    return store;
}

/**
 * `watchPresence` only starts delivering events once its internal change
 * stream(s) are actually open (no history replay on a fresh `{}` watch).
 * Prime it with a sentinel checkIn on `key` — a brand-new userId always
 * yields an `online` event — retrying until one is observed, then proceed
 * with the test's real assertions. Mirrors
 * `tests/contracts/calendar/presence.contract.test.ts`'s `waitForWatchOpen`,
 * parameterized by key since fm's store spans more than one database.
 */
async function waitForWatchOpen(tracked: Tracker<PresenceEvent>, store: PresenceStore, key: string): Promise<void> {
    for (let attempt = 0; attempt < 30; attempt++) {
        await store.checkIn({ userId: `sentinel-${randomUUID()}`, clientId: randomUUID(), scopeId: 'sentinel', key });
        const result = await Promise.race([tracked.next(), delay(200).then(() => 'timeout' as const)]);
        if (result !== 'timeout' && !result.done) return;
    }
    throw new Error('watchPresence never opened within timeout');
}

describe('fm presence contract', () => {
    afterAll(() => stopReplSet());

    it('never conflates the same userId across two tenant databases', async () => {
        const store = await makeTwoKeyStore();
        const userId = randomUUID();

        await store.checkIn({ userId, clientId: randomUUID(), scopeId: 'org1', key: 'a' });
        await store.checkIn({ userId, clientId: randomUUID(), scopeId: 'org1', key: 'b' });

        expect(await store.countUserRows(userId, 'a')).toBe(1);
        expect(await store.countUserRows(userId, 'b')).toBe(1);
        expect(await store.collectionFor('a').countDocuments({})).toBe(1);
        expect(await store.collectionFor('b').countDocuments({})).toBe(1);
    });

    it('announces online once per user per tenant, not once per connection', async () => {
        const store = await makeTwoKeyStore();
        const shutdown = new ValueSignal(false);
        const tracked = trackNext(watchPresence({ store, shutdown }));
        try {
            await waitForWatchOpen(tracked, store, 'a');

            const userId = randomUUID();
            await store.checkIn({ userId, clientId: randomUUID(), scopeId: 'org1', key: 'a' });
            const online = await expectEvent(tracked);
            expect(online).toMatchObject({ type: 'online', userId, key: 'a' });

            // A second connection for the SAME user, SAME tenant — already
            // online in that database, so no second announcement.
            await store.checkIn({ userId, clientId: randomUUID(), scopeId: 'org1', key: 'a' });
            await expectNoEvent(tracked);
        } finally {
            shutdown.next(true);
        }
    });

    it("announces offline only when the user's last row in that tenant goes", async () => {
        const store = await makeTwoKeyStore();
        const shutdown = new ValueSignal(false);
        const tracked = trackNext(watchPresence({ store, shutdown }));
        try {
            await waitForWatchOpen(tracked, store, 'a');

            const userId = randomUUID();
            const clientA = randomUUID();
            const clientB = randomUUID();
            await store.checkIn({
                userId,
                clientId: clientA,
                scopeId: 'org1',
                key: 'a',
                meta: { email: 'u1@example.com' },
            });
            await expectEvent(tracked); // online

            await store.checkIn({
                userId,
                clientId: clientB,
                scopeId: 'org1',
                key: 'a',
                meta: { email: 'u1@example.com' },
            });
            await expectNoEvent(tracked); // already online

            await store.checkOut(userId, clientA, 'a');
            await expectNoEvent(tracked); // one connection remains in this tenant

            await store.checkOut(userId, clientB, 'a');
            const offline = await expectEvent(tracked);
            // scopeId and meta are recovered from the deleted row's pre-image
            // (PresenceStore.ensureIndexes enables pre/post images) — the
            // document itself is gone by the time watchPresence reads the
            // delete event.
            expect(offline).toMatchObject({
                type: 'offline',
                userId,
                scopeId: 'org1',
                key: 'a',
                meta: { email: 'u1@example.com' },
            });
        } finally {
            shutdown.next(true);
        }
    });

    it("clearServer removes only this server's rows, in every tenant database", async () => {
        const resolver = await makeTwoKeyResolver('presence-fm-clear');
        const collectionName = uniqueCollection('presence-fm-clear');
        const serverId1 = randomUUID();
        const serverId2 = randomUUID();
        const store1 = new PresenceStore({ db: resolver, serverId: serverId1, collectionName });
        const store2 = new PresenceStore({ db: resolver, serverId: serverId2, collectionName });
        await store1.ensureIndexes();

        // Two "pods" (distinct serverIds) writing through the SAME resolver —
        // each checks a user in into both tenant databases.
        await store1.checkIn({ userId: 'u1', clientId: 'c1', scopeId: 'org1', key: 'a' });
        await store1.checkIn({ userId: 'u1', clientId: 'c1', scopeId: 'org1', key: 'b' });
        await store2.checkIn({ userId: 'u2', clientId: 'c2', scopeId: 'org1', key: 'a' });
        await store2.checkIn({ userId: 'u2', clientId: 'c2', scopeId: 'org1', key: 'b' });

        await store1.clearServer();

        expect(await store1.collectionFor('a').countDocuments({ serverId: serverId1 })).toBe(0);
        expect(await store1.collectionFor('b').countDocuments({ serverId: serverId1 })).toBe(0);
        expect(await store1.collectionFor('a').countDocuments({ serverId: serverId2 })).toBe(1);
        expect(await store1.collectionFor('b').countDocuments({ serverId: serverId2 })).toBe(1);
    });
});
