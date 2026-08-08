import { randomUUID } from 'node:crypto';
import { describe, it, expect, afterAll, vi } from 'vitest';
import { watchPresence, type PresenceEvent } from '../../src/presence/watch-presence.js';
import { PresenceStore } from '../../src/presence/presence-store.js';
import { ValueSignal } from '../../src/common/signal.js';
import { startReplSet, stopReplSet, uniqueCollection } from '../helpers/replset.js';
import { delay, trackNext, expectEvent, expectNoEvent, expectDone, type Tracker } from '../helpers/timing.js';

// ---------------------------------------------------------------------------
// Replica-set-backed tests for watchPresence's 0/1 transition detection and
// cross-node ownedByThisServer flag. Driven with TWO PresenceStores (distinct
// serverId) sharing one db + collection, mirroring how two backend pods would
// share the `onlineUsers` collection in production.
// ---------------------------------------------------------------------------

/**
 * `watchPresence` only starts delivering events once its internal change
 * stream is actually open (no history replay on a fresh `{}` watch). Prime it
 * with a sentinel checkIn — a brand-new userId always yields an `online`
 * event — retrying until one is observed, then proceed with the test's real
 * assertions. Mirrors `waitForWatchOpen` in tests/core/watch-collection.test.ts.
 */
async function waitForWatchOpen(tracked: Tracker<PresenceEvent>, store: PresenceStore): Promise<void> {
    for (let attempt = 0; attempt < 30; attempt++) {
        await store.checkIn({ userId: `sentinel-${randomUUID()}`, clientId: randomUUID(), scopeId: 'sentinel' });
        const result = await Promise.race([tracked.next(), delay(200).then(() => 'timeout' as const)]);
        if (result !== 'timeout' && !result.done) return;
    }
    throw new Error('watchPresence never opened within timeout');
}

async function makeStores(): Promise<{ store1: PresenceStore; store2: PresenceStore }> {
    const { db } = await startReplSet();
    const collectionName = uniqueCollection('presence-watch');
    const store1 = new PresenceStore({ db, serverId: randomUUID(), collectionName });
    const store2 = new PresenceStore({ db, serverId: randomUUID(), collectionName });
    await store1.ensureIndexes();
    return { store1, store2 };
}

describe('watchPresence', () => {
    afterAll(() => stopReplSet());

    it('first checkIn for a user yields online (0→1 transition), owned by this server', async () => {
        const { store1 } = await makeStores();
        const shutdown = new ValueSignal(false);
        const tracked = trackNext(watchPresence({ store: store1, shutdown }));
        await waitForWatchOpen(tracked, store1);

        const userId = randomUUID();
        await store1.checkIn({ userId, clientId: randomUUID(), scopeId: 'scope-1' });

        const event = await expectEvent(tracked);
        expect(event).toEqual({
            type: 'online',
            userId,
            scopeId: 'scope-1',
            key: '__default__',
            ownedByThisServer: true,
            meta: {},
        });

        shutdown.next(true);
    });

    it('a second checkIn for the same user (different clientId) yields nothing — already online', async () => {
        const { store1 } = await makeStores();
        const shutdown = new ValueSignal(false);
        const tracked = trackNext(watchPresence({ store: store1, shutdown }));
        await waitForWatchOpen(tracked, store1);

        const userId = randomUUID();
        await store1.checkIn({ userId, clientId: randomUUID(), scopeId: 'scope-1' });
        await expectEvent(tracked);

        await store1.checkIn({ userId, clientId: randomUUID(), scopeId: 'scope-1' });
        await expectNoEvent(tracked);

        shutdown.next(true);
    });

    it('two concurrent first-time checkIns for a brand-new user yield EXACTLY ONE online event', async () => {
        // Regression for the order-dependent "total count === 1" bug: two racers
        // inserting near-simultaneously would each read a total of 2 once both
        // writes had landed, and neither would ever announce online. The
        // order-stable "any older row?" check must instead let exactly the
        // racer with the smallest _id through, regardless of read timing.
        const { store1 } = await makeStores();
        const shutdown = new ValueSignal(false);
        const tracked = trackNext(watchPresence({ store: store1, shutdown }));
        await waitForWatchOpen(tracked, store1);

        const userId = randomUUID();
        await Promise.all([
            store1.checkIn({ userId, clientId: randomUUID(), scopeId: 'scope-1' }),
            store1.checkIn({ userId, clientId: randomUUID(), scopeId: 'scope-1' }),
        ]);

        const event = await expectEvent(tracked);
        expect(event).toMatchObject({ type: 'online', userId, scopeId: 'scope-1', ownedByThisServer: true });

        // No second online event trailing behind the first.
        await expectNoEvent(tracked);

        shutdown.next(true);
    });

    it('checkOut of one of two rows yields nothing; checkOut of the last row yields offline (1→0 transition)', async () => {
        const { store1 } = await makeStores();
        const shutdown = new ValueSignal(false);
        const tracked = trackNext(watchPresence({ store: store1, shutdown }));
        await waitForWatchOpen(tracked, store1);

        const userId = randomUUID();
        const clientA = randomUUID();
        const clientB = randomUUID();
        await store1.checkIn({ userId, clientId: clientA, scopeId: 'scope-1' });
        await expectEvent(tracked);
        await store1.checkIn({ userId, clientId: clientB, scopeId: 'scope-1' });
        await expectNoEvent(tracked);

        await store1.checkOut(userId, clientA);
        await expectNoEvent(tracked);

        await store1.checkOut(userId, clientB);
        const event = await expectEvent(tracked);
        expect(event).toEqual({
            type: 'offline',
            userId,
            scopeId: 'scope-1',
            key: '__default__',
            ownedByThisServer: true,
            meta: {},
        });

        shutdown.next(true);
    });

    it('a row inserted by ANOTHER store still yields online here, with ownedByThisServer: false', async () => {
        const { store1, store2 } = await makeStores();
        const shutdown = new ValueSignal(false);
        const tracked = trackNext(watchPresence({ store: store1, shutdown }));
        await waitForWatchOpen(tracked, store1);

        const userId = randomUUID();
        await store2.checkIn({ userId, clientId: randomUUID(), scopeId: 'scope-2' });

        const event = await expectEvent(tracked);
        expect(event).toEqual({
            type: 'online',
            userId,
            scopeId: 'scope-2',
            key: '__default__',
            ownedByThisServer: false,
            meta: {},
        });

        shutdown.next(true);
    });

    it('keepAlive updates yield nothing', async () => {
        const { store1 } = await makeStores();
        const shutdown = new ValueSignal(false);
        const tracked = trackNext(watchPresence({ store: store1, shutdown }));
        await waitForWatchOpen(tracked, store1);

        const userId = randomUUID();
        const clientId = randomUUID();
        await store1.checkIn({ userId, clientId, scopeId: 'scope-1' });
        await expectEvent(tracked);

        await store1.keepAlive(userId, clientId);
        await expectNoEvent(tracked);

        shutdown.next(true);
    });

    it('a transient rejection from countOlderUserRows is logged and skipped, without killing the generator', async () => {
        // Regression: toPresenceEvent's awaited count query used to throw
        // straight out of the for-await loop, permanently ending
        // watchPresence on one transient Mongo blip (server-selection
        // timeout, failover) until process restart. It must instead log and
        // skip just that event, leaving the generator alive for the next one.
        const { store1 } = await makeStores();
        const shutdown = new ValueSignal(false);
        const tracked = trackNext(watchPresence({ store: store1, shutdown }));
        await waitForWatchOpen(tracked, store1);

        // spyOn + mockImplementationOnce rejects exactly the NEXT call, then
        // falls back to the real implementation for every call after — no
        // manual restore needed for the second checkIn below.
        vi.spyOn(store1, 'countOlderUserRows').mockImplementationOnce(() =>
            Promise.reject(new Error('simulated transient rejection'))
        );

        const failingUserId = randomUUID();
        await store1.checkIn({ userId: failingUserId, clientId: randomUUID(), scopeId: 'scope-1' });
        // The event for this checkIn is skipped (its count query rejected) —
        // no online event, but no thrown error either.
        await expectNoEvent(tracked);

        // A subsequent, unrelated event still flows through the same generator.
        const userId = randomUUID();
        await store1.checkIn({ userId, clientId: randomUUID(), scopeId: 'scope-1' });
        const event = await expectEvent(tracked);
        expect(event).toEqual({
            type: 'online',
            userId,
            scopeId: 'scope-1',
            key: '__default__',
            ownedByThisServer: true,
            meta: {},
        });

        shutdown.next(true);
    });

    it('yields an activity event carrying the new page when setPage writes it', async () => {
        const { store1 } = await makeStores();
        const shutdown = new ValueSignal(false);
        const tracked = trackNext(watchPresence({ store: store1, shutdown }));
        await waitForWatchOpen(tracked, store1);

        const userId = randomUUID();
        const clientId = randomUUID();
        await store1.checkIn({ userId, clientId, scopeId: 'scope-1' });
        await expectEvent(tracked); // the online event

        await store1.setPage(userId, clientId, '/calendar');

        const event = await expectEvent(tracked);
        expect(event).toMatchObject({ type: 'activity', userId, scopeId: 'scope-1', page: '/calendar' });

        shutdown.next(true);
    });

    it('does not yield an activity event for a keepAlive, which only writes lastSeen', async () => {
        const { store1 } = await makeStores();
        const shutdown = new ValueSignal(false);
        const tracked = trackNext(watchPresence({ store: store1, shutdown }));
        await waitForWatchOpen(tracked, store1);

        const userId = randomUUID();
        const clientId = randomUUID();
        await store1.checkIn({ userId, clientId, scopeId: 'scope-1' });
        await expectEvent(tracked); // the online event

        await store1.keepAlive(userId, clientId);
        await expectNoEvent(tracked);

        shutdown.next(true);
    });

    it('threads the row meta onto online and offline events', async () => {
        const { store1 } = await makeStores();
        const shutdown = new ValueSignal(false);
        const tracked = trackNext(watchPresence({ store: store1, shutdown }));
        await waitForWatchOpen(tracked, store1);

        const userId = randomUUID();
        const clientId = randomUUID();
        await store1.checkIn({ userId, clientId, scopeId: 'scope-1', meta: { email: 'a@b.c' } });
        const online = await expectEvent(tracked);
        expect(online).toMatchObject({ type: 'online', meta: { email: 'a@b.c' } });

        await store1.checkOut(userId, clientId);
        const offline = await expectEvent(tracked);
        expect(offline).toMatchObject({ type: 'offline', meta: { email: 'a@b.c' } });

        shutdown.next(true);
    });

    it('shutdown ends the generator', async () => {
        const { store1 } = await makeStores();
        const shutdown = new ValueSignal(false);
        const tracked = trackNext(watchPresence({ store: store1, shutdown }));
        await waitForWatchOpen(tracked, store1);

        shutdown.next(true);
        await expectDone(tracked);
    });
});
