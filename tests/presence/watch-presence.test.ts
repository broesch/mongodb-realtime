import { randomUUID } from 'node:crypto';
import { describe, it, expect, afterAll } from 'vitest';
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

    it('shutdown ends the generator', async () => {
        const { store1 } = await makeStores();
        const shutdown = new ValueSignal(false);
        const tracked = trackNext(watchPresence({ store: store1, shutdown }));
        await waitForWatchOpen(tracked, store1);

        shutdown.next(true);
        await expectDone(tracked);
    });
});
