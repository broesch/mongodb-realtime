import { randomUUID } from 'node:crypto';
import { describe, it, expect, afterAll } from 'vitest';
import { watchPresence, type PresenceEvent } from '../../../src/presence/watch-presence.js';
import { PresenceStore } from '../../../src/presence/presence-store.js';
import { ValueSignal } from '../../../src/common/signal.js';
import { startReplSet, stopReplSet, uniqueCollection } from '../../helpers/replset.js';
import { delay, trackNext, expectEvent, expectNoEvent, type Tracker } from '../../helpers/timing.js';

/**
 * CONTRACT: calendar's online-user pusher (apps/api/src/realtime/online-user.pusher.ts).
 *
 * DIVERGENCE FROM THE LIVE PUSHER: the calendar source hand-rolls this
 * composition — it watches its own `onlineUsers` Mongoose collection
 * directly and computes the 0/1 transition itself with a bespoke
 * `countDocuments({ userId, _id: { $lt: row._id } })` "any older row?"
 * query, unscoped by serverId "on purpose — a connection on another
 * instance means the user was already online" (its own comment). This
 * library's `watchPresence` + `PresenceStore` is the reusable replacement
 * for exactly that hand-rolled logic (see `watchPresence`'s own doc
 * comment: "Ported from calendar's `online-user.pusher.ts` refinement").
 * This contract pins that the library's implementation preserves the
 * composition calendar's pusher relies on: exactly-once online/offline per
 * user (not per connection), order-stable under concurrent first/last
 * connections, and multi-instance visibility via a shared collection — driven
 * here with two `PresenceStore`s carrying distinct `serverId`s, mirroring
 * two backend pods sharing one `onlineUsers` collection.
 */

/**
 * `watchPresence` only starts delivering events once its internal change
 * stream is actually open (no history replay on a fresh `{}` watch). Prime it
 * with a sentinel checkIn — a brand-new userId always yields an `online`
 * event — retrying until one is observed, then proceed with the test's real
 * assertions.
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
    const collectionName = uniqueCollection('presence-contract');
    const store1 = new PresenceStore({ db, serverId: randomUUID(), collectionName });
    const store2 = new PresenceStore({ db, serverId: randomUUID(), collectionName });
    await store1.ensureIndexes();
    return { store1, store2 };
}

describe('calendar contract: online-user (presence) pusher composition', () => {
    afterAll(() => stopReplSet());

    it('a user connecting twice — even from two different server instances — announces online exactly once', async () => {
        const { store1, store2 } = await makeStores();
        const shutdown = new ValueSignal(false);
        const tracked = trackNext(watchPresence({ store: store1, shutdown }));
        await waitForWatchOpen(tracked, store1);

        const userId = randomUUID();
        await store1.checkIn({ userId, clientId: randomUUID(), scopeId: 'org1' });
        const online = await expectEvent(tracked);
        expect(online).toMatchObject({ type: 'online', userId, scopeId: 'org1' });

        // Second connection for the same user, from the OTHER instance — the
        // user is already online, so no second announcement.
        await store2.checkIn({ userId, clientId: randomUUID(), scopeId: 'org1' });
        await expectNoEvent(tracked);

        shutdown.next(true);
    });

    it('offline is announced only once the LAST connection across both instances checks out', async () => {
        const { store1, store2 } = await makeStores();
        const shutdown = new ValueSignal(false);
        const tracked = trackNext(watchPresence({ store: store1, shutdown }));
        await waitForWatchOpen(tracked, store1);

        const userId = randomUUID();
        const clientA = randomUUID();
        const clientB = randomUUID();
        await store1.checkIn({ userId, clientId: clientA, scopeId: 'org1' });
        await expectEvent(tracked); // online

        await store2.checkIn({ userId, clientId: clientB, scopeId: 'org1' });
        await expectNoEvent(tracked); // already online

        await store1.checkOut(userId, clientA);
        await expectNoEvent(tracked); // one connection remains, on the other instance

        await store2.checkOut(userId, clientB);
        const offline = await expectEvent(tracked);
        expect(offline).toMatchObject({ type: 'offline', userId, scopeId: 'org1' });

        shutdown.next(true);
    });

    it('setPage yields an activity event carrying the page the user navigated to', async () => {
        const { store1 } = await makeStores();
        const shutdown = new ValueSignal(false);
        const tracked = trackNext(watchPresence({ store: store1, shutdown }));
        await waitForWatchOpen(tracked, store1);

        const userId = randomUUID();
        const clientId = randomUUID();
        await store1.checkIn({ userId, clientId, scopeId: 'org1' });
        await expectEvent(tracked); // online

        await store1.setPage(userId, clientId, '/calendar');
        const activity = await expectEvent(tracked);
        expect(activity).toMatchObject({ type: 'activity', userId, scopeId: 'org1', page: '/calendar' });

        shutdown.next(true);
    });

    it('meta passed to checkIn is threaded onto the online event', async () => {
        const { store1 } = await makeStores();
        const shutdown = new ValueSignal(false);
        const tracked = trackNext(watchPresence({ store: store1, shutdown }));
        await waitForWatchOpen(tracked, store1);

        const userId = randomUUID();
        await store1.checkIn({ userId, clientId: randomUUID(), scopeId: 'org1', meta: { email: 'u1@example.com' } });
        const online = await expectEvent(tracked);
        expect(online).toMatchObject({ type: 'online', userId, meta: { email: 'u1@example.com' } });

        shutdown.next(true);
    });
});
