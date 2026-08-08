import type { Collection } from 'mongodb';
import { describe, it, expect, afterAll } from 'vitest';
import { watchCollection, tagTenant, collectionWatchFactory } from '../../src/core/watch-collection.js';
import { mergeAsyncGenerators } from '../../src/core/merge-async-generators.js';
import { ChangeStreamHub } from '../../src/core/change-stream-hub.js';
import { ValueSignal } from '../../src/common/signal.js';
import type { ChangeEvent } from '../../src/core/change-event.js';
import { startReplSet, stopReplSet, uniqueCollection } from '../helpers/replset.js';
import { delay, trackNext, expectEvent, expectNoEvent, type Tracker } from '../helpers/timing.js';

// ---------------------------------------------------------------------------
// Replica-set-backed tests for watchCollection's two source modes (direct /
// hub) plus tagTenant. A handful of pure tagTenant tests run first with no DB
// at all.
// ---------------------------------------------------------------------------

describe('tagTenant (pure)', () => {
    async function* one(event: ChangeEvent): AsyncGenerator<ChangeEvent> {
        yield event;
    }

    it('passes events through unchanged when tenant is undefined', async () => {
        const event: ChangeEvent = { type: 'changed', scopeId: 'org1', data: { n: 1 }, operation: 'insert' };
        const tagged = tagTenant(one(event), undefined);
        const result = await tagged.next();
        expect(result.value).toEqual(event);
        expect((result.value as ChangeEvent & { tenant?: unknown }).tenant).toBeUndefined();
    });

    it('stamps every event with the given tenant', async () => {
        async function* two(): AsyncGenerator<ChangeEvent> {
            yield { type: 'changed', scopeId: 'org1', data: { n: 1 }, operation: 'insert' };
            yield { type: 'removed', id: 'abc', scopeId: 'org1', via: 'delete' };
        }
        const tagged = tagTenant(two(), { dbName: 'tenant-a' });
        const first = await tagged.next();
        const second = await tagged.next();
        expect(first.value).toMatchObject({ type: 'changed', tenant: { dbName: 'tenant-a' } });
        expect(second.value).toMatchObject({ type: 'removed', tenant: { dbName: 'tenant-a' } });
    });
});

/**
 * A `watchCollection` output only starts delivering events once its internal
 * change stream is actually open (no history replay on a fresh `{}` watch).
 * Prime it by inserting a scoped doc — which always maps to a real 'changed'
 * event — and retrying until one is observed, then proceed with the test's
 * real inserts. Works uniformly for both direct and hub mode, since priming
 * only depends on the resulting `ChangeEvent` stream, not the source's wiring.
 */
async function waitForWatchOpen(tracked: Tracker<ChangeEvent>, coll: Collection): Promise<void> {
    for (let attempt = 0; attempt < 30; attempt++) {
        await coll.insertOne({ organizationId: 'prime', attempt });
        const result = await Promise.race([tracked.next(), delay(200).then(() => 'timeout' as const)]);
        if (result !== 'timeout' && !result.done) return;
    }
    throw new Error('watchCollection never opened within timeout');
}

describe('watchCollection', () => {
    afterAll(() => stopReplSet());

    describe('direct mode', () => {
        it('insert yields a changed event with scopeId', async () => {
            const { db } = await startReplSet();
            const collName = uniqueCollection('direct-changed');
            const coll = db.collection(collName);
            const shutdown = new ValueSignal(false);

            const tracked = trackNext(
                watchCollection({
                    source: { watch: collectionWatchFactory(coll), db, collection: collName },
                    shutdown,
                })
            );
            await waitForWatchOpen(tracked, coll);

            await coll.insertOne({ organizationId: 'org-1', name: 'widget' });
            const event = await expectEvent(tracked);
            expect(event.type).toBe('changed');
            expect(event).toMatchObject({ type: 'changed', scopeId: 'org-1' });

            shutdown.next(true);
        });

        it('soft delete (deleted: true) yields a removed event', async () => {
            const { db } = await startReplSet();
            const collName = uniqueCollection('direct-soft-delete');
            const coll = db.collection(collName);
            const shutdown = new ValueSignal(false);

            const tracked = trackNext(
                watchCollection({
                    source: { watch: collectionWatchFactory(coll), db, collection: collName },
                    shutdown,
                })
            );
            await waitForWatchOpen(tracked, coll);

            const { insertedId } = await coll.insertOne({ organizationId: 'org-1', deleted: false });
            const inserted = await expectEvent(tracked);
            expect(inserted.type).toBe('changed');

            await coll.updateOne({ _id: insertedId }, { $set: { deleted: true } });
            const removed = await expectEvent(tracked);
            expect(removed).toMatchObject({ type: 'removed', scopeId: 'org-1', id: insertedId.toString() });

            shutdown.next(true);
        });

        it('hard delete yields a removed event with scopeId recovered from the pre-image', async () => {
            const { db } = await startReplSet();
            const collName = uniqueCollection('direct-hard-delete');
            const coll = db.collection(collName);
            const shutdown = new ValueSignal(false);

            const tracked = trackNext(
                watchCollection({
                    source: { watch: collectionWatchFactory(coll), db, collection: collName },
                    shutdown,
                })
            );
            await waitForWatchOpen(tracked, coll);

            const { insertedId } = await coll.insertOne({ organizationId: 'org-1' });
            const inserted = await expectEvent(tracked);
            expect(inserted.type).toBe('changed');

            await coll.deleteOne({ _id: insertedId });
            const removed = await expectEvent(tracked);
            expect(removed).toMatchObject({ type: 'removed', scopeId: 'org-1', id: insertedId.toString() });

            shutdown.next(true);
        });

        it('a document with no scope field yields no event', async () => {
            const { db } = await startReplSet();
            const collName = uniqueCollection('direct-no-scope');
            const coll = db.collection(collName);
            const shutdown = new ValueSignal(false);

            const tracked = trackNext(
                watchCollection({
                    source: { watch: collectionWatchFactory(coll), db, collection: collName },
                    shutdown,
                })
            );
            await waitForWatchOpen(tracked, coll);

            await coll.insertOne({ name: 'no organizationId here' });
            await expectNoEvent(tracked);

            shutdown.next(true);
        });

        it('shutdown ends the generator', async () => {
            const { db } = await startReplSet();
            const collName = uniqueCollection('direct-shutdown');
            const coll = db.collection(collName);
            const shutdown = new ValueSignal(false);

            const tracked = trackNext(
                watchCollection({
                    source: { watch: collectionWatchFactory(coll), db, collection: collName },
                    shutdown,
                })
            );
            await waitForWatchOpen(tracked, coll);

            const pending = tracked.next();
            shutdown.next(true);
            const result = await Promise.race([pending, delay(5_000).then(() => 'timeout' as const)]);
            if (result === 'timeout') throw new Error('generator did not end after shutdown');
            expect(result.done).toBe(true);
        });

        it('carries operation and updatedFields through to the yielded ChangeEvent', async () => {
            const { db } = await startReplSet();
            const name = uniqueCollection('direct-operation-fields');
            const coll = db.collection(name);
            const shutdown = new ValueSignal(false);
            const events: ChangeEvent[] = [];

            const watcher = watchCollection({
                source: { watch: collectionWatchFactory(coll), db, collection: name },
                shutdown,
            });
            const pump = (async () => {
                for await (const event of watcher) {
                    events.push(event);
                    if (events.length === 2) break;
                }
            })();

            await delay(300); // let the change stream open before writing
            const { insertedId } = await coll.insertOne({ organizationId: 'org1', status: 'pending' });
            await coll.updateOne({ _id: insertedId }, { $set: { status: 'ready' } });

            await pump;
            shutdown.next(true);

            expect(events[0]).toMatchObject({ type: 'changed', operation: 'insert' });
            expect(events[1]).toMatchObject({ type: 'changed', operation: 'update', updatedFields: ['status'] });
        });
    });

    describe('hub mode', () => {
        it('insert yields a changed event with scopeId', async () => {
            const { db } = await startReplSet();
            const key = uniqueCollection('hub-changed-key');
            const collName = uniqueCollection('hub-changed');
            const coll = db.collection(collName);
            const shutdown = new ValueSignal(false);
            const hub = new ChangeStreamHub();

            const tracked = trackNext(
                watchCollection({
                    source: { hub, key, db, collection: collName },
                    shutdown,
                })
            );
            await waitForWatchOpen(tracked, coll);

            await coll.insertOne({ organizationId: 'org-1', name: 'widget' });
            const event = await expectEvent(tracked);
            expect(event).toMatchObject({ type: 'changed', scopeId: 'org-1' });

            shutdown.next(true);
            await hub.close();
        });

        it('soft delete (deleted: true) yields a removed event', async () => {
            const { db } = await startReplSet();
            const key = uniqueCollection('hub-soft-delete-key');
            const collName = uniqueCollection('hub-soft-delete');
            const coll = db.collection(collName);
            const shutdown = new ValueSignal(false);
            const hub = new ChangeStreamHub();

            const tracked = trackNext(
                watchCollection({
                    source: { hub, key, db, collection: collName },
                    shutdown,
                })
            );
            await waitForWatchOpen(tracked, coll);

            const { insertedId } = await coll.insertOne({ organizationId: 'org-1', deleted: false });
            await expectEvent(tracked);

            await coll.updateOne({ _id: insertedId }, { $set: { deleted: true } });
            const removed = await expectEvent(tracked);
            expect(removed).toMatchObject({ type: 'removed', scopeId: 'org-1', id: insertedId.toString() });

            shutdown.next(true);
            await hub.close();
        });

        it('hard delete yields a removed event with scopeId recovered from the pre-image', async () => {
            const { db } = await startReplSet();
            const key = uniqueCollection('hub-hard-delete-key');
            const collName = uniqueCollection('hub-hard-delete');
            const coll = db.collection(collName);
            const shutdown = new ValueSignal(false);
            const hub = new ChangeStreamHub();

            const tracked = trackNext(
                watchCollection({
                    source: { hub, key, db, collection: collName },
                    shutdown,
                })
            );
            await waitForWatchOpen(tracked, coll);

            const { insertedId } = await coll.insertOne({ organizationId: 'org-1' });
            await expectEvent(tracked);

            await coll.deleteOne({ _id: insertedId });
            const removed = await expectEvent(tracked);
            expect(removed).toMatchObject({ type: 'removed', scopeId: 'org-1', id: insertedId.toString() });

            shutdown.next(true);
            await hub.close();
        });

        it('a document with no scope field yields no event', async () => {
            const { db } = await startReplSet();
            const key = uniqueCollection('hub-no-scope-key');
            const collName = uniqueCollection('hub-no-scope');
            const coll = db.collection(collName);
            const shutdown = new ValueSignal(false);
            const hub = new ChangeStreamHub();

            const tracked = trackNext(
                watchCollection({
                    source: { hub, key, db, collection: collName },
                    shutdown,
                })
            );
            await waitForWatchOpen(tracked, coll);

            await coll.insertOne({ name: 'no organizationId here' });
            await expectNoEvent(tracked);

            shutdown.next(true);
            await hub.close();
        });

        it('shutdown ends the generator without tearing down the hub', async () => {
            const { db } = await startReplSet();
            const key = uniqueCollection('hub-shutdown-key');
            const collName = uniqueCollection('hub-shutdown');
            const coll = db.collection(collName);
            const shutdown = new ValueSignal(false);
            const hub = new ChangeStreamHub();

            const tracked = trackNext(
                watchCollection({
                    source: { hub, key, db, collection: collName },
                    shutdown,
                })
            );
            await waitForWatchOpen(tracked, coll);

            const pending = tracked.next();
            shutdown.next(true);
            const result = await Promise.race([pending, delay(5_000).then(() => 'timeout' as const)]);
            if (result === 'timeout') throw new Error('generator did not end after shutdown');
            expect(result.done).toBe(true);

            await hub.close();
        });
    });

    describe('tenant tagging', () => {
        it('stamps every yielded event with options.tenant', async () => {
            const { db } = await startReplSet();
            const collName = uniqueCollection('tenant-tag');
            const coll = db.collection(collName);
            const shutdown = new ValueSignal(false);
            const tenant = { dbName: 'x' };

            const tracked = trackNext(
                watchCollection({
                    source: { watch: collectionWatchFactory(coll), db, collection: collName },
                    shutdown,
                    tenant,
                })
            );
            await waitForWatchOpen(tracked, coll);
            // The priming event itself must already carry the tag.
            const primeResult = await Promise.race([tracked.next(), delay(5_000).then(() => 'timeout' as const)]);

            await coll.insertOne({ organizationId: 'org-1' });
            const event = await expectEvent(tracked);
            expect(event).toMatchObject({ tenant });
            if (primeResult !== 'timeout' && !primeResult.done) {
                expect(primeResult.value).toMatchObject({ tenant });
            }

            shutdown.next(true);
        });
    });

    describe('multi-tenant merge', () => {
        it('merges two watchCollection generators from two dbs, tagged differently', async () => {
            const { client, db: dbA } = await startReplSet();
            const dbB = client.db(uniqueCollection('watch-collection-tenant-b'));

            const collNameA = uniqueCollection('merge-a');
            const collNameB = uniqueCollection('merge-b');
            const collA = dbA.collection(collNameA);
            const collB = dbB.collection(collNameB);

            const shutdownA = new ValueSignal(false);
            const shutdownB = new ValueSignal(false);

            const genA = watchCollection({
                source: { watch: collectionWatchFactory(collA), db: dbA, collection: collNameA },
                shutdown: shutdownA,
                tenant: { dbName: 'tenant-a' },
            });
            const genB = watchCollection({
                source: { watch: collectionWatchFactory(collB), db: dbB, collection: collNameB },
                shutdown: shutdownB,
                tenant: { dbName: 'tenant-b' },
            });

            const trackedA = trackNext(genA);
            const trackedB = trackNext(genB);
            await waitForWatchOpen(trackedA, collA);
            await waitForWatchOpen(trackedB, collB);

            const merged = trackNext(mergeAsyncGenerators([genA, genB]));

            await collA.insertOne({ organizationId: 'org-1', from: 'a' });
            const first = await expectEvent(merged);
            expect(first).toMatchObject({ tenant: { dbName: 'tenant-a' } });

            await collB.insertOne({ organizationId: 'org-1', from: 'b' });
            const second = await expectEvent(merged);
            expect(second).toMatchObject({ tenant: { dbName: 'tenant-b' } });

            shutdownA.next(true);
            shutdownB.next(true);
        });
    });
});
