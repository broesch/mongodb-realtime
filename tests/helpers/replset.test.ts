import { startReplSet, stopReplSet, uniqueCollection } from './replset.js';
import { streamStart } from './stream-start.js';

describe('replset helper', () => {
    afterAll(() => stopReplSet());

    it('boots a replica set that supports change streams', async () => {
        const { db } = await startReplSet();
        const coll = db.collection(uniqueCollection('smoke'));
        // Open the stream at a cluster time taken BEFORE the insert: a cursor
        // only sees writes after it is established, and on a freshly booted
        // replica set (CI) the open can lose the race with the insert, leaving
        // next() waiting until the test timeout.
        const stream = coll.watch([], { startAtOperationTime: await streamStart(db) });
        const nextPromise = stream.next();
        await coll.insertOne({ hello: 'world' });
        const change = await nextPromise;
        expect(change.operationType).toBe('insert');
        await stream.close();
    });
});
