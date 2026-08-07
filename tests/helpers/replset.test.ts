import { startReplSet, stopReplSet, uniqueCollection } from './replset.js';

describe('replset helper', () => {
    afterAll(() => stopReplSet());

    it('boots a replica set that supports change streams', async () => {
        const { db } = await startReplSet();
        const coll = db.collection(uniqueCollection('smoke'));
        const stream = coll.watch();
        const nextPromise = stream.next();
        await coll.insertOne({ hello: 'world' });
        const change = await nextPromise;
        expect(change.operationType).toBe('insert');
        await stream.close();
    });
});
