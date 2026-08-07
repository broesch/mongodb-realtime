import { startReplSet, stopReplSet, uniqueCollection } from '../helpers/replset.js';
import { enablePrePostImages, __resetPrePostImageCacheForTest } from '../../src/core/enable-pre-post-images.js';

describe('enablePrePostImages', () => {
    beforeEach(() => __resetPrePostImageCacheForTest());
    afterAll(() => stopReplSet());

    it('creates a missing collection and enables pre/post images', async () => {
        const { db } = await startReplSet();
        const name = uniqueCollection('ppi');
        await enablePrePostImages(db, [name]);
        const info = await db.listCollections({ name }, { nameOnly: false }).toArray();
        expect(info[0]?.options?.changeStreamPreAndPostImages).toEqual({ enabled: true });
    });

    it('is idempotent (second call is a cached no-op) and caches per db+collection', async () => {
        const { db } = await startReplSet();
        const name = uniqueCollection('ppi');
        await enablePrePostImages(db, [name]);
        await enablePrePostImages(db, [name]); // must not throw / re-run collMod
    });

    it('delete events carry fullDocumentBeforeChange once enabled', async () => {
        const { db } = await startReplSet();
        const name = uniqueCollection('ppi');
        await enablePrePostImages(db, [name]);
        const coll = db.collection(name);
        const { insertedId } = await coll.insertOne({ organizationId: 'org1' });
        const stream = coll.watch([], { fullDocumentBeforeChange: 'whenAvailable' });
        const nextPromise = stream.next();
        await coll.deleteOne({ _id: insertedId });
        const change = await nextPromise;
        expect(change.operationType).toBe('delete');
        expect(
            (change as { fullDocumentBeforeChange?: { organizationId?: string } }).fullDocumentBeforeChange
                ?.organizationId
        ).toBe('org1');
        await stream.close();
    });
});
