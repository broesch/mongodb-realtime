import { vi } from 'vitest';
import type { Db } from 'mongodb';
import type { Logger } from '../../src/common/logger.js';
import { startReplSet, stopReplSet, uniqueCollection } from '../helpers/replset.js';
import { enablePrePostImages, __resetPrePostImageCacheForTest } from '../../src/core/enable-pre-post-images.js';
import { streamStart } from '../helpers/stream-start.js';

/**
 * Fake `Db` whose `command` (the `collMod` call) rejects on its first
 * invocation and resolves on every subsequent one. The collection already
 * "exists" (`listCollections` returns one match) so `createCollection` is
 * never reached — this isolates the collMod failure/retry path from the
 * create-collection-first path the other tests already cover.
 */
function fakeDbFailingCollModOnce(name: string): { db: Db; commandCalls: () => number } {
    let calls = 0;
    const db = {
        databaseName: 'testdb',
        listCollections: () => ({
            toArray: async () => [{ name }],
        }),
        createCollection: vi.fn(),
        command: vi.fn(async () => {
            calls += 1;
            if (calls === 1) {
                throw new Error('collMod boom');
            }
            return {};
        }),
    } as unknown as Db;
    return { db, commandCalls: () => calls };
}

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
        // Start before the delete, so a slow cursor open cannot miss it. The
        // start time is inclusive and may replay the insert: watch deletes only.
        const stream = coll.watch([{ $match: { operationType: 'delete' } }], {
            fullDocumentBeforeChange: 'whenAvailable',
            startAtOperationTime: await streamStart(db),
        });
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

    it('does not cache a failed collMod: the failure is logged, the next call retries it, and only a successful retry gets cached', async () => {
        const name = 'ppi-retry-test';
        const { db, commandCalls } = fakeDbFailingCollModOnce(name);
        const warnSpy = vi.fn();
        const mockLogger: Logger = {
            debug: () => undefined,
            log: () => undefined,
            warn: warnSpy,
            error: () => undefined,
        };

        // First call: collMod rejects. The function swallows the error itself
        // (it never rethrows — the watch must keep working even when
        // pre/post-images can't be enabled) so the only observable signs are
        // the warning and — the actual regression this test pins — that the
        // collection is NOT recorded as enabled.
        await enablePrePostImages(db, [name], mockLogger);
        expect(commandCalls()).toBe(1);
        expect(warnSpy).toHaveBeenCalledTimes(1);
        expect(warnSpy.mock.calls[0]?.[0]).toContain('collMod boom');

        // Second call: nothing was cached after the failure, so collMod is
        // retried rather than short-circuiting on a falsely-cached "done".
        // This is the ordering the shipped bug got backwards (cache written
        // before the await, so a first-boot failure was misremembered as
        // success and never retried without a process restart).
        await enablePrePostImages(db, [name], mockLogger);
        expect(commandCalls()).toBe(2);

        // Third call: the retry succeeded, so THIS call is now cached — no
        // third collMod invocation.
        await enablePrePostImages(db, [name], mockLogger);
        expect(commandCalls()).toBe(2);
    });
});
