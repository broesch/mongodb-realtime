import type { Db } from 'mongodb';
import { assertChangeStreamPreflight } from '../../src/nestjs/preflight.js';
import { __resetPrePostImageCacheForTest } from '../../src/core/enable-pre-post-images.js';
import { startReplSet, stopReplSet, uniqueCollection } from '../helpers/replset.js';

/** Minimal fake `Db` reporting a standalone `hello` (no setName, no isdbgrid msg). */
function fakeStandaloneDb(): Db {
    return {
        admin: () => ({
            command: async () => ({}),
        }),
    } as unknown as Db;
}

describe('assertChangeStreamPreflight', () => {
    beforeEach(() => __resetPrePostImageCacheForTest());
    afterAll(() => stopReplSet());

    it('passes on a real replica set', async () => {
        const { db } = await startReplSet();
        await expect(assertChangeStreamPreflight(db)).resolves.toBeUndefined();
    });

    it('passes on a replica set and enables pre/post images on supplied collections', async () => {
        const { db } = await startReplSet();
        const name = uniqueCollection('preflight');
        await expect(assertChangeStreamPreflight(db, { collections: [name] })).resolves.toBeUndefined();

        const info = await db.listCollections({ name }, { nameOnly: false }).toArray();
        expect(info[0]?.options?.changeStreamPreAndPostImages).toEqual({ enabled: true });
    });

    it('requireChangeStreams: false on a standalone warns and returns instead of throwing', async () => {
        const db = fakeStandaloneDb();
        const warnings: string[] = [];
        await expect(
            assertChangeStreamPreflight(db, {
                requireChangeStreams: false,
                logger: {
                    debug: () => undefined,
                    log: () => undefined,
                    warn: message => warnings.push(message),
                    error: () => undefined,
                },
            })
        ).resolves.toBeUndefined();
        expect(warnings.some(w => w.includes('requireChangeStreams'))).toBe(true);
    });

    it('defaults to throwing on a standalone with an actionable message', async () => {
        const db = fakeStandaloneDb();
        await expect(assertChangeStreamPreflight(db)).rejects.toThrow(/--replSet/);
    });
});
