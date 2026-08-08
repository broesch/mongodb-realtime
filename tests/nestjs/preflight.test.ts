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
        await expect(assertChangeStreamPreflight(db)).resolves.toEqual({ ok: true });
    });

    it('passes on a replica set and enables pre/post images on supplied collections', async () => {
        const { db } = await startReplSet();
        const name = uniqueCollection('preflight');
        await expect(assertChangeStreamPreflight(db, { collections: [name] })).resolves.toEqual({ ok: true });

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
        ).resolves.toEqual({ ok: false, reason: 'not-a-replica-set' });
        expect(warnings.some(w => w.includes('requireChangeStreams'))).toBe(true);
    });

    it('defaults to throwing on a standalone with an actionable message', async () => {
        const db = fakeStandaloneDb();
        await expect(assertChangeStreamPreflight(db)).rejects.toThrow(/--replSet/);
    });

    it('returns ok:true on a replica set', async () => {
        const { db } = await startReplSet();
        await expect(assertChangeStreamPreflight(db)).resolves.toEqual({ ok: true });
    });

    it('returns ok:false with a reason on a standalone when not required', async () => {
        const db = fakeStandaloneDb();
        await expect(assertChangeStreamPreflight(db, { requireChangeStreams: false })).resolves.toEqual({
            ok: false,
            reason: 'not-a-replica-set',
        });
    });

    it('the pre-image failure message names the exact remediation commands', async () => {
        // `enablePrePostImages` runs before the verification and would succeed
        // against a real server, so the post-enable verification failure is only
        // reachable with a stub whose collMod silently does nothing — which is
        // exactly the real-world case this error exists for (an older MongoDB
        // that accepts the command and ignores the flag).
        const stub = {
            admin: () => ({ command: () => Promise.resolve({ setName: 'rs0' }) }),
            command: () => Promise.resolve({ ok: 1 }),
            listCollections: () => ({
                toArray: () => Promise.resolve([{ name: 'widgets', options: {} }]),
            }),
        } as unknown as Db;

        await expect(assertChangeStreamPreflight(stub, { collections: ['widgets'] })).rejects.toThrow(/collMod/);
    });
});
