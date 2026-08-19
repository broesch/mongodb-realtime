import { DEFAULT_KEY, toDbResolver, type DbResolver } from '../../src/core/db-resolver.js';
import { startReplSet, stopReplSet } from '../helpers/replset.js';

describe('toDbResolver', () => {
    afterAll(() => stopReplSet());

    it('normalises a bare Db to the single default key', async () => {
        const { db } = await startReplSet();
        const resolver = toDbResolver(db);

        expect(resolver.keys()).toEqual([DEFAULT_KEY]);
        expect(resolver.dbFor(DEFAULT_KEY)).toBe(db);
        // A bare Db has exactly one database, so any key resolves to it —
        // callers of a single-db store must not have to know the key name.
        expect(resolver.dbFor('anything')).toBe(db);
    });

    it('passes a resolver through unchanged', async () => {
        const { client } = await startReplSet();
        const a = client.db('tenant-a');
        const b = client.db('tenant-b');
        const resolver: DbResolver = {
            keys: () => ['a', 'b'],
            dbFor: key => (key === 'a' ? a : b),
        };

        expect(toDbResolver(resolver)).toBe(resolver);
        expect(toDbResolver(resolver).dbFor('b')).toBe(b);
    });

    it('duck-types a Db apart from a resolver without instanceof', async () => {
        const { db } = await startReplSet();
        // A driver Db has neither `keys` nor `dbFor`; the discriminator must
        // not rely on `instanceof Db`, which fails across duplicated driver
        // copies (mongoose vendors its own).
        expect(typeof (db as unknown as DbResolver).dbFor).toBe('undefined');
        expect(toDbResolver(db).keys()).toEqual([DEFAULT_KEY]);
    });
});
