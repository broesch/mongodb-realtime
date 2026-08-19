import type { Db } from 'mongodb';

/**
 * The key used when a consumer has exactly one database, or when a connection
 * registers without naming one. Shared by `DbResolver`, `ConnectionRegistry`
 * and `PresenceStore` so "no key" means the same string everywhere — the hub's
 * parking compares `connectedKeys` against subscription keys, and a mismatch
 * there silently parks a watch while clients are connected.
 */
export const DEFAULT_KEY = '__default__';

/**
 * Resolves a key to the database it names. Lets one library instance span N
 * databases (one per tenant, per region, per shard — the library does not care
 * what the key means) while a single-database consumer passes a bare `Db` and
 * never sees a key at all.
 *
 * `keys()` is called at boot and whenever a store needs to act across every
 * database (index creation, shutdown cleanup). It is NOT re-polled per
 * operation, so a consumer that provisions databases at runtime must supply a
 * resolver whose `keys()` reflects the current set each time it is called.
 */
export interface DbResolver {
    keys(): string[];
    dbFor(key: string): Db;
}

function isDbResolver(source: Db | DbResolver): source is DbResolver {
    // Structural, not `instanceof`: a consumer whose driver copy is duplicated
    // (mongoose vendors its own mongodb) would fail an identity check against
    // this package's copy. A driver `Db` has neither of these methods.
    const candidate = source as Partial<DbResolver>;
    return typeof candidate.dbFor === 'function' && typeof candidate.keys === 'function';
}

/**
 * Normalises either accepted form to a resolver. A bare `Db` becomes a
 * single-key resolver that answers with that database for ANY key, so a
 * single-database consumer never has to learn or pass {@link DEFAULT_KEY}.
 */
export function toDbResolver(source: Db | DbResolver): DbResolver {
    if (isDbResolver(source)) return source;
    return {
        keys: () => [DEFAULT_KEY],
        dbFor: () => source,
    };
}
