import type { Db } from 'mongodb';
import type { Logger } from '../common/logger.js';
import type { ReadonlySignal } from '../common/signal.js';
import type { ChangeEvent, ChangeEventConventions } from './change-event.js';
import type { ChangeStreamHub } from './change-stream-hub.js';
import { mergeAsyncGenerators } from './merge-async-generators.js';
import { collectionWatchFactory, watchCollection } from './watch-collection.js';

/** One database's copy of a collection to watch, and the tenant its events belong to. */
export interface WatchTarget<TTenant = unknown> {
    /** Hub subscription key — the database's identity. See `DbResolver`. */
    key: string;
    db: Db;
    collection: string;
    /** Stamped onto every event from this target, for downstream fan-out. */
    tenant?: TTenant;
}

export interface WatchAcrossTargetsOptions<TTenant> {
    targets: Array<WatchTarget<TTenant>>;
    shutdown: ReadonlySignal<boolean>;
    /**
     * When supplied every target subscribes through the shared hub under its
     * own `key`, so the process holds one DB-level cursor per key rather than
     * one per (key, collection). Without it each target opens a dedicated
     * collection-level change stream.
     */
    hub?: ChangeStreamHub;
    conventions?: ChangeEventConventions;
    name?: string;
    logger?: Logger;
    initialBackoffMs?: number;
    maxBackoffMs?: number;
}

/**
 * Guards against two hazards `ChangeStreamHub.ensureStream` cannot catch on
 * its own, because it keys purely by `key` and silently discards whichever
 * `db` argument loses the race to open the stream first:
 *
 *   - the same `(key, collection)` pair twice → the second target's
 *     `subscribe()` hands back a second consumer on the same multi-consumer
 *     `Fanout`, so every event from that key/collection is delivered TWICE;
 *   - the same `key` with two non-identical `Db` instances → the second
 *     target is silently watched against the FIRST target's database, so a
 *     database-per-tenant deployment gets a tenant whose events never arrive,
 *     with no error at all.
 *
 * Both failure modes are silent in hub mode, so this throws at composition
 * time instead — a config mistake here is exactly what a registry-built
 * `targets` array from a database-per-tenant deployment makes easy.
 */
function assertNoConflictingTargets<TTenant>(targets: Array<WatchTarget<TTenant>>): void {
    const seenPairs = new Set<string>();
    const dbByKey = new Map<string, Db>();
    for (const target of targets) {
        const pairKey = JSON.stringify([target.key, target.collection]);
        if (seenPairs.has(pairKey)) {
            throw new Error(
                `watchAcrossTargets: duplicate target for key '${target.key}', collection '${target.collection}' — ` +
                    'two targets sharing a (key, collection) pair both subscribe to the same hub Fanout, so every ' +
                    'event would be delivered twice. Remove the duplicate target.'
            );
        }
        seenPairs.add(pairKey);

        const existingDb = dbByKey.get(target.key);
        if (existingDb && existingDb !== target.db) {
            throw new Error(
                `watchAcrossTargets: key '${target.key}' is used with two different Db instances — ` +
                    'in hub mode ChangeStreamHub.ensureStream(key, db) reuses the FIRST db it saw for a key and ' +
                    'silently discards the rest, so the second target would be watched against the wrong ' +
                    'database. Give each database its own key.'
            );
        }
        dbByKey.set(target.key, target.db);
    }
}

/**
 * Watch the same collection across N databases and merge the results into one
 * stream of canonical `ChangeEvent`s, each tagged with its target's tenant.
 *
 * This is the multi-database counterpart of `watchCollection`: it is exactly
 * `mergeAsyncGenerators` over one `watchCollection` per target, and exists so
 * every multi-tenant consumer does not re-derive the same composition (plus
 * the subtleties `mergeAsyncGenerators` encodes about non-blocking teardown).
 *
 * An empty `targets` list yields nothing and completes immediately, rather
 * than hanging — a consumer whose tenant registry is empty at boot gets a
 * finished generator, not a stalled one.
 *
 * Throws immediately (before opening any stream) if two targets collide —
 * see {@link assertNoConflictingTargets}.
 */
export async function* watchAcrossTargets<TTenant = unknown>(
    options: WatchAcrossTargetsOptions<TTenant>
): AsyncGenerator<ChangeEvent<TTenant>> {
    assertNoConflictingTargets(options.targets);

    const streams = options.targets.map(target =>
        watchCollection<TTenant>({
            source: options.hub
                ? { hub: options.hub, key: target.key, db: target.db, collection: target.collection }
                : {
                      watch: collectionWatchFactory(target.db.collection(target.collection)),
                      db: target.db,
                      collection: target.collection,
                  },
            shutdown: options.shutdown,
            conventions: options.conventions,
            tenant: target.tenant,
            name: options.name ? `${options.name}[${target.key}]` : `watch:${target.collection}[${target.key}]`,
            logger: options.logger,
            initialBackoffMs: options.initialBackoffMs,
            maxBackoffMs: options.maxBackoffMs,
        })
    );

    yield* mergeAsyncGenerators(streams);
}
