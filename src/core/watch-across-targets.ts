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
 */
export async function* watchAcrossTargets<TTenant = unknown>(
    options: WatchAcrossTargetsOptions<TTenant>
): AsyncGenerator<ChangeEvent<TTenant>> {
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
