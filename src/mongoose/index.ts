import type { Db } from 'mongodb';
import type { Logger } from '../common/logger.js';
import type { ReadonlySignal } from '../common/signal.js';
import type { ChangeEvent, ChangeEventConventions } from '../core/change-event.js';
import type { ChangeStreamHub } from '../core/change-stream-hub.js';
import { DEFAULT_KEY } from '../core/db-resolver.js';
import { watchAcrossTargets, type WatchTarget } from '../core/watch-across-targets.js';

/**
 * The part of a mongoose `Model` this adapter reads. Declared STRUCTURALLY so
 * this package takes no mongoose dependency — not even a peer. Any object with
 * these three members works, which also makes the adapter trivially fakeable.
 */
export interface MongooseModelLike {
    modelName: string;
    db: { db?: Db | null };
    collection: { name: string };
}

/**
 * Convert a mongoose model into a {@link WatchTarget}.
 *
 * The collection name comes from the model rather than being hardcoded, so a
 * schema-level `collection:` rename cannot silently detach the watcher.
 *
 * Throws when the model's connection has no driver handle — a model on an
 * unconnected connection would otherwise produce a target that fails later,
 * inside the change-stream loop, with a far less obvious message.
 */
export function modelTarget<TTenant = unknown>(
    model: MongooseModelLike,
    options: { key?: string; tenant?: TTenant } = {}
): WatchTarget<TTenant> {
    const db = model.db.db;
    if (!db) {
        throw new Error(
            `Mongoose connection for ${model.modelName} has no database handle; cannot open a change stream.`
        );
    }
    return {
        key: options.key ?? DEFAULT_KEY,
        db,
        collection: model.collection.name,
        tenant: options.tenant,
    };
}

/**
 * Watch one mongoose model — or the same model across N tenant connections —
 * and yield canonical `ChangeEvent`s.
 *
 * Hub mode when `hub` is supplied: every collection under a given key shares
 * ONE database-level cursor, so the process holds O(keys) change-stream
 * cursors instead of one per watcher, and each cursor parks entirely while
 * nobody is connected on that key. `hub` stays optional so callers can migrate
 * one watcher at a time.
 */
export async function* watchModel<TTenant = unknown>(options: {
    models: MongooseModelLike | Array<{ model: MongooseModelLike; key?: string; tenant?: TTenant }>;
    shutdown: ReadonlySignal<boolean>;
    hub?: ChangeStreamHub;
    conventions?: ChangeEventConventions;
    name?: string;
    logger?: Logger;
}): AsyncGenerator<ChangeEvent<TTenant>> {
    const entries = Array.isArray(options.models) ? options.models : [{ model: options.models }];
    const targets = entries.map(entry =>
        modelTarget<TTenant>(entry.model, { key: entry.key, tenant: (entry as { tenant?: TTenant }).tenant })
    );

    yield* watchAcrossTargets<TTenant>({
        targets,
        shutdown: options.shutdown,
        hub: options.hub,
        conventions: options.conventions,
        name: options.name,
        logger: options.logger,
    });
}

// Re-exported so this subpath's public signatures are fully self-describing
// for tooling (e.g. api-extractor) that only ever analyzes this entry point
// in isolation — these types are defined elsewhere but appear in signatures
// exported above (modelTarget, watchModel).
export type { WatchTarget } from '../core/watch-across-targets.js';
export type {
    ChangeEvent,
    ChangeEventConventions,
    ChangeOperation,
    EntityChanged,
    EntityRemoved,
} from '../core/change-event.js';
export type { ReadonlySignal } from '../common/signal.js';
export type { ChangeStreamHub, ChangeStreamHubOptions } from '../core/change-stream-hub.js';
export type { Logger } from '../common/logger.js';
