import type { ChangeStream, ChangeStreamDocument, Collection, Db, Document } from 'mongodb';
import { delayUnlessStopped } from '../common/delay-unless-stopped.js';
import { Logger, consoleLogger, prefixed } from '../common/logger.js';
import { ReadonlySignal, ValueSignal } from '../common/signal.js';
import { changeDocToEvent } from './change-doc-to-event.js';
import type { ChangeEvent, ChangeEventConventions } from './change-event.js';
import { ChangeStreamHub } from './change-stream-hub.js';
import { ChangeStreamSubscription } from './change-stream-subscription.js';
import type { ResumeOptions } from './change-stream-subscription.js';
import { enablePrePostImages } from './enable-pre-post-images.js';

/** Reconnect backoff bounds for the hub-consumer self-heal path — see {@link hubConsumerLoop}. */
const DEFAULT_INITIAL_BACKOFF_MS = 1_000;
const DEFAULT_MAX_BACKOFF_MS = 30_000;

export type WatchSource =
    | { hub: ChangeStreamHub; key: string; db: Db; collection: string }
    | {
          watch: (options: ResumeOptions) => ChangeStream<Document, ChangeStreamDocument<Document>>;
          db: Db;
          collection: string;
      };

export interface WatchCollectionOptions<TTenant> {
    source: WatchSource;
    shutdown: ReadonlySignal<boolean>;
    conventions?: ChangeEventConventions;
    tenant?: TTenant;
    name?: string;
    logger?: Logger;
    /**
     * Reconnect backoff bounds: govern the hub-consumer self-heal path in hub
     * mode, and are forwarded to `ChangeStreamSubscription`'s own reopen
     * backoff in direct mode (`directLoop` passes both through). Defaults
     * 1_000 / 30_000.
     */
    initialBackoffMs?: number;
    maxBackoffMs?: number;
}

type HubSource = Extract<WatchSource, { hub: ChangeStreamHub }>;
type DirectSource = Exclude<WatchSource, HubSource>;

/**
 * Build a `WatchSource['watch']` factory for a plain driver `Collection`.
 *
 * A caller supplying their own `watch` factory for direct mode must spread the
 * passed resume options into the driver call and should set
 * `fullDocument: 'updateLookup'`, `fullDocumentBeforeChange: 'whenAvailable'` so
 * updates carry the full post-image and hard-deletes carry a pre-image (needed
 * to recover `scopeId`). This builds a factory that does exactly that,
 * restricted to the four operation types `changeDocToEvent` understands.
 */
export function collectionWatchFactory(
    coll: Collection
): (options: ResumeOptions) => ChangeStream<Document, ChangeStreamDocument<Document>> {
    return options =>
        coll.watch([{ $match: { operationType: { $in: ['insert', 'update', 'replace', 'delete'] } } }], {
            fullDocument: 'updateLookup',
            fullDocumentBeforeChange: 'whenAvailable',
            ...options,
        });
}

/**
 * Stamp every event from `source` with `tenant` so downstream fan-out can
 * discriminate which tenant an event originated from (e.g. after merging
 * several tenants' streams with {@link mergeAsyncGenerators}). With `tenant`
 * undefined, events pass through unchanged — the single-tenant /
 * no-multi-tenancy case.
 */
export async function* tagTenant<TTenant>(
    source: AsyncGenerator<ChangeEvent<TTenant>>,
    tenant: TTenant | undefined
): AsyncGenerator<ChangeEvent<TTenant>> {
    if (tenant === undefined) {
        yield* source;
        return;
    }
    for await (const event of source) {
        yield { ...event, tenant };
    }
}

/**
 * Resolve once `shutdown` is (or becomes) `true`. The hub's per-collection
 * `Fanout` consumer has no shutdown awareness of its own — it only ends when
 * the hub itself closes the fanout in ITS OWN (independently-timed) shutdown
 * path. Racing the consumer's `next()` against this latch (see
 * {@link hubConsumerLoop}) restores the self-termination the direct
 * (non-hub) path already gets for free from `ChangeStreamSubscription`'s
 * shutdown race.
 */
function whenShutdown(shutdown: ReadonlySignal<boolean>): Promise<void> {
    return new Promise<void>(resolve => {
        if (shutdown.getValue()) {
            resolve();
            return;
        }
        const sub = shutdown.subscribe(v => {
            if (v) {
                // Defer unsubscribe so we don't unsubscribe from inside our own
                // notification (rxjs-derived caution; harmless here too, but this
                // keeps it unambiguous).
                queueMicrotask(() => sub.unsubscribe());
                resolve();
            }
        });
    });
}

/**
 * Subscribe one collection's raw change docs from the hub. Ported from
 * pactsheet.fm's `adaptCollection` (watch-entity-changes-all-tenants.ts).
 *
 * Self-heals a terminally-crashed hub reader: the hub's `evictIfCrashed`
 * (change-stream-hub.ts) closes our fanout and evicts the dead stream when its
 * `db.watch()` crashes terminally, but it has no way to re-subscribe itself —
 * that's entirely on the caller. When our consumer ends with `done` and we are
 * NOT shutting down, that's exactly what happened, so we re-`subscribe()` for
 * a fresh consumer instead of returning (returning would silently and
 * PERMANENTLY drop this source out of `mergeAsyncGenerators`, which just
 * removes a `done` source from its pull set with no error and no retry).
 * Capped exponential backoff bounds the retry rate so a persistently-throwing
 * `db.watch()` (e.g. the DB is genuinely unreachable) can't tight-loop.
 */
async function* hubConsumerLoop<TTenant>(
    source: HubSource,
    options: WatchCollectionOptions<TTenant>,
    log: Logger
): AsyncGenerator<ChangeStreamDocument<Document>> {
    const { hub, key, db, collection } = source;
    const initialBackoffMs = options.initialBackoffMs ?? DEFAULT_INITIAL_BACKOFF_MS;
    const maxBackoffMs = options.maxBackoffMs ?? DEFAULT_MAX_BACKOFF_MS;
    let consumer = hub.subscribe(key, db, collection);
    // Tagged race result so "shutdown won" narrows cleanly against a change doc.
    const stop = whenShutdown(options.shutdown).then(() => ({ stop: true }) as const);
    let reconnectAttempt = 0;
    try {
        while (true) {
            // Race the next change doc against shutdown. Without this the consumer's
            // pending `next()` would only ever resolve when the hub closes the fanout
            // (its own, independently-timed shutdown path) — deadlocking teardown.
            const next = await Promise.race([consumer.next().then(res => ({ stop: false as const, res })), stop]);
            if (next.stop) return;
            if (next.res.done) {
                // The `done` could in principle be the hub's OWN orderly shutdown
                // teardown racing in right as we checked `next.stop` above — don't
                // reconnect on the way out.
                if (options.shutdown.getValue()) return;
                const backoffMs = Math.min(initialBackoffMs * 2 ** reconnectAttempt, maxBackoffMs);
                reconnectAttempt += 1;
                log.warn(
                    `hub consumer for ${key}/${collection} ended unexpectedly (reconnect attempt ${reconnectAttempt}) — retrying in ${backoffMs}ms`
                );
                const waited = await delayUnlessStopped(backoffMs, stop);
                if (waited === 'stopped') return;
                consumer = hub.subscribe(key, db, collection);
                continue;
            }
            if (reconnectAttempt > 0) {
                // Only claim recovery once a real event has actually flowed through
                // the new consumer — not merely because we resubscribed (the fresh
                // stream could itself crash immediately, e.g. a persistent outage).
                log.log(
                    `hub consumer for ${key}/${collection} recovered after ${reconnectAttempt} reconnect attempt(s)`
                );
                reconnectAttempt = 0;
            }
            yield next.res.value;
        }
    } finally {
        // Best-effort detach — DO NOT await. The Fanout consumer is suspended at its
        // internal `await` (waiting for the next pushed item), and calling
        // `.return()` on a generator parked at an `await` cannot resume it until
        // that promise settles — which only happens when the hub closes the fanout
        // in its OWN (independently-timed) shutdown path. Awaiting here would
        // therefore re-introduce the very deadlock the shutdown race above exists to
        // break. Firing it unawaited lets this generator return NOW; `Fanout.close()`
        // then settles the pending promise, the consumer's own `finally` removes it
        // from the fanout set, and this queued `.return()` resolves — no leak.
        void consumer.return(undefined).catch(() => undefined);
    }
}

/**
 * Drive a single change stream directly via `ChangeStreamSubscription`, ported
 * from pactsheet.fm's `watchEntityChanges`. Uses a constant non-empty scope so
 * the watch stays open for the process lifetime rather than restarting on
 * scope changes.
 *
 * The scope signal is constructed fresh HERE (not hoisted to a shared
 * module-level constant) so it is garbage-collected together with the
 * `ChangeStreamSubscription` that subscribes to it once this generator ends.
 * `ChangeStreamSubscription` never unsubscribes its own `subscribeToNewData`
 * listener on normal completion (only on the next scope emission or its own
 * teardown internals) — a shared constant instance would accumulate one
 * permanently-retained listener per `directLoop` call for the life of the
 * process. The signal's value itself is immutable (`.next()` is never called
 * on it), so a fresh instance per call is cheap and behaves identically.
 */
async function* directLoop<TTenant>(
    source: DirectSource,
    options: WatchCollectionOptions<TTenant>,
    log: Logger
): AsyncGenerator<ChangeStreamDocument<Document>> {
    const scope = new ValueSignal<number[]>([1]);
    const subscription = new ChangeStreamSubscription<number, ChangeStreamDocument<Document>>(
        scope,
        options.shutdown,
        (_scope, resumeOptions) => source.watch(resumeOptions),
        {
            name: options.name,
            logger: log,
            initialBackoffMs: options.initialBackoffMs,
            maxBackoffMs: options.maxBackoffMs,
        }
    );
    yield* subscription;
}

/**
 * Watch one collection for changes and yield canonical `ChangeEvent`s.
 *
 * Two source modes, discriminated by `'hub' in options.source`:
 *   - `{ hub, key, db, collection }` — subscribe through a shared
 *     `ChangeStreamHub`, so this collection's watch shares the hub's one
 *     DB-level cursor per `key` with every other collection subscribed under
 *     that key.
 *   - `{ watch, db, collection }` — open a dedicated change stream directly.
 *     Your `watch` factory must spread the passed resume options into the
 *     driver call and should set `fullDocument: 'updateLookup'`,
 *     `fullDocumentBeforeChange: 'whenAvailable'` — {@link collectionWatchFactory}
 *     builds a correct factory from a plain driver `Collection`.
 *
 * Pre/post images are enabled once up front (needed for hard-delete scope
 * recovery via `fullDocumentBeforeChange`), regardless of source mode.
 */
export async function* watchCollection<TTenant = unknown>(
    options: WatchCollectionOptions<TTenant>
): AsyncGenerator<ChangeEvent<TTenant>> {
    const log = prefixed(options.logger ?? consoleLogger, options.name ?? `watch:${options.source.collection}`);
    await enablePrePostImages(options.source.db, [options.source.collection], log);
    const inner =
        'hub' in options.source
            ? hubConsumerLoop(options.source, options, log)
            : directLoop(options.source, options, log);

    async function* events(): AsyncGenerator<ChangeEvent<TTenant>> {
        for await (const change of inner) {
            const event = changeDocToEvent(change, options.conventions);
            if (event) yield event as ChangeEvent<TTenant>;
        }
    }

    yield* tagTenant(events(), options.tenant);
}
