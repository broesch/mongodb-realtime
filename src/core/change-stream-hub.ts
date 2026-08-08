import type {
    ChangeStreamDeleteDocument,
    ChangeStreamDocument,
    ChangeStreamInsertDocument,
    ChangeStreamReplaceDocument,
    ChangeStreamUpdateDocument,
    Db,
    Document,
} from 'mongodb';
import { Logger, consoleLogger, prefixed } from '../common/logger.js';
import { ReadonlySignal, ValueSignal, subscribeToChanges } from '../common/signal.js';
import { ChangeStreamSubscription } from './change-stream-subscription.js';
import { Fanout } from './fanout.js';

// The `$match` pipeline below restricts the DB-level watch to exactly these
// four operation types — `RawChangeDoc` is the union ChangeStreamSubscription
// is generic over (`TChange`). The mongodb driver types `db.watch(...)`'s
// return as a `ChangeStream` whose doc generics don't line up exactly with
// that constraint, so the factory return is cast at the driver boundary
// (`as never`) exactly as a collection-level `model.watch(...)` would be —
// `RawChangeDoc` is what actually flows through `.next()` / `yield`.
type RawChangeDoc =
    | ChangeStreamInsertDocument<Document>
    | ChangeStreamUpdateDocument<Document>
    | ChangeStreamReplaceDocument<Document>
    | ChangeStreamDeleteDocument<Document>;

const DEFAULT_GRACE_MS = 45_000;
const DEFAULT_MAX_QUEUE = 10_000;

export interface ChangeStreamHubOptions {
    /**
     * Distinct keys with a live connection on this pod. Omit to have every
     * stream start (and stay) open — connection-driven parking is opt-in.
     */
    connectedKeys?: ReadonlySignal<string[]>;
    /**
     * Grace window kept open after a key's connection drops, so a quick
     * reconnect (page reload, brief network blip) never tears the watch down.
     * Default 45_000. Ignored when `connectedKeys` is omitted.
     */
    graceMs?: number;
    /** Collection names whose changes are never delivered to any subscriber. Default []. */
    ignoreCollections?: string[];
    /** Per-collection `Fanout` consumer buffer size. Default 10_000. */
    maxQueue?: number;
    logger?: Logger;
}

interface HubStream {
    fanouts: Map<string, Fanout<ChangeStreamDocument>>;
    // Connection-driven parking. `[1]` while the key has a live connection on
    // this pod (watch open); `[]` once it has been disconnected past the grace
    // window (watch parked — see ChangeStreamSubscription's empty-scope branch).
    // Always `[1]` and never revisited when no `connectedKeys` signal was
    // supplied to the hub.
    scope$: ValueSignal<number[]>;
    shutdown: ValueSignal<boolean>;
    reader: Promise<void>;
    // Pending grace-close timer armed when the key disconnects; cleared if it
    // reconnects within graceMs so the watch never actually closes (no flicker).
    graceTimer?: NodeJS.Timeout;
}

/**
 * Per-process hub that opens ONE DB-level `db.watch()` per key and
 * demultiplexes its change events by `ns.coll` to per-collection consumers.
 *
 * Instead of every domain watcher opening its own `model.watch(...)` cursor
 * (one server-side change-stream cursor each), a key gets a single DB-level
 * cursor and the hub fans each event out to the collections that have
 * subscribers. The number of MongoDB change-stream cursors per process drops
 * from O(watchers) to O(keys).
 *
 * The single DB-level stream is driven through {@link ChangeStreamSubscription},
 * so resume-token handling, reopen-on-error, the first-event resume window,
 * and shutdown are all reused unchanged — the only difference is the factory
 * calls `db.watch(...)` (DB-level) rather than `collection.watch(...)`
 * (collection-level).
 *
 * When `options.connectedKeys` is supplied, each key's stream is driven by
 * connectivity: the DB-level watch is open only while that key is connected
 * (per the signal). When a key disconnects, the watch is held open for a
 * grace window and only then parked (`scope$ -> []`); a reconnect within the
 * window cancels the close so there is no flicker. A real close→reopen
 * resumes from the last resume token / captured operation time, so it is
 * lossless within the oplog window. This cuts open change-stream cursors to
 * O(connected keys). When `connectedKeys` is omitted, every stream starts and
 * stays open — connection-driven parking is opt-in.
 */
export class ChangeStreamHub {
    private readonly log: Logger;
    private readonly streams = new Map<string, HubStream>();
    private readonly graceMs: number;
    private readonly ignoreCollections: Set<string>;
    private readonly maxQueue: number;

    // Distinct keys with a live connection on this pod, mirrored from
    // `options.connectedKeys`. `undefined` when no signal was supplied — every
    // stream is then always open and `reconcile()` is a no-op.
    private connected: Set<string> | undefined;
    private readonly connectedSub: { unsubscribe(): void } | undefined;
    private closed = false;

    constructor(private readonly options: ChangeStreamHubOptions = {}) {
        this.log = prefixed(options.logger ?? consoleLogger, ChangeStreamHub.name);
        this.graceMs = options.graceMs ?? DEFAULT_GRACE_MS;
        this.ignoreCollections = new Set(options.ignoreCollections ?? []);
        this.maxQueue = options.maxQueue ?? DEFAULT_MAX_QUEUE;

        if (options.connectedKeys) {
            const connectedKeys = options.connectedKeys;
            // `connectedKeys` is BehaviorSubject/ValueSignal-compatible, so seed
            // synchronously from the current value on construction too.
            this.connected = new Set(connectedKeys.getValue());
            this.connectedSub = subscribeToChanges(connectedKeys, keys => {
                this.connected = new Set(keys);
                this.reconcile();
            });
        }
    }

    /**
     * Subscribe to one collection's raw change docs on a key's DB. The key's
     * stream is opened lazily on first subscribe and reused (keyed by `key`,
     * e.g. a tenant DB name or a fixed default); every collection on the same
     * key shares that one DB-level `db.watch()`.
     */
    subscribe(key: string, db: Db, collectionName: string): AsyncGenerator<ChangeStreamDocument> {
        if (this.closed) {
            // Terminal by design: after close() the reader loops are gone, so
            // a new subscription would either open an untracked cursor
            // (new key) or hand back a generator parked forever on a closed
            // fanout (existing key). Fail loudly instead.
            throw new Error('ChangeStreamHub is closed — subscribe() is not valid after close()');
        }
        const stream = this.ensureStream(key, db);
        let fan = stream.fanouts.get(collectionName);
        if (!fan) {
            fan = new Fanout<ChangeStreamDocument>({
                maxQueue: this.maxQueue,
                label: `${key}:${collectionName}`,
                logger: this.options.logger,
            });
            stream.fanouts.set(collectionName, fan);
        }
        return fan.consumer();
    }

    private ensureStream(key: string, db: Db): HubStream {
        const existing = this.streams.get(key);
        if (existing) return existing;

        const ignored = [...this.ignoreCollections];

        // Start open only if the key is already connected — or unconditionally
        // when no `connectedKeys` signal was supplied at all; otherwise start
        // parked (`[]`) so the reader waits in ChangeStreamSubscription's
        // empty-scope branch until reconcile() opens it on connect.
        const isConnected = !this.connected || this.connected.has(key);
        const scope$ = new ValueSignal<number[]>(isConnected ? [1] : []);
        const shutdown = new ValueSignal<boolean>(false);
        const fanouts = new Map<string, Fanout<ChangeStreamDocument>>();

        const sub = new ChangeStreamSubscription<number, RawChangeDoc>(
            scope$,
            shutdown,
            (_scope, options) =>
                db.watch(
                    [
                        {
                            $match: {
                                operationType: {
                                    $in: ['insert', 'update', 'replace', 'delete'],
                                },
                                // Filter ignored collections HERE, not in the
                                // reader loop: a DB-level watch produces a
                                // change document (with an `updateLookup`
                                // full document) for every write in the whole
                                // database, so filtering after the fact still
                                // pays the server-side lookup and the
                                // transport for collections nobody consumes.
                                // The reader-loop check below stays as the
                                // backstop for collections with no fanout.
                                ...(ignored.length > 0 ? { 'ns.coll': { $nin: ignored } } : {}),
                            },
                        },
                    ],
                    {
                        fullDocument: 'updateLookup',
                        fullDocumentBeforeChange: 'whenAvailable',
                        ...options,
                    }
                ) as never,
            { name: `ChangeStreamHub[${key}]`, logger: this.options.logger }
        );

        const reader = (async () => {
            for await (const change of sub) {
                const coll = (change as ChangeStreamDocument & { ns?: { coll?: string } }).ns?.coll;
                if (!coll || this.ignoreCollections.has(coll)) continue;
                fanouts.get(coll)?.push(change);
            }
        })()
            .catch(err => this.log.error(`hub reader ${key} crashed: ${(err as Error).message}`))
            .finally(() => this.evictIfCrashed(key, shutdown, fanouts));

        const stream: HubStream = { fanouts, scope$, shutdown, reader };
        this.streams.set(key, stream);
        return stream;
    }

    /**
     * Self-heal a terminally-dead reader. `ChangeStreamSubscription` already
     * reopens on transient errors, so the reader loop only ends when it
     * (a) shut down, or (b) crashed terminally (e.g. `db.watch()` threw
     * because the connection was torn down). In case (b) — shutdown NOT set —
     * the dead stream would linger in `this.streams` and `ensureStream` would
     * keep handing back its stale fanouts, silently halting that key's
     * realtime. Evict it and close its fanouts (so parked consumers
     * complete) — the NEXT `subscribe` recreates a fresh stream. No-op on the
     * orderly-shutdown path (`close()` owns teardown) and if the map entry was
     * already replaced.
     */
    private evictIfCrashed(
        key: string,
        shutdown: ValueSignal<boolean>,
        fanouts: Map<string, Fanout<ChangeStreamDocument>>
    ): void {
        if (shutdown.getValue()) return;
        const current = this.streams.get(key);
        if (current && current.fanouts === fanouts) {
            this.streams.delete(key);
        }
        fanouts.forEach(f => f.close());
        this.log.warn(`hub reader ${key} terminated unexpectedly — stream evicted; next subscribe will recreate it`);
    }

    /**
     * Align every existing stream's open/closed state with the current set of
     * connected keys. Called whenever `connectedKeys` emits. No-op when the
     * hub was constructed without a `connectedKeys` signal.
     *
     * - Connected: cancel any pending grace-close and (re)open if parked.
     * - Disconnected: arm a grace-close timer (once) that parks the watch
     *   after `graceMs`; a reconnect before it fires clears it, so the watch
     *   never actually closed.
     *
     * Streams not yet created (no `subscribe()` for that key) are simply
     * absent from `this.streams`; `ensureStream` seeds their initial scope
     * from `this.connected`, so they pick up the right state when created.
     */
    private reconcile(): void {
        if (!this.connected) return;
        const connected = this.connected;
        for (const [key, s] of this.streams) {
            if (connected.has(key)) {
                if (s.graceTimer) {
                    clearTimeout(s.graceTimer);
                    s.graceTimer = undefined;
                }
                // Reopen only if parked; leave an already-open stream untouched.
                if (s.scope$.getValue().length === 0) s.scope$.next([1]);
            } else if (!s.graceTimer) {
                s.graceTimer = setTimeout(() => {
                    s.scope$.next([]);
                    s.graceTimer = undefined;
                }, this.graceMs);
                // Don't let a pending grace timer keep the process alive.
                if (typeof s.graceTimer.unref === 'function') s.graceTimer.unref();
            }
        }
    }

    /**
     * Tear down every stream: clear pending grace timers, signal shutdown,
     * close all fanouts (completing any parked consumer generators), and
     * await every reader loop's orderly exit.
     */
    async close(): Promise<void> {
        this.closed = true;
        this.connectedSub?.unsubscribe();
        for (const s of this.streams.values()) {
            // Clear any pending grace-close so it can't fire (and touch scope$)
            // after we've already signalled shutdown.
            if (s.graceTimer) {
                clearTimeout(s.graceTimer);
                s.graceTimer = undefined;
            }
            s.shutdown.next(true);
            s.fanouts.forEach(f => f.close());
        }
        await Promise.allSettled([...this.streams.values()].map(s => s.reader));
    }
}
