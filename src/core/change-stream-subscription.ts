import type {
    ChangeStream,
    ChangeStreamDocument,
    ChangeStreamOptions as MongoChangeStreamOptions,
    Db,
    Document,
    Timestamp,
} from 'mongodb';
// Value imports for runtime `instanceof` narrowing of a stream's `parent`
// (MongoClient | Db | Collection) down to a `Db` we can run `hello` on.
import { Collection, Db as DbClass, MongoClient } from 'mongodb';
import { deepEqual } from '../common/deep-equal.js';
import { delayUnlessStopped } from '../common/delay-unless-stopped.js';
import { Logger, consoleLogger, prefixed } from '../common/logger.js';
import { ReadonlySignal, subscribeToChanges } from '../common/signal.js';

export type ResumeOptions = MongoChangeStreamOptions & { writeConcern?: never };

/** Reconnect backoff bounds — capped exponential, doubling from initial. */
const DEFAULT_INITIAL_BACKOFF_MS = 1_000;
const DEFAULT_MAX_BACKOFF_MS = 30_000;

export interface ChangeStreamSubscriptionOptions<TScope> {
    name?: string;
    logger?: Logger;
    initialBackoffMs?: number;
    maxBackoffMs?: number;
    isEqual?: (a: TScope[], b: TScope[]) => boolean;
}

/**
 * Server error codes that no amount of retrying can fix, because they describe
 * the DEPLOYMENT rather than a transient fault:
 *   40573 — `$changeStream` is only supported on replica sets
 *   40574 — `$changeStream` may not be opened on the internal config database
 *
 * Retrying these forever is what turns a misconfigured mongod into a silent
 * failure: the watch loop spins at the backoff rate and the only symptom is a
 * warning per attempt. Rethrowing instead surfaces a named error through the
 * pusher's bootstrap `.catch` (or the hub reader's), so the operator sees it.
 */
const FATAL_CHANGE_STREAM_CODES = new Set([40573, 40574]);

/**
 * Read a string-valued property off an unknown thrown value. Driver errors are
 * plain `Error`s with extra fields, but nothing guarantees a `catch` binding is
 * one — so anything non-string reads as absent rather than being coerced.
 */
function errorField(error: unknown, key: 'message' | 'codeName'): string {
    const value = (error as Record<string, unknown> | null | undefined)?.[key];
    return typeof value === 'string' ? value : '';
}

export function isFatalChangeStreamError(error: unknown): boolean {
    const code = (error as { code?: unknown })?.code;
    if (typeof code === 'number' && FATAL_CHANGE_STREAM_CODES.has(code)) {
        return true;
    }
    return errorField(error, 'message').includes('only supported on replica sets');
}

/**
 * Errors that say "the resume point you are holding can never be used again".
 * Distinct from {@link isFatalChangeStreamError}: the deployment is fine, but
 * reopening from the CURRENT `resumeToken` / `startAtOperationTime` is
 * guaranteed to fail the same way every time — a poison resume point that the
 * plain retry path would replay forever without ever making progress:
 *
 *   - 286 / `ChangeStreamHistoryLost` — the token has aged out of the oplog.
 *   - a missing pre-image — the delete's `fullDocumentBeforeChange` has been
 *     expired by the pre-image retention window, so the event that the token
 *     points just before can never be re-read.
 *
 * The recovery is to DROP the resume point and reopen fresh: the changes in the
 * gap are lost (unavoidable — Mongo no longer has them), but realtime keeps
 * working instead of wedging.
 */
export function isUnresumableChangeStreamError(error: unknown): boolean {
    if ((error as { code?: unknown })?.code === 286) return true;
    if (errorField(error, 'codeName') === 'ChangeStreamHistoryLost') return true;
    const message = errorField(error, 'message');
    return (
        message.includes('ChangeStreamHistoryLost') ||
        message.includes('PreImageNotFoundForChangeStream') ||
        message.includes('pre-image was not found')
    );
}

type ChangeStreamFactory<TScope, TChange extends ChangeStreamDocument<Document>> = (
    scope: TScope[],
    options: ResumeOptions
) => ChangeStream<Document, TChange>;

export class ChangeStreamSubscription<
    TScope,
    TChange extends ChangeStreamDocument<Document>,
> implements AsyncIterable<TChange> {
    private logger: Logger;

    private resumeToken: unknown;
    // Cluster operation time captured at (just after) each stream open. Used as
    // `startAtOperationTime` on reopen when no `resumeToken` has been captured
    // yet — i.e. when the very first `next()` rejects before any event was
    // yielded — so changes since the original open are not lost. Cleared if a
    // resume from it fails (e.g. it has aged past the oplog window), falling
    // back to a fresh `{}` open like a stale resume token would.
    private startAtOperationTime: Timestamp | undefined;
    private newDataPromise: Promise<{ newData: true; data: TScope[] }> | undefined;
    private newDataSubscription: { unsubscribe(): void } | undefined;
    private shutdownPromise: Promise<{ shutdown: true }> | undefined;
    private shutdownSubscription: { unsubscribe(): void } | undefined;
    private _currentData: TScope[] | undefined;
    private readonly initialBackoffMs: number;
    private readonly maxBackoffMs: number;
    private readonly isEqual: (a: TScope[], b: TScope[]) => boolean;

    constructor(
        private readonly scope: ReadonlySignal<TScope[]>,
        private readonly shutdown: ReadonlySignal<boolean>,
        private readonly changeStreamFactory: ChangeStreamFactory<TScope, TChange>,
        options: ChangeStreamSubscriptionOptions<TScope> = {}
    ) {
        this.currentData = scope.getValue();
        this.subscribeToNewData();
        this.subscribeToShutdown();
        this.logger = prefixed(options.logger ?? consoleLogger, options.name || ChangeStreamSubscription.name);
        this.initialBackoffMs = options.initialBackoffMs ?? DEFAULT_INITIAL_BACKOFF_MS;
        this.maxBackoffMs = options.maxBackoffMs ?? DEFAULT_MAX_BACKOFF_MS;
        this.isEqual = options.isEqual ?? deepEqual;
    }

    private set currentData(value: TScope[] | undefined) {
        this._currentData = value ? [...value] : undefined; //we need to make a copy to avoid reference issues
    }

    private hasDataChanged(newData: TScope[]): boolean {
        if (!this._currentData) {
            return true;
        }
        if (this._currentData.length !== newData.length) {
            return true;
        }

        this.logger.debug(
            `Comparing current data: ${JSON.stringify(this._currentData)} with new data: ${JSON.stringify(newData)}`
        );

        return !this.isEqual(this._currentData, newData);
    }

    private subscribeToNewData() {
        this.newDataSubscription?.unsubscribe();
        this.newDataPromise = new Promise<{ newData: true; data: TScope[] }>(resolve => {
            this.newDataSubscription = subscribeToChanges(this.scope, data => {
                this.logger.debug(`Detected new data: ${JSON.stringify(data)}`);
                if (this.hasDataChanged(data)) {
                    this.newDataSubscription?.unsubscribe();
                    resolve({ newData: true, data });
                } else {
                    this.logger.debug('Data has not changed, ignoring');
                }
            });
        });
    }

    private subscribeToShutdown() {
        this.shutdownSubscription?.unsubscribe();
        this.shutdownPromise = new Promise<{ shutdown: true }>(resolve => {
            this.shutdownSubscription = subscribeToChanges(this.shutdown, v => {
                if (v) {
                    this.logger.log('Shutdown signal received, stopping change stream subscription');
                    this.shutdownSubscription?.unsubscribe();
                    resolve({ shutdown: true });
                }
            });
        });
    }

    public async *[Symbol.asyncIterator](): AsyncGenerator<TChange> {
        // NOTE: do NOT reintroduce a pre-loop "await initial data" block here. The
        // while loop's empty-scope branch below already waits for initial data AND
        // races `shutdownPromise`. A separate pre-loop `await this.newDataPromise`
        // (with no shutdown race) hangs `app.close()`/SIGTERM forever when the scope
        // STARTS empty and no data ever arrives — e.g. 4B: a per-tenant hub watch
        // for a tenant that never gets a connected socket on this pod. (Caught by
        // the backend e2e suite as afterAll `app.close()` hook timeouts.)
        this.logger.debug(`Starting change stream subscription for data: ${JSON.stringify(this._currentData)}`);
        // Capped exponential reconnect backoff. Lives outside the loop so repeated
        // failures compound (1s → 2s → … → 30s) instead of hammering a persistently
        // failing stream once a second forever, per subscription. Reset to the
        // initial delay as soon as an event is successfully yielded, so an isolated
        // blip never leaves a healthy stream stuck at a long delay.
        let backoffMs = this.initialBackoffMs;

        // Tracks the currently-open driver change stream (if any), so the outer
        // `finally` below can close it on ANY exit from the loop — including an
        // external `.return()`/`.throw()` fired on THIS generator while it's
        // suspended at the `yield` further down (e.g. a consumer like
        // `mergeAsyncGenerators` tearing down early). That path forces a return
        // completion right at the yield, which unwinds straight through the inner
        // `finally` below (a no-op there, since `restartedDueToError` is false on
        // the happy path) without ever reaching the shutdown/new-data/error
        // branches that close explicitly — so without this, the driver cursor
        // would leak. Every branch that already closes the stream explicitly
        // clears this back to `undefined` right after, so the outer `finally`
        // below never double-closes.
        let openStream: ChangeStream<Document, TChange> | undefined;

        try {
            while (true) {
                // Skip opening a watch when scope is empty (e.g. all users disconnected).
                // Wait for new data or shutdown instead.
                if (!this._currentData || this._currentData.length === 0) {
                    const result = await Promise.race([this.newDataPromise!, this.shutdownPromise!]);
                    if ('shutdown' in result) return;
                    this.currentData = result.data;
                    this.subscribeToNewData();
                    continue;
                }

                // Choose the resume point for this (re)open. `startAfter`/`resumeAfter`
                // and `startAtOperationTime` are mutually exclusive — pass exactly one.
                //   - resumeToken set (≥1 event yielded): resume right after it.
                //   - else, T0 captured from a prior open: start at that cluster time so
                //     changes since the original open are not missed even if the very
                //     first `next()` rejected before any token existed.
                //   - else (first ever open): a fresh `{}` watch.
                const openedFromOperationTime = !this.resumeToken && this.startAtOperationTime !== undefined;
                const changesStream = this.changeStreamFactory(
                    this._currentData,
                    this.resumeToken
                        ? { startAfter: this.resumeToken }
                        : this.startAtOperationTime
                          ? { startAtOperationTime: this.startAtOperationTime }
                          : {}
                );
                openStream = changesStream;
                // Capture the cluster operation time right after opening so that if this
                // stream errors before yielding anything (no resumeToken yet), the next
                // reopen can resume from here. The [open, capture] window is a single
                // `hello` round-trip and is covered by the live stream itself; a tiny
                // overlap is harmless (change streams are at-least-once on resume).
                await this.captureOperationTime(changesStream);
                let restartedDueToError = false;
                try {
                    let result: TChange | { newData: true; data: TScope[] } | { shutdown: true } | undefined;
                    while (true) {
                        try {
                            result = await Promise.race([
                                changesStream.next(),
                                this.newDataPromise!,
                                this.shutdownPromise!,
                            ]);
                        } catch (err) {
                            // Permanent deployment fault (not a replica set, config DB): no
                            // reopen can ever succeed, so retrying just spins silently at the
                            // backoff rate. Rethrow so the consumer's bootstrap `.catch` logs a
                            // named error and the operator learns realtime is dead. Closed
                            // here because the `finally` below only closes on the retry path.
                            if (isFatalChangeStreamError(err)) {
                                this.logger.error(`Fatal change stream error, not retrying: ${(err as Error).message}`);
                                await this.safeClose(changesStream);
                                openStream = undefined;
                                throw err;
                            }

                            // The resume point itself is unusable (token aged out of the oplog,
                            // pre-image expired). Reopening from it is guaranteed to fail the
                            // same way every time — a poison resume point the plain retry path
                            // below would replay forever without making progress. Drop it so
                            // the reopen degrades to a fresh `{}` watch: the gap is lost
                            // (Mongo no longer has it) but realtime keeps working.
                            if (isUnresumableChangeStreamError(err)) {
                                this.logger.warn(
                                    `Change stream resume point is no longer usable; restarting from now — changes during the gap were not delivered: ${(err as Error).message}`
                                );
                                this.resumeToken = undefined;
                                this.startAtOperationTime = undefined;
                                restartedDueToError = true;
                                break;
                            }

                            // Transient driver / network error. We rebuild the watch and pick
                            // up where we left off:
                            //   - if ≥1 event was yielded, `resumeToken` is set and the reopen
                            //     resumes right after it (no events lost);
                            //   - if NOTHING was yielded yet, `resumeToken` is still undefined,
                            //     so the reopen uses `startAtOperationTime` (T0, captured at
                            //     open) to cover changes since the original open — closing the
                            //     first-event gap that a bare `{}` reopen would have left.
                            if (this.resumeToken) {
                                this.logger.warn(
                                    `Change stream errored, will resume from token: ${(err as Error).message}`
                                );
                            } else if (openedFromOperationTime) {
                                // This stream was itself opened from a captured operation time
                                // and still failed before yielding anything. T0 may have aged
                                // past the oplog window (same failure class as a stale resume
                                // token). Drop it so the next reopen degrades to a fresh `{}`
                                // watch rather than retrying an un-resumable time forever.
                                this.logger.warn(
                                    `Change stream re-opened from operation time still errored before any event; dropping it and reopening fresh: ${(err as Error).message}`
                                );
                                this.startAtOperationTime = undefined;
                            } else if (this.startAtOperationTime) {
                                this.logger.warn(
                                    `Change stream errored before any event, will resume from captured operation time: ${(err as Error).message}`
                                );
                            } else {
                                this.logger.warn(
                                    `Change stream errored before any event and no operation time was captured, reopening fresh: ${(err as Error).message}`
                                );
                            }
                            restartedDueToError = true;
                            break;
                        }
                        if (!result) break;
                        this.logger.debug(`Change stream result: ${JSON.stringify(result)}`);
                        if ('shutdown' in result && result.shutdown === true) {
                            this.logger.log('Shutting down change stream subscription');
                            await this.safeClose(changesStream);
                            openStream = undefined;
                            return;
                        }
                        if ('newData' in result && result.newData) {
                            this.logger.debug('Detected new data, restarting change stream with new data');
                            this.currentData = result.data;
                            this.resumeToken = changesStream.resumeToken ?? this.resumeToken;
                            await this.safeClose(changesStream);
                            openStream = undefined;
                            this.subscribeToNewData();
                            break;
                        }

                        this.resumeToken = changesStream.resumeToken ?? this.resumeToken;
                        // An event made it through: the stream is healthy again, so a later
                        // blip starts backing off from the bottom rather than from whatever
                        // ceiling an earlier outage climbed to.
                        backoffMs = this.initialBackoffMs;
                        yield result as TChange;
                    }
                } finally {
                    if (restartedDueToError) {
                        await this.safeClose(changesStream);
                        openStream = undefined;
                    }
                }

                // Reconnect pause — deliberately OUTSIDE the `finally` so shutdown during
                // it can `return` without swallowing an in-flight throw. Cancellable, so
                // raising the ceiling to 30s does not add 30s to SIGTERM; unref'd, so a
                // pending backoff never holds the process open.
                if (restartedDueToError) {
                    const waited = await delayUnlessStopped(backoffMs, this.shutdownPromise!);
                    backoffMs = Math.min(backoffMs * 2, this.maxBackoffMs);
                    if (waited === 'stopped') {
                        this.logger.log(
                            'Shutdown signalled during reconnect backoff, stopping change stream subscription'
                        );
                        return;
                    }
                }
            }
        } finally {
            // Catches the one path nothing above covers: this generator's `.return()`
            // (or `.throw()`) called externally while suspended at the `yield` above —
            // e.g. `mergeAsyncGenerators`' teardown firing `.return()` on every source,
            // including one that's currently parked mid-yield rather than mid-`next()`.
            // That forces a return/throw completion right at the yield, unwinding
            // through here without ever passing through the shutdown/new-data/error
            // branches that already close explicitly (and clear `openStream`) — so
            // without this, the driver cursor underneath would leak. Idempotent: every
            // other exit path already closed and cleared `openStream` to `undefined`.
            if (openStream) {
                await this.safeClose(openStream);
            }
        }
    }

    private async safeClose(stream: ChangeStream<Document, TChange>): Promise<void> {
        try {
            await stream.close();
        } catch (err) {
            this.logger.debug(`Error closing change stream (ignored): ${(err as Error).message}`);
        }
    }

    /**
     * Capture the current cluster operation time and stash it in
     * `this.startAtOperationTime` for use as a `startAtOperationTime` resume
     * point on a later reopen (when no resume token exists yet). The time is
     * read with `db.admin().command({ hello: 1 })`, whose reply carries an
     * `operationTime` BSON Timestamp on a replica set / sharded cluster.
     *
     * Best-effort: if the command fails or the deployment doesn't return an
     * `operationTime` (e.g. a standalone, where change streams aren't supported
     * anyway), the previously captured value is left untouched and we simply
     * fall back to the existing behaviour. Never throws.
     */
    private async captureOperationTime(stream: ChangeStream<Document, TChange>): Promise<void> {
        try {
            const db = this.resolveDb(stream);
            if (!db) return;
            const hello = await db.admin().command({ hello: 1 });
            const operationTime = (hello as { operationTime?: Timestamp }).operationTime;
            if (operationTime) {
                this.startAtOperationTime = operationTime;
            }
        } catch (err) {
            this.logger.debug(`Could not capture cluster operation time (ignored): ${(err as Error).message}`);
        }
    }

    /**
     * Resolve a `Db` handle from a change stream's `parent`, which the driver
     * types as `MongoClient | Db | Collection`. We only need it to run the
     * `hello` admin command, so any database on the same client works.
     */
    private resolveDb(stream: ChangeStream<Document, TChange>): Db | undefined {
        const parent: unknown = stream.parent;
        if (parent instanceof DbClass) return parent;
        if (parent instanceof Collection) return parent.db;
        if (parent instanceof MongoClient) return parent.db();
        return undefined;
    }
}
