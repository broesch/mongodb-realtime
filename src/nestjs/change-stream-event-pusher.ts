import type { OnApplicationBootstrap, OnApplicationShutdown } from '@nestjs/common';
import type { ChangeStream, ChangeStreamDocument, Document } from 'mongodb';
import { consoleLogger, prefixed, type Logger } from '../common/logger.js';
import { ValueSignal } from '../common/signal.js';
import { ChangeStreamSubscription, type ResumeOptions } from '../core/change-stream-subscription.js';

export interface ChangeStreamEventPusherOptions {
    logger?: Logger;
    /**
     * When `false`, `pushChangeEvents()` is never invoked and the shutdown
     * latch is resolved immediately — the pusher is inert but its lifecycle
     * still completes. Wire this from a preflight result (see
     * `assertChangeStreamPreflight` and the `REALTIME_ENABLED` token) so an
     * app can boot on a deployment without change streams instead of
     * crash-looping.
     */
    enabled?: boolean;
}

/**
 * Lifecycle base for a change-stream fan-out loop. Subclasses implement
 * {@link pushChangeEvents} — typically by calling one of the free push
 * functions in `../socketio/` — and call {@link loopEnded} in its `finally`
 * so orderly shutdown can await drain.
 *
 * Ported from fm's `change-stream-event-pusher.ts`: only the lifecycle glue
 * (bootstrap/shutdown/crash-latch) moves here. The push loops themselves were
 * already ported as free functions in Tasks 12–13 (`pushScoped`,
 * `pushScopedFiltered`, `pushOwnerScoped`).
 */
export abstract class ChangeStreamEventPusher implements OnApplicationBootstrap, OnApplicationShutdown {
    protected readonly shutdown: ValueSignal<boolean> = new ValueSignal<boolean>(false);
    protected readonly logger: Logger;
    protected readonly enabled: boolean;
    private watching = false;

    private loopEndedResolve!: (value: boolean) => void;
    private readonly stopped: Promise<boolean> = new Promise<boolean>(resolve => {
        this.loopEndedResolve = resolve;
    });

    constructor(options?: ChangeStreamEventPusherOptions) {
        this.logger = options?.logger
            ? prefixed(options.logger, this.constructor?.name ?? 'ChangeStreamEventPusher')
            : prefixed(consoleLogger, this.constructor?.name ?? 'ChangeStreamEventPusher');
        this.enabled = options?.enabled !== false;
    }

    protected abstract pushChangeEvents(): Promise<void>;

    /**
     * Declares that a watch is live. Subclasses driving their own loop call
     * this once the stream is open (`watchOne` does it for you) so the base
     * class can tell "the loop ended early" — an operator-visible failure —
     * from "the loop never started", which is normal when realtime is off.
     */
    protected markWatching(): void {
        this.watching = true;
    }

    /**
     * Resolves the internal stopped-latch. Replaces fm's `shutdownResolve`:
     * subclasses call this in the `finally` of their push loop, and the
     * crash latch in `onApplicationBootstrap` calls it too so a pusher that
     * dies before reaching its loop still lets `onApplicationShutdown` return.
     * Idempotent — resolving an already-resolved promise is a no-op.
     */
    protected loopEnded(): void {
        this.loopEndedResolve(true);
    }

    onApplicationBootstrap(): void {
        if (!this.enabled) {
            this.logger.warn('Realtime is disabled — not watching for changes. No live events will be delivered.');
            this.loopEnded();
            return;
        }

        // Fire-and-forget: the change-stream loop runs in the background for
        // the lifetime of the host. `void` marks the promise as intentionally
        // unawaited.
        void this.pushChangeEvents()
            .then(() => {
                if (this.watching && !this.shutdown.getValue()) {
                    this.logger.error(
                        `${this.constructor?.name ?? 'pusher'} stopped watching before shutdown was requested — ` +
                            'no further events will be delivered. Restart the service or check the change stream ' +
                            'for an uncaught rejection.'
                    );
                }
            })
            .catch((err: unknown) => {
                this.logger.error(
                    `Change-stream pusher crashed during startup: ${(err as Error)?.message ?? String(err)}`,
                    (err as Error)?.stack
                );
            })
            // Latch BOTH paths. Previously only the rejection path resolved
            // it, so a loop that returned normally without calling
            // `loopEnded()` in its own `finally` hung `onApplicationShutdown`
            // forever. `loopEnded` is idempotent.
            .finally(() => this.loopEnded());
    }

    async onApplicationShutdown(): Promise<void> {
        this.shutdown.next(true);
        await this.stopped;
    }

    /**
     * Watch one collection and run `handler` for every raw change document.
     *
     * The common shape by a wide margin, and the one the canonical
     * `watchCollection` + `pushScoped` pipeline does NOT cover: a consumer
     * that needs the raw change document (to branch on `operationType`, or to
     * read `updateDescription` for something `ChangeEvent` doesn't model)
     * would otherwise hand-roll the subscription, the constant scope signal
     * and the loop. Prefer `watchCollection` + a push loop when the canonical
     * event is enough.
     *
     * Latching: `watchOne` deliberately does NOT resolve the shutdown latch
     * itself — the base class latches when `pushChangeEvents()` settles. A
     * pusher running several watches must therefore `await Promise.all` of
     * its `watchOne` calls, so shutdown drains ALL of them; a per-call latch
     * here would let the first-to-finish release shutdown early.
     *
     * The scope signal is constructed fresh per call — `ChangeStreamSubscription`
     * never unsubscribes its own scope listener on normal completion, so a
     * shared instance would retain one listener per call for the process's
     * life. Its value is never mutated, so a fresh `[1]` is equivalent.
     */
    protected async watchOne(options: {
        /** Used in log messages only. */
        collectionName: string;
        /** Opens the stream; MUST spread the passed resume options into the driver call. */
        watch: (resumeOptions: ResumeOptions) => ChangeStream<Document, ChangeStreamDocument<Document>>;
        handler: (change: ChangeStreamDocument<Document>) => void | Promise<void>;
    }): Promise<void> {
        const scope = new ValueSignal<number[]>([1]);
        const subscription = new ChangeStreamSubscription<number, ChangeStreamDocument<Document>>(
            scope,
            this.shutdown,
            (_scope, resumeOptions) => options.watch(resumeOptions),
            { name: `${this.constructor?.name ?? 'pusher'}:${options.collectionName}`, logger: this.logger }
        );

        this.markWatching();
        for await (const change of subscription) {
            // One malformed document must not stop fan-out for every
            // subsequent change, so a throwing handler is logged and
            // skipped rather than breaking the loop.
            try {
                await options.handler(change);
            } catch (err) {
                this.logger.warn(
                    `Handler failed for a ${change.operationType} change on ${options.collectionName} — skipping: ${(err as Error)?.message ?? String(err)}`
                );
            }
        }
        // NO loopEnded() here — see the doc comment: the base's bootstrap
        // `.finally` latches when pushChangeEvents() settles, which for a
        // multi-watch pusher is after ALL its awaited watchOne calls end.
    }
}
