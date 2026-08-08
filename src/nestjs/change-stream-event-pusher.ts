import type { OnApplicationBootstrap, OnApplicationShutdown } from '@nestjs/common';
import { consoleLogger, prefixed, type Logger } from '../common/logger.js';
import { ValueSignal } from '../common/signal.js';

export interface ChangeStreamEventPusherOptions {
    logger?: Logger;
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

    private loopEndedResolve!: (value: boolean) => void;
    private readonly stopped: Promise<boolean> = new Promise<boolean>(resolve => {
        this.loopEndedResolve = resolve;
    });

    constructor(options?: ChangeStreamEventPusherOptions) {
        this.logger = options?.logger
            ? prefixed(options.logger, this.constructor?.name ?? 'ChangeStreamEventPusher')
            : prefixed(consoleLogger, this.constructor?.name ?? 'ChangeStreamEventPusher');
    }

    protected abstract pushChangeEvents(): Promise<void>;

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
        // Fire-and-forget: the change-stream loop runs in the background for
        // the lifetime of the host. `void` marks the promise as intentionally
        // unawaited.
        //
        // The `.catch` is load-bearing: if `pushChangeEvents()` rejects BEFORE
        // reaching its own try/finally (e.g. the watcher throws during setup),
        // without it we'd (1) emit an unhandled promise rejection and (2) never
        // resolve `this.stopped`, hanging `onApplicationShutdown` forever.
        // Calling `loopEnded()` here is safe even on the normal path —
        // re-resolving an already-resolved promise is a no-op.
        void this.pushChangeEvents().catch((err: unknown) => {
            this.logger.error(
                `Change-stream pusher crashed during startup: ${(err as Error)?.message ?? String(err)}`,
                (err as Error)?.stack
            );
            this.loopEnded();
        });
    }

    async onApplicationShutdown(): Promise<void> {
        this.shutdown.next(true);
        await this.stopped;
    }
}
