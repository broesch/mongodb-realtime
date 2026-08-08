import type { ChangeEvent } from '../core/change-event.js';
import type { Logger } from '../common/logger.js';
import type { ReadonlySignal } from '../common/signal.js';

/** Event names a fan-out loop emits under. `removed` is optional — omit it to never emit deletes. */
export interface PushEvents {
    changed: string;
    removed?: string;
}

export interface PushScopedOptions<TTenant> {
    /** Signals a graceful shutdown in progress; suppresses the "watcher returned early" error log. */
    shutdown: ReadonlySignal<boolean>;
    /** Room an event fans out to. Default: `namespacedRoom(tenantNamespace?.(tenant), scopeRoom(scopeId))`. */
    roomOf?: (event: ChangeEvent<TTenant>) => string;
    /** Derives the room namespace (e.g. tenant DB name) from the opaque tenant carried on the event. */
    tenantNamespace?: (tenant: TTenant) => string | undefined;
    /**
     * Transforms a changed document before emitting it. A throwing/rejecting
     * mapper skips that one event. The return is `await`ed by the fan-out
     * loop, so `unknown` already covers a Promise-returning mapper — spelling
     * out `| Promise<unknown>` is redundant and trips
     * @typescript-eslint/no-redundant-type-constituents.
     */
    mapData?: (data: unknown) => unknown;
    logger?: Logger;
    /** Log-message prefix; identifies which pusher an operator-visible log line came from. */
    name?: string;
}

export interface PushOwnerScopedOptions<TTenant> extends Omit<PushScopedOptions<TTenant>, 'roomOf'> {
    /** Resolves the owning user id from a changed document. Removed events use `event.ownerId` instead. */
    ownerIdOf: (data: unknown) => string | null | undefined;
}
