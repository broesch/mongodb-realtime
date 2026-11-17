import type { RemoteSocket } from 'socket.io';
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
    /**
     * Room(s) an event fans out to. Default:
     * `namespacedRoom(tenantNamespace?.(tenant), scopeRoom(scopeId))`.
     * Returning a list emits once to all of them in a single `to()` call, so
     * a socket in several receives one copy; returning an empty list drops
     * the event (emitting to no rooms would broadcast to everyone).
     */
    roomOf?: (event: ChangeEvent<TTenant>) => string | string[];
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

/**
 * Two-variant fan-out hook (PII gating). `render` runs ONCE per changed doc,
 * producing a `{ full, redacted }` pair; the per-socket delivery loop then
 * emits whichever one `pick(socket)` selects. This is DELIBERATELY not a
 * generic per-socket transformer — `pick` can only ever choose between the
 * two pre-rendered values, so a recipient can never be sent an arbitrary
 * bespoke payload. Mutually exclusive with `mapData`.
 */
export interface FanoutVariants {
    render: (data: unknown) => { full: unknown; redacted: unknown };
    pick: (socket: RemoteSocket<never, unknown>) => 'full' | 'redacted';
}

export interface PushScopedFilteredOptions<TTenant> extends PushScopedOptions<TTenant> {
    /**
     * Permission filter run against every socket connected to the event's
     * room. Sockets that return false (or reject) receive nothing. Look up
     * the connected user from `socket.data` and delegate to the host app's
     * permissions layer.
     */
    canReceive: (
        socket: RemoteSocket<never, unknown>,
        data: unknown,
        // Once-per-event value produced by `prepareEvent`; `undefined` when no
        // `prepareEvent` is supplied (a `canReceive` that ignores the 3rd
        // argument is unaffected).
        prepared?: unknown
    ) => boolean | Promise<boolean>;
    /**
     * Once-per-event hook (perf): for work that is a property of the EVENT,
     * not the recipient — e.g. resolving an entity's hierarchy scope. Without
     * it, a `canReceive` that does such a resolution repeats the SAME
     * tenant-scoped lookup for every one of the K connected sockets. When
     * supplied, the hook runs ONCE per event — inside the same `runInContext`
     * block that wraps `variants.render`/`mapData` — and its result is
     * threaded to every `canReceive` call as the 3rd argument. The return is
     * `await`ed by the fan-out loop, so `unknown` already covers a
     * Promise-returning hook.
     */
    prepareEvent?: (data: unknown) => unknown;
    /** Two-variant PII rendering; mutually exclusive with `mapData` (asserted at call time). */
    variants?: FanoutVariants;
    /**
     * Generalizes a request-scoped context (e.g. NestJS CLS) around the
     * once-per-event block and each per-recipient `canReceive` check, so
     * tenant-scoped reads inside those hooks hit the right tenant. Omit it
     * (or `tenantOf`) to run the hooks directly — transition-safe default.
     *
     * `ctx.tenant` may be `undefined` even when `runInContext` is supplied:
     * the once-per-event call always wraps `runOnce` whenever the hook is
     * present, seeded from `event.tenant` regardless of whether that's
     * defined, and `tenantOf` returning `undefined` for a given socket has
     * the same effect on the per-recipient call — the wrap keys on HOOK
     * PRESENCE, not on the tenant value being defined. Adapters that only
     * want to enter CLS when there's an actual tenant should check
     * `ctx.tenant !== undefined` and call `fn()` directly otherwise.
     */
    runInContext?: <R>(ctx: { tenant?: unknown }, fn: () => Promise<R>) => Promise<R>;
    /** Resolves a connected socket's tenant, for seeding its `canReceive` context via `runInContext`. */
    tenantOf?: (socket: RemoteSocket<never, unknown>) => unknown;
    /**
     * Scope exit: on a `changed` event that carries a pre-image
     * (`event.before`), send `events.removed` (the bare id) to every socket in
     * the event's room that could see the record before the write and cannot
     * see it after — `canReceive(before) && !canReceive(after)`. Without it, a
     * record that moves out of a socket's reach stays in that client's list
     * until a reload. Requires `events.removed` (asserted at call time).
     * See {@link EvictOnScopeExit}.
     */
    evictOnScopeExit?: EvictOnScopeExit;
}

/**
 * Options for `pushScopedFiltered`'s scope-exit eviction.
 *
 * Cost: the extra `canReceive(before)` check runs only when
 * `audienceKey(before) !== audienceKey(after)`, and only for sockets that
 * fail `canReceive(after)` — so a socket never receives both `changed` and
 * `removed` for one event, and an ordinary edit costs nothing extra.
 * `prepareEvent` (when supplied) runs once more for the pre-image, so the
 * pre-image is judged with its own prepared value.
 *
 * Errors in `audienceKey` or in `prepareEvent(before)` are logged; the event
 * is then delivered exactly as without this option. An update without a
 * pre-image skips eviction with a warning (the collection records no
 * pre-images); an `insert` never runs it.
 */
export interface EvictOnScopeExit {
    /**
     * Every field the audience depends on, folded into one string (e.g. the
     * property, building and scope ids). Called with the pre-image and with
     * the new document; equal keys mean the audience cannot have changed.
     */
    audienceKey: (record: Record<string, unknown>) => string;
}

/** What {@link pushMapped}'s selector asks the loop to emit for one change event. */
export interface MappedEmission {
    /** socket.io event name. */
    name: string;
    payload: unknown;
    /**
     * One or more rooms. Emitted in a SINGLE `server.to(rooms)` call, so a
     * socket belonging to several of them receives exactly one copy.
     */
    rooms: string | string[];
}

export interface PushMappedOptions {
    /** Signals a graceful shutdown in progress; suppresses the "watcher returned early" error log. */
    shutdown: ReadonlySignal<boolean>;
    logger?: Logger;
    /** Log-message prefix; identifies which pusher an operator-visible log line came from. */
    name?: string;
}
