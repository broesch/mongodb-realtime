import type { RemoteSocket, Server } from 'socket.io';
import type { ChangeEvent } from '../core/change-event.js';
import { consoleLogger, prefixed } from '../common/logger.js';
import { namespacedRoom, scopeRoom, toRooms } from './rooms.js';
import type { PushEvents, PushScopedFilteredOptions } from './types.js';

/**
 * Permission-filtered scope fan-out. Like `pushScoped`, but instead of
 * blasting every connected socket in the scope room, it fetches the room's
 * sockets and runs the supplied `canReceive` filter against each one.
 * Sockets that return false are skipped.
 *
 * Use this when the event carries entity data that the recipient might not
 * be allowed to view (a record scoped to a sub-resource the socket's user
 * isn't permitted to see, etc). The filter function is called with
 * `(socket, eventData, prepared)`; it should look up the connected user from
 * `socket.data` and delegate to the host app's permissions layer.
 *
 * Per-event overhead: O(connected_sockets_in_room) permission checks. A
 * `canReceive` backed by a short-TTL per-user cache keeps the cost amortized
 * in the common case where the same user receives many events in a row.
 *
 * `prepareEvent` (optional, see `PushScopedFilteredOptions`): a once-per-event
 * hook for work that is a property of the EVENT, not the recipient. When
 * supplied, it runs ONCE per event — inside the same `runInContext` block
 * that wraps the once-per-event render — and its result is threaded to every
 * `canReceive` call as the 3rd argument. The per-socket `canReceive` still
 * runs per recipient, because different users have different permissions;
 * only the shared resolution is hoisted.
 *
 * `runInContext`/`tenantOf` (optional): generalizes a request-scoped context
 * (e.g. NestJS CLS) that tenant-scoped work inside `canReceive`/`mapData`/
 * `variants.render`/`prepareEvent` needs to resolve the right tenant. The
 * fan-out loop itself has no such context, so without these hooks a
 * `canReceive` that calls into tenant-scoped services would resolve against
 * whatever the DEFAULT context is. When `runInContext` is supplied, the
 * once-per-event block runs inside a context seeded from the EVENT's tenant,
 * and (when `tenantOf` is ALSO supplied) each recipient's `canReceive` runs
 * inside a context seeded from that socket's own tenant. Transition-safe: no
 * `runInContext` (or no `tenantOf`) → the corresponding hooks run directly,
 * exactly as before.
 *
 * Two-variant fan-out (PII gating, see `FanoutVariants`). When `variants` is
 * supplied, the changed doc is rendered ONCE into `{ full, redacted }`
 * (replacing the single `mapData` payload path) and each recipient that
 * PASSES `canReceive` receives whichever variant `variants.pick(socket)`
 * selects. `canReceive` still runs FIRST and unchanged — a socket that fails
 * it gets nothing, regardless of variant. MUTUALLY EXCLUSIVE with `mapData`
 * (asserted below): a caller renders EITHER one payload for everyone OR the
 * two-variant pair. The `removed` payload is always the bare id.
 *
 * Removals (0.4+). A `removed` event is delivered per socket too: `canReceive`
 * judges the PRE-IMAGE (`event.before`) — who could see the record until now —
 * inside the socket's tenant context, with `prepareEvent(before)` as its 3rd
 * argument. An event without a pre-image (the collection records none, a
 * deployment bug) falls back to the pre-0.4 behaviour, the bare id to the
 * whole room, and logs a warning. `canReceive` must therefore accept a
 * pre-image as well as a post-image; both are full documents.
 *
 * Scope exit (`evictOnScopeExit`, 0.4+): a socket that could see the record
 * before an update and cannot after receives `removed` with the bare id. See
 * `EvictOnScopeExit`.
 *
 * Free function, no lifecycle: same contract as `pushScoped`.
 */
export async function pushScopedFiltered<TTenant>(
    server: Server,
    watcher: AsyncGenerator<ChangeEvent<TTenant>>,
    events: PushEvents,
    options: PushScopedFilteredOptions<TTenant>
): Promise<void> {
    if (options.mapData && options.variants) {
        // Programming error at wiring time, not a per-event condition — throw
        // so it surfaces immediately in tests / on boot rather than silently
        // preferring one path. Checked before the watcher is ever touched.
        throw new Error(
            'pushScopedFiltered: `mapData` and `variants` are mutually exclusive — pass one or the other, not both'
        );
    }
    if (options.evictOnScopeExit && !events.removed) {
        throw new Error(
            'pushScopedFiltered: `evictOnScopeExit` needs `events.removed` — the event a client drops a row on'
        );
    }
    const log = prefixed(options.logger ?? consoleLogger, options.name ?? 'pushScopedFiltered');
    const roomOf =
        options.roomOf ??
        ((event: ChangeEvent<TTenant>): string =>
            namespacedRoom(
                event.tenant !== undefined ? options.tenantNamespace?.(event.tenant) : undefined,
                scopeRoom(event.scopeId ?? '')
            ));
    const roomsFor = (event: ChangeEvent<TTenant>, eventName: string): string[] | null => {
        const rooms = toRooms(roomOf(event));
        if (rooms.length === 0) {
            log.warn(`Dropping ${eventName} — roomOf resolved no rooms to address`);
            return null;
        }
        return rooms;
    };

    // The per-recipient context: `canReceive` runs inside a context seeded
    // from the SOCKET's tenant when both hooks are supplied, else directly.
    const inSocketContext = <R>(remote: RemoteSocket<never, unknown>, fn: () => Promise<R>): Promise<R> =>
        options.runInContext && options.tenantOf
            ? options.runInContext({ tenant: options.tenantOf(remote) }, fn)
            : fn();
    // The once-per-event context, seeded from the EVENT's tenant.
    const inEventContext = <R>(event: ChangeEvent<TTenant>, fn: () => Promise<R>): Promise<R> =>
        options.runInContext ? options.runInContext({ tenant: event.tenant }, fn) : fn();
    const prepare = async (data: unknown): Promise<unknown> =>
        options.prepareEvent ? await Promise.resolve(options.prepareEvent(data)) : undefined;
    const errorText = (err: unknown): string => (err as Error)?.message ?? String(err);
    // socket.io's `fetchSockets` returns `RemoteSocket`, a subset of `Socket`
    // — the hooks only need `socket.data` and `socket.id`. Cast at the
    // boundary rather than threading the concrete `Server`'s generic types
    // through every option.
    const asRemote = (socket: unknown): RemoteSocket<never, unknown> => socket as RemoteSocket<never, unknown>;

    /**
     * A removal reaches the sockets that could see the record until now:
     * `canReceive` judges the PRE-IMAGE (`event.before`). The payload is the
     * bare id, whatever `mapData`/`variants` say. Without a pre-image (the
     * collection records none — a deployment bug) the bare id goes to the
     * whole room, as before 0.4, with a warning.
     */
    const pushRemoved = async (event: Extract<ChangeEvent<TTenant>, { type: 'removed' }>, removed: string) => {
        if (!event.scopeId) {
            // Pre/post images weren't available for this delete (collMod
            // failed or unsupported). Best effort: don't leak across
            // scopes — drop the notification rather than broadcast globally.
            log.warn(`Dropping ${removed} for id=${event.id} — no scopeId on the change event`);
            return;
        }
        const rooms = roomsFor(event, removed);
        if (!rooms) return;
        const before = event.before;
        if (!before) {
            log.warn(
                `No pre-image on ${removed} for id=${event.id} — emitting the bare id to the whole room. ` +
                    'Enable changeStreamPreAndPostImages on the collection so removals reach only sockets that could see the record.'
            );
            server.to(rooms).emit(removed, event.id);
            return;
        }
        let prepared: unknown;
        try {
            prepared = await inEventContext(event, () => prepare(before));
        } catch (err) {
            // Fail closed, like a failing `prepareEvent` on a changed event.
            log.error(`prepareEvent failed for ${removed} id=${event.id} — skipping emit: ${errorText(err)}`);
            return;
        }
        const sockets = await server.in(rooms).fetchSockets();
        await Promise.all(
            sockets.map(async socket => {
                const remote = asRemote(socket);
                try {
                    const sawIt = await inSocketContext(remote, () =>
                        Promise.resolve(options.canReceive(remote, before, prepared))
                    );
                    if (sawIt) socket.emit(removed, event.id);
                } catch (err) {
                    log.warn(`canReceive check failed for socket ${socket.id} (${removed}): ${errorText(err)}`);
                }
            })
        );
    };

    /**
     * Resolves the scope-exit pass for a changed event: `undefined` when the
     * option is off, the event is an insert, there is no pre-image, the
     * audience key did not change, or resolving it failed (logged). Never
     * throws — the changed event goes out regardless.
     */
    const scopeExitFor = async (
        event: Extract<ChangeEvent<TTenant>, { type: 'changed' }>
    ): Promise<{ before: Record<string, unknown>; prepared: unknown; id: string } | undefined> => {
        const spec = options.evictOnScopeExit;
        if (!spec || event.operation === 'insert') return undefined;
        const before = event.before;
        if (!before) {
            log.warn(
                `No pre-image on ${events.changed} (${event.operation}) — scope-exit eviction skipped. ` +
                    'Enable changeStreamPreAndPostImages on the collection.'
            );
            return undefined;
        }
        try {
            const after = event.data as Record<string, unknown>;
            if (spec.audienceKey(before) === spec.audienceKey(after)) return undefined;
            const id = (after._id as { toString(): string } | null | undefined)?.toString();
            if (!id) {
                log.warn(`scope-exit eviction skipped for ${events.changed}: the document has no _id`);
                return undefined;
            }
            // The pre-image is judged with ITS OWN prepared value — the
            // post-image's (e.g. a resolved hierarchy scope) would be wrong.
            const prepared = await inEventContext(event, () => prepare(before));
            return { before, prepared, id };
        } catch (err) {
            log.warn(`scope-exit eviction skipped for ${events.changed}: ${errorText(err)}`);
            return undefined;
        }
    };

    for await (const event of watcher) {
        if (event.type === 'removed') {
            if (events.removed) await pushRemoved(event, events.removed);
            continue;
        }

        const rooms = roomsFor(event, events.changed);
        if (!rooms) continue;
        const sockets = await server.in(rooms).fetchSockets();

        // `mapData`/`variants.render` and `prepareEvent` may be async AND
        // tenant-scoped. The fan-out loop has no ambient context of its own,
        // so — when supplied — seed one from the event's tenant for the
        // duration of this once-per-event block. Both run ONCE per event in
        // the SAME block. Transition-safe: no `runInContext` → they run
        // directly. `await` is a no-op for synchronous hooks.
        let payload: unknown;
        let prepared: unknown;
        // Two-variant render (mutually exclusive with mapData, asserted
        // above). Rendered ONCE per event inside the same context block;
        // each recipient later picks full-or-redacted from this single pair.
        let rendered: { full: unknown; redacted: unknown } | undefined;
        try {
            await inEventContext(event, async () => {
                if (options.variants) {
                    // Render both variants once. `payload` stays unused on
                    // this path; the per-socket loop reads `rendered` via
                    // `variants.pick`.
                    rendered = await Promise.resolve(options.variants.render(event.data));
                } else {
                    payload = await Promise.resolve(options.mapData ? options.mapData(event.data) : event.data);
                }
                // Resolve the once-per-event value inside the same context.
                // Left `undefined` when no `prepareEvent` is supplied so
                // existing `canReceive` callbacks (which ignore the 3rd arg)
                // are unaffected.
                prepared = await prepare(event.data);
            });
        } catch (err) {
            log.error(`mapData failed for ${events.changed} event — skipping emit: ${errorText(err)}`);
            continue;
        }
        const eventData = event.data;
        const exit = await scopeExitFor(event);

        await Promise.all(
            sockets.map(async socket => {
                const remote = asRemote(socket);
                try {
                    // One socket context for both checks. A socket that may
                    // see the new state gets `changed`; only one that may NOT
                    // is asked about the old state — so it never gets both.
                    const outcome = await inSocketContext(remote, async () => {
                        if (await Promise.resolve(options.canReceive(remote, eventData, prepared))) return 'changed';
                        if (exit && (await Promise.resolve(options.canReceive(remote, exit.before, exit.prepared)))) {
                            return 'removed';
                        }
                        return undefined;
                    });
                    if (outcome === 'changed') {
                        // Two-variant path: the gate passed, now pick
                        // full-vs-redacted from the single pre-rendered pair
                        // (synchronous room test, no async work in the hot
                        // loop). Otherwise emit the single payload exactly as
                        // before. `pick` can ONLY choose between the two
                        // pre-rendered values (`FanoutVariants`), never a
                        // bespoke one.
                        if (rendered) {
                            const which = options.variants!.pick(remote);
                            socket.emit(events.changed, which === 'full' ? rendered.full : rendered.redacted);
                        } else {
                            socket.emit(events.changed, payload);
                        }
                    } else if (outcome === 'removed') {
                        // `scopeExitFor` only resolves when `events.removed` is set (asserted at call time).
                        socket.emit(events.removed!, exit!.id);
                    }
                } catch (err) {
                    // Permission-check failures must never block the fan-out
                    // loop. Log and skip this recipient.
                    log.warn(`canReceive check failed for socket ${socket.id}: ${errorText(err)}`);
                }
            })
        );
    }
    // If the watcher completes without `shutdown` being set, that's a bug in
    // the caller's reconnect logic (the inner change-stream loop is supposed
    // to keep itself alive). Don't silently stop fanning out — log so the
    // operator can see the pusher stopped.
    if (!options.shutdown.getValue()) {
        log.error(
            'Change-stream watcher returned before shutdown — gateway will no longer fan out events. Restart the service or check the watcher for an uncaught rejection.'
        );
    }
}
