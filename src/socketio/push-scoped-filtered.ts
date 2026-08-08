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
 * two-variant pair. The `removed` (bare-id) path is unaffected.
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

    for await (const event of watcher) {
        if (event.type === 'removed') {
            if (!events.removed) continue;
            if (!event.scopeId) {
                // Pre/post images weren't available for this delete (collMod
                // failed or unsupported). Best effort: don't leak across
                // scopes — drop the notification rather than broadcast globally.
                log.warn(`Dropping ${events.removed} for id=${event.id} — no scopeId on the change event`);
                continue;
            }
            // Removed events carry no document, so the per-recipient
            // `canReceive` filter below cannot run: it derives its decision
            // from the document's fields, which are absent on a delete.
            // Emit the bare id to the whole room instead — exactly like
            // `pushScoped`'s removed path: an id leaks no entity data, and
            // any client holding the entity drops it.
            const rooms = roomsFor(event, events.removed);
            if (!rooms) continue;
            server.to(rooms).emit(events.removed, event.id);
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
            const runOnce = async (): Promise<void> => {
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
                if (options.prepareEvent) {
                    prepared = await Promise.resolve(options.prepareEvent(event.data));
                }
            };
            if (options.runInContext) {
                await options.runInContext({ tenant: event.tenant }, runOnce);
            } else {
                await runOnce();
            }
        } catch (err) {
            log.error(
                `mapData failed for ${events.changed} event — skipping emit: ${(err as Error)?.message ?? String(err)}`
            );
            continue;
        }
        const eventData = event.data;

        await Promise.all(
            sockets.map(async socket => {
                try {
                    // socket.io's `fetchSockets` returns `RemoteSocket`, a
                    // subset of `Socket` — the hooks only need `socket.data`
                    // and `socket.id`. Cast at the boundary rather than
                    // threading the concrete `Server`'s generic types through
                    // every option.
                    const remote = socket as unknown as RemoteSocket<never, unknown>;
                    const check = (): Promise<boolean> =>
                        Promise.resolve(options.canReceive(remote, eventData, prepared));
                    const tenant = options.tenantOf?.(remote);
                    const allowed =
                        options.runInContext && options.tenantOf
                            ? await options.runInContext({ tenant }, check)
                            : await check();
                    if (allowed) {
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
                    }
                } catch (err) {
                    // Permission-check failures must never block the fan-out
                    // loop. Log and skip this recipient.
                    log.warn(
                        `canReceive check failed for socket ${socket.id}: ${(err as Error)?.message ?? String(err)}`
                    );
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
