import type { Server } from 'socket.io';
import type { ChangeEvent } from '../core/change-event.js';
import { consoleLogger, prefixed } from '../common/logger.js';
import { namespacedRoom, scopeRoom, toRooms } from './rooms.js';
import type { PushEvents, PushScopedOptions } from './types.js';

/**
 * Standard scope-fan-out loop: reads canonical `ChangeEvent`s from a watcher
 * and emits them to the scope's socket.io room (never globally), so deletes
 * and updates stay confined to the scope they belong to.
 *
 * A removal goes to the room(s) of its scope, which for a hard delete comes
 * from the pre-image. `roomOf` receives the event with `event.before`, so a
 * custom `roomOf` can address the rooms of whoever could see the record until
 * now. A removal without a pre-image is still emitted and logs a warning.
 * For a per-socket audience on removals, use `pushScopedFiltered`.
 *
 * This is a free function, not a class method — it owns no lifecycle. The
 * caller (a later NestJS base class) is responsible for latching shutdown
 * and awaiting this promise draining; this function just returns once the
 * watcher completes.
 */
export async function pushScoped<TTenant>(
    server: Server,
    watcher: AsyncGenerator<ChangeEvent<TTenant>>,
    events: PushEvents,
    options: PushScopedOptions<TTenant>
): Promise<void> {
    const log = prefixed(options.logger ?? consoleLogger, options.name ?? 'pushScoped');
    const roomOf =
        options.roomOf ??
        ((event: ChangeEvent<TTenant>): string =>
            namespacedRoom(
                event.tenant !== undefined ? options.tenantNamespace?.(event.tenant) : undefined,
                // Only reached once `scopeId` has already been confirmed present at
                // the call sites below — the `?? ''` fallback is unreachable in
                // practice and exists only to satisfy the union's optional field.
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
            if (!event.before) {
                // A watched collection always records pre-images
                // (`watchCollection` enables them), so their absence is a
                // deployment bug worth surfacing. The emit itself is the same
                // either way: this loop has no per-socket filter.
                log.warn(
                    `No pre-image on ${events.removed} for id=${event.id} — enable changeStreamPreAndPostImages on the collection`
                );
            }
            const rooms = roomsFor(event, events.removed);
            if (!rooms) continue;
            server.to(rooms).emit(events.removed, event.id);
        } else {
            let payload: unknown;
            try {
                payload = options.mapData ? await options.mapData(event.data) : event.data;
            } catch (err) {
                log.error(
                    `mapData failed for ${events.changed} event — skipping emit: ${(err as Error)?.message ?? String(err)}`
                );
                continue;
            }
            const rooms = roomsFor(event, events.changed);
            if (!rooms) continue;
            server.to(rooms).emit(events.changed, payload);
        }
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
