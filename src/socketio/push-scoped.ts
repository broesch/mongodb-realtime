import type { Server } from 'socket.io';
import type { ChangeEvent } from '../core/change-event.js';
import { consoleLogger, prefixed } from '../common/logger.js';
import { namespacedRoom, scopeRoom } from './rooms.js';
import type { PushEvents, PushScopedOptions } from './types.js';

/**
 * Standard scope-fan-out loop: reads canonical `ChangeEvent`s from a watcher
 * and emits them to the scope's socket.io room (never globally), so deletes
 * and updates stay confined to the scope they belong to.
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
            server.to(roomOf(event)).emit(events.removed, event.id);
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
            server.to(roomOf(event)).emit(events.changed, payload);
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
