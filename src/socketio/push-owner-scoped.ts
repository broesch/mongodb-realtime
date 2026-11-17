import type { Server } from 'socket.io';
import type { ChangeEvent } from '../core/change-event.js';
import { consoleLogger, prefixed } from '../common/logger.js';
import { namespacedRoom, userRoom } from './rooms.js';
import type { PushEvents, PushOwnerScopedOptions } from './types.js';

/**
 * Per-owner fan-out loop. Some entities are scoped to a single recipient,
 * not a whole tenant/organization — the owning user id is resolved via
 * `options.ownerIdOf(event.data)` for changed events, and with
 * `options.ownerIdOf(event.before)` — the pre-image's owner — for removed
 * events (0.4+). A removed event without a pre-image falls back to
 * `event.ownerId` (the conventions' owner field) and logs a warning. A missing owner
 * id is dropped with a warning rather than broadcast — the per-owner
 * analogue of `pushScoped` dropping a removed event missing its `scopeId`.
 *
 * Free function, no lifecycle: same contract as `pushScoped`.
 */
export async function pushOwnerScoped<TTenant>(
    server: Server,
    watcher: AsyncGenerator<ChangeEvent<TTenant>>,
    events: PushEvents,
    options: PushOwnerScopedOptions<TTenant>
): Promise<void> {
    const log = prefixed(options.logger ?? consoleLogger, options.name ?? 'pushOwnerScoped');
    const roomFor = (event: ChangeEvent<TTenant>, ownerId: string): string =>
        namespacedRoom(
            event.tenant !== undefined ? options.tenantNamespace?.(event.tenant) : undefined,
            userRoom(ownerId)
        );

    for await (const event of watcher) {
        if (event.type === 'removed') {
            if (!events.removed) continue;
            // The owner who could see the record until now: the gateway's own
            // resolver applied to the pre-image, so `removed` reaches the same
            // room the record's `changed` events did. Without a pre-image,
            // fall back to the conventions-derived `event.ownerId` and warn.
            let ownerId: string | null | undefined;
            if (event.before) {
                ownerId = options.ownerIdOf(event.before);
            } else {
                log.warn(
                    `No pre-image on ${events.removed} for id=${event.id} — falling back to event.ownerId; enable changeStreamPreAndPostImages on the collection`
                );
                ownerId = event.ownerId;
            }
            if (!ownerId) {
                // No owning user for the removed record. Drop rather than
                // broadcast: a per-owner delete must only ever reach its owner's room.
                log.warn(`Dropping ${events.removed} for id=${event.id} — no ownerId on the change event`);
                continue;
            }
            server.to(roomFor(event, ownerId)).emit(events.removed, event.id);
        } else {
            const ownerId = options.ownerIdOf(event.data);
            if (!ownerId) {
                log.warn(`Dropping ${events.changed} push — change event has no resolvable ownerId`);
                continue;
            }
            let payload: unknown;
            try {
                payload = options.mapData ? await options.mapData(event.data) : event.data;
            } catch (err) {
                log.error(
                    `mapData failed for ${events.changed} event — skipping emit: ${(err as Error)?.message ?? String(err)}`
                );
                continue;
            }
            server.to(roomFor(event, ownerId)).emit(events.changed, payload);
        }
    }
    if (!options.shutdown.getValue()) {
        log.error(
            'Change-stream watcher returned before shutdown — gateway will no longer fan out events. Restart the service or check the watcher for an uncaught rejection.'
        );
    }
}
