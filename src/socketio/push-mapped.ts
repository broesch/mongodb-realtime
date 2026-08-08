import type { Server } from 'socket.io';
import type { ChangeEvent } from '../core/change-event.js';
import { consoleLogger, prefixed } from '../common/logger.js';
import { toRooms } from './rooms.js';
import type { MappedEmission, PushMappedOptions } from './types.js';

/**
 * General fan-out loop: the consumer decides, per event, what to emit and to
 * whom — or that nothing should be emitted at all.
 *
 * `pushScoped`/`pushOwnerScoped`/`pushScopedFiltered` cover the case where
 * one collection maps to one pair of event names addressed to one derived
 * room. This covers the rest: application-semantic event names chosen from
 * the document's own state, hand-built DTOs, and recipient sets that aren't a
 * single room. Returning `null` from `select` skips the event, which is how a
 * consumer expresses "this write isn't interesting" (e.g. an update that
 * didn't touch the field this pusher exists to announce — see `didChange`).
 *
 * Free function, no lifecycle: same contract as `pushScoped`. A `select` that
 * throws or rejects skips that one event and is logged; it never breaks the
 * loop, because one malformed document must not stop fan-out for every
 * subsequent change.
 *
 * SECURITY NOTE: this loop has NO per-socket permission gate — routing is by
 * room membership alone. Do not use it for entities where recipients inside
 * the same room differ in what they may see; that is `pushScopedFiltered`'s
 * job (per-socket `canReceive`, two-variant PII rendering).
 */
export async function pushMapped<TTenant>(
    server: Server,
    watcher: AsyncGenerator<ChangeEvent<TTenant>>,
    select: (event: ChangeEvent<TTenant>) => MappedEmission | null | Promise<MappedEmission | null>,
    options: PushMappedOptions
): Promise<void> {
    const log = prefixed(options.logger ?? consoleLogger, options.name ?? 'pushMapped');

    for await (const event of watcher) {
        let emission: MappedEmission | null;
        try {
            emission = await Promise.resolve(select(event));
        } catch (err) {
            log.error(
                `select failed for a ${event.type} event — skipping emit: ${(err as Error)?.message ?? String(err)}`
            );
            continue;
        }
        if (!emission) continue;

        const rooms = toRooms(emission.rooms);
        if (rooms.length === 0) {
            // Emitting with an empty room list would broadcast to every
            // connected socket — the opposite of what a caller that resolved
            // no recipients meant.
            log.warn(`Dropping ${emission.name} — select resolved no rooms to address`);
            continue;
        }
        server.to(rooms).emit(emission.name, emission.payload);
    }

    if (!options.shutdown.getValue()) {
        log.error(
            'Change-stream watcher returned before shutdown — gateway will no longer fan out events. Restart the service or check the watcher for an uncaught rejection.'
        );
    }
}
