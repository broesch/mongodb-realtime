import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import type { Server } from 'socket.io';
import { pushOwnerScoped } from '../../../src/socketio/push-owner-scoped.js';
import { userRoom } from '../../../src/socketio/rooms.js';
import type { ChangeEvent } from '../../../src/core/change-event.js';
import type { Logger } from '../../../src/common/logger.js';
import { ValueSignal } from '../../../src/common/signal.js';
import {
    startHarness,
    stopHarness,
    connectClient,
    joinRoom,
    receivedEvents,
    waitForCount,
    delay,
} from '../../socketio/harness.js';

/**
 * CONTRACT: calendar's notification pusher (apps/api/src/notification/notification.pusher.ts).
 * Uses `pushOwnerScoped`, not `pushMapped` — a notification has exactly one
 * recipient, resolved from `ownerIdOf`/`event.ownerId`, never a room list.
 *
 * NOTE ON SCOPE: the live pusher today only reacts to `insert` — per its own
 * comment, "Only inserts matter: the sole update is `read: true`, which the
 * client originated and has already applied optimistically" — so it never
 * routes a `removed` event at all. The two `removed` cases below pin
 * `pushOwnerScoped`'s general per-owner-delete contract (the same guarantee
 * `booking`/other owner-scoped consumers rely on): a removal with an
 * `ownerId` reaches only that owner's room, and a removal missing one is
 * dropped with a warning rather than broadcast. If notification delivery
 * ever grows a delete path, this is the behavior it inherits for free.
 */
type NotificationDoc = { id: string; userId: string; type: string; title: string; body: string; read?: boolean };

const ownerIdOf = (data: unknown): string | null | undefined => (data as NotificationDoc | undefined)?.userId;

const mapData = (data: unknown): unknown => {
    const doc = data as NotificationDoc;
    return { id: doc.id, type: doc.type, title: doc.title, body: doc.body, read: doc.read ?? false };
};

async function* gen(events: ChangeEvent[]): AsyncGenerator<ChangeEvent> {
    for (const event of events) yield event;
}

function mockLogger(): { logger: Logger; calls: { level: string; message: string }[] } {
    const calls: { level: string; message: string }[] = [];
    return {
        calls,
        logger: {
            debug: message => calls.push({ level: 'debug', message }),
            log: message => calls.push({ level: 'log', message }),
            warn: message => calls.push({ level: 'warn', message }),
            error: message => calls.push({ level: 'error', message }),
        },
    };
}

let server: Server;
let port: number;
beforeEach(async () => ({ server, port } = await startHarness()));
afterEach(async () => stopHarness());

describe('calendar contract: notification pusher composition', () => {
    it('an insert (the only change the live pusher reacts to) is delivered to the owner room only', async () => {
        const owner = await connectClient(port);
        const other = await connectClient(port);
        await joinRoom(owner, userRoom('u1'));
        await joinRoom(other, userRoom('u2'));
        const ownerEvents = receivedEvents(owner, 'notification:new');
        const otherEvents = receivedEvents(other, 'notification:new');

        const doc: NotificationDoc = {
            id: 'n1',
            userId: 'u1',
            type: 'booking_created',
            title: 'New booking',
            body: 'x',
        };
        const event: ChangeEvent = { type: 'changed', scopeId: 'org1', data: doc, operation: 'insert' };

        await pushOwnerScoped(
            server,
            gen([event]),
            { changed: 'notification:new' },
            {
                shutdown: new ValueSignal(true),
                ownerIdOf,
                mapData,
            }
        );

        await waitForCount(ownerEvents, 1);
        expect(ownerEvents).toEqual([
            { id: 'n1', type: 'booking_created', title: 'New booking', body: 'x', read: false },
        ]);

        await delay(200);
        expect(otherEvents).toHaveLength(0);
    });

    it('a removed event with an ownerId emits the bare id to that owner room, general pushOwnerScoped contract', async () => {
        const owner = await connectClient(port);
        await joinRoom(owner, userRoom('u1'));
        const removedEvents = receivedEvents(owner, 'notification:removed');

        const event: ChangeEvent = { type: 'removed', id: 'n1', ownerId: 'u1', via: 'delete' };
        await pushOwnerScoped(
            server,
            gen([event]),
            { changed: 'notification:new', removed: 'notification:removed' },
            { shutdown: new ValueSignal(true), ownerIdOf }
        );

        await waitForCount(removedEvents, 1);
        expect(removedEvents).toEqual(['n1']);
    });

    it('a removed event without an ownerId is dropped and never broadcast — logged as a warning', async () => {
        const owner = await connectClient(port);
        await joinRoom(owner, userRoom('u1'));
        const removedEvents = receivedEvents(owner, 'notification:removed');
        const { logger, calls } = mockLogger();

        const event: ChangeEvent = { type: 'removed', id: 'n1', via: 'delete' };
        await pushOwnerScoped(
            server,
            gen([event]),
            { changed: 'notification:new', removed: 'notification:removed' },
            { shutdown: new ValueSignal(true), ownerIdOf, logger }
        );

        await delay(200);
        expect(removedEvents).toHaveLength(0);
        expect(calls.some(c => c.level === 'warn' && c.message.includes('ownerId'))).toBe(true);
    });
});
