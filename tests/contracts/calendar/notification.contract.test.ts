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
 * recipient, resolved from `ownerIdOf`, and the mapped payload is the full
 * `toNotificationPayload` shape (id, type, title, body, metadata, read,
 * createdAt), not a partial DTO.
 *
 * SCOPE: the live pusher today only reacts to `insert` — per its own
 * comment, "Only inserts matter: the sole update is `read: true`, which the
 * client originated and has already applied optimistically." It never
 * watches for deletes at all. The BINDING block below covers exactly that
 * insert path; the `removed`-event behavior of `pushOwnerScoped` is real and
 * useful, but calendar does not exercise it through this pusher today, so it
 * lives in the separate forward-looking block below instead of here.
 */
type NotificationDoc = {
    id: string;
    userId: string;
    type: string;
    title: string;
    body: string;
    metadata?: Record<string, unknown>;
    read?: boolean;
    createdAt?: string;
};

const ownerIdOf = (data: unknown): string | null | undefined => (data as NotificationDoc | undefined)?.userId;

// Mirrors notification.mapper.ts's toNotificationPayload exactly.
const mapData = (data: unknown): unknown => {
    const doc = data as NotificationDoc;
    return {
        id: doc.id,
        type: doc.type,
        title: doc.title,
        body: doc.body,
        metadata: doc.metadata,
        read: doc.read ?? false,
        createdAt: new Date(doc.createdAt ?? 0).toISOString(),
    };
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
    it('an insert (the only change the live pusher reacts to) is delivered to the owner room with the full payload', async () => {
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
            metadata: { bookingId: 'b1' },
            createdAt: '2026-08-09T00:00:00.000Z',
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
            {
                id: 'n1',
                type: 'booking_created',
                title: 'New booking',
                body: 'x',
                metadata: { bookingId: 'b1' },
                read: false,
                createdAt: '2026-08-09T00:00:00.000Z',
            },
        ]);

        await delay(200);
        expect(otherEvents).toHaveLength(0);
    });
});

// Exempt from this suite's coordination rule (see ./README.md): these pin
// `pushOwnerScoped`'s general per-owner-delete contract, the same guarantee
// other owner-scoped consumers rely on — but calendar's live
// NotificationPusher never reaches this branch (it only handles `insert`,
// see the SCOPE note above). A red test here means the shared primitive
// changed, not that calendar broke; it does not by itself require
// coordinating with the calendar repo. Kept as forward-looking coverage for
// if/when notification delivery grows a delete path.
describe('forward-looking (not yet used by calendar — non-binding)', () => {
    it('a removed event with an ownerId emits the bare id to that owner room', async () => {
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
