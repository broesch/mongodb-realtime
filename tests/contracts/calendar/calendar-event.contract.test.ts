import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import type { Server } from 'socket.io';
import { pushMapped } from '../../../src/socketio/push-mapped.js';
import { scopeRoom, userRoom } from '../../../src/socketio/rooms.js';
import type { ChangeEvent } from '../../../src/core/change-event.js';
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
 * CONTRACT: calendar's calendar-event pusher (apps/api/src/calendar/calendar-event.pusher.ts).
 * Straight from the source comment on its handler: "Calendar events are
 * personal — never broadcast them to the org room." Insert, update, AND
 * replace all route to `userRoom(doc.userId)` alone (the pusher's switch
 * handles `update`/`replace` identically). A delete carries no
 * `fullDocument`, so the pusher recovers `userId` from the pre-image
 * (`fullDocumentBeforeChange`) — the very reason this collection needs
 * pre-images enabled at all; when that recovery fails (`!userId`), the
 * pusher's own branch is "skip with a warn", not "broadcast org-wide".
 * There is no org-room fan-out path at all: a bug that added one, or that
 * substituted the org room for the user room, must fail here.
 */
type CalendarEventDoc = { id: string; userId: string; organizationId: string };

const select = (event: ChangeEvent) => {
    if (event.type === 'removed') {
        // No pre-image ownerId — mirrors the pusher's `!userId` warn-and-skip,
        // never a broadcast.
        if (!event.ownerId) return null;
        return {
            name: 'calendar:updated',
            payload: { eventId: event.id, operation: 'deleted' },
            rooms: userRoom(event.ownerId),
        };
    }
    const doc = event.data as CalendarEventDoc;
    const operation = event.operation === 'insert' ? 'created' : 'updated'; // update AND replace both land here
    return { name: 'calendar:updated', payload: { eventId: doc.id, operation }, rooms: userRoom(doc.userId) };
};

const changed = (doc: CalendarEventDoc, operation: 'insert' | 'update' | 'replace'): ChangeEvent => ({
    type: 'changed',
    scopeId: doc.organizationId,
    data: doc,
    operation,
});

const removed = (id: string, ownerId?: string): ChangeEvent => ({ type: 'removed', id, ownerId, via: 'delete' });

async function* gen(events: ChangeEvent[]): AsyncGenerator<ChangeEvent> {
    for (const event of events) yield event;
}

let server: Server;
let port: number;
beforeEach(async () => ({ server, port } = await startHarness()));
afterEach(async () => stopHarness());

describe('calendar contract: calendar-event pusher composition', () => {
    const doc: CalendarEventDoc = { id: 'e1', userId: 'u1', organizationId: 'org1' };

    it('an insert reaches the owner user room; a socket in the org room receives nothing', async () => {
        const owner = await connectClient(port);
        const orgMember = await connectClient(port);
        await joinRoom(owner, userRoom('u1'));
        await joinRoom(orgMember, scopeRoom('org1'));
        const ownerEvents = receivedEvents(owner, 'calendar:updated');
        const orgEvents = receivedEvents(orgMember, 'calendar:updated');

        await pushMapped(server, gen([changed(doc, 'insert')]), select, { shutdown: new ValueSignal(true) });

        await waitForCount(ownerEvents, 1);
        expect(ownerEvents).toEqual([{ eventId: 'e1', operation: 'created' }]);

        await delay(200);
        expect(orgEvents).toHaveLength(0);
    });

    it('an update routes the same way as an insert — still user-room only', async () => {
        const owner = await connectClient(port);
        const orgMember = await connectClient(port);
        await joinRoom(owner, userRoom('u1'));
        await joinRoom(orgMember, scopeRoom('org1'));
        const ownerEvents = receivedEvents(owner, 'calendar:updated');
        const orgEvents = receivedEvents(orgMember, 'calendar:updated');

        await pushMapped(server, gen([changed(doc, 'update')]), select, { shutdown: new ValueSignal(true) });

        await waitForCount(ownerEvents, 1);
        expect(ownerEvents).toEqual([{ eventId: 'e1', operation: 'updated' }]);

        await delay(200);
        expect(orgEvents).toHaveLength(0);
    });

    it('a replace routes the same way too — the pusher’s switch treats update and replace identically', async () => {
        const owner = await connectClient(port);
        const ownerEvents = receivedEvents(owner, 'calendar:updated');
        await joinRoom(owner, userRoom('u1'));

        await pushMapped(server, gen([changed(doc, 'replace')]), select, { shutdown: new ValueSignal(true) });

        await waitForCount(ownerEvents, 1);
        expect(ownerEvents).toEqual([{ eventId: 'e1', operation: 'updated' }]);
    });

    it('a delete recovers the owner from the pre-image ownerId and routes only there; the org room gets nothing', async () => {
        const owner = await connectClient(port);
        const orgMember = await connectClient(port);
        await joinRoom(owner, userRoom('u1'));
        await joinRoom(orgMember, scopeRoom('org1'));
        const ownerEvents = receivedEvents(owner, 'calendar:updated');
        const orgEvents = receivedEvents(orgMember, 'calendar:updated');

        await pushMapped(server, gen([removed('e1', 'u1')]), select, { shutdown: new ValueSignal(true) });

        await waitForCount(ownerEvents, 1);
        expect(ownerEvents).toEqual([{ eventId: 'e1', operation: 'deleted' }]);

        await delay(200);
        expect(orgEvents).toHaveLength(0);
    });

    it('a delete without a pre-image ownerId is dropped, mirroring the pusher’s missing-userId warn-and-skip', async () => {
        const owner = await connectClient(port);
        await joinRoom(owner, userRoom('u1'));
        const ownerEvents = receivedEvents(owner, 'calendar:updated');

        await pushMapped(server, gen([removed('e1')]), select, { shutdown: new ValueSignal(true) });

        await delay(300);
        expect(ownerEvents).toHaveLength(0);
    });
});
