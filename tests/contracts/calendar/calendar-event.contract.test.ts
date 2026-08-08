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
 * personal — never broadcast them to the org room." Every change (insert,
 * update, and — per the pusher's delete branch, which recovers `userId` from
 * the pre-image — a removal too) routes to `userRoom(doc.userId)` alone.
 * There is no org-room fan-out path at all: a bug that added one, or that
 * substituted the org room for the user room, must fail here.
 */
type CalendarEventDoc = { id: string; userId: string; organizationId: string };

const select = (event: ChangeEvent) => {
    if (event.type !== 'changed') return null;
    const doc = event.data as CalendarEventDoc;
    const operation = event.operation === 'insert' ? 'created' : 'updated';
    return { name: 'calendar:updated', payload: { eventId: doc.id, operation }, rooms: userRoom(doc.userId) };
};

const changed = (doc: CalendarEventDoc, operation: 'insert' | 'update'): ChangeEvent => ({
    type: 'changed',
    scopeId: doc.organizationId,
    data: doc,
    operation,
});

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
});
