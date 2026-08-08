import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import type { Server } from 'socket.io';
import { pushMapped } from '../../../src/socketio/push-mapped.js';
import { scopeRoom, userRoom } from '../../../src/socketio/rooms.js';
import { didChange, type ChangeEvent } from '../../../src/core/change-event.js';
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
 * CONTRACT: calendar's booking pusher (apps/api/src/booking/booking.pusher.ts).
 * A booking insert announces 'booking:created'; an update announces
 * 'booking:cancelled' only when the write actually touched `status` and the
 * document now says cancelled; recipients are every participant's user room
 * PLUS the org room, with a socket in several of those rooms receiving
 * exactly one copy.
 */
type BookingDoc = { id: string; status: string; participants: string[]; organizationId: string };

const select = (event: ChangeEvent) => {
    if (event.type !== 'changed') return null;
    const doc = event.data as BookingDoc;
    const rooms = [...doc.participants.map(userRoom), scopeRoom(doc.organizationId)];
    if (event.operation === 'insert') return { name: 'booking:created', payload: { id: doc.id }, rooms };
    if (didChange(event, 'status') && doc.status === 'cancelled')
        return { name: 'booking:cancelled', payload: { id: doc.id }, rooms };
    return null; // an update that didn't flip status is not announced
};

const changed = (
    doc: BookingDoc,
    operation: 'insert' | 'update' | 'replace',
    updatedFields?: string[]
): ChangeEvent => ({ type: 'changed', scopeId: doc.organizationId, data: doc, operation, updatedFields });

async function* gen(events: ChangeEvent[]): AsyncGenerator<ChangeEvent> {
    for (const event of events) yield event;
}

let server: Server;
let port: number;
beforeEach(async () => ({ server, port } = await startHarness()));
afterEach(async () => stopHarness());

describe('calendar contract: booking pusher composition', () => {
    const doc: BookingDoc = { id: 'b1', status: 'cancelled', participants: ['u1', 'u2'], organizationId: 'org1' };

    it('a participant who is also in the org room receives exactly one copy', async () => {
        const participant = await connectClient(port);
        await joinRoom(participant, userRoom('u1'));
        await joinRoom(participant, scopeRoom('org1'));
        const received = receivedEvents(participant, 'booking:cancelled');

        await pushMapped(server, gen([changed(doc, 'update', ['status'])]), select, {
            shutdown: new ValueSignal(true),
        });

        await waitForCount(received, 1);
        await delay(200);
        expect(received).toEqual([{ id: 'b1' }]);
    });

    it('an org member who is not a participant still receives the announcement', async () => {
        const orgMember = await connectClient(port);
        await joinRoom(orgMember, scopeRoom('org1'));
        const received = receivedEvents(orgMember, 'booking:created');

        await pushMapped(server, gen([changed(doc, 'insert')]), select, { shutdown: new ValueSignal(true) });

        await waitForCount(received, 1);
        expect(received).toEqual([{ id: 'b1' }]);
    });

    it('an update that did not touch status is silent, even though the doc says cancelled', async () => {
        const orgMember = await connectClient(port);
        await joinRoom(orgMember, scopeRoom('org1'));
        const received = receivedEvents(orgMember, 'booking:cancelled');

        await pushMapped(server, gen([changed(doc, 'update', ['externalEventIds'])]), select, {
            shutdown: new ValueSignal(true),
        });

        await delay(300);
        expect(received).toHaveLength(0);
    });

    it('a replace that leaves the doc cancelled IS announced (replace counts as writing every field)', async () => {
        const orgMember = await connectClient(port);
        await joinRoom(orgMember, scopeRoom('org1'));
        const received = receivedEvents(orgMember, 'booking:cancelled');

        await pushMapped(server, gen([changed(doc, 'replace')]), select, { shutdown: new ValueSignal(true) });

        await waitForCount(received, 1);
        expect(received).toEqual([{ id: 'b1' }]);
    });
});
