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
 * `resolveTransition` has THREE outcomes, not two: a booking insert
 * announces 'booking:created'; an update whose write touched `status`
 * announces 'booking:cancelled' when the document now says cancelled, OR
 * 'meeting:rescheduled' when it says rescheduled — any other status (or a
 * status-less field write) is silent. Recipients are every participant's
 * user room PLUS the org room, but the org room is conditional
 * (`if (booking.organizationId)` in the source — a booking can be orgless),
 * with a socket in several of those rooms receiving exactly one copy.
 */
type BookingDoc = { id: string; status: string; participants: string[]; organizationId?: string };

const select = (event: ChangeEvent) => {
    if (event.type !== 'changed') return null;
    const doc = event.data as BookingDoc;
    const rooms = doc.participants.map(userRoom);
    if (doc.organizationId) rooms.push(scopeRoom(doc.organizationId)); // mirrors `if (booking.organizationId)`
    if (event.operation === 'insert') return { name: 'booking:created', payload: { id: doc.id }, rooms };
    if (!didChange(event, 'status')) return null; // an update that didn't flip status is not announced
    if (doc.status === 'cancelled') return { name: 'booking:cancelled', payload: { id: doc.id }, rooms };
    if (doc.status === 'rescheduled') return { name: 'meeting:rescheduled', payload: { id: doc.id }, rooms };
    return null;
};

const changed = (
    doc: BookingDoc,
    operation: 'insert' | 'update' | 'replace',
    updatedFields?: string[]
): ChangeEvent => ({ type: 'changed', scopeId: doc.organizationId ?? '', data: doc, operation, updatedFields });

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

    it('an update whose status flips to rescheduled announces meeting:rescheduled — resolveTransition’s third outcome', async () => {
        const orgMember = await connectClient(port);
        await joinRoom(orgMember, scopeRoom('org1'));
        const received = receivedEvents(orgMember, 'meeting:rescheduled');

        const rescheduledDoc: BookingDoc = { ...doc, status: 'rescheduled' };
        await pushMapped(server, gen([changed(rescheduledDoc, 'update', ['status'])]), select, {
            shutdown: new ValueSignal(true),
        });

        await waitForCount(received, 1);
        expect(received).toEqual([{ id: 'b1' }]);
    });

    it('a booking with no organizationId omits the org room — only its participants are addressed', async () => {
        const participant = await connectClient(port);
        const orgMember = await connectClient(port);
        await joinRoom(participant, userRoom('u1'));
        await joinRoom(orgMember, scopeRoom('org1'));
        const participantEvents = receivedEvents(participant, 'booking:created');
        const orgEvents = receivedEvents(orgMember, 'booking:created');

        const orphanDoc: BookingDoc = { id: 'b2', status: 'pending', participants: ['u1'] };
        await pushMapped(server, gen([changed(orphanDoc, 'insert')]), select, { shutdown: new ValueSignal(true) });

        await waitForCount(participantEvents, 1);
        expect(participantEvents).toEqual([{ id: 'b2' }]);

        await delay(200);
        expect(orgEvents).toHaveLength(0);
    });
});
