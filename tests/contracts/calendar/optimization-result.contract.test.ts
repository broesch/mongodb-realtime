import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import type { Server } from 'socket.io';
import { pushMapped } from '../../../src/socketio/push-mapped.js';
import { userRoom } from '../../../src/socketio/rooms.js';
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
 * CONTRACT: calendar's optimization-result pusher
 * (apps/api/src/optimization-result/optimization-result.pusher.ts).
 * Announces readiness only — from the source comment: "The slots themselves
 * are fetched over REST — a full result can be large and the client only
 * needs to know one is available." An insert or update is announced only
 * when the document's `status` is `'ready'` (gated on `didChange(event,
 * 'status')` for updates, exactly like the booking pusher's cancellation
 * gate), and the emitted DTO never carries the `slots` array — it is a
 * hand-built object, not a spread of the document.
 */
type OptimizationResultDoc = {
    id: string;
    requestedBy: string;
    status: string;
    type: string;
    bookingLinkId?: string;
    expiresAt: string;
    slots: unknown[];
};

const select = (event: ChangeEvent) => {
    if (event.type !== 'changed') return null;
    const doc = event.data as OptimizationResultDoc;
    const isReady =
        event.operation === 'insert' ? doc.status === 'ready' : didChange(event, 'status') && doc.status === 'ready';
    if (!isReady) return null;
    return {
        name: 'optimization:ready',
        payload: {
            optimizationResultId: doc.id,
            type: doc.type,
            bookingLinkId: doc.bookingLinkId,
            expiresAt: doc.expiresAt,
        },
        rooms: userRoom(doc.requestedBy),
    };
};

const changed = (
    doc: OptimizationResultDoc,
    operation: 'insert' | 'update' | 'replace',
    updatedFields?: string[]
): ChangeEvent => ({ type: 'changed', scopeId: 'org1', data: doc, operation, updatedFields });

async function* gen(events: ChangeEvent[]): AsyncGenerator<ChangeEvent> {
    for (const event of events) yield event;
}

let server: Server;
let port: number;
beforeEach(async () => ({ server, port } = await startHarness()));
afterEach(async () => stopHarness());

describe('calendar contract: optimization-result pusher composition', () => {
    const readyDoc: OptimizationResultDoc = {
        id: 'r1',
        requestedBy: 'u1',
        status: 'ready',
        type: 'availability',
        bookingLinkId: 'link1',
        expiresAt: '2026-08-09T00:00:00.000Z',
        slots: [{ start: '2026-08-09T09:00:00.000Z', end: '2026-08-09T09:30:00.000Z' }],
    };

    it('an insert that is already ready is announced with a payload that carries no slots field', async () => {
        const requester = await connectClient(port);
        await joinRoom(requester, userRoom('u1'));
        const received = receivedEvents(requester, 'optimization:ready');

        await pushMapped(server, gen([changed(readyDoc, 'insert')]), select, { shutdown: new ValueSignal(true) });

        await waitForCount(received, 1);
        expect(received).toEqual([
            { optimizationResultId: 'r1', type: 'availability', bookingLinkId: 'link1', expiresAt: readyDoc.expiresAt },
        ]);
        expect((received[0] as { slots?: unknown }).slots).toBeUndefined();
    });

    it('an insert that is not yet ready is silent', async () => {
        const requester = await connectClient(port);
        await joinRoom(requester, userRoom('u1'));
        const received = receivedEvents(requester, 'optimization:ready');

        await pushMapped(server, gen([changed({ ...readyDoc, status: 'pending' }, 'insert')]), select, {
            shutdown: new ValueSignal(true),
        });

        await delay(300);
        expect(received).toHaveLength(0);
    });

    it('an update that flips status to ready is announced', async () => {
        const requester = await connectClient(port);
        await joinRoom(requester, userRoom('u1'));
        const received = receivedEvents(requester, 'optimization:ready');

        await pushMapped(server, gen([changed(readyDoc, 'update', ['status'])]), select, {
            shutdown: new ValueSignal(true),
        });

        await waitForCount(received, 1);
        expect(received).toHaveLength(1);
    });

    it('an update that does not touch status is silent, even though the doc already says ready', async () => {
        const requester = await connectClient(port);
        await joinRoom(requester, userRoom('u1'));
        const received = receivedEvents(requester, 'optimization:ready');

        await pushMapped(server, gen([changed(readyDoc, 'update', ['computedAt'])]), select, {
            shutdown: new ValueSignal(true),
        });

        await delay(300);
        expect(received).toHaveLength(0);
    });
});
