import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import type { Server } from 'socket.io';
import { pushMapped } from '../../src/socketio/push-mapped.js';
import type { ChangeEvent } from '../../src/core/change-event.js';
import type { Logger } from '../../src/common/logger.js';
import { ValueSignal } from '../../src/common/signal.js';
import { scopeRoom, userRoom } from '../../src/socketio/rooms.js';
import { startHarness, stopHarness, connectClient, joinRoom, receivedEvents, waitForCount, delay } from './harness.js';

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

const changed = (data: unknown): ChangeEvent => ({
    type: 'changed',
    scopeId: 'org1',
    data,
    operation: 'insert',
});

let server: Server;
let port: number;

beforeEach(async () => {
    const started = await startHarness();
    server = started.server;
    port = started.port;
});

afterEach(async () => {
    await stopHarness();
});

describe('pushMapped', () => {
    it('emits under the name and payload the selector returns', async () => {
        const client = await connectClient(port);
        await joinRoom(client, scopeRoom('org1'));
        const received = receivedEvents(client, 'booking:created');

        await pushMapped(
            server,
            gen([changed({ id: 'b1' })]),
            () => ({ name: 'booking:created', payload: { bookingId: 'b1' }, rooms: scopeRoom('org1') }),
            { shutdown: new ValueSignal(true) }
        );

        await waitForCount(received, 1);
        expect(received[0]).toEqual({ bookingId: 'b1' });
    });

    it('a socket in two of the target rooms receives exactly one copy', async () => {
        const client = await connectClient(port);
        await joinRoom(client, userRoom('u1'));
        await joinRoom(client, scopeRoom('org1'));
        const received = receivedEvents(client, 'booking:created');

        await pushMapped(
            server,
            gen([changed({ id: 'b1' })]),
            () => ({ name: 'booking:created', payload: {}, rooms: [userRoom('u1'), scopeRoom('org1')] }),
            { shutdown: new ValueSignal(true) }
        );

        await waitForCount(received, 1);
        await delay(200);
        expect(received).toHaveLength(1);
    });

    it('a selector returning null skips the event entirely', async () => {
        const client = await connectClient(port);
        await joinRoom(client, scopeRoom('org1'));
        const received = receivedEvents(client, 'booking:created');

        await pushMapped(server, gen([changed({ id: 'b1' })]), () => null, { shutdown: new ValueSignal(true) });

        await delay(300);
        expect(received).toHaveLength(0);
    });

    it('a throwing selector skips that one event and keeps the loop running', async () => {
        const client = await connectClient(port);
        await joinRoom(client, scopeRoom('org1'));
        const received = receivedEvents(client, 'e');
        const { logger, calls } = mockLogger();

        await pushMapped(
            server,
            gen([changed({ id: 'bad' }), changed({ id: 'good' })]),
            event => {
                if ((event as { data: { id: string } }).data.id === 'bad') throw new Error('boom');
                return { name: 'e', payload: 'ok', rooms: scopeRoom('org1') };
            },
            { shutdown: new ValueSignal(true), logger }
        );

        await waitForCount(received, 1);
        expect(received).toEqual(['ok']);
        expect(calls.some(c => c.level === 'error' && c.message.includes('boom'))).toBe(true);
    });

    it('drops an emission addressed to no rooms rather than broadcasting', async () => {
        const client = await connectClient(port);
        await joinRoom(client, scopeRoom('org1'));
        const received = receivedEvents(client, 'e');
        const { logger, calls } = mockLogger();

        await pushMapped(server, gen([changed({})]), () => ({ name: 'e', payload: 'x', rooms: [] }), {
            shutdown: new ValueSignal(true),
            logger,
        });

        await delay(300);
        expect(received).toHaveLength(0);
        expect(calls.some(c => c.level === 'warn' && c.message.includes('no rooms'))).toBe(true);
    });

    it('logs an error when the watcher returns before shutdown was signalled', async () => {
        const { logger, calls } = mockLogger();

        await pushMapped(server, gen([]), () => null, { shutdown: new ValueSignal(false), logger });

        expect(calls.some(c => c.level === 'error' && c.message.includes('returned before shutdown'))).toBe(true);
    });
});
