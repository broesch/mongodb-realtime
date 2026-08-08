import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { pushScoped } from '../../src/socketio/push-scoped.js';
import type { ChangeEvent } from '../../src/core/change-event.js';
import type { Logger } from '../../src/common/logger.js';
import { ValueSignal } from '../../src/common/signal.js';
import { scopeRoom, namespacedRoom } from '../../src/socketio/rooms.js';
import { startHarness, stopHarness, connectClient, joinRoom, receivedEvents, waitForCount, delay } from './harness.js';
import type { Server } from 'socket.io';

async function* gen<TTenant>(events: ChangeEvent<TTenant>[]): AsyncGenerator<ChangeEvent<TTenant>> {
    for (const event of events) yield event;
}

interface LogCall {
    level: 'debug' | 'log' | 'warn' | 'error';
    message: string;
}

function mockLogger(): { logger: Logger; calls: LogCall[] } {
    const calls: LogCall[] = [];
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

beforeEach(async () => {
    const started = await startHarness();
    server = started.server;
    port = started.port;
});

afterEach(async () => {
    await stopHarness();
});

describe('pushScoped', () => {
    it('changed event is emitted only to scopeRoom(scopeId) — another scope receives nothing', async () => {
        const inScope = await connectClient(port);
        const otherScope = await connectClient(port);
        await joinRoom(inScope, scopeRoom('scope-1'));
        await joinRoom(otherScope, scopeRoom('scope-2'));
        const inScopeEvents = receivedEvents(inScope, 'doc:changed');
        const otherScopeEvents = receivedEvents(otherScope, 'doc:changed');

        const event: ChangeEvent = {
            type: 'changed',
            scopeId: 'scope-1',
            data: { id: '1', value: 'a' },
            operation: 'insert',
        };
        await pushScoped(server, gen([event]), { changed: 'doc:changed' }, { shutdown: new ValueSignal(true) });

        await waitForCount(inScopeEvents, 1);
        expect(inScopeEvents[0]).toEqual({ id: '1', value: 'a' });

        await delay(200);
        expect(otherScopeEvents).toHaveLength(0);
    });

    it('tenantNamespace: a tenant-tagged event lands only in the namespaced room', async () => {
        const namespaced = await connectClient(port);
        const bareRoom = await connectClient(port);
        await joinRoom(namespaced, namespacedRoom('tenantX', scopeRoom('scope-1')));
        await joinRoom(bareRoom, scopeRoom('scope-1'));
        const namespacedEvents = receivedEvents(namespaced, 'doc:changed');
        const bareRoomEvents = receivedEvents(bareRoom, 'doc:changed');

        const event: ChangeEvent<string> = {
            type: 'changed',
            scopeId: 'scope-1',
            data: { id: '1' },
            operation: 'insert',
            tenant: 'tenantX',
        };
        await pushScoped(
            server,
            gen([event]),
            { changed: 'doc:changed' },
            { shutdown: new ValueSignal(true), tenantNamespace: (t: string) => t }
        );

        await waitForCount(namespacedEvents, 1);
        expect(namespacedEvents[0]).toEqual({ id: '1' });

        await delay(200);
        expect(bareRoomEvents).toHaveLength(0);
    });

    it('removed event emits the bare id to the scope room under events.removed', async () => {
        const client = await connectClient(port);
        await joinRoom(client, scopeRoom('scope-1'));
        const removedEvents = receivedEvents(client, 'doc:removed');

        const event: ChangeEvent = { type: 'removed', id: 'entity-1', scopeId: 'scope-1', via: 'delete' };
        await pushScoped(
            server,
            gen([event]),
            { changed: 'doc:changed', removed: 'doc:removed' },
            { shutdown: new ValueSignal(true) }
        );

        await waitForCount(removedEvents, 1);
        expect(removedEvents[0]).toBe('entity-1');
    });

    it('removed event without scopeId is dropped (no emission to anyone) and logs a warning', async () => {
        const client = await connectClient(port);
        await joinRoom(client, scopeRoom('scope-1'));
        const removedEvents = receivedEvents(client, 'doc:removed');
        const { logger, calls } = mockLogger();

        const event: ChangeEvent = { type: 'removed', id: 'entity-1', via: 'delete' };
        await pushScoped(
            server,
            gen([event]),
            { changed: 'doc:changed', removed: 'doc:removed' },
            { shutdown: new ValueSignal(true), logger }
        );

        await delay(200);
        expect(removedEvents).toHaveLength(0);
        expect(calls.some(c => c.level === 'warn' && c.message.includes('scopeId'))).toBe(true);
    });

    it('mapData transforms the payload; a throwing mapData skips that event and the loop continues', async () => {
        const client = await connectClient(port);
        await joinRoom(client, scopeRoom('scope-1'));
        const changedEvents = receivedEvents(client, 'doc:changed');
        const { logger, calls } = mockLogger();

        const events: ChangeEvent[] = [
            { type: 'changed', scopeId: 'scope-1', data: { id: 'bad' }, operation: 'insert' },
            { type: 'changed', scopeId: 'scope-1', data: { id: 'good' }, operation: 'insert' },
        ];
        await pushScoped(
            server,
            gen(events),
            { changed: 'doc:changed' },
            {
                shutdown: new ValueSignal(true),
                logger,
                mapData: (data: unknown) => {
                    const doc = data as { id: string };
                    if (doc.id === 'bad') throw new Error('mapData boom');
                    return { id: doc.id, mapped: true };
                },
            }
        );

        await waitForCount(changedEvents, 1);
        await delay(100);
        expect(changedEvents).toEqual([{ id: 'good', mapped: true }]);
        expect(calls.some(c => c.level === 'error' && c.message.includes('mapData boom'))).toBe(true);
    });

    it('logs the operator-visible error when the watcher returns before shutdown is set', async () => {
        const { logger, calls } = mockLogger();
        await pushScoped(
            server,
            gen<unknown>([]),
            { changed: 'doc:changed' },
            { shutdown: new ValueSignal(false), logger }
        );

        expect(calls.some(c => c.level === 'error' && c.message.includes('before shutdown'))).toBe(true);
    });

    it('does not log the early-return error when the watcher completes after shutdown was signalled', async () => {
        const { logger, calls } = mockLogger();
        const shutdown = new ValueSignal(false);
        shutdown.next(true);
        await pushScoped(server, gen<unknown>([]), { changed: 'doc:changed' }, { shutdown, logger });

        expect(calls.some(c => c.level === 'error' && c.message.includes('before shutdown'))).toBe(false);
    });

    it('roomOf returning several rooms delivers one copy to a socket in two of them', async () => {
        const client = await connectClient(port);
        await joinRoom(client, scopeRoom('org1'));
        await joinRoom(client, 'extra-room');
        const received = receivedEvents(client, 'doc:changed');

        const event: ChangeEvent = { type: 'changed', scopeId: 'org1', data: { v: 1 }, operation: 'insert' };
        await pushScoped(
            server,
            gen([event]),
            { changed: 'doc:changed' },
            {
                shutdown: new ValueSignal(true),
                roomOf: () => [scopeRoom('org1'), 'extra-room'],
            }
        );

        await waitForCount(received, 1);
        await delay(200);
        expect(received).toHaveLength(1);
    });

    it('drops an event whose roomOf resolves to no rooms rather than broadcasting', async () => {
        const client = await connectClient(port);
        await joinRoom(client, scopeRoom('org1'));
        const received = receivedEvents(client, 'doc:changed');
        const { logger, calls } = mockLogger();

        const event: ChangeEvent = { type: 'changed', scopeId: 'org1', data: { v: 1 }, operation: 'insert' };
        await pushScoped(
            server,
            gen([event]),
            { changed: 'doc:changed' },
            {
                shutdown: new ValueSignal(true),
                roomOf: () => [],
                logger,
            }
        );

        await delay(300);
        expect(received).toHaveLength(0);
        expect(calls.some(c => c.level === 'warn' && c.message.includes('no rooms'))).toBe(true);
    });
});
