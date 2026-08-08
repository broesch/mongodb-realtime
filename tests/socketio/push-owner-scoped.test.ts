import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { pushOwnerScoped } from '../../src/socketio/push-owner-scoped.js';
import type { ChangeEvent } from '../../src/core/change-event.js';
import type { Logger } from '../../src/common/logger.js';
import { ValueSignal } from '../../src/common/signal.js';
import { userRoom, namespacedRoom } from '../../src/socketio/rooms.js';
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

const ownerIdOf = (data: unknown): string | null | undefined => (data as { ownerId?: string })?.ownerId;

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

describe('pushOwnerScoped', () => {
    it('changed event is emitted only to userRoom(ownerId) — another owner receives nothing', async () => {
        const owner1 = await connectClient(port);
        const owner2 = await connectClient(port);
        await joinRoom(owner1, userRoom('user-1'));
        await joinRoom(owner2, userRoom('user-2'));
        const owner1Events = receivedEvents(owner1, 'doc:changed');
        const owner2Events = receivedEvents(owner2, 'doc:changed');

        const event: ChangeEvent = {
            type: 'changed',
            scopeId: 'scope-1',
            data: { id: '1', ownerId: 'user-1' },
            operation: 'insert',
        };
        await pushOwnerScoped(
            server,
            gen([event]),
            { changed: 'doc:changed' },
            { shutdown: new ValueSignal(true), ownerIdOf }
        );

        await waitForCount(owner1Events, 1);
        expect(owner1Events[0]).toEqual({ id: '1', ownerId: 'user-1' });

        await delay(200);
        expect(owner2Events).toHaveLength(0);
    });

    it('tenantNamespace: a tenant-tagged event lands only in the namespaced owner room', async () => {
        const namespaced = await connectClient(port);
        const bareRoom = await connectClient(port);
        await joinRoom(namespaced, namespacedRoom('tenantX', userRoom('user-1')));
        await joinRoom(bareRoom, userRoom('user-1'));
        const namespacedEvents = receivedEvents(namespaced, 'doc:changed');
        const bareRoomEvents = receivedEvents(bareRoom, 'doc:changed');

        const event: ChangeEvent<string> = {
            type: 'changed',
            scopeId: 'scope-1',
            data: { id: '1', ownerId: 'user-1' },
            operation: 'insert',
            tenant: 'tenantX',
        };
        await pushOwnerScoped(
            server,
            gen([event]),
            { changed: 'doc:changed' },
            { shutdown: new ValueSignal(true), ownerIdOf, tenantNamespace: (t: string) => t }
        );

        await waitForCount(namespacedEvents, 1);
        expect(namespacedEvents[0]).toEqual({ id: '1', ownerId: 'user-1' });

        await delay(200);
        expect(bareRoomEvents).toHaveLength(0);
    });

    it('removed event emits the bare id to the owner room from event.ownerId', async () => {
        const client = await connectClient(port);
        await joinRoom(client, userRoom('user-1'));
        const removedEvents = receivedEvents(client, 'doc:removed');

        const event: ChangeEvent = { type: 'removed', id: 'entity-1', ownerId: 'user-1', via: 'delete' };
        await pushOwnerScoped(
            server,
            gen([event]),
            { changed: 'doc:changed', removed: 'doc:removed' },
            { shutdown: new ValueSignal(true), ownerIdOf }
        );

        await waitForCount(removedEvents, 1);
        expect(removedEvents[0]).toBe('entity-1');
    });

    it('removed event without ownerId is dropped (no emission to anyone) and logs a warning', async () => {
        const client = await connectClient(port);
        await joinRoom(client, userRoom('user-1'));
        const removedEvents = receivedEvents(client, 'doc:removed');
        const { logger, calls } = mockLogger();

        const event: ChangeEvent = { type: 'removed', id: 'entity-1', via: 'delete' };
        await pushOwnerScoped(
            server,
            gen([event]),
            { changed: 'doc:changed', removed: 'doc:removed' },
            { shutdown: new ValueSignal(true), ownerIdOf, logger }
        );

        await delay(200);
        expect(removedEvents).toHaveLength(0);
        expect(calls.some(c => c.level === 'warn' && c.message.includes('ownerId'))).toBe(true);
    });

    it('changed event whose ownerIdOf resolves nothing is dropped and logs a warning', async () => {
        const client = await connectClient(port);
        await joinRoom(client, userRoom('user-1'));
        const changedEvents = receivedEvents(client, 'doc:changed');
        const { logger, calls } = mockLogger();

        const event: ChangeEvent = { type: 'changed', scopeId: 'scope-1', data: { id: '1' }, operation: 'insert' };
        await pushOwnerScoped(
            server,
            gen([event]),
            { changed: 'doc:changed' },
            { shutdown: new ValueSignal(true), ownerIdOf, logger }
        );

        await delay(200);
        expect(changedEvents).toHaveLength(0);
        expect(calls.some(c => c.level === 'warn' && c.message.includes('ownerId'))).toBe(true);
    });

    it('mapData transforms the payload; a throwing mapData skips that event and the loop continues', async () => {
        const client = await connectClient(port);
        await joinRoom(client, userRoom('user-1'));
        const changedEvents = receivedEvents(client, 'doc:changed');
        const { logger, calls } = mockLogger();

        const events: ChangeEvent[] = [
            { type: 'changed', scopeId: 'scope-1', data: { id: 'bad', ownerId: 'user-1' }, operation: 'insert' },
            { type: 'changed', scopeId: 'scope-1', data: { id: 'good', ownerId: 'user-1' }, operation: 'insert' },
        ];
        await pushOwnerScoped(
            server,
            gen(events),
            { changed: 'doc:changed' },
            {
                shutdown: new ValueSignal(true),
                ownerIdOf,
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
        await pushOwnerScoped(
            server,
            gen<unknown>([]),
            { changed: 'doc:changed' },
            { shutdown: new ValueSignal(false), ownerIdOf, logger }
        );

        expect(calls.some(c => c.level === 'error' && c.message.includes('before shutdown'))).toBe(true);
    });

    it('does not log the early-return error when the watcher completes after shutdown was signalled', async () => {
        const { logger, calls } = mockLogger();
        const shutdown = new ValueSignal(false);
        shutdown.next(true);
        await pushOwnerScoped(server, gen<unknown>([]), { changed: 'doc:changed' }, { shutdown, ownerIdOf, logger });

        expect(calls.some(c => c.level === 'error' && c.message.includes('before shutdown'))).toBe(false);
    });
});
