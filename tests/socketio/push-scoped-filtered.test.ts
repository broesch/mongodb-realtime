import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { pushScopedFiltered } from '../../src/socketio/push-scoped-filtered.js';
import type { ChangeEvent } from '../../src/core/change-event.js';
import type { Logger } from '../../src/common/logger.js';
import { ValueSignal } from '../../src/common/signal.js';
import { scopeRoom } from '../../src/socketio/rooms.js';
import {
    startHarness,
    stopHarness,
    connectClient,
    joinRoom,
    joinRoomWithData,
    receivedEvents,
    waitForCount,
    delay,
} from './harness.js';
import type { RemoteSocket, Server } from 'socket.io';

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

type TestSocket = RemoteSocket<never, unknown>;

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

describe('pushScopedFiltered', () => {
    it('only sockets whose canReceive returns true receive the changed payload', async () => {
        const allowed = await connectClient(port);
        const denied = await connectClient(port);
        await joinRoomWithData(allowed, scopeRoom('scope-1'), { canSee: true });
        await joinRoomWithData(denied, scopeRoom('scope-1'), { canSee: false });
        const allowedEvents = receivedEvents(allowed, 'doc:changed');
        const deniedEvents = receivedEvents(denied, 'doc:changed');

        const event: ChangeEvent = { type: 'changed', scopeId: 'scope-1', data: { id: '1', value: 'a' } };
        await pushScopedFiltered(
            server,
            gen([event]),
            { changed: 'doc:changed' },
            {
                shutdown: new ValueSignal(true),
                canReceive: (socket: TestSocket) => (socket.data as { canSee: boolean }).canSee,
            }
        );

        await waitForCount(allowedEvents, 1);
        expect(allowedEvents[0]).toEqual({ id: '1', value: 'a' });

        await delay(200);
        expect(deniedEvents).toHaveLength(0);
    });

    it('a throwing canReceive skips that recipient only (others still receive) + logs a warning', async () => {
        const boom = await connectClient(port);
        const fine = await connectClient(port);
        await joinRoomWithData(boom, scopeRoom('scope-1'), { shouldThrow: true });
        await joinRoomWithData(fine, scopeRoom('scope-1'), { shouldThrow: false });
        const boomEvents = receivedEvents(boom, 'doc:changed');
        const fineEvents = receivedEvents(fine, 'doc:changed');
        const { logger, calls } = mockLogger();

        const event: ChangeEvent = { type: 'changed', scopeId: 'scope-1', data: { id: '1' } };
        await pushScopedFiltered(
            server,
            gen([event]),
            { changed: 'doc:changed' },
            {
                shutdown: new ValueSignal(true),
                logger,
                canReceive: (socket: TestSocket) => {
                    if ((socket.data as { shouldThrow: boolean }).shouldThrow) throw new Error('canReceive boom');
                    return true;
                },
            }
        );

        await waitForCount(fineEvents, 1);
        await delay(200);
        expect(boomEvents).toHaveLength(0);
        expect(calls.some(c => c.level === 'warn' && c.message.includes('canReceive boom'))).toBe(true);
    });

    it('prepareEvent runs once per event; its result is passed to every canReceive', async () => {
        const first = await connectClient(port);
        const second = await connectClient(port);
        await joinRoom(first, scopeRoom('scope-1'));
        await joinRoom(second, scopeRoom('scope-1'));
        const firstEvents = receivedEvents(first, 'doc:changed');
        const secondEvents = receivedEvents(second, 'doc:changed');

        let prepareCalls = 0;
        const preparedValuesSeen: unknown[] = [];
        const event: ChangeEvent = { type: 'changed', scopeId: 'scope-1', data: { id: '1' } };
        await pushScopedFiltered(
            server,
            gen([event]),
            { changed: 'doc:changed' },
            {
                shutdown: new ValueSignal(true),
                prepareEvent: () => {
                    prepareCalls += 1;
                    return { scopeHint: 'resolved-once' };
                },
                canReceive: (_socket, _data, prepared) => {
                    preparedValuesSeen.push(prepared);
                    return true;
                },
            }
        );

        await waitForCount(firstEvents, 1);
        await waitForCount(secondEvents, 1);
        expect(prepareCalls).toBe(1);
        expect(preparedValuesSeen).toEqual([{ scopeHint: 'resolved-once' }, { scopeHint: 'resolved-once' }]);
    });

    it('removed events bypass filtering: bare id reaches every socket in the scope room', async () => {
        const first = await connectClient(port);
        const second = await connectClient(port);
        // Both would be denied by canReceive, but removed events never call it.
        await joinRoom(first, scopeRoom('scope-1'));
        await joinRoom(second, scopeRoom('scope-1'));
        const firstEvents = receivedEvents(first, 'doc:removed');
        const secondEvents = receivedEvents(second, 'doc:removed');

        let canReceiveCalls = 0;
        const event: ChangeEvent = { type: 'removed', id: 'entity-1', scopeId: 'scope-1' };
        await pushScopedFiltered(
            server,
            gen([event]),
            { changed: 'doc:changed', removed: 'doc:removed' },
            {
                shutdown: new ValueSignal(true),
                canReceive: () => {
                    canReceiveCalls += 1;
                    return false;
                },
            }
        );

        await waitForCount(firstEvents, 1);
        await waitForCount(secondEvents, 1);
        expect(firstEvents[0]).toBe('entity-1');
        expect(secondEvents[0]).toBe('entity-1');
        expect(canReceiveCalls).toBe(0);
    });

    describe('variants', () => {
        it('render is called once; each passing socket gets full or redacted per pick', async () => {
            const wantsFull = await connectClient(port);
            const wantsRedacted = await connectClient(port);
            await joinRoomWithData(wantsFull, scopeRoom('scope-1'), { variant: 'full' });
            await joinRoomWithData(wantsRedacted, scopeRoom('scope-1'), { variant: 'redacted' });
            const fullEvents = receivedEvents(wantsFull, 'doc:changed');
            const redactedEvents = receivedEvents(wantsRedacted, 'doc:changed');

            let renderCalls = 0;
            const event: ChangeEvent = { type: 'changed', scopeId: 'scope-1', data: { id: '1', ssn: '123-45-6789' } };
            await pushScopedFiltered(
                server,
                gen([event]),
                { changed: 'doc:changed' },
                {
                    shutdown: new ValueSignal(true),
                    canReceive: () => true,
                    variants: {
                        render: (data: unknown) => {
                            renderCalls += 1;
                            const doc = data as { id: string; ssn: string };
                            return { full: doc, redacted: { id: doc.id, ssn: '***' } };
                        },
                        pick: (socket: TestSocket) => (socket.data as { variant: 'full' | 'redacted' }).variant,
                    },
                }
            );

            await waitForCount(fullEvents, 1);
            await waitForCount(redactedEvents, 1);
            expect(renderCalls).toBe(1);
            expect(fullEvents[0]).toEqual({ id: '1', ssn: '123-45-6789' });
            expect(redactedEvents[0]).toEqual({ id: '1', ssn: '***' });
        });

        it('passing both mapData and variants throws synchronously (before consuming the watcher)', async () => {
            let watcherStarted = false;
            async function* trackedGen(): AsyncGenerator<ChangeEvent> {
                watcherStarted = true;
                yield { type: 'changed', scopeId: 'scope-1', data: { id: '1' } };
            }

            await expect(
                pushScopedFiltered(
                    server,
                    trackedGen(),
                    { changed: 'doc:changed' },
                    {
                        shutdown: new ValueSignal(true),
                        canReceive: () => true,
                        mapData: (data: unknown) => data,
                        variants: {
                            render: (data: unknown) => ({ full: data, redacted: data }),
                            pick: () => 'full',
                        },
                    }
                )
            ).rejects.toThrow(/mutually exclusive/);

            expect(watcherStarted).toBe(false);
        });
    });

    it('runInContext wraps the once-per-event block with the EVENT tenant and each canReceive with the SOCKET tenant', async () => {
        const alice = await connectClient(port);
        const bob = await connectClient(port);
        await joinRoomWithData(alice, scopeRoom('scope-1'), { tenant: 'tenant-alice' });
        await joinRoomWithData(bob, scopeRoom('scope-1'), { tenant: 'tenant-bob' });
        const aliceEvents = receivedEvents(alice, 'doc:changed');
        const bobEvents = receivedEvents(bob, 'doc:changed');

        const contextsSeen: unknown[] = [];
        const event: ChangeEvent<string> = {
            type: 'changed',
            scopeId: 'scope-1',
            data: { id: '1' },
            tenant: 'tenant-event',
        };
        await pushScopedFiltered(
            server,
            gen([event]),
            { changed: 'doc:changed' },
            {
                shutdown: new ValueSignal(true),
                canReceive: () => true,
                prepareEvent: () => 'prepared',
                runInContext: async (ctx, fn) => {
                    contextsSeen.push(ctx.tenant);
                    return fn();
                },
                tenantOf: (socket: TestSocket) => (socket.data as { tenant: string }).tenant,
            }
        );

        await waitForCount(aliceEvents, 1);
        await waitForCount(bobEvents, 1);
        // One once-per-event context (event tenant) + one per socket (its own tenant).
        expect(contextsSeen).toHaveLength(3);
        expect(contextsSeen[0]).toBe('tenant-event');
        expect(contextsSeen.slice(1).sort()).toEqual(['tenant-alice', 'tenant-bob']);
    });
});
