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

        const event: ChangeEvent = {
            type: 'changed',
            scopeId: 'scope-1',
            data: { id: '1', value: 'a' },
            operation: 'insert',
        };
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

        const event: ChangeEvent = { type: 'changed', scopeId: 'scope-1', data: { id: '1' }, operation: 'insert' };
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
        const event: ChangeEvent = { type: 'changed', scopeId: 'scope-1', data: { id: '1' }, operation: 'insert' };
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

    it('removed events WITHOUT a pre-image bypass filtering: bare id reaches every socket in the scope room', async () => {
        const first = await connectClient(port);
        const second = await connectClient(port);
        // Both would be denied by canReceive, but with no pre-image there is nothing to judge.
        await joinRoom(first, scopeRoom('scope-1'));
        await joinRoom(second, scopeRoom('scope-1'));
        const firstEvents = receivedEvents(first, 'doc:removed');
        const secondEvents = receivedEvents(second, 'doc:removed');

        let canReceiveCalls = 0;
        const event: ChangeEvent = { type: 'removed', id: 'entity-1', scopeId: 'scope-1', via: 'delete' };
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
            const event: ChangeEvent = {
                type: 'changed',
                scopeId: 'scope-1',
                data: { id: '1', ssn: '123-45-6789' },
                operation: 'insert',
            };
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
                yield { type: 'changed', scopeId: 'scope-1', data: { id: '1' }, operation: 'insert' };
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
            operation: 'insert',
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

// ---------------------------------------------------------------------------
// v0.4: removals addressed by the pre-image, and scope exit
// ---------------------------------------------------------------------------

/** canReceive used below: a socket sees a record iff its `properties` include the record's `propertyId`. */
const seesProperty = (socket: TestSocket, data: unknown): boolean =>
    ((socket.data as { properties?: string[] }).properties ?? []).includes(
        (data as { propertyId?: string }).propertyId ?? ''
    );

describe('pushScopedFiltered — removed addressed by the pre-image', () => {
    it('emits the bare id only to sockets where canReceive(socket, event.before) holds', async () => {
        const sawIt = await connectClient(port);
        const neverSawIt = await connectClient(port);
        await joinRoomWithData(sawIt, scopeRoom('scope-1'), { properties: ['p1'] });
        await joinRoomWithData(neverSawIt, scopeRoom('scope-1'), { properties: ['p2'] });
        const sawEvents = receivedEvents(sawIt, 'doc:removed');
        const neverEvents = receivedEvents(neverSawIt, 'doc:removed');

        const seen: unknown[] = [];
        const before = { _id: 'entity-1', organizationId: 'scope-1', propertyId: 'p1' };
        const event: ChangeEvent = { type: 'removed', id: 'entity-1', scopeId: 'scope-1', via: 'delete', before };
        await pushScopedFiltered(
            server,
            gen([event]),
            { changed: 'doc:changed', removed: 'doc:removed' },
            {
                shutdown: new ValueSignal(true),
                canReceive: (socket: TestSocket, data: unknown) => {
                    seen.push(data);
                    return seesProperty(socket, data);
                },
            }
        );

        await waitForCount(sawEvents, 1);
        await delay(150);
        expect(sawEvents).toEqual(['entity-1']);
        expect(neverEvents).toHaveLength(0);
        // canReceive judged the pre-image, once per socket.
        expect(seen).toEqual([before, before]);
    });

    it('a soft delete is filtered by its pre-image the same way', async () => {
        const sawIt = await connectClient(port);
        const neverSawIt = await connectClient(port);
        await joinRoomWithData(sawIt, scopeRoom('scope-1'), { properties: ['p1'] });
        await joinRoomWithData(neverSawIt, scopeRoom('scope-1'), { properties: ['p2'] });
        const sawEvents = receivedEvents(sawIt, 'doc:removed');
        const neverEvents = receivedEvents(neverSawIt, 'doc:removed');

        const event: ChangeEvent = {
            type: 'removed',
            id: 'entity-1',
            scopeId: 'scope-1',
            via: 'softDelete',
            before: { _id: 'entity-1', propertyId: 'p1' },
        };
        await pushScopedFiltered(
            server,
            gen([event]),
            { changed: 'doc:changed', removed: 'doc:removed' },
            { shutdown: new ValueSignal(true), canReceive: seesProperty }
        );

        await waitForCount(sawEvents, 1);
        await delay(150);
        expect(neverEvents).toHaveLength(0);
    });

    it('runs each canReceive inside the socket tenant context, and prepareEvent(before) once in the event tenant context', async () => {
        const alice = await connectClient(port);
        const bob = await connectClient(port);
        await joinRoomWithData(alice, scopeRoom('scope-1'), { tenant: 'tenant-alice', properties: ['p1'] });
        await joinRoomWithData(bob, scopeRoom('scope-1'), { tenant: 'tenant-bob', properties: ['p1'] });
        const aliceEvents = receivedEvents(alice, 'doc:removed');
        const bobEvents = receivedEvents(bob, 'doc:removed');

        // A stand-in for CLS: `current` is the tenant of the innermost
        // runInContext call, restored when that call settles.
        let current: unknown;
        const canReceiveSaw: { socketTenant: unknown; ctx: unknown; prepared: unknown }[] = [];
        const prepareSaw: { data: unknown; ctx: unknown }[] = [];
        const before = { _id: 'entity-1', propertyId: 'p1' };
        const event: ChangeEvent<string> = {
            type: 'removed',
            id: 'entity-1',
            scopeId: 'scope-1',
            via: 'delete',
            before,
            tenant: 'tenant-event',
        };
        await pushScopedFiltered(
            server,
            gen([event]),
            { changed: 'doc:changed', removed: 'doc:removed' },
            {
                shutdown: new ValueSignal(true),
                prepareEvent: data => {
                    prepareSaw.push({ data, ctx: current });
                    return 'prepared-before';
                },
                canReceive: (socket: TestSocket, data: unknown, prepared?: unknown) => {
                    canReceiveSaw.push({
                        socketTenant: (socket.data as { tenant: string }).tenant,
                        ctx: current,
                        prepared,
                    });
                    return seesProperty(socket, data);
                },
                runInContext: async (ctx, fn) => {
                    const outer = current;
                    current = ctx.tenant;
                    try {
                        return await fn();
                    } finally {
                        current = outer;
                    }
                },
                tenantOf: (socket: TestSocket) => (socket.data as { tenant: string }).tenant,
            }
        );

        await waitForCount(aliceEvents, 1);
        await waitForCount(bobEvents, 1);
        expect(prepareSaw).toEqual([{ data: before, ctx: 'tenant-event' }]);
        expect(canReceiveSaw).toHaveLength(2);
        for (const call of canReceiveSaw) {
            expect(call.prepared).toBe('prepared-before');
        }
        // Each check ran with ITS socket's tenant as the ambient context.
        for (const call of canReceiveSaw) {
            expect(call.ctx).toBe(call.socketTenant);
        }
        expect(canReceiveSaw.map(c => c.ctx).sort()).toEqual(['tenant-alice', 'tenant-bob']);
    });

    it('a throwing canReceive skips that socket only and logs a warning', async () => {
        const ok = await connectClient(port);
        const broken = await connectClient(port);
        await joinRoomWithData(ok, scopeRoom('scope-1'), { properties: ['p1'] });
        await joinRoomWithData(broken, scopeRoom('scope-1'), { broken: true });
        const okEvents = receivedEvents(ok, 'doc:removed');
        const brokenEvents = receivedEvents(broken, 'doc:removed');
        const { logger, calls } = mockLogger();

        const event: ChangeEvent = {
            type: 'removed',
            id: 'entity-1',
            scopeId: 'scope-1',
            via: 'delete',
            before: { propertyId: 'p1' },
        };
        await pushScopedFiltered(
            server,
            gen([event]),
            { changed: 'doc:changed', removed: 'doc:removed' },
            {
                shutdown: new ValueSignal(true),
                logger,
                canReceive: (socket: TestSocket, data: unknown) => {
                    if ((socket.data as { broken?: boolean }).broken) throw new Error('permissions down');
                    return seesProperty(socket, data);
                },
            }
        );

        await waitForCount(okEvents, 1);
        await delay(150);
        expect(brokenEvents).toHaveLength(0);
        expect(calls.some(c => c.level === 'warn' && c.message.includes('permissions down'))).toBe(true);
    });

    it('a prepareEvent that throws on the pre-image fails closed: no socket gets removed, an error is logged', async () => {
        const client = await connectClient(port);
        await joinRoomWithData(client, scopeRoom('scope-1'), { properties: ['p1'] });
        const removedEvents = receivedEvents(client, 'doc:removed');
        const { logger, calls } = mockLogger();

        const event: ChangeEvent = {
            type: 'removed',
            id: 'entity-1',
            scopeId: 'scope-1',
            via: 'delete',
            before: { propertyId: 'p1' },
        };
        await pushScopedFiltered(
            server,
            gen([event]),
            { changed: 'doc:changed', removed: 'doc:removed' },
            {
                shutdown: new ValueSignal(true),
                logger,
                prepareEvent: () => {
                    throw new Error('hierarchy lookup failed');
                },
                canReceive: seesProperty,
            }
        );

        await delay(200);
        expect(removedEvents).toHaveLength(0);
        expect(calls.some(c => c.level === 'error' && c.message.includes('hierarchy lookup failed'))).toBe(true);
    });

    it('with variants the removed payload is still the bare id (pick is not consulted)', async () => {
        const client = await connectClient(port);
        await joinRoomWithData(client, scopeRoom('scope-1'), { properties: ['p1'] });
        const removedEvents = receivedEvents(client, 'doc:removed');
        let picks = 0;

        const event: ChangeEvent = {
            type: 'removed',
            id: 'entity-1',
            scopeId: 'scope-1',
            via: 'delete',
            before: { propertyId: 'p1', secret: 'x' },
        };
        await pushScopedFiltered(
            server,
            gen([event]),
            { changed: 'doc:changed', removed: 'doc:removed' },
            {
                shutdown: new ValueSignal(true),
                canReceive: seesProperty,
                variants: {
                    render: data => ({ full: data, redacted: {} }),
                    pick: () => {
                        picks += 1;
                        return 'full';
                    },
                },
            }
        );

        await waitForCount(removedEvents, 1);
        expect(removedEvents[0]).toBe('entity-1');
        expect(picks).toBe(0);
    });

    it('without a pre-image falls back to the room-wide bare id and logs a warning', async () => {
        const first = await connectClient(port);
        const second = await connectClient(port);
        await joinRoom(first, scopeRoom('scope-1'));
        await joinRoom(second, scopeRoom('scope-1'));
        const firstEvents = receivedEvents(first, 'doc:removed');
        const secondEvents = receivedEvents(second, 'doc:removed');
        const { logger, calls } = mockLogger();
        let canReceiveCalls = 0;

        const event: ChangeEvent = { type: 'removed', id: 'entity-1', scopeId: 'scope-1', via: 'delete' };
        await pushScopedFiltered(
            server,
            gen([event]),
            { changed: 'doc:changed', removed: 'doc:removed' },
            {
                shutdown: new ValueSignal(true),
                logger,
                canReceive: () => {
                    canReceiveCalls += 1;
                    return false;
                },
            }
        );

        await waitForCount(firstEvents, 1);
        await waitForCount(secondEvents, 1);
        expect(canReceiveCalls).toBe(0);
        const warnings = calls.filter(c => c.level === 'warn');
        expect(warnings).toHaveLength(1);
        expect(warnings[0].message).toMatch(/pre-image/);
        expect(warnings[0].message).toContain('entity-1');
    });
});

describe('pushScopedFiltered — evictOnScopeExit', () => {
    const audienceKey = (doc: Record<string, unknown>): string => String(doc.propertyId);

    it('skips the eviction pass when audienceKey(before) === audienceKey(after)', async () => {
        const client = await connectClient(port);
        await joinRoomWithData(client, scopeRoom('scope-1'), { properties: ['p1'] });
        const changed = receivedEvents(client, 'doc:changed');
        const removed = receivedEvents(client, 'doc:removed');
        const judged: unknown[] = [];

        const before = { _id: 'entity-1', propertyId: 'p1', title: 'old' };
        const after = { _id: 'entity-1', propertyId: 'p1', title: 'new' };
        const event: ChangeEvent = { type: 'changed', scopeId: 'scope-1', data: after, operation: 'update', before };
        await pushScopedFiltered(
            server,
            gen([event]),
            { changed: 'doc:changed', removed: 'doc:removed' },
            {
                shutdown: new ValueSignal(true),
                canReceive: (socket: TestSocket, data: unknown) => {
                    judged.push(data);
                    return seesProperty(socket, data);
                },
                evictOnScopeExit: { audienceKey },
            }
        );

        await waitForCount(changed, 1);
        await delay(150);
        expect(removed).toHaveLength(0);
        // Only the post-image was judged — no extra permission check for an ordinary edit.
        expect(judged).toEqual([after]);
    });

    it('sends removed to sockets with canReceive(before) && !canReceive(after), changed to the new audience, nothing to others', async () => {
        const stays = await connectClient(port); // sees p1 and p2
        const loses = await connectClient(port); // sees p1 only
        const gains = await connectClient(port); // sees p2 only
        const never = await connectClient(port); // sees neither
        await joinRoomWithData(stays, scopeRoom('scope-1'), { properties: ['p1', 'p2'] });
        await joinRoomWithData(loses, scopeRoom('scope-1'), { properties: ['p1'] });
        await joinRoomWithData(gains, scopeRoom('scope-1'), { properties: ['p2'] });
        await joinRoomWithData(never, scopeRoom('scope-1'), { properties: ['p9'] });
        const got = Object.fromEntries(
            (
                [
                    ['stays', stays],
                    ['loses', loses],
                    ['gains', gains],
                    ['never', never],
                ] as const
            ).map(([name, c]) => [
                name,
                { changed: receivedEvents(c, 'doc:changed'), removed: receivedEvents(c, 'doc:removed') },
            ])
        );

        const before = { _id: 'entity-1', propertyId: 'p1' };
        const after = { _id: 'entity-1', propertyId: 'p2' };
        const event: ChangeEvent = { type: 'changed', scopeId: 'scope-1', data: after, operation: 'update', before };
        await pushScopedFiltered(
            server,
            gen([event]),
            { changed: 'doc:changed', removed: 'doc:removed' },
            { shutdown: new ValueSignal(true), canReceive: seesProperty, evictOnScopeExit: { audienceKey } }
        );

        await waitForCount(got.stays.changed, 1);
        await waitForCount(got.gains.changed, 1);
        await waitForCount(got.loses.removed, 1);
        await delay(150);
        expect(got.stays).toEqual({ changed: [after], removed: [] });
        expect(got.gains).toEqual({ changed: [after], removed: [] });
        expect(got.loses).toEqual({ changed: [], removed: ['entity-1'] });
        expect(got.never).toEqual({ changed: [], removed: [] });
    });

    it('evicts inside the socket tenant context, with prepareEvent run on the pre-image', async () => {
        const loses = await connectClient(port);
        await joinRoomWithData(loses, scopeRoom('scope-1'), { tenant: 'tenant-loses', properties: ['p1'] });
        const removed = receivedEvents(loses, 'doc:removed');

        let current: unknown;
        const calls: { data: unknown; prepared: unknown; ctx: unknown }[] = [];
        const before = { _id: 'entity-1', propertyId: 'p1' };
        const after = { _id: 'entity-1', propertyId: 'p2' };
        const event: ChangeEvent<string> = {
            type: 'changed',
            scopeId: 'scope-1',
            data: after,
            operation: 'update',
            before,
            tenant: 'tenant-event',
        };
        await pushScopedFiltered(
            server,
            gen([event]),
            { changed: 'doc:changed', removed: 'doc:removed' },
            {
                shutdown: new ValueSignal(true),
                prepareEvent: data => `prepared:${(data as { propertyId: string }).propertyId}`,
                canReceive: (socket: TestSocket, data: unknown, prepared?: unknown) => {
                    calls.push({ data, prepared, ctx: current });
                    return seesProperty(socket, data);
                },
                runInContext: async (ctx, fn) => {
                    const outer = current;
                    current = ctx.tenant;
                    try {
                        return await fn();
                    } finally {
                        current = outer;
                    }
                },
                tenantOf: (socket: TestSocket) => (socket.data as { tenant: string }).tenant,
                evictOnScopeExit: { audienceKey },
            }
        );

        await waitForCount(removed, 1);
        // After-state judged with the after-state's prepared value, the
        // pre-image with its own — never crossed.
        expect(calls).toEqual([
            { data: after, prepared: 'prepared:p2', ctx: 'tenant-loses' },
            { data: before, prepared: 'prepared:p1', ctx: 'tenant-loses' },
        ]);
    });

    it('an audienceKey that throws is logged and the changed event goes out unchanged', async () => {
        const stays = await connectClient(port);
        const loses = await connectClient(port);
        await joinRoomWithData(stays, scopeRoom('scope-1'), { properties: ['p2'] });
        await joinRoomWithData(loses, scopeRoom('scope-1'), { properties: ['p1'] });
        const staysChanged = receivedEvents(stays, 'doc:changed');
        const losesRemoved = receivedEvents(loses, 'doc:removed');
        const { logger, calls } = mockLogger();

        const after = { _id: 'entity-1', propertyId: 'p2' };
        const event: ChangeEvent = {
            type: 'changed',
            scopeId: 'scope-1',
            data: after,
            operation: 'update',
            before: { _id: 'entity-1', propertyId: 'p1' },
        };
        await pushScopedFiltered(
            server,
            gen([event]),
            { changed: 'doc:changed', removed: 'doc:removed' },
            {
                shutdown: new ValueSignal(true),
                logger,
                canReceive: seesProperty,
                evictOnScopeExit: {
                    audienceKey: () => {
                        throw new Error('bad key');
                    },
                },
            }
        );

        await waitForCount(staysChanged, 1);
        await delay(150);
        expect(staysChanged).toEqual([after]);
        expect(losesRemoved).toHaveLength(0);
        expect(calls.some(c => c.level === 'warn' && c.message.includes('bad key'))).toBe(true);
    });

    it('a prepareEvent that throws on the pre-image is logged, no eviction, the changed event goes out', async () => {
        const stays = await connectClient(port);
        const loses = await connectClient(port);
        await joinRoomWithData(stays, scopeRoom('scope-1'), { properties: ['p2'] });
        await joinRoomWithData(loses, scopeRoom('scope-1'), { properties: ['p1'] });
        const staysChanged = receivedEvents(stays, 'doc:changed');
        const losesRemoved = receivedEvents(loses, 'doc:removed');
        const { logger, calls } = mockLogger();

        const before = { _id: 'entity-1', propertyId: 'p1' };
        const event: ChangeEvent = {
            type: 'changed',
            scopeId: 'scope-1',
            data: { _id: 'entity-1', propertyId: 'p2' },
            operation: 'update',
            before,
        };
        await pushScopedFiltered(
            server,
            gen([event]),
            { changed: 'doc:changed', removed: 'doc:removed' },
            {
                shutdown: new ValueSignal(true),
                logger,
                prepareEvent: data => {
                    if (data === before) throw new Error('cannot resolve old scope');
                    return undefined;
                },
                canReceive: seesProperty,
                evictOnScopeExit: { audienceKey },
            }
        );

        await waitForCount(staysChanged, 1);
        await delay(150);
        expect(losesRemoved).toHaveLength(0);
        expect(calls.some(c => c.level === 'warn' && c.message.includes('cannot resolve old scope'))).toBe(true);
    });

    it('a canReceive that throws on the pre-image skips that socket only and logs a warning', async () => {
        const loses = await connectClient(port);
        const broken = await connectClient(port);
        await joinRoomWithData(loses, scopeRoom('scope-1'), { properties: ['p1'] });
        await joinRoomWithData(broken, scopeRoom('scope-1'), { properties: ['p1'], brokenOnBefore: true });
        const losesRemoved = receivedEvents(loses, 'doc:removed');
        const brokenRemoved = receivedEvents(broken, 'doc:removed');
        const { logger, calls } = mockLogger();

        const before = { _id: 'entity-1', propertyId: 'p1' };
        const event: ChangeEvent = {
            type: 'changed',
            scopeId: 'scope-1',
            data: { _id: 'entity-1', propertyId: 'p2' },
            operation: 'update',
            before,
        };
        await pushScopedFiltered(
            server,
            gen([event]),
            { changed: 'doc:changed', removed: 'doc:removed' },
            {
                shutdown: new ValueSignal(true),
                logger,
                canReceive: (socket: TestSocket, data: unknown) => {
                    if (data === before && (socket.data as { brokenOnBefore?: boolean }).brokenOnBefore) {
                        throw new Error('old-scope check failed');
                    }
                    return seesProperty(socket, data);
                },
                evictOnScopeExit: { audienceKey },
            }
        );

        await waitForCount(losesRemoved, 1);
        await delay(150);
        expect(brokenRemoved).toHaveLength(0);
        expect(calls.some(c => c.level === 'warn' && c.message.includes('old-scope check failed'))).toBe(true);
    });

    it('an update without a pre-image passes through with no eviction and a warning', async () => {
        const loses = await connectClient(port);
        await joinRoomWithData(loses, scopeRoom('scope-1'), { properties: ['p1'] });
        const removed = receivedEvents(loses, 'doc:removed');
        const { logger, calls } = mockLogger();

        const event: ChangeEvent = {
            type: 'changed',
            scopeId: 'scope-1',
            data: { _id: 'entity-1', propertyId: 'p2' },
            operation: 'update',
        };
        await pushScopedFiltered(
            server,
            gen([event]),
            { changed: 'doc:changed', removed: 'doc:removed' },
            { shutdown: new ValueSignal(true), logger, canReceive: seesProperty, evictOnScopeExit: { audienceKey } }
        );

        await delay(200);
        expect(removed).toHaveLength(0);
        expect(calls.some(c => c.level === 'warn' && /pre-image/.test(c.message))).toBe(true);
    });

    it('an insert never runs the eviction pass and logs nothing', async () => {
        const client = await connectClient(port);
        await joinRoomWithData(client, scopeRoom('scope-1'), { properties: ['p1'] });
        const changed = receivedEvents(client, 'doc:changed');
        const { logger, calls } = mockLogger();
        let keys = 0;

        const event: ChangeEvent = {
            type: 'changed',
            scopeId: 'scope-1',
            data: { _id: 'entity-1', propertyId: 'p1' },
            operation: 'insert',
        };
        await pushScopedFiltered(
            server,
            gen([event]),
            { changed: 'doc:changed', removed: 'doc:removed' },
            {
                shutdown: new ValueSignal(true),
                logger,
                canReceive: seesProperty,
                evictOnScopeExit: {
                    audienceKey: doc => {
                        keys += 1;
                        return audienceKey(doc);
                    },
                },
            }
        );

        await waitForCount(changed, 1);
        expect(keys).toBe(0);
        expect(calls.filter(c => c.level === 'warn' || c.level === 'error')).toHaveLength(0);
    });

    it('throws at wiring time when evictOnScopeExit is set but events.removed is not', async () => {
        let pulled = false;
        async function* watcher(): AsyncGenerator<ChangeEvent> {
            pulled = true;
            yield { type: 'changed', scopeId: 'scope-1', data: {}, operation: 'insert' };
        }
        await expect(
            pushScopedFiltered(
                server,
                watcher(),
                { changed: 'doc:changed' },
                {
                    shutdown: new ValueSignal(true),
                    canReceive: () => true,
                    evictOnScopeExit: { audienceKey },
                }
            )
        ).rejects.toThrow(/evictOnScopeExit.*removed/);
        expect(pulled).toBe(false);
    });
});
