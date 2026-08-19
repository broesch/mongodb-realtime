import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import type { RemoteSocket, Server } from 'socket.io';
import { pushOwnerScoped } from '../../../src/socketio/push-owner-scoped.js';
import { pushScopedFiltered } from '../../../src/socketio/push-scoped-filtered.js';
import { namespacedRoom, scopeRoom, userRoom } from '../../../src/socketio/rooms.js';
import { ValueSignal } from '../../../src/common/signal.js';
import type { ChangeEvent } from '../../../src/core/change-event.js';
import {
    connectClient,
    joinRoomWithData,
    receivedEvents,
    startHarness,
    stopHarness,
    waitForCount,
    delay,
} from '../../socketio/harness.js';

/**
 * CONTRACT: fm's hierarchy and per-owner gateways (see ./README.md).
 *
 * Written before fm migrates — this pins the compositions fm's adoption
 * design commits to, not a diff against a live gateway. `pushScopedFiltered`
 * with `variants` + `prepareEvent` + `runInContext` + `tenantOf` all supplied
 * together is fm-specific: no other consumer of this library uses every hook
 * at once. `pushOwnerScoped` with a tenant namespace pins fm's per-owner
 * (notification-style) gateways.
 */

/** fm's tenant shape, reduced to what the fan-out reads. */
interface Tenant {
    dbName: string;
}

const TENANT: Tenant = { dbName: 'tenant_a' };
const ORG = 'org1';
const ORG_ROOM = namespacedRoom(TENANT.dbName, scopeRoom(ORG));
const PII_ROOM = `${ORG_ROOM}:pii`;

async function* gen(events: ChangeEvent<Tenant>[]): AsyncGenerator<ChangeEvent<Tenant>> {
    for (const event of events) yield event;
}

function changed(data: unknown): ChangeEvent<Tenant> {
    return { type: 'changed', scopeId: ORG, data, operation: 'update', tenant: TENANT };
}

/** fm's `tenantFanout(cls)`, in the exact shape fm ships. */
function tenantFanout(observed: unknown[]) {
    return {
        tenantNamespace: (tenant: Tenant) => tenant.dbName,
        runInContext: <R>(ctx: { tenant?: unknown }, fn: () => Promise<R>): Promise<R> => {
            observed.push(ctx.tenant);
            // fm branches on the VALUE here; the library wraps on hook PRESENCE.
            return fn();
        },
        tenantOf: (socket: RemoteSocket<never, unknown>) => (socket.data as { tenant?: Tenant } | undefined)?.tenant,
    };
}

let server: Server;
let port: number;

beforeEach(async () => {
    const started = await startHarness();
    server = started.server;
    port = started.port;
});

afterEach(() => stopHarness());

describe('fm entity fan-out contract', () => {
    it('runs prepareEvent once per event and canReceive once per socket, each in context', async () => {
        const allowed = await connectClient(port);
        const denied = await connectClient(port);
        await joinRoomWithData(allowed, ORG_ROOM, { tenant: TENANT, allow: true });
        await joinRoomWithData(denied, ORG_ROOM, { tenant: TENANT, allow: false });

        const allowedEvents = receivedEvents(allowed, 'thing:changed');
        const deniedEvents = receivedEvents(denied, 'thing:changed');

        const contexts: unknown[] = [];
        const preparedSeen: unknown[] = [];
        let prepareCalls = 0;
        let canReceiveCalls = 0;

        await pushScopedFiltered<Tenant>(
            server,
            gen([changed({ _id: '1', name: 'a' })]),
            { changed: 'thing:changed', removed: 'thing:removed' },
            {
                shutdown: new ValueSignal(true),
                ...tenantFanout(contexts),
                canReceive: (socket, _data, prepared) => {
                    canReceiveCalls += 1;
                    preparedSeen.push(prepared);
                    return (socket.data as { allow?: boolean }).allow === true;
                },
                prepareEvent: () => {
                    prepareCalls += 1;
                    return { scope: 'property-1' };
                },
                mapData: data => ({ ...(data as object), mapped: true }),
            }
        );

        await waitForCount(allowedEvents, 1);
        await delay(100);

        expect(prepareCalls).toBe(1);
        expect(canReceiveCalls).toBe(2);
        // The once-per-event value reaches every recipient.
        expect(preparedSeen).toEqual([{ scope: 'property-1' }, { scope: 'property-1' }]);
        // Once for the event block, once per recipient socket.
        expect(contexts).toEqual([TENANT, TENANT, TENANT]);
        expect(allowedEvents).toEqual([{ _id: '1', name: 'a', mapped: true }]);
        expect(deniedEvents).toEqual([]);
    });

    it('reports ctx.tenant undefined for a socket that has no tenant', async () => {
        // fm's adapter branches on ctx.tenant so a tenantless socket does not
        // enter CLS. This pins that the library reports the socket's own value
        // faithfully rather than substituting the event's tenant.
        const client = await connectClient(port);
        await joinRoomWithData(client, ORG_ROOM, {});
        const events = receivedEvents(client, 'thing:changed');

        const contexts: unknown[] = [];
        await pushScopedFiltered<Tenant>(
            server,
            gen([changed({ _id: '1' })]),
            { changed: 'thing:changed' },
            {
                shutdown: new ValueSignal(true),
                ...tenantFanout(contexts),
                canReceive: () => true,
            }
        );

        await waitForCount(events, 1);
        // First entry is the once-per-event block (the EVENT has a tenant);
        // the second is this socket's check, which does not.
        expect(contexts).toEqual([TENANT, undefined]);
    });

    it('renders both PII variants once and delivers the one pick selects', async () => {
        const withPii = await connectClient(port);
        const withoutPii = await connectClient(port);
        await joinRoomWithData(withPii, ORG_ROOM, { tenant: TENANT });
        await joinRoomWithData(withPii, PII_ROOM, { tenant: TENANT });
        await joinRoomWithData(withoutPii, ORG_ROOM, { tenant: TENANT });

        const fullEvents = receivedEvents(withPii, 'contact:changed');
        const redactedEvents = receivedEvents(withoutPii, 'contact:changed');

        let renderCalls = 0;
        await pushScopedFiltered<Tenant>(
            server,
            gen([changed({ _id: '1', email: 'a@b.c' })]),
            { changed: 'contact:changed' },
            {
                shutdown: new ValueSignal(true),
                ...tenantFanout([]),
                canReceive: () => true,
                variants: {
                    render: data => {
                        renderCalls += 1;
                        return {
                            full: data,
                            redacted: { ...(data as object), email: null, piiRedacted: true },
                        };
                    },
                    pick: socket => (socket.rooms.has(PII_ROOM) ? 'full' : 'redacted'),
                },
            }
        );

        await waitForCount(fullEvents, 1);
        await waitForCount(redactedEvents, 1);

        expect(renderCalls).toBe(1);
        expect(fullEvents).toEqual([{ _id: '1', email: 'a@b.c' }]);
        expect(redactedEvents).toEqual([{ _id: '1', email: null, piiRedacted: true }]);
    });

    it('rejects mapData and variants supplied together', async () => {
        await expect(
            pushScopedFiltered<Tenant>(
                server,
                gen([]),
                { changed: 'thing:changed' },
                {
                    shutdown: new ValueSignal(true),
                    ...tenantFanout([]),
                    canReceive: () => true,
                    mapData: data => data,
                    variants: { render: data => ({ full: data, redacted: data }), pick: () => 'full' },
                }
            )
        ).rejects.toThrow(/mutually exclusive/);
    });

    it('emits a removed event to the whole room without consulting canReceive', async () => {
        // fm relies on this: a delete carries no document, so a scope-derived
        // canReceive would deny every recipient and the deleted row would
        // linger in their lists until a refresh.
        const client = await connectClient(port);
        await joinRoomWithData(client, ORG_ROOM, { tenant: TENANT });
        const events = receivedEvents(client, 'thing:removed');

        let canReceiveCalls = 0;
        await pushScopedFiltered<Tenant>(
            server,
            gen([{ type: 'removed', id: '42', scopeId: ORG, via: 'delete', tenant: TENANT }]),
            { changed: 'thing:changed', removed: 'thing:removed' },
            {
                shutdown: new ValueSignal(true),
                ...tenantFanout([]),
                canReceive: () => {
                    canReceiveCalls += 1;
                    return false;
                },
            }
        );

        await waitForCount(events, 1);
        expect(events).toEqual(['42']);
        expect(canReceiveCalls).toBe(0);
    });

    it('routes per-owner events by ownerIdOf, and removals by event.ownerId', async () => {
        const owner = await connectClient(port);
        const other = await connectClient(port);
        const ownerRoom = namespacedRoom(TENANT.dbName, userRoom('user-1'));
        const otherRoom = namespacedRoom(TENANT.dbName, userRoom('user-2'));
        await joinRoomWithData(owner, ownerRoom, { tenant: TENANT });
        await joinRoomWithData(other, otherRoom, { tenant: TENANT });

        const ownerChanged = receivedEvents(owner, 'notification');
        const ownerRemoved = receivedEvents(owner, 'notification:removed');
        const otherChanged = receivedEvents(other, 'notification');

        await pushOwnerScoped<Tenant>(
            server,
            gen([
                changed({ _id: '1', userId: 'user-1' }),
                { type: 'removed', id: '1', ownerId: 'user-1', via: 'delete', tenant: TENANT },
            ]),
            { changed: 'notification', removed: 'notification:removed' },
            {
                shutdown: new ValueSignal(true),
                tenantNamespace: (tenant: Tenant) => tenant.dbName,
                ownerIdOf: data => (data as { userId?: string }).userId,
            }
        );

        await waitForCount(ownerChanged, 1);
        await waitForCount(ownerRemoved, 1);
        await delay(100);

        expect(ownerChanged).toEqual([{ _id: '1', userId: 'user-1' }]);
        expect(ownerRemoved).toEqual(['1']);
        expect(otherChanged).toEqual([]);
    });
});
