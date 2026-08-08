/**
 * Shared socket.io test harness (Task 12 + reused by Task 13's NestJS
 * gateway tests). Boots a real `Server` on an ephemeral http port and
 * connects real `socket.io-client` sockets against it, so `pushScoped` /
 * `pushOwnerScoped` are exercised against actual room delivery rather than a
 * mocked `Server`. The server joins a connected client to a room on a
 * `join` message (acked, so a test can await the join before emitting).
 */
import { createServer, type Server as HttpServer } from 'node:http';
import { Server } from 'socket.io';
import { io as ioClient, type Socket as ClientSocket } from 'socket.io-client';

interface RunningHarness {
    httpServer: HttpServer;
    server: Server;
    clients: Set<ClientSocket>;
}

let running: RunningHarness | undefined;

export interface StartedHarness {
    server: Server;
    port: number;
}

/** Boots the server. Call once per test (paired with `stopHarness`). */
export async function startHarness(): Promise<StartedHarness> {
    if (running) throw new Error('startHarness called while a harness is already running — call stopHarness first');

    const httpServer = createServer();
    const server = new Server(httpServer, { transports: ['websocket'] });
    server.on('connection', socket => {
        socket.on('join', (room: string, ack?: () => void) => {
            void socket.join(room);
            ack?.();
        });
    });

    await new Promise<void>((resolve, reject) => {
        httpServer.once('error', reject);
        httpServer.listen(0, resolve);
    });
    const address = httpServer.address();
    const port = typeof address === 'object' && address !== null ? address.port : 0;

    running = { httpServer, server, clients: new Set() };
    return { server, port };
}

/** Tears down every connected client and the server started by `startHarness`. */
export async function stopHarness(): Promise<void> {
    if (!running) return;
    const { httpServer, server, clients } = running;
    running = undefined;
    for (const client of clients) client.disconnect();
    await new Promise<void>(resolve => {
        void server.close(() => resolve());
    });
    await new Promise<void>(resolve => httpServer.close(() => resolve()));
}

/** Connects a client socket to the running harness and waits for the connection to open. */
export async function connectClient(port: number): Promise<ClientSocket> {
    if (!running) throw new Error('connectClient called before startHarness');
    const client = ioClient(`http://localhost:${port}`, {
        transports: ['websocket'],
        reconnection: false,
    });
    running.clients.add(client);
    await new Promise<void>((resolve, reject) => {
        client.once('connect', () => resolve());
        client.once('connect_error', reject);
    });
    return client;
}

/** Has `client`'s server-side socket join `room`, resolving once the server has acked the join. */
export async function joinRoom(client: ClientSocket, room: string): Promise<void> {
    await new Promise<void>(resolve => {
        client.emit('join', room, () => resolve());
    });
}

/** Starts collecting every payload `client` receives for `eventName`. Returns the live (mutated in place) array. */
export function receivedEvents(client: ClientSocket, eventName: string): unknown[] {
    const events: unknown[] = [];
    client.on(eventName, (payload: unknown) => events.push(payload));
    return events;
}

export const delay = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Polls `events` (as returned by `receivedEvents`) until it has at least
 * `count` entries or `ms` elapses. Bounded so a regression that drops an
 * emission fails the test instead of hanging the suite.
 */
export async function waitForCount(events: unknown[], count: number, ms = 2_000): Promise<void> {
    const start = Date.now();
    while (events.length < count) {
        if (Date.now() - start > ms) {
            throw new Error(`expected ${count} event(s) within ${ms}ms but got ${events.length}`);
        }
        await delay(10);
    }
}

export type { ClientSocket };
