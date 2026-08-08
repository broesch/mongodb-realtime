import type { Server } from 'socket.io';

/**
 * Late-bound holder for the socket.io `Server`.
 *
 * A Nest gateway constructs the server itself and only surfaces it in
 * `afterInit`, so a pusher cannot inject it directly — and the ordering of
 * `afterInit` against `onApplicationBootstrap` is not something an
 * application should have to depend on. The gateway calls `setServer` in
 * `afterInit`; a pusher awaits `whenReady()` at the top of its loop and is
 * correct either way round.
 */
export class SocketServerHandle {
    private server: Server | null = null;
    private resolveReady!: (server: Server) => void;
    private readonly ready: Promise<Server> = new Promise<Server>(resolve => {
        this.resolveReady = resolve;
    });

    /**
     * Call from the gateway's `afterInit(server)`. Idempotent; a re-set replaces
     * the handle for later readers of `get()`/`isReady()`, which always reflect
     * the LATEST server passed here. `whenReady()` is the odd one out: its
     * promise resolves permanently to the FIRST server ever set — a later
     * re-set does not re-resolve it, so a caller already past its
     * `await whenReady()` keeps whichever server it first got, not this one.
     */
    setServer(server: Server): void {
        this.server = server;
        // Resolving an already-resolved promise is a no-op, so a re-set never
        // rejects or double-resolves an in-flight `whenReady()`.
        this.resolveReady(server);
    }

    isReady(): boolean {
        return this.server !== null;
    }

    /** The server, or a throw naming the fix. Prefer `whenReady()` anywhere an await is possible. */
    get(): Server {
        if (!this.server) {
            throw new Error(
                'Socket.io server is not available yet — call SocketServerHandle.setServer(server) from your ' +
                    "gateway's afterInit(server) hook, or await whenReady() instead of get()."
            );
        }
        return this.server;
    }

    /** Resolves once the gateway has handed the server over; already-resolved if it has. */
    whenReady(): Promise<Server> {
        return this.ready;
    }
}
