import { describe, it, expect } from 'vitest';
import type { Server } from 'socket.io';
import { SocketServerHandle } from '../../src/nestjs/socket-server-handle.js';

const fakeServer = {} as Server;

describe('SocketServerHandle', () => {
    it('is not ready before a server is set', () => {
        expect(new SocketServerHandle().isReady()).toBe(false);
    });

    it('get() throws with an actionable message before a server is set', () => {
        expect(() => new SocketServerHandle().get()).toThrow(/afterInit/);
    });

    it('get() returns the server once set', () => {
        const handle = new SocketServerHandle();
        handle.setServer(fakeServer);
        expect(handle.isReady()).toBe(true);
        expect(handle.get()).toBe(fakeServer);
    });

    it('whenReady() resolves for a server set after the wait began', async () => {
        const handle = new SocketServerHandle();
        const waiting = handle.whenReady();
        handle.setServer(fakeServer);
        await expect(waiting).resolves.toBe(fakeServer);
    });

    it('whenReady() resolves immediately for a server already set', async () => {
        const handle = new SocketServerHandle();
        handle.setServer(fakeServer);
        await expect(handle.whenReady()).resolves.toBe(fakeServer);
    });
});
