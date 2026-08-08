import { describe, it, expect } from 'vitest';
import { userRoom, scopeRoom, namespacedRoom } from '../../src/socketio/rooms.js';

describe('rooms', () => {
    it('userRoom prefixes the id with user:', () => {
        expect(userRoom('u1')).toBe('user:u1');
    });

    it('scopeRoom prefixes the id with org:', () => {
        expect(scopeRoom('o1')).toBe('org:o1');
    });

    it('namespacedRoom returns the room unchanged when ns is undefined', () => {
        expect(namespacedRoom(undefined, 'org:o1')).toBe('org:o1');
    });

    it('namespacedRoom prefixes with ns:: when ns is defined', () => {
        expect(namespacedRoom('tenantA', 'org:o1')).toBe('tenantA::org:o1');
    });

    it('namespacedRoom returns the room unchanged when ns is the empty string', () => {
        expect(namespacedRoom('', 'org:o1')).toBe('org:o1');
    });
});
