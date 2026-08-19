import { ConnectionRegistry, type ConnectionEntry } from '../../src/presence/connection-registry.js';
import { subscribeToChanges } from '../../src/common/signal.js';
import { DEFAULT_KEY } from '../../src/core/db-resolver.js';

describe('ConnectionRegistry', () => {
    it('add exposes the entry via connectedUsers/byScope/byClientId; key defaults to __default__', () => {
        const registry = new ConnectionRegistry();

        registry.add({ clientId: 'c1', userId: 'u1', scopeId: 's1' });

        const expected: ConnectionEntry = { clientId: 'c1', userId: 'u1', scopeId: 's1', key: '__default__' };
        expect(registry.connectedUsers.getValue()).toEqual([expected]);
        expect(registry.byScope('s1')).toEqual([expected]);
        expect(registry.byClientId('c1')).toEqual(expected);
    });

    it('add with an existing clientId replaces that entry (no duplicates)', () => {
        const registry = new ConnectionRegistry();
        registry.add({ clientId: 'c1', userId: 'u1', scopeId: 's1' });

        registry.add({ clientId: 'c1', userId: 'u2', scopeId: 's2', key: 'tenant-a', meta: { rejoined: true } });

        const users = registry.connectedUsers.getValue();
        expect(users).toHaveLength(1);
        expect(users[0]).toEqual({
            clientId: 'c1',
            userId: 'u2',
            scopeId: 's2',
            key: 'tenant-a',
            meta: { rejoined: true },
        });
    });

    it('remove returns the removed entry and updates all three signals; unknown clientId returns undefined with no emission', () => {
        const registry = new ConnectionRegistry();
        registry.add({ clientId: 'c1', userId: 'u1', scopeId: 's1' });

        const usersSeen: ConnectionEntry[][] = [];
        const scopesSeen: string[][] = [];
        const keysSeen: string[][] = [];
        subscribeToChanges(registry.connectedUsers, v => usersSeen.push(v));
        subscribeToChanges(registry.connectedScopes, v => scopesSeen.push(v));
        subscribeToChanges(registry.connectedKeys, v => keysSeen.push(v));

        expect(registry.remove('unknown-client')).toBeUndefined();
        expect(usersSeen).toEqual([]);
        expect(scopesSeen).toEqual([]);
        expect(keysSeen).toEqual([]);

        const removed = registry.remove('c1');

        expect(removed).toEqual({ clientId: 'c1', userId: 'u1', scopeId: 's1', key: '__default__' });
        expect(registry.connectedUsers.getValue()).toEqual([]);
        expect(registry.connectedScopes.getValue()).toEqual([]);
        expect(registry.connectedKeys.getValue()).toEqual([]);
        expect(usersSeen).toEqual([[]]);
        expect(scopesSeen).toEqual([[]]);
        expect(keysSeen).toEqual([[]]);
    });

    it('connectedScopes/connectedKeys are distinct and sorted', () => {
        const registry = new ConnectionRegistry();

        registry.add({ clientId: 'c1', userId: 'u1', scopeId: 's2', key: 'tenant-b' });
        registry.add({ clientId: 'c2', userId: 'u2', scopeId: 's1', key: 'tenant-a' });
        registry.add({ clientId: 'c3', userId: 'u3', scopeId: 's1', key: 'tenant-a' });

        expect(registry.connectedScopes.getValue()).toEqual(['s1', 's2']);
        expect(registry.connectedKeys.getValue()).toEqual(['tenant-a', 'tenant-b']);
    });

    it('derived signals emit only on real set changes: a second user in the same scope/key does not re-emit', () => {
        const registry = new ConnectionRegistry();
        registry.add({ clientId: 'c1', userId: 'u1', scopeId: 's1', key: 'tenant-a' });

        const usersSeen: ConnectionEntry[][] = [];
        const scopesSeen: string[][] = [];
        const keysSeen: string[][] = [];
        subscribeToChanges(registry.connectedUsers, v => usersSeen.push(v));
        subscribeToChanges(registry.connectedScopes, v => scopesSeen.push(v));
        subscribeToChanges(registry.connectedKeys, v => keysSeen.push(v));

        registry.add({ clientId: 'c2', userId: 'u2', scopeId: 's1', key: 'tenant-a' });

        expect(usersSeen).toHaveLength(1); // connectedUsers itself did change...
        expect(scopesSeen).toEqual([]); // ...but the distinct scope set did not
        expect(keysSeen).toEqual([]); // ...nor did the distinct key set

        registry.add({ clientId: 'c3', userId: 'u3', scopeId: 's2', key: 'tenant-b' });

        expect(scopesSeen).toEqual([['s1', 's2']]);
        expect(keysSeen).toEqual([['tenant-a', 'tenant-b']]);
    });

    it('updates are copy-on-write: an array captured from an earlier emission is not mutated by later adds/removes', () => {
        const registry = new ConnectionRegistry();
        registry.add({ clientId: 'c1', userId: 'u1', scopeId: 's1' });

        const capturedUsers = registry.connectedUsers.getValue();
        const capturedScopes = registry.connectedScopes.getValue();
        const capturedKeys = registry.connectedKeys.getValue();
        expect(capturedUsers).toHaveLength(1);
        expect(capturedScopes).toEqual(['s1']);
        expect(capturedKeys).toEqual(['__default__']);

        registry.add({ clientId: 'c2', userId: 'u2', scopeId: 's2', key: 'tenant-a' });
        registry.remove('c1');

        // the arrays captured before these mutations are untouched
        expect(capturedUsers).toHaveLength(1);
        expect(capturedUsers[0].clientId).toBe('c1');
        expect(capturedScopes).toEqual(['s1']);
        expect(capturedKeys).toEqual(['__default__']);

        // the registry's current state reflects both mutations
        expect(registry.connectedUsers.getValue()).toEqual([
            { clientId: 'c2', userId: 'u2', scopeId: 's2', key: 'tenant-a' },
        ]);
        expect(registry.connectedScopes.getValue()).toEqual(['s2']);
        expect(registry.connectedKeys.getValue()).toEqual(['tenant-a']);
    });

    it('defaults a keyless connection to the shared DEFAULT_KEY constant', () => {
        const registry = new ConnectionRegistry();
        registry.add({ clientId: 'c1', userId: 'u1', scopeId: 's1' });

        expect(registry.connectedKeys.getValue()).toEqual([DEFAULT_KEY]);
        expect(registry.byClientId('c1')?.key).toBe(DEFAULT_KEY);
    });
});
