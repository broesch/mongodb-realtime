import { ValueSignal, type ReadonlySignal } from '../common/signal.js';
import { DEFAULT_KEY } from '../core/db-resolver.js';

/** A single client's live connection: one entry per connected socket/clientId. */
export interface ConnectionEntry {
    clientId: string;
    userId: string;
    scopeId: string;
    key: string;
    meta?: unknown;
}

/**
 * Per-node in-memory registry of currently connected clients, with derived
 * scope/key signals for driving change-stream watch scope (fm
 * `online-user.service.ts`'s `_userOnServer`/`recomputeOrgs`/`recomputeTenants`).
 *
 * Every mutation replaces `connectedUsers` with a brand-new array — the array
 * a subscriber captured from an earlier emission is never mutated by a later
 * `add`/`remove` (fm's older sibling had an in-place-mutation defect here).
 * `connectedScopes`/`connectedKeys` only emit when the distinct, sorted set
 * they derive actually changes.
 */
export class ConnectionRegistry {
    private readonly _connectedUsers = new ValueSignal<ConnectionEntry[]>([]);
    private readonly _connectedScopes = new ValueSignal<string[]>([]);
    private readonly _connectedKeys = new ValueSignal<string[]>([]);

    readonly connectedUsers: ReadonlySignal<ConnectionEntry[]> = this._connectedUsers;
    /** Distinct scopeIds with at least one connection on this node, sorted. */
    readonly connectedScopes: ReadonlySignal<string[]> = this._connectedScopes;
    /** Distinct keys with at least one connection on this node, sorted. */
    readonly connectedKeys: ReadonlySignal<string[]> = this._connectedKeys;

    /** Adds a connection. An existing entry for the same `clientId` is replaced, not duplicated. */
    add(entry: { clientId: string; userId: string; scopeId: string; key?: string; meta?: unknown }): void {
        const next: ConnectionEntry = {
            clientId: entry.clientId,
            userId: entry.userId,
            scopeId: entry.scopeId,
            key: entry.key ?? DEFAULT_KEY,
            meta: entry.meta,
        };
        const updated = this._connectedUsers.getValue().filter(u => u.clientId !== entry.clientId);
        updated.push(next);
        this.publish(updated);
    }

    /** Removes the connection for `clientId`. Returns the removed entry, or `undefined` if it wasn't present. */
    remove(clientId: string): ConnectionEntry | undefined {
        const current = this._connectedUsers.getValue();
        const found = current.find(u => u.clientId === clientId);
        if (!found) return undefined;

        this.publish(current.filter(u => u.clientId !== clientId));
        return found;
    }

    byScope(scopeId: string): ConnectionEntry[] {
        return this._connectedUsers.getValue().filter(u => u.scopeId === scopeId);
    }

    byClientId(clientId: string): ConnectionEntry | undefined {
        return this._connectedUsers.getValue().find(u => u.clientId === clientId);
    }

    private publish(updated: ConnectionEntry[]): void {
        this._connectedUsers.next(updated);
        this.recomputeDistinct(this._connectedScopes, updated, u => u.scopeId);
        this.recomputeDistinct(this._connectedKeys, updated, u => u.key);
    }

    private recomputeDistinct(
        signal: ValueSignal<string[]>,
        users: ConnectionEntry[],
        select: (u: ConnectionEntry) => string
    ): void {
        const next = Array.from(new Set(users.map(select))).sort();
        const prev = signal.getValue();
        if (prev.length !== next.length || prev.some((v, i) => v !== next[i])) {
            signal.next(next);
        }
    }
}
