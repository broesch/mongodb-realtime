/**
 * socket.io room-naming helpers. One `Server` may be shared by many logical
 * scopes (tenants, organizations, users) — these functions are the single
 * source of truth for turning an id into a room key, used symmetrically at
 * both the join site (application code) and the emit site (`pushScoped` /
 * `pushOwnerScoped`).
 */

/** The room carrying a single user's events. */
export function userRoom(id: string): string {
    return `user:${id}`;
}

/** The room carrying every event for a scope (organization/tenant-scoped entity). */
export function scopeRoom(id: string): string {
    return `org:${id}`;
}

/**
 * Namespaces a room by tenant, when a tenant namespace is supplied. Lets one
 * socket.io `Server` serve multiple tenants without id collisions across
 * tenant DBs — `room` alone is not guaranteed globally unique (fixed seed
 * ids are hand-assigned and could collide across tenants).
 */
export function namespacedRoom(ns: string | undefined, room: string): string {
    return ns ? `${ns}::${room}` : room;
}

/**
 * Normalises a room specifier to a list, dropping empty entries. A single
 * `to(rooms)` call with the full list is not the same as one `to()` per room:
 * socket.io deduplicates recipients WITHIN a call, so a socket that belongs
 * to two of the target rooms receives one copy from the former and two from
 * the latter.
 */
export function toRooms(rooms: string | string[]): string[] {
    return (Array.isArray(rooms) ? rooms : [rooms]).filter(Boolean);
}
