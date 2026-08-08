import type { Db } from 'mongodb';
import type { Logger } from '../common/logger.js';

/** {@link PresenceStoreOptions.collectionName}'s default — also consumed by `RealtimeModule` to default-ignore the presence collection on the hub (see realtime-module.ts). */
export const DEFAULT_PRESENCE_COLLECTION = 'onlineUsers';

/** Fields whose write produces an `activity` event, when the caller doesn't name their own. */
export const DEFAULT_ACTIVITY_FIELDS = ['page'];

/** A single client's presence row: one document per (userId, clientId, serverId). */
export interface PresenceDoc {
    userId: string;
    clientId: string;
    serverId: string;
    scopeId: string;
    key: string; // tenant key, '__default__' when single-DB
    lastSeen: Date;
    page?: string;
    meta?: Record<string, unknown>;
}

export interface PresenceStoreOptions {
    db: Db;
    serverId: string;
    collectionName?: string; // default 'onlineUsers'
    ttlSeconds?: number; // default 300
    logger?: Logger;
}
