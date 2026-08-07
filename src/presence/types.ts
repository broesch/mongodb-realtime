import type { Db } from 'mongodb';
import type { Logger } from '../common/logger.js';

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
