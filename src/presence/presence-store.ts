import type { Collection, Db, ObjectId } from 'mongodb';
import { enablePrePostImages } from '../core/enable-pre-post-images.js';
import { consoleLogger, prefixed, type Logger } from '../common/logger.js';
import { DEFAULT_PRESENCE_COLLECTION, type PresenceDoc, type PresenceStoreOptions } from './types.js';

const DEFAULT_TTL_SECONDS = 300;

/**
 * DB-backed presence store: one row per (userId, clientId, serverId), TTL'd
 * on `lastSeen` so a pod that dies without an orderly `clearServer()` still
 * self-heals once the row expires. The in-memory registry that layers on top
 * of this (join/leave hooks, live snapshot) is a later task — this class only
 * owns the collection.
 */
export class PresenceStore {
    readonly serverId: string;
    readonly collection: Collection<PresenceDoc>;
    private readonly db: Db;
    private readonly collectionName: string;
    private readonly ttlSeconds: number;
    private readonly logger: Logger;

    constructor(options: PresenceStoreOptions) {
        this.db = options.db;
        this.serverId = options.serverId;
        this.collectionName = options.collectionName ?? DEFAULT_PRESENCE_COLLECTION;
        this.ttlSeconds = options.ttlSeconds ?? DEFAULT_TTL_SECONDS;
        this.logger = options.logger
            ? prefixed(options.logger, 'PresenceStore')
            : prefixed(consoleLogger, 'PresenceStore');
        this.collection = this.db.collection<PresenceDoc>(this.collectionName);
    }

    /** Idempotent: safe to call on every startup. */
    async ensureIndexes(): Promise<void> {
        await this.collection.createIndex({ userId: 1, clientId: 1, serverId: 1 }, { unique: true });
        await this.collection.createIndex({ lastSeen: 1 }, { expireAfterSeconds: this.ttlSeconds });
        await enablePrePostImages(this.db, [this.collectionName], this.logger);
    }

    async checkIn(entry: {
        userId: string;
        clientId: string;
        scopeId: string;
        key?: string;
        meta?: Record<string, unknown>;
    }): Promise<{ isNewConnection: boolean }> {
        // The DB arbitrates new-vs-rejoin: with upsert + returnDocument 'before', exactly one of any
        // set of concurrent racers for the same key observes `null` (fm audit-2026-05-21 B1d).
        //
        // includeResultMetadata: false is load-bearing, not cosmetic: the peer range's floor,
        // mongodb@5.7.0, defaults findOneAndUpdate to returning a `ModifyResult` wrapper
        // (`{ ok, value, lastErrorObject }`), which is truthy even for a brand-new upsert — so
        // `before === null` would silently always be false on that floor. The option was added in
        // exactly 5.7 (hence the floor) to make the direct-document return shape used below
        // deterministic across the whole declared 5.7–7.x range.
        const before = await this.collection.findOneAndUpdate(
            { userId: entry.userId, clientId: entry.clientId, serverId: this.serverId },
            {
                $set: {
                    userId: entry.userId,
                    clientId: entry.clientId,
                    serverId: this.serverId,
                    scopeId: entry.scopeId,
                    key: entry.key ?? '__default__',
                    meta: entry.meta ?? {},
                    lastSeen: new Date(),
                },
            },
            { upsert: true, returnDocument: 'before', includeResultMetadata: false }
        );
        return { isNewConnection: before === null };
    }

    async keepAlive(userId: string, clientId: string): Promise<void> {
        await this.collection.updateOne(
            { userId, clientId, serverId: this.serverId },
            { $set: { lastSeen: new Date() } }
        );
    }

    async setPage(userId: string, clientId: string, page: string): Promise<void> {
        await this.collection.updateOne({ userId, clientId, serverId: this.serverId }, { $set: { page } });
    }

    async checkOut(userId: string, clientId: string): Promise<void> {
        await this.collection.deleteOne({ userId, clientId, serverId: this.serverId });
    }

    /** Orderly shutdown: drop every row this server owns. */
    async clearServer(): Promise<void> {
        await this.collection.deleteMany({ serverId: this.serverId });
    }

    /** Count of this user's rows across all servers. */
    async countUserRows(userId: string): Promise<number> {
        return this.collection.countDocuments({ userId });
    }

    /**
     * Count of this user's rows (across all servers) whose `_id` sorts before
     * `beforeId` — i.e. rows inserted earlier. Order-stable 0→1 online
     * detection (`watchPresence`) asks "is any older row present?" instead of
     * a live total count: with N concurrent first-time check-ins, every
     * racer's row lands with a distinct `_id`, and this answers identically
     * regardless of when it's evaluated relative to the other inserts —
     * exactly the row with the smallest `_id` sees zero older rows.
     */
    async countOlderUserRows(userId: string, beforeId: ObjectId): Promise<number> {
        return this.collection.countDocuments({ userId, _id: { $lt: beforeId } });
    }
}
