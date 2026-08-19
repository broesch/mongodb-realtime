import { randomUUID } from 'node:crypto';
import type { Db } from 'mongodb';
import { DEFAULT_KEY, type DbResolver } from '../../src/core/db-resolver.js';
import { PresenceStore } from '../../src/presence/presence-store.js';
import { startReplSet, stopReplSet, uniqueCollection } from '../helpers/replset.js';

describe('PresenceStore', () => {
    afterAll(() => stopReplSet());

    async function makeStore(overrides: { ttlSeconds?: number } = {}): Promise<PresenceStore> {
        const { db } = await startReplSet();
        const store = new PresenceStore({
            db,
            serverId: randomUUID(),
            collectionName: uniqueCollection('presence'),
            ttlSeconds: overrides.ttlSeconds ?? 300,
        });
        await store.ensureIndexes();
        return store;
    }

    it('ensureIndexes creates the unique index and a TTL index on lastSeen, and enables pre/post images', async () => {
        const { db } = await startReplSet();
        const collectionName = uniqueCollection('presence');
        const store = new PresenceStore({ db, serverId: randomUUID(), collectionName, ttlSeconds: 120 });
        await store.ensureIndexes();

        const indexes = await store.collectionFor().indexes();
        const unique = indexes.find(idx => idx.unique === true);
        expect(unique?.key).toEqual({ userId: 1, clientId: 1, serverId: 1 });

        const ttl = indexes.find(idx => typeof idx.expireAfterSeconds === 'number');
        expect(ttl?.key).toEqual({ lastSeen: 1 });
        expect(ttl?.expireAfterSeconds).toBe(120);

        const info = await db.listCollections({ name: collectionName }, { nameOnly: false }).toArray();
        expect(info[0]?.options?.changeStreamPreAndPostImages).toEqual({ enabled: true });
    });

    it('first checkIn reports isNewConnection: true; the same (userId, clientId) again reports false', async () => {
        const store = await makeStore();
        const userId = randomUUID();
        const clientId = randomUUID();

        const first = await store.checkIn({ userId, clientId, scopeId: 'scope-1' });
        expect(first.isNewConnection).toBe(true);

        const second = await store.checkIn({ userId, clientId, scopeId: 'scope-1' });
        expect(second.isNewConnection).toBe(false);
    });

    it('10 concurrent checkIns for the same (userId, clientId) yield exactly ONE isNewConnection: true', async () => {
        const store = await makeStore();
        const userId = randomUUID();
        const clientId = randomUUID();

        const results = await Promise.all(
            Array.from({ length: 10 }, () => store.checkIn({ userId, clientId, scopeId: 'scope-1' }))
        );
        const newConnectionCount = results.filter(r => r.isNewConnection).length;
        expect(newConnectionCount).toBe(1);

        // and no duplicate rows were created by the race
        const rows = await store.collectionFor().countDocuments({ userId, clientId, serverId: store.serverId });
        expect(rows).toBe(1);
    });

    it('checkIn stamps userId/clientId/serverId/scopeId/key/lastSeen on the row; key defaults to __default__', async () => {
        const store = await makeStore();
        const userId = randomUUID();
        const clientId = randomUUID();
        const before = new Date();

        await store.checkIn({ userId, clientId, scopeId: 'scope-1', meta: { room: 'lobby' } });

        const row = await store.collectionFor().findOne({ userId, clientId, serverId: store.serverId });
        expect(row).toMatchObject({
            userId,
            clientId,
            serverId: store.serverId,
            scopeId: 'scope-1',
            key: '__default__',
            meta: { room: 'lobby' },
        });
        expect(row?.lastSeen.getTime()).toBeGreaterThanOrEqual(before.getTime());
    });

    it('checkIn honours an explicit key instead of defaulting', async () => {
        const store = await makeStore();
        const userId = randomUUID();
        const clientId = randomUUID();

        await store.checkIn({ userId, clientId, scopeId: 'scope-1', key: 'tenant-a' });

        const row = await store.collectionFor().findOne({ userId, clientId, serverId: store.serverId });
        expect(row?.key).toBe('tenant-a');
    });

    it('keepAlive advances lastSeen', async () => {
        const store = await makeStore();
        const userId = randomUUID();
        const clientId = randomUUID();
        await store.checkIn({ userId, clientId, scopeId: 'scope-1' });

        const before = await store.collectionFor().findOne({ userId, clientId, serverId: store.serverId });
        await new Promise(resolve => setTimeout(resolve, 10));
        await store.keepAlive(userId, clientId);
        const after = await store.collectionFor().findOne({ userId, clientId, serverId: store.serverId });

        expect(after?.lastSeen.getTime()).toBeGreaterThan(before!.lastSeen.getTime());
    });

    it('setPage stamps the page field on the row', async () => {
        const store = await makeStore();
        const userId = randomUUID();
        const clientId = randomUUID();
        await store.checkIn({ userId, clientId, scopeId: 'scope-1' });

        await store.setPage(userId, clientId, '/dashboard');

        const row = await store.collectionFor().findOne({ userId, clientId, serverId: store.serverId });
        expect(row?.page).toBe('/dashboard');
    });

    it('checkOut removes exactly that (userId, clientId, serverId) row — a second client’s row survives', async () => {
        const store = await makeStore();
        const userId = randomUUID();
        const clientA = randomUUID();
        const clientB = randomUUID();
        await store.checkIn({ userId, clientId: clientA, scopeId: 'scope-1' });
        await store.checkIn({ userId, clientId: clientB, scopeId: 'scope-1' });

        await store.checkOut(userId, clientA);

        const rowA = await store.collectionFor().findOne({ userId, clientId: clientA, serverId: store.serverId });
        const rowB = await store.collectionFor().findOne({ userId, clientId: clientB, serverId: store.serverId });
        expect(rowA).toBeNull();
        expect(rowB).not.toBeNull();
    });

    it('clearServer removes only this serverId’s rows', async () => {
        const store = await makeStore();
        const userId = randomUUID();
        const clientId = randomUUID();
        await store.checkIn({ userId, clientId, scopeId: 'scope-1' });

        // seed a row belonging to a different server directly
        const foreignServerId = randomUUID();
        await store.collectionFor().insertOne({
            userId,
            clientId: randomUUID(),
            serverId: foreignServerId,
            scopeId: 'scope-1',
            key: '__default__',
            lastSeen: new Date(),
        });

        await store.clearServer();

        const ownRows = await store.collectionFor().countDocuments({ serverId: store.serverId });
        const foreignRows = await store.collectionFor().countDocuments({ serverId: foreignServerId });
        expect(ownRows).toBe(0);
        expect(foreignRows).toBe(1);
    });

    it('countUserRows counts rows across servers for one user', async () => {
        const store = await makeStore();
        const userId = randomUUID();
        await store.checkIn({ userId, clientId: randomUUID(), scopeId: 'scope-1' });
        await store.checkIn({ userId, clientId: randomUUID(), scopeId: 'scope-1' });

        // a row for the same user on a different server
        await store.collectionFor().insertOne({
            userId,
            clientId: randomUUID(),
            serverId: randomUUID(),
            scopeId: 'scope-1',
            key: '__default__',
            lastSeen: new Date(),
        });

        const count = await store.countUserRows(userId);
        expect(count).toBe(3);
    });

    it('countOlderUserRows counts only this user’s rows inserted before the given _id, across servers', async () => {
        const store = await makeStore();
        const userId = randomUUID();
        const otherUserId = randomUUID();

        const first = await store.collectionFor().insertOne({
            userId,
            clientId: randomUUID(),
            serverId: randomUUID(),
            scopeId: 'scope-1',
            key: '__default__',
            lastSeen: new Date(),
        });
        const second = await store.collectionFor().insertOne({
            userId,
            clientId: randomUUID(),
            serverId: randomUUID(),
            scopeId: 'scope-1',
            key: '__default__',
            lastSeen: new Date(),
        });
        // a row for a DIFFERENT user, inserted between the two above — must not count.
        await store.collectionFor().insertOne({
            userId: otherUserId,
            clientId: randomUUID(),
            serverId: randomUUID(),
            scopeId: 'scope-1',
            key: '__default__',
            lastSeen: new Date(),
        });

        expect(await store.countOlderUserRows(userId, first.insertedId)).toBe(0);
        expect(await store.countOlderUserRows(userId, second.insertedId)).toBe(1);
    });
});

describe('PresenceStore across keys', () => {
    it('routes each key to its own database and keeps counts per key', async () => {
        const { client } = await startReplSet();
        const dbA = client.db(`presence-a-${randomUUID().slice(0, 8)}`);
        const dbB = client.db(`presence-b-${randomUUID().slice(0, 8)}`);
        const resolver: DbResolver = {
            keys: () => ['a', 'b'],
            dbFor: key => (key === 'a' ? dbA : dbB),
        };
        const collectionName = uniqueCollection('presence');
        const store = new PresenceStore({ db: resolver, serverId: randomUUID(), collectionName });
        await store.ensureIndexes();

        await store.checkIn({ userId: 'u1', clientId: 'c1', scopeId: 's1', key: 'a' });
        await store.checkIn({ userId: 'u1', clientId: 'c2', scopeId: 's1', key: 'b' });

        // Same user id in two databases must not be conflated.
        expect(await store.countUserRows('u1', 'a')).toBe(1);
        expect(await store.countUserRows('u1', 'b')).toBe(1);
        expect(await store.collectionFor('a').countDocuments({})).toBe(1);
        expect(await store.collectionFor('b').countDocuments({})).toBe(1);
    });

    it('ensureIndexes and clearServer act on every key', async () => {
        const { client } = await startReplSet();
        const dbA = client.db(`presence-c-${randomUUID().slice(0, 8)}`);
        const dbB = client.db(`presence-d-${randomUUID().slice(0, 8)}`);
        const resolver: DbResolver = { keys: () => ['a', 'b'], dbFor: k => (k === 'a' ? dbA : dbB) };
        const collectionName = uniqueCollection('presence');
        const store = new PresenceStore({ db: resolver, serverId: randomUUID(), collectionName });
        await store.ensureIndexes();

        for (const key of ['a', 'b']) {
            const indexes = await store.collectionFor(key).indexes();
            expect(indexes.find(i => i.unique === true)?.key).toEqual({ userId: 1, clientId: 1, serverId: 1 });
        }

        await store.checkIn({ userId: 'u1', clientId: 'c1', scopeId: 's1', key: 'a' });
        await store.checkIn({ userId: 'u2', clientId: 'c2', scopeId: 's1', key: 'b' });
        await store.clearServer();

        expect(await store.collectionFor('a').countDocuments({})).toBe(0);
        expect(await store.collectionFor('b').countDocuments({})).toBe(0);
    });

    it('a bare Db still works with no key argument anywhere', async () => {
        const { db } = await startReplSet();
        const store = new PresenceStore({ db, serverId: randomUUID(), collectionName: uniqueCollection('presence') });
        await store.ensureIndexes();

        const { isNewConnection } = await store.checkIn({ userId: 'u9', clientId: 'c9', scopeId: 's9' });
        expect(isNewConnection).toBe(true);
        await store.keepAlive('u9', 'c9');
        await store.setPage('u9', 'c9', '/home');
        expect(await store.countUserRows('u9')).toBe(1);
        expect(store.collectionFor().collectionName).toBe(store.collectionFor(DEFAULT_KEY).collectionName);
        await store.checkOut('u9', 'c9');
        expect(await store.countUserRows('u9')).toBe(0);
    });

    it("collectionFor throws an actionable error for a key the resolver doesn't know", () => {
        // A Map-backed resolver (the common shape) returns undefined for an
        // unregistered key, same as a consumer who forgot to add a tenant.
        const resolver: DbResolver = { keys: () => ['a'], dbFor: () => undefined as unknown as Db };
        const store = new PresenceStore({ db: resolver, serverId: randomUUID(), collectionName: 'presence' });
        expect(() => store.collectionFor('missing')).toThrow("No database for key 'missing'");
    });
});
