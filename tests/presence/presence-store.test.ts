import { randomUUID } from 'node:crypto';
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

        const indexes = await store.collection.indexes();
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
        const rows = await store.collection.countDocuments({ userId, clientId, serverId: store.serverId });
        expect(rows).toBe(1);
    });

    it('checkIn stamps userId/clientId/serverId/scopeId/key/lastSeen on the row; key defaults to __default__', async () => {
        const store = await makeStore();
        const userId = randomUUID();
        const clientId = randomUUID();
        const before = new Date();

        await store.checkIn({ userId, clientId, scopeId: 'scope-1', meta: { room: 'lobby' } });

        const row = await store.collection.findOne({ userId, clientId, serverId: store.serverId });
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

        const row = await store.collection.findOne({ userId, clientId, serverId: store.serverId });
        expect(row?.key).toBe('tenant-a');
    });

    it('keepAlive advances lastSeen', async () => {
        const store = await makeStore();
        const userId = randomUUID();
        const clientId = randomUUID();
        await store.checkIn({ userId, clientId, scopeId: 'scope-1' });

        const before = await store.collection.findOne({ userId, clientId, serverId: store.serverId });
        await new Promise(resolve => setTimeout(resolve, 10));
        await store.keepAlive(userId, clientId);
        const after = await store.collection.findOne({ userId, clientId, serverId: store.serverId });

        expect(after?.lastSeen.getTime()).toBeGreaterThan(before!.lastSeen.getTime());
    });

    it('setPage stamps the page field on the row', async () => {
        const store = await makeStore();
        const userId = randomUUID();
        const clientId = randomUUID();
        await store.checkIn({ userId, clientId, scopeId: 'scope-1' });

        await store.setPage(userId, clientId, '/dashboard');

        const row = await store.collection.findOne({ userId, clientId, serverId: store.serverId });
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

        const rowA = await store.collection.findOne({ userId, clientId: clientA, serverId: store.serverId });
        const rowB = await store.collection.findOne({ userId, clientId: clientB, serverId: store.serverId });
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
        await store.collection.insertOne({
            userId,
            clientId: randomUUID(),
            serverId: foreignServerId,
            scopeId: 'scope-1',
            key: '__default__',
            lastSeen: new Date(),
        });

        await store.clearServer();

        const ownRows = await store.collection.countDocuments({ serverId: store.serverId });
        const foreignRows = await store.collection.countDocuments({ serverId: foreignServerId });
        expect(ownRows).toBe(0);
        expect(foreignRows).toBe(1);
    });

    it('countUserRows counts rows across servers for one user', async () => {
        const store = await makeStore();
        const userId = randomUUID();
        await store.checkIn({ userId, clientId: randomUUID(), scopeId: 'scope-1' });
        await store.checkIn({ userId, clientId: randomUUID(), scopeId: 'scope-1' });

        // a row for the same user on a different server
        await store.collection.insertOne({
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
});
