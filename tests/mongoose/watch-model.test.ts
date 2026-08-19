import { randomUUID } from 'node:crypto';
import mongoose from 'mongoose';
import { ValueSignal } from '../../src/common/signal.js';
import { modelTarget, watchModel } from '../../src/mongoose/index.js';
import { DEFAULT_KEY } from '../../src/core/db-resolver.js';
import type { ChangeEvent } from '../../src/core/change-event.js';
import { startReplSet, stopReplSet, uniqueCollection } from '../helpers/replset.js';

describe('watchModel', () => {
    const connections: mongoose.Connection[] = [];

    afterAll(async () => {
        await Promise.all(connections.map(c => c.close()));
        await stopReplSet();
    });

    async function connect(dbName: string): Promise<mongoose.Connection> {
        const { uri } = await startReplSet();
        const connection = mongoose.createConnection(uri, { dbName });
        await connection.asPromise();
        connections.push(connection);
        return connection;
    }

    it('modelTarget reads the driver Db and collection name off a real mongoose model', async () => {
        const dbName = `wm-${randomUUID().slice(0, 8)}`;
        const connection = await connect(dbName);
        const collectionName = uniqueCollection('widgets');
        const model = connection.model('Widget', new mongoose.Schema({ organizationId: String }), collectionName);

        const target = modelTarget(model as never);
        expect(target.key).toBe(DEFAULT_KEY);
        expect(target.collection).toBe(collectionName);
        expect(target.db.databaseName).toBe(dbName);
    });

    it("watches two tenants' models and tags each event", async () => {
        const collectionName = uniqueCollection('widgets');
        const schema = new mongoose.Schema({ organizationId: String, name: String });
        const connA = await connect(`wm-a-${randomUUID().slice(0, 8)}`);
        const connB = await connect(`wm-b-${randomUUID().slice(0, 8)}`);
        const modelA = connA.model('WidgetA', schema, collectionName);
        const modelB = connB.model('WidgetB', schema, collectionName);
        await modelA.createCollection();
        await modelB.createCollection();

        const shutdown = new ValueSignal(false);
        const seen: ChangeEvent<{ slug: string }>[] = [];
        const collecting = (async () => {
            for await (const event of watchModel<{ slug: string }>({
                models: [
                    { model: modelA as never, key: 'a', tenant: { slug: 'a' } },
                    { model: modelB as never, key: 'b', tenant: { slug: 'b' } },
                ],
                shutdown,
                name: 'widgets',
            })) {
                seen.push(event);
                if (seen.length === 2) break;
            }
        })();

        await new Promise(resolve => setTimeout(resolve, 1_000));
        await modelA.create({ organizationId: 'org-a', name: 'a' });
        await modelB.create({ organizationId: 'org-b', name: 'b' });

        await collecting;
        shutdown.next(true);

        expect(new Set(seen.map(e => e.tenant?.slug))).toEqual(new Set(['a', 'b']));
    });

    it('throws a named error when a model has no driver database handle', () => {
        const detached = { modelName: 'Detached', db: { db: null }, collection: { name: 'x' } };
        expect(() => modelTarget(detached)).toThrow(/Detached/);
    });
});
