import { randomUUID } from 'node:crypto';
import type { Db } from 'mongodb';
import { ValueSignal } from '../../src/common/signal.js';
import { watchAcrossTargets } from '../../src/core/watch-across-targets.js';
import type { ChangeEvent } from '../../src/core/change-event.js';
import { startReplSet, stopReplSet, uniqueCollection } from '../helpers/replset.js';

describe('watchAcrossTargets', () => {
    afterAll(() => stopReplSet());

    it('merges events from every target and tags each with its tenant', async () => {
        const { client } = await startReplSet();
        const dbA = client.db(`wat-a-${randomUUID().slice(0, 8)}`);
        const dbB = client.db(`wat-b-${randomUUID().slice(0, 8)}`);
        const collection = uniqueCollection('widgets');
        await dbA.createCollection(collection);
        await dbB.createCollection(collection);

        const shutdown = new ValueSignal(false);
        const watcher = watchAcrossTargets<{ slug: string }>({
            targets: [
                { key: 'a', db: dbA, collection, tenant: { slug: 'a' } },
                { key: 'b', db: dbB, collection, tenant: { slug: 'b' } },
            ],
            shutdown,
            name: 'widgets',
        });

        const seen: ChangeEvent<{ slug: string }>[] = [];
        const collecting = (async () => {
            for await (const event of watcher) {
                seen.push(event);
                if (seen.length === 2) break;
            }
        })();

        // Give both change streams time to open before writing.
        await new Promise(resolve => setTimeout(resolve, 1_000));
        await dbA.collection(collection).insertOne({ organizationId: 'org-a', name: 'from-a' });
        await dbB.collection(collection).insertOne({ organizationId: 'org-b', name: 'from-b' });

        await collecting;
        shutdown.next(true);

        const bySlug = new Map(seen.map(e => [e.tenant?.slug, e]));
        expect(bySlug.get('a')?.type).toBe('changed');
        expect((bySlug.get('a') as { scopeId: string }).scopeId).toBe('org-a');
        expect((bySlug.get('b') as { scopeId: string }).scopeId).toBe('org-b');
    });

    it('yields nothing and completes immediately when given no targets', async () => {
        const shutdown = new ValueSignal(false);
        const seen: unknown[] = [];
        for await (const event of watchAcrossTargets({ targets: [], shutdown })) {
            seen.push(event);
        }
        expect(seen).toEqual([]);
    });

    // These two guards run before any target's change stream is opened, so
    // they don't need a real replica set — a placeholder `Db` is enough,
    // since it's never touched.
    it('throws when two targets share the same (key, collection) pair', async () => {
        const shutdown = new ValueSignal(false);
        const db = {} as unknown as Db;
        const generator = watchAcrossTargets({
            targets: [
                { key: 'a', db, collection: 'widgets' },
                { key: 'a', db, collection: 'widgets' },
            ],
            shutdown,
        });
        await expect(generator.next()).rejects.toThrow(/duplicate target for key 'a', collection 'widgets'/);
    });

    it('throws when the same key is used with two different Db instances', async () => {
        const shutdown = new ValueSignal(false);
        const dbA = {} as unknown as Db;
        const dbB = {} as unknown as Db;
        const generator = watchAcrossTargets({
            targets: [
                { key: 'a', db: dbA, collection: 'widgets' },
                { key: 'a', db: dbB, collection: 'gadgets' },
            ],
            shutdown,
        });
        await expect(generator.next()).rejects.toThrow(/key 'a' is used with two different Db instances/);
    });

    it('does NOT throw when the same key repeats with the identical Db instance across different collections', async () => {
        const { client } = await startReplSet();
        const db = client.db(`wat-same-${randomUUID().slice(0, 8)}`);
        const widgets = uniqueCollection('widgets');
        const gadgets = uniqueCollection('gadgets');
        await db.createCollection(widgets);
        await db.createCollection(gadgets);

        const shutdown = new ValueSignal(false);
        const generator = watchAcrossTargets<{ slug: string }>({
            targets: [
                { key: 'a', db, collection: widgets, tenant: { slug: 'widgets' } },
                { key: 'a', db, collection: gadgets, tenant: { slug: 'gadgets' } },
            ],
            shutdown,
        });

        const seen: ChangeEvent<{ slug: string }>[] = [];
        const collecting = (async () => {
            for await (const event of generator) {
                seen.push(event);
                if (seen.length === 2) break;
            }
        })();

        await new Promise(resolve => setTimeout(resolve, 1_000));
        await db.collection(widgets).insertOne({ organizationId: 'org-a', name: 'from-widgets' });
        await db.collection(gadgets).insertOne({ organizationId: 'org-a', name: 'from-gadgets' });

        await collecting;
        shutdown.next(true);

        const bySlug = new Map(seen.map(e => [e.tenant?.slug, e]));
        expect(bySlug.get('widgets')).toBeDefined();
        expect(bySlug.get('gadgets')).toBeDefined();
    });
});
