import { randomUUID } from 'node:crypto';
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
});
