import { randomUUID } from 'node:crypto';
import { describe, it, expect, afterAll } from 'vitest';
import { ValueSignal } from '../../../src/common/signal.js';
import { watchAcrossTargets } from '../../../src/core/watch-across-targets.js';
import { ChangeStreamHub } from '../../../src/core/change-stream-hub.js';
import type { ChangeEvent, EntityChanged } from '../../../src/core/change-event.js';
import { startReplSet, stopReplSet, uniqueCollection } from '../../helpers/replset.js';
import { delay } from '../../helpers/timing.js';

/**
 * CONTRACT: fm's multi-tenant realtime watcher (see ./README.md).
 *
 * fm runs one database per tenant and fans a single socket.io `Server` out
 * across all of them. Its adoption design wires `watchAcrossTargets` in HUB
 * mode — one `ChangeStreamHub` per pod, one DB-level `db.watch()` per
 * connected tenant, rather than one collection-level cursor per (tenant,
 * model) pair. This pins two things fm's design depends on:
 *
 *   1. tenant isolation: two targets sharing a collection NAME across two
 *      DIFFERENT databases never cross-deliver, and every event carries the
 *      tenant of the database it came from;
 *   2. `connectedKeys`-driven parking: a tenant with no connected socket on
 *      this pod is parked (no open cursor) rather than watched forever, and
 *      reconnecting resumes losslessly from the captured resume point — an
 *      insert made while parked is not lost.
 *
 * Consumption style mirrors `tests/core/watch-across-targets.test.ts`
 * exactly: a `for await` loop started as an immediately-invoked async
 * function (NOT a lazily-started wrapper like `trackNext`), so the
 * underlying hub subscriptions actually open before the settle delay that
 * precedes the first write — `watchAcrossTargets` is an async generator, and
 * nothing in its body runs until something first calls `.next()` on it.
 */

interface Tenant {
    slug: string;
}

function asChanged(event: ChangeEvent<Tenant>): EntityChanged<Tenant> {
    if (event.type !== 'changed') throw new Error(`expected a 'changed' event, got '${event.type}'`);
    return event;
}

function nameOf(event: ChangeEvent<Tenant>): unknown {
    return (asChanged(event).data as { name?: string }).name;
}

/** Bounded poll so a regression that drops/delays an event fails fast with a clear message. */
async function waitForLength<T>(arr: T[], length: number, ms = 8_000): Promise<void> {
    const start = Date.now();
    while (arr.length < length) {
        if (Date.now() - start > ms) {
            throw new Error(`expected ${length} event(s) within ${ms}ms but got ${arr.length}`);
        }
        await delay(20);
    }
}

describe('fm multi-tenant watch contract', () => {
    afterAll(() => stopReplSet());

    it("hub mode isolates tenants: each target sees only its own database's events", async () => {
        const { client } = await startReplSet();
        const dbA = client.db(`mtw-a-${randomUUID().slice(0, 8)}`);
        const dbB = client.db(`mtw-b-${randomUUID().slice(0, 8)}`);
        const collection = uniqueCollection('widgets');
        await dbA.createCollection(collection);
        await dbB.createCollection(collection);

        const connectedKeys = new ValueSignal<string[]>(['a', 'b']);
        const hub = new ChangeStreamHub({ connectedKeys, graceMs: 300 });
        const shutdown = new ValueSignal(false);

        const seen: ChangeEvent<Tenant>[] = [];
        const collecting = (async () => {
            for await (const event of watchAcrossTargets<Tenant>({
                targets: [
                    { key: 'a', db: dbA, collection, tenant: { slug: 'a' } },
                    { key: 'b', db: dbB, collection, tenant: { slug: 'b' } },
                ],
                hub,
                shutdown,
                name: 'widgets',
            })) {
                seen.push(event);
            }
        })();

        // Teardown runs even if an assertion below throws, so a failing
        // assertion can never leak the hub's open cursors and dangling
        // `for await` loop into afterAll (see docs/follow-ups-0.1.x.md).
        try {
            // Give both hub-level change streams time to open before writing.
            await delay(1_000);
            // Write to BOTH databases: a regression where target B's stream
            // never opens at all must fail this test, not pass it silently
            // because B was never exercised.
            await dbA.collection(collection).insertOne({ organizationId: 'org-a', name: 'from-a' });
            await dbB.collection(collection).insertOne({ organizationId: 'org-b', name: 'from-b' });

            await waitForLength(seen, 2);
            // Settle a bit longer: proves nothing further trickles in beyond
            // exactly one event per tenant — no duplicate delivery of either
            // insert, and no cross-tenant leakage in either direction.
            await delay(500);

            expect(seen).toHaveLength(2);
            const bySlug = new Map(seen.map(event => [asChanged(event).tenant?.slug, event]));
            const fromA = bySlug.get('a');
            const fromB = bySlug.get('b');
            expect(fromA).toBeDefined();
            expect(fromB).toBeDefined();
            expect(asChanged(fromA!).scopeId).toBe('org-a');
            expect(nameOf(fromA!)).toBe('from-a');
            expect(asChanged(fromB!).scopeId).toBe('org-b');
            expect(nameOf(fromB!)).toBe('from-b');
        } finally {
            shutdown.next(true);
            await collecting;
            await hub.close();
        }
    });

    it('parks a key absent from connectedKeys and resumes losslessly when it returns', async () => {
        const { client } = await startReplSet();
        const dbA = client.db(`mtw-park-${randomUUID().slice(0, 8)}`);
        const collection = uniqueCollection('widgets');
        await dbA.createCollection(collection);

        const graceMs = 300;
        const connectedKeys = new ValueSignal<string[]>(['a']);
        const hub = new ChangeStreamHub({ connectedKeys, graceMs });
        const shutdown = new ValueSignal(false);

        const seen: ChangeEvent<Tenant>[] = [];
        const collecting = (async () => {
            for await (const event of watchAcrossTargets<Tenant>({
                targets: [{ key: 'a', db: dbA, collection, tenant: { slug: 'a' } }],
                hub,
                shutdown,
                name: 'widgets',
            })) {
                seen.push(event);
            }
        })();

        // Teardown runs even if an assertion below throws — see the same
        // comment on the isolation test above.
        try {
            await delay(1_000);

            // Sanity + establishes a resume point: connected, so a live insert is
            // delivered normally, proving the hub's DB-level cursor is actually
            // open before we park it below.
            await dbA.collection(collection).insertOne({ organizationId: 'org-a', name: 'before-park' });
            await waitForLength(seen, 1);
            expect(nameOf(seen[0])).toBe('before-park');

            // Disconnect this pod from tenant 'a' and outlast the grace window —
            // the hub actually parks the watch (no open cursor), not merely
            // pausing delivery.
            connectedKeys.next([]);
            await delay(graceMs + 500);

            // Insert while parked: no cursor is open, so nothing is delivered yet.
            await dbA.collection(collection).insertOne({ organizationId: 'org-a', name: 'while-parked' });
            await delay(500);
            expect(seen).toHaveLength(1); // still just 'before-park' — nothing arrived while parked

            // Reconnect: the hub reopens the watch from its last captured resume
            // point, so the insert made during the park is not lost.
            connectedKeys.next(['a']);
            await waitForLength(seen, 2);

            const resumed = seen.find(e => nameOf(e) === 'while-parked');
            expect(resumed).toBeDefined();
            const resumedChanged = asChanged(resumed!);
            expect(resumedChanged.tenant).toEqual({ slug: 'a' });
            expect(resumedChanged.scopeId).toBe('org-a');
        } finally {
            shutdown.next(true);
            await collecting;
            await hub.close();
        }
    });
});
