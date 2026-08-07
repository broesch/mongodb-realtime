import { Db as DbClass } from 'mongodb';
import type { Collection, Db } from 'mongodb';
import { describe, it, expect, afterAll, vi } from 'vitest';
import { ChangeStreamHub } from '../../src/core/change-stream-hub.js';
import { ValueSignal } from '../../src/common/signal.js';
import { startReplSet, stopReplSet, uniqueCollection } from '../helpers/replset.js';

// ---------------------------------------------------------------------------
// These tests drive the hub against a REAL replica set (no fakes/mocks): each
// `subscribe()` opens (or reuses) an actual `db.watch()` cursor, and we insert
// real documents and read what the hub delivers.
// ---------------------------------------------------------------------------

type LooseChangeDoc = {
    operationType: string;
    ns?: { coll?: string };
    fullDocument?: { n?: string | number; prime?: number };
};

const coll = (d: unknown): string | undefined => (d as LooseChangeDoc).ns?.coll;
const n = (d: unknown): string | number | undefined => (d as LooseChangeDoc).fullDocument?.n;

const delay = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Wraps an AsyncGenerator so repeated bounded waits (a `Promise.race` against
 * a timeout) can be composed safely. `AsyncGenerator.next()` calls are
 * serialized by the JS engine — a call "abandoned" by a losing `Promise.race`
 * is still pending internally, and a later `next()` call on the same
 * generator queues behind it rather than starting fresh. This tracker keeps
 * at most one in-flight `next()` call and lets every bounded wait
 * (`expectNoEvent`, `expectEvent`, `collectUntil`) share it until it actually
 * resolves, so a timed-out wait never desyncs a subsequent one.
 */
function trackNext<T>(gen: AsyncGenerator<T>): { next(): Promise<IteratorResult<T>> } {
    let pending: Promise<IteratorResult<T>> | undefined;
    return {
        next(): Promise<IteratorResult<T>> {
            if (!pending) {
                const p = gen.next();
                pending = p;
                void p.finally(() => {
                    if (pending === p) pending = undefined;
                });
            }
            return pending;
        },
    };
}

type Tracker<T> = ReturnType<typeof trackNext<T>>;

/** Bounded wait: resolves 'timeout' if no event arrives within `ms`. */
async function expectNoEvent<T>(tracker: Tracker<T>, ms = 300): Promise<void> {
    const result = await Promise.race([tracker.next(), delay(ms).then(() => 'timeout' as const)]);
    if (result === 'timeout') return;
    throw new Error(`expected no event within ${ms}ms but got: ${JSON.stringify(result)}`);
}

/** Bounded wait for the next event, so a stuck stream fails fast with a clear message instead of the suite timeout. */
async function expectEvent<T>(tracker: Tracker<T>, ms = 10_000): Promise<T> {
    const result = await Promise.race([tracker.next(), delay(ms).then(() => 'timeout' as const)]);
    if (result === 'timeout') throw new Error(`expected an event within ${ms}ms but none arrived`);
    if (result.done) throw new Error('generator ended unexpectedly while waiting for an event');
    return result.value;
}

/**
 * A DB-level `db.watch()` cursor only sees inserts that happen AFTER it has
 * actually been established — a naive insert-then-assert races the async
 * watch-open. This primes the shared cursor for `key` by repeatedly inserting
 * into a dedicated throwaway collection (via its own subscriber) until one
 * insert is observed, proving the cursor is live. Priming inserts made before
 * the cursor opens are simply never delivered (no resume token exists yet
 * on a first-ever open), so retrying is safe.
 */
async function waitForStreamOpen(hub: ChangeStreamHub, db: Db, key: string): Promise<void> {
    const primeCollName = uniqueCollection('prime');
    const primeColl: Collection = db.collection(primeCollName);
    const primeGen = trackNext(hub.subscribe(key, db, primeCollName));
    for (let attempt = 0; attempt < 30; attempt++) {
        await primeColl.insertOne({ prime: attempt });
        const result = await Promise.race([primeGen.next(), delay(200).then(() => 'timeout' as const)]);
        if (result !== 'timeout' && !result.done) return;
    }
    throw new Error('change stream never opened within timeout');
}

/** Reads events off `tracker` until one matches `predicate`, bounded by `maxEvents` / a per-read timeout. */
async function collectUntil<T>(
    tracker: Tracker<T>,
    predicate: (value: T) => boolean,
    maxEvents = 5,
    timeoutMs = 5_000
): Promise<T> {
    for (let i = 0; i < maxEvents; i++) {
        const result = await Promise.race([tracker.next(), delay(timeoutMs).then(() => 'timeout' as const)]);
        if (result === 'timeout') throw new Error('timed out waiting for expected event');
        if (result.done) throw new Error('generator ended unexpectedly while waiting for expected event');
        if (predicate(result.value)) return result.value;
    }
    throw new Error(`expected event not seen within ${maxEvents} events`);
}

describe('ChangeStreamHub', () => {
    afterAll(() => stopReplSet());

    it('demuxes: subscribers on different collections each receive only their collection’s docs', async () => {
        const { db } = await startReplSet();
        const key = uniqueCollection('demux-key');
        const collA = uniqueCollection('demux-a');
        const collB = uniqueCollection('demux-b');

        const hub = new ChangeStreamHub();
        const genA = hub.subscribe(key, db, collA);
        const genB = trackNext(hub.subscribe(key, db, collB));
        await waitForStreamOpen(hub, db, key);

        await db.collection(collA).insertOne({ n: 'a1' });
        await db.collection(collB).insertOne({ n: 'b1' });
        await db.collection(collA).insertOne({ n: 'a2' });

        const a1 = await genA.next();
        const a2 = await genA.next();
        const b1 = await genB.next();

        expect(coll(a1.value)).toBe(collA);
        expect(n(a1.value)).toBe('a1');
        expect(coll(a2.value)).toBe(collA);
        expect(n(a2.value)).toBe('a2');
        expect(coll(b1.value)).toBe(collB);
        expect(n(b1.value)).toBe('b1');

        // No B (or other) events leaked into A's consumer beyond what it got.
        await expectNoEvent(genB);

        await hub.close();
    });

    it('fanout: two subscribers on the same collection both receive each change', async () => {
        const { db } = await startReplSet();
        const key = uniqueCollection('fanout-key');
        const collName = uniqueCollection('fanout-coll');

        const hub = new ChangeStreamHub();
        const gen1 = hub.subscribe(key, db, collName);
        const gen2 = hub.subscribe(key, db, collName);
        await waitForStreamOpen(hub, db, key);

        await db.collection(collName).insertOne({ n: 'shared' });

        const r1 = await gen1.next();
        const r2 = await gen2.next();

        expect(coll(r1.value)).toBe(collName);
        expect(n(r1.value)).toBe('shared');
        expect(coll(r2.value)).toBe(collName);
        expect(n(r2.value)).toBe('shared');

        await hub.close();
    });

    it('ignoreCollections: changes there are never delivered to any subscriber', async () => {
        const { db } = await startReplSet();
        const key = uniqueCollection('ignore-key');
        const ignoredColl = uniqueCollection('ignored');
        const normalColl = uniqueCollection('normal');

        const hub = new ChangeStreamHub({ ignoreCollections: [ignoredColl] });
        const ignoredGen = trackNext(hub.subscribe(key, db, ignoredColl));
        const normalGen = hub.subscribe(key, db, normalColl);
        await waitForStreamOpen(hub, db, key);

        await db.collection(ignoredColl).insertOne({ n: 'should-not-arrive' });
        await db.collection(normalColl).insertOne({ n: 'should-arrive' });

        const normalResult = await normalGen.next();
        expect(coll(normalResult.value)).toBe(normalColl);
        expect(n(normalResult.value)).toBe('should-arrive');

        // The ignored collection's own subscriber never receives anything —
        // neither its own insert nor (obviously) the normal collection's.
        await expectNoEvent(ignoredGen);

        await hub.close();
    });

    it('close() ends all consumer generators', async () => {
        const { db } = await startReplSet();
        const key = uniqueCollection('close-key');
        const collA = uniqueCollection('close-a');
        const collB = uniqueCollection('close-b');

        const hub = new ChangeStreamHub();
        const genA = hub.subscribe(key, db, collA);
        const genB1 = hub.subscribe(key, db, collB);
        const genB2 = hub.subscribe(key, db, collB);

        const pendingA = genA.next();
        const pendingB1 = genB1.next();
        const pendingB2 = genB2.next();

        await hub.close();

        expect((await pendingA).done).toBe(true);
        expect((await pendingB1).done).toBe(true);
        expect((await pendingB2).done).toBe(true);
    });

    describe('connectedKeys: connection-driven parking', () => {
        it('parks until connect, survives a within-grace disconnect/reconnect, and resumes delivery after a real park', async () => {
            const { db } = await startReplSet();
            const key = uniqueCollection('gated-key');
            const collName = uniqueCollection('gated-coll');
            // Generous relative to real scheduling jitter (real timers, real
            // network round-trips to MongoDB) — the within-grace step waits a
            // small fraction of this and the past-grace step waits several
            // multiples of it, leaving wide margins on both sides.
            const graceMs = 400;

            const connectedKeys = new ValueSignal<string[]>([]); // key starts disconnected
            const hub = new ChangeStreamHub({ connectedKeys, graceMs });

            const gen = trackNext(hub.subscribe(key, db, collName));

            // Parked from the start: no cursor at all, so an insert now is never seen.
            await db.collection(collName).insertOne({ n: 'before-connect' });
            await expectNoEvent(gen);

            // Connect: the watch opens (fresh — nothing before this point is replayed).
            connectedKeys.next([key]);
            await waitForStreamOpen(hub, db, key);

            await db.collection(collName).insertOne({ n: 'after-connect' });
            const afterConnect = await expectEvent(gen);
            expect(n(afterConnect)).toBe('after-connect');

            // Disconnect, then reconnect well within the grace window: the grace
            // timer never fires, so the cursor is never actually closed and an
            // insert made during the "disconnected" window is delivered normally.
            connectedKeys.next([]);
            await delay(50); // well inside the 400ms grace window
            await db.collection(collName).insertOne({ n: 'during-grace' });
            connectedKeys.next([key]);

            const duringGrace = await expectEvent(gen);
            expect(n(duringGrace)).toBe('during-grace');

            // Disconnect and this time outlast the grace window: the watch truly
            // parks (no cursor). An insert made while parked is not delivered.
            connectedKeys.next([]);
            await delay(graceMs + 500); // well past the grace window firing
            await db.collection(collName).insertOne({ n: 'while-parked' });
            await expectNoEvent(gen);

            // Reconnect: the watch reopens and delivery resumes for new changes.
            connectedKeys.next([key]);
            await waitForStreamOpen(hub, db, key);
            await db.collection(collName).insertOne({ n: 'after-reopen' });

            // The reopen resumes from the last captured resume token, so the
            // 'while-parked' insert may also be replayed ahead of this one — search
            // rather than asserting exact position, since either order proves the
            // required behavior (delivery resumes and nothing after reconnect is lost).
            const found = await collectUntil(gen, d => n(d) === 'after-reopen');
            expect(n(found)).toBe('after-reopen');

            await hub.close();
        });
    });

    describe('reader crash self-heal (evictIfCrashed)', () => {
        it('evicts a terminally-crashed stream, completing its consumers, and a later subscribe() creates a fresh live stream', async () => {
            const { db } = await startReplSet();
            const key = uniqueCollection('crash-key');
            const collName = uniqueCollection('crash-coll');

            // Fault injection needs a scripted fake here — a healthy real replica
            // set cannot be made to reject a change-stream read with an arbitrary
            // fatal error code on demand, and this is exactly the terminal-crash
            // path (`isFatalChangeStreamError` -> rethrow instead of retry) that a
            // real deployment can only hit via genuine misconfiguration. Shaped
            // like change-stream-subscription.test.ts's makeFakeStream: a real
            // `Db.prototype` `parent` so ChangeStreamSubscription's `instanceof Db`
            // narrowing in resolveDb() succeeds, and a scripted rejecting `next()`.
            const fatalError = Object.assign(new Error('$changeStream is only supported on replica sets'), {
                code: 40573,
            });
            const parentDb: Db = Object.create(DbClass.prototype) as Db;
            (parentDb as unknown as { admin: () => unknown }).admin = () => ({
                command: vi.fn().mockResolvedValue({}),
            });
            const fakeStream = {
                parent: parentDb,
                resumeToken: undefined,
                next: vi.fn().mockRejectedValue(fatalError),
                close: vi.fn().mockResolvedValue(undefined),
            };
            const crashingDb: Db = Object.create(DbClass.prototype) as Db;
            (crashingDb as unknown as { watch: unknown }).watch = vi.fn().mockReturnValue(fakeStream);

            const hub = new ChangeStreamHub();
            const gen = hub.subscribe(key, crashingDb, collName);

            // The fatal error terminally throws out of ChangeStreamSubscription's
            // async generator, killing the hub reader; evictIfCrashed then closes
            // the stream's fanouts, completing this consumer.
            const result = await Promise.race([gen.next(), delay(5_000).then(() => 'timeout' as const)]);
            if (result === 'timeout') {
                throw new Error("expected the crashed stream's consumer to complete, but it never resolved");
            }
            expect(result.done).toBe(true);

            const streams = (hub as unknown as { streams: Map<string, unknown> }).streams;
            expect(streams.has(key)).toBe(false);

            // A later subscribe() for the SAME key, now against a healthy real db,
            // must create a fresh stream rather than handing back the dead one.
            const freshGen = trackNext(hub.subscribe(key, db, collName));
            // Not immediately done — proves it's a live, freshly-opened stream
            // (a stale/closed fanout would have resolved with done:true here).
            await expectNoEvent(freshGen);

            // And it actually works: prime it and confirm a real insert flows.
            await waitForStreamOpen(hub, db, key);
            await db.collection(collName).insertOne({ n: 'after-recreate' });
            const delivered = await expectEvent(freshGen);
            expect(n(delivered)).toBe('after-recreate');

            expect(streams.has(key)).toBe(true);

            await hub.close();
        });
    });

    describe('multi-key isolation', () => {
        it('two keys on two different databases each deliver only their own events', async () => {
            const { client, db: dbA } = await startReplSet();
            const dbB = client.db('test2');

            const keyA = uniqueCollection('multi-key-a');
            const keyB = uniqueCollection('multi-key-b');
            const collX = uniqueCollection('multi-x');
            const collY = uniqueCollection('multi-y');

            const hub = new ChangeStreamHub();
            const genX = trackNext(hub.subscribe(keyA, dbA, collX));
            const genY = trackNext(hub.subscribe(keyB, dbB, collY));

            await waitForStreamOpen(hub, dbA, keyA);
            await waitForStreamOpen(hub, dbB, keyB);

            await dbA.collection(collX).insertOne({ n: 'x1' });
            await dbB.collection(collY).insertOne({ n: 'y1' });

            const x1 = await expectEvent(genX);
            expect(n(x1)).toBe('x1');
            const y1 = await expectEvent(genY);
            expect(n(y1)).toBe('y1');

            // Causal-ordering trick (as in the ignoreCollections test): each key
            // has its own independent cursor, so insert again on ONE side and use
            // the confirmed receipt of that key's own event to anchor the bounded
            // no-event check on the OTHER key's consumer, rather than racing a
            // fixed timeout against two unrelated async cursors.
            await dbA.collection(collX).insertOne({ n: 'x2' });
            const x2 = await expectEvent(genX);
            expect(n(x2)).toBe('x2');
            await expectNoEvent(genY);

            await dbB.collection(collY).insertOne({ n: 'y2' });
            const y2 = await expectEvent(genY);
            expect(n(y2)).toBe('y2');
            await expectNoEvent(genX);

            await hub.close();
        });
    });
});
