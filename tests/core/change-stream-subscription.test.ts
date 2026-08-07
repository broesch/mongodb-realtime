import { Db, Timestamp } from 'mongodb';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { ValueSignal } from '../../src/common/signal.js';
import { ChangeStreamSubscription } from '../../src/core/change-stream-subscription.js';
import { startReplSet, stopReplSet, uniqueCollection } from '../helpers/replset.js';

// ---------------------------------------------------------------------------
// These tests cover the first-event resume-token window (ported from fm's
// audit-fix p4 regression): if the very FIRST `next()` on a freshly opened
// stream rejects before any event has been yielded, `resumeToken` is still
// undefined. The reopen must then use `startAtOperationTime` (a cluster time
// captured at open) so that changes since the original open are not lost —
// NOT a bare `{}` watch.
// ---------------------------------------------------------------------------

type FactoryOptions = Record<string, unknown>;

// A fake ChangeStream good enough for ChangeStreamSubscription:
//   - `next()` resolves/rejects per a scripted queue,
//   - `resumeToken` is configurable,
//   - `parent` is a real `Db` instance (so the production `instanceof Db`
//     narrowing in `resolveDb` succeeds) with a stubbed `admin().command`
//     returning a fixed `operationTime`.
function makeFakeStream(opts: {
    // each entry: either resolve with a value, or reject with an error
    nextScript: Array<{ resolve: unknown } | { reject: Error }>;
    resumeToken?: unknown;
    operationTime?: Timestamp;
}) {
    let call = 0;
    const operationTime = opts.operationTime;

    // Real Db instance so `parent instanceof Db` is true in resolveDb().
    const db: Db = Object.create(Db.prototype);
    (db as unknown as { admin: () => unknown }).admin = () => ({
        command: vi.fn().mockResolvedValue(operationTime ? { operationTime } : {}),
    });

    const stream = {
        parent: db,
        resumeToken: opts.resumeToken,
        next: vi.fn().mockImplementation(() => {
            const step = opts.nextScript[Math.min(call, opts.nextScript.length - 1)];
            call += 1;
            if ('reject' in step) return Promise.reject(step.reject);
            // Never-resolving for sentinel so the race resolves via shutdown instead.
            if (step.resolve === '__never__') return new Promise(() => undefined);
            return Promise.resolve(step.resolve);
        }),
        close: vi.fn().mockResolvedValue(undefined),
    };
    return { stream, db };
}

describe('ChangeStreamSubscription — first-event resume window', () => {
    beforeEach(() => {
        vi.useFakeTimers();
    });
    afterEach(() => {
        vi.useRealTimers();
        vi.restoreAllMocks();
    });

    it('reopens with startAtOperationTime (not {}) when the first next() rejects before any event', async () => {
        const scope = new ValueSignal<string[]>(['org-1']);
        const shutdown = new ValueSignal<boolean>(false);
        const T0 = new Timestamp({ t: 1_700_000_000, i: 7 });

        const factoryCalls: FactoryOptions[] = [];

        // 1st open: first next() rejects immediately (no event yielded → no token).
        const first = makeFakeStream({
            nextScript: [{ reject: new Error('boom: first read failed') }],
            resumeToken: undefined,
            operationTime: T0,
        });
        // 2nd open (the reopen): never resolves next(); we end via shutdown so we
        // can inspect the options it was constructed with.
        const second = makeFakeStream({
            nextScript: [{ resolve: '__never__' }],
            resumeToken: undefined,
            operationTime: T0,
        });

        const streams = [first.stream, second.stream];
        const factory = vi.fn((_scope: string[], options: FactoryOptions) => {
            factoryCalls.push(options);
            return streams[Math.min(factoryCalls.length - 1, streams.length - 1)] as never;
        });

        const sub = new ChangeStreamSubscription<string, never>(scope, shutdown, factory as never, {
            name: 'test',
            initialBackoffMs: 5,
            maxBackoffMs: 20,
        });

        const iterator = sub[Symbol.asyncIterator]();
        // Kick off the generator. It opens stream #1, captures T0, the first
        // next() rejects, it closes and (after the backoff) reopens stream #2.
        const pending = iterator.next();

        // Let the rejection + close + capture microtasks settle, then run the
        // reconnect backoff.
        await vi.advanceTimersByTimeAsync(50);

        // The reopen must have happened with startAtOperationTime, not {}.
        expect(factoryCalls.length).toBe(2);

        const reopenOptions = factoryCalls[1];
        expect(reopenOptions).toHaveProperty('startAtOperationTime', T0);
        expect(reopenOptions).not.toHaveProperty('startAfter');
        expect(reopenOptions).not.toHaveProperty('resumeAfter');

        // The first (original) open had neither: no token, no prior T0.
        expect(factoryCalls[0]).toEqual({});

        // Shut down cleanly so the pending generator step resolves.
        shutdown.next(true);
        await vi.advanceTimersByTimeAsync(10);
        await pending;
    });

    it('reopens with startAfter (token) — not startAtOperationTime — once an event has been yielded', async () => {
        const scope = new ValueSignal<string[]>(['org-1']);
        const shutdown = new ValueSignal<boolean>(false);
        const T0 = new Timestamp({ t: 1_700_000_000, i: 7 });
        const token = { _data: 'resume-token-abc' };

        const factoryCalls: FactoryOptions[] = [];

        // 1st open: yields one event (sets resumeToken), then next() rejects.
        const first = makeFakeStream({
            nextScript: [{ resolve: { operationType: 'insert' } }, { reject: new Error('boom after first event') }],
            resumeToken: token,
            operationTime: T0,
        });
        const second = makeFakeStream({
            nextScript: [{ resolve: '__never__' }],
            resumeToken: token,
            operationTime: T0,
        });

        const streams = [first.stream, second.stream];
        const factory = vi.fn((_scope: string[], options: FactoryOptions) => {
            factoryCalls.push(options);
            return streams[Math.min(factoryCalls.length - 1, streams.length - 1)] as never;
        });

        const sub = new ChangeStreamSubscription<string, never>(scope, shutdown, factory as never, {
            name: 'test',
            initialBackoffMs: 5,
            maxBackoffMs: 20,
        });

        const iterator = sub[Symbol.asyncIterator]();
        // First .next() should yield the one event.
        const firstResult = await iterator.next();
        expect(firstResult.done).toBe(false);
        expect(firstResult.value).toEqual({ operationType: 'insert' });

        // Second .next() drives the stream: the scripted reject fires, then it
        // reopens after the backoff.
        const pending = iterator.next();
        await vi.advanceTimersByTimeAsync(50);

        expect(factoryCalls.length).toBe(2);
        const reopenOptions = factoryCalls[1];
        expect(reopenOptions).toHaveProperty('startAfter', token);
        expect(reopenOptions).not.toHaveProperty('startAtOperationTime');

        shutdown.next(true);
        await vi.advanceTimersByTimeAsync(10);
        await pending;
    });
});

describe('ChangeStreamSubscription — shutdown of a parked (empty-scope) subscription', () => {
    beforeEach(() => {
        vi.useFakeTimers();
    });
    afterEach(() => {
        vi.useRealTimers();
        vi.restoreAllMocks();
    });

    // Regression guard for the 4B parked-watch shutdown hang: when the scope
    // signal STARTS empty (a per-tenant hub watch for a tenant with no connected
    // socket on this pod) and no data ever arrives, the subscription must still
    // terminate on shutdown. A pre-loop `await this.newDataPromise` that did not
    // race `shutdownPromise` hung `app.close()`/SIGTERM forever. It must also
    // never open a watch while the scope is empty.
    it('terminates on shutdown when scope starts empty and no data ever arrives; never opens a watch', async () => {
        const scope = new ValueSignal<string[]>([]); // empty from the start
        const shutdown = new ValueSignal<boolean>(false);
        const factory = vi.fn(() => {
            throw new Error('factory must NOT be called while the scope is empty');
        });

        const sub = new ChangeStreamSubscription<string, never>(scope, shutdown, factory, { name: 'parked' });

        const iterator = sub[Symbol.asyncIterator]();
        const pending = iterator.next(); // parks on the empty-scope race

        await vi.advanceTimersByTimeAsync(50);
        expect(factory).not.toHaveBeenCalled(); // no watch opened for an empty scope

        // Before the fix this hung forever; now shutdown must end the generator.
        shutdown.next(true);
        const result = await pending;
        expect(result.done).toBe(true);
    });
});

describe('ChangeStreamSubscription — scope changes', () => {
    beforeEach(() => {
        vi.useFakeTimers();
    });
    afterEach(() => {
        vi.useRealTimers();
        vi.restoreAllMocks();
    });

    it('a scope change closes the current stream and reopens with the new scope', async () => {
        const scope = new ValueSignal<string[]>(['org-1']);
        const shutdown = new ValueSignal<boolean>(false);
        const scopesPassed: string[][] = [];

        const first = makeFakeStream({ nextScript: [{ resolve: '__never__' }] });
        const second = makeFakeStream({ nextScript: [{ resolve: '__never__' }] });
        const streams = [first.stream, second.stream];
        const factory = vi.fn((s: string[]) => {
            scopesPassed.push(s);
            return streams[Math.min(scopesPassed.length - 1, streams.length - 1)] as never;
        });

        const sub = new ChangeStreamSubscription<string, never>(scope, shutdown, factory, { name: 'rescope' });

        const iterator = sub[Symbol.asyncIterator]();
        const pending = iterator.next();
        await vi.advanceTimersByTimeAsync(10);
        expect(scopesPassed).toEqual([['org-1']]);

        scope.next(['org-1', 'org-2']);
        await vi.advanceTimersByTimeAsync(10);

        expect(first.stream.close).toHaveBeenCalled();
        expect(scopesPassed).toEqual([['org-1'], ['org-1', 'org-2']]);

        shutdown.next(true);
        await vi.advanceTimersByTimeAsync(10);
        await pending;
    });

    // Regression guard: the scope-change reopen path (the `newData` branch) must
    // carry the resume token forward via `this.resumeToken = changesStream.resumeToken
    // ?? this.resumeToken` BEFORE closing and reopening — same as the error-retry
    // path does. This must hold even when no event has been YIELDED yet on the
    // current open (so the yield-branch's own `this.resumeToken = ...` assignment
    // hasn't already covered it) — e.g. the driver's change stream can carry a live
    // `resumeToken` (from the initial aggregate's postBatchResumeToken) before any
    // document has been read. Without line 310's assignment, a scope change in that
    // window silently drops back to a fresh `{}` watch, re-delivering or losing the gap.
    it('a scope change before any event is yielded still carries the stream resumeToken forward', async () => {
        const scope = new ValueSignal<string[]>(['org-1']);
        const shutdown = new ValueSignal<boolean>(false);
        const token = { _data: 'tok1' };
        const factoryCalls: Array<{ scope: string[]; options: FactoryOptions }> = [];

        // 1st open: never yields an event (so the yield-branch assignment at line
        // 316 never runs), but the stream already carries a live `resumeToken` —
        // the scope change below is what ends this stream.
        const first = makeFakeStream({ nextScript: [{ resolve: '__never__' }], resumeToken: token });
        const second = makeFakeStream({ nextScript: [{ resolve: '__never__' }] });
        const streams = [first.stream, second.stream];
        const factory = vi.fn((s: string[], options: FactoryOptions) => {
            factoryCalls.push({ scope: s, options });
            return streams[Math.min(factoryCalls.length - 1, streams.length - 1)] as never;
        });

        const sub = new ChangeStreamSubscription<string, never>(scope, shutdown, factory as never, {
            name: 'token-carry',
        });

        const iterator = sub[Symbol.asyncIterator]();
        const pending = iterator.next();
        await vi.advanceTimersByTimeAsync(5);
        expect(factoryCalls.length).toBe(1);
        expect(factoryCalls[0].options).toEqual({});

        scope.next(['org-2']); // scope change before any event was ever yielded
        await vi.advanceTimersByTimeAsync(10);

        expect(first.stream.close).toHaveBeenCalled();
        expect(factoryCalls.length).toBe(2);
        expect(factoryCalls[1].scope).toEqual(['org-2']);
        expect(factoryCalls[1].options).toEqual({ startAfter: token });

        shutdown.next(true);
        await vi.advanceTimersByTimeAsync(10);
        await pending;
    });

    it('a scope change to [] parks (factory not called again) until scope refills', async () => {
        const scope = new ValueSignal<string[]>(['org-1']);
        const shutdown = new ValueSignal<boolean>(false);
        const scopesPassed: string[][] = [];

        const first = makeFakeStream({ nextScript: [{ resolve: '__never__' }] });
        const second = makeFakeStream({ nextScript: [{ resolve: '__never__' }] });
        const streams = [first.stream, second.stream];
        const factory = vi.fn((s: string[]) => {
            scopesPassed.push(s);
            return streams[Math.min(scopesPassed.length - 1, streams.length - 1)] as never;
        });

        const sub = new ChangeStreamSubscription<string, never>(scope, shutdown, factory, {
            name: 'park-refill',
        });

        const iterator = sub[Symbol.asyncIterator]();
        const pending = iterator.next();
        await vi.advanceTimersByTimeAsync(10);
        expect(scopesPassed).toEqual([['org-1']]);

        scope.next([]); // scope drains — should close and park, no new factory call
        await vi.advanceTimersByTimeAsync(10);
        expect(first.stream.close).toHaveBeenCalled();
        expect(scopesPassed).toEqual([['org-1']]); // still just the one call

        scope.next(['org-2']); // refills — must reopen
        await vi.advanceTimersByTimeAsync(10);
        expect(scopesPassed).toEqual([['org-1'], ['org-2']]);

        shutdown.next(true);
        await vi.advanceTimersByTimeAsync(10);
        await pending;
    });

    it('an identical scope re-emission (deepEqual) does not restart the stream', async () => {
        const scope = new ValueSignal<string[]>(['org-1']);
        const shutdown = new ValueSignal<boolean>(false);

        const first = makeFakeStream({ nextScript: [{ resolve: '__never__' }] });
        const factory = vi.fn(() => first.stream as never);

        const sub = new ChangeStreamSubscription<string, never>(scope, shutdown, factory, {
            name: 'no-restart',
        });

        const iterator = sub[Symbol.asyncIterator]();
        const pending = iterator.next();
        await vi.advanceTimersByTimeAsync(10);
        expect(factory).toHaveBeenCalledTimes(1);

        scope.next(['org-1']); // same value — deepEqual — must be ignored
        await vi.advanceTimersByTimeAsync(10);
        expect(factory).toHaveBeenCalledTimes(1);
        expect(first.stream.close).not.toHaveBeenCalled();

        shutdown.next(true);
        await vi.advanceTimersByTimeAsync(10);
        await pending;
    });
});

describe('ChangeStreamSubscription — shutdown while awaiting an event', () => {
    beforeEach(() => {
        vi.useFakeTimers();
    });
    afterEach(() => {
        vi.useRealTimers();
        vi.restoreAllMocks();
    });

    it('ends the iteration promptly when shutdown fires while awaiting next()', async () => {
        const scope = new ValueSignal<string[]>(['org-1']);
        const shutdown = new ValueSignal<boolean>(false);
        const stream = makeFakeStream({ nextScript: [{ resolve: '__never__' }] });
        const factory = vi.fn(() => stream.stream as never);

        const sub = new ChangeStreamSubscription<string, never>(scope, shutdown, factory, {
            name: 'shutdown-await',
        });

        const pending = sub[Symbol.asyncIterator]().next();
        await vi.advanceTimersByTimeAsync(10);

        shutdown.next(true);
        const result = await pending;
        expect(result.done).toBe(true);
        expect(stream.stream.close).toHaveBeenCalled();
    });
});

// ---------------------------------------------------------------------------
// Error classification + reconnect backoff.
//
// Every error is either FATAL (deployment problem — rethrow, never retry),
// UNRESUMABLE (the resume point is poisoned — drop it, reopen fresh), or
// transient (reopen and retry with capped exponential backoff).
// ---------------------------------------------------------------------------
describe('ChangeStreamSubscription — fatal errors', () => {
    beforeEach(() => {
        vi.useFakeTimers();
    });
    afterEach(() => {
        vi.useRealTimers();
        vi.restoreAllMocks();
    });

    function fatal(code: number, message: string): Error {
        return Object.assign(new Error(message), { code });
    }

    it('rethrows 40573 (not a replica set) instead of retrying forever', async () => {
        const scope = new ValueSignal<string[]>(['org-1']);
        const shutdown = new ValueSignal<boolean>(false);

        const stream = makeFakeStream({
            nextScript: [{ reject: fatal(40573, '$changeStream is only supported on replica sets') }],
        });
        const factory = vi.fn(() => stream.stream as never);

        const sub = new ChangeStreamSubscription<string, never>(scope, shutdown, factory, {
            name: 'fatal',
            initialBackoffMs: 5,
            maxBackoffMs: 20,
        });

        const pending = sub[Symbol.asyncIterator]().next();
        await expect(pending).rejects.toThrow('only supported on replica sets');

        // The whole point: no reopen. One open, one failure, out.
        await vi.advanceTimersByTimeAsync(100);
        expect(factory).toHaveBeenCalledTimes(1);
        expect(stream.stream.close).toHaveBeenCalled();
    });

    it('rethrows 40574 (config database) instead of retrying forever', async () => {
        const scope = new ValueSignal<string[]>(['org-1']);
        const shutdown = new ValueSignal<boolean>(false);
        const stream = makeFakeStream({
            nextScript: [{ reject: fatal(40574, 'changeStream may not be opened on config') }],
        });
        const factory = vi.fn(() => stream.stream as never);

        const sub = new ChangeStreamSubscription<string, never>(scope, shutdown, factory, {
            name: 'fatal',
            initialBackoffMs: 5,
            maxBackoffMs: 20,
        });

        await expect(sub[Symbol.asyncIterator]().next()).rejects.toThrow();
        expect(factory).toHaveBeenCalledTimes(1);
    });

    it('classifies a replica-set error by message even with no code', async () => {
        const scope = new ValueSignal<string[]>(['org-1']);
        const shutdown = new ValueSignal<boolean>(false);
        const stream = makeFakeStream({
            nextScript: [{ reject: new Error('$changeStream is only supported on replica sets') }],
        });
        const factory = vi.fn(() => stream.stream as never);

        const sub = new ChangeStreamSubscription<string, never>(scope, shutdown, factory, {
            name: 'fatal',
            initialBackoffMs: 5,
            maxBackoffMs: 20,
        });

        await expect(sub[Symbol.asyncIterator]().next()).rejects.toThrow();
        expect(factory).toHaveBeenCalledTimes(1);
    });

    it('keeps retrying an ordinary transient error (not misclassified as fatal)', async () => {
        const scope = new ValueSignal<string[]>(['org-1']);
        const shutdown = new ValueSignal<boolean>(false);

        const first = makeFakeStream({ nextScript: [{ reject: new Error('ECONNRESET') }] });
        const second = makeFakeStream({ nextScript: [{ resolve: '__never__' }] });
        const streams = [first.stream, second.stream];
        let call = 0;
        const factory = vi.fn(() => {
            const s = streams[Math.min(call, streams.length - 1)];
            call += 1;
            return s as never;
        });

        const sub = new ChangeStreamSubscription<string, never>(scope, shutdown, factory, {
            name: 'transient',
            initialBackoffMs: 5,
            maxBackoffMs: 20,
        });

        const pending = sub[Symbol.asyncIterator]().next();
        await vi.advanceTimersByTimeAsync(50);
        expect(factory).toHaveBeenCalledTimes(2);

        shutdown.next(true);
        await vi.advanceTimersByTimeAsync(10);
        await pending;
    });
});

describe('ChangeStreamSubscription — unusable resume point', () => {
    beforeEach(() => {
        vi.useFakeTimers();
    });
    afterEach(() => {
        vi.useRealTimers();
        vi.restoreAllMocks();
    });

    // With a token held, the plain transient path reopens with
    // `startAfter: <token>` — which fails identically every time once the token
    // has aged out of the oplog. The recovery is to drop the resume point and
    // reopen fresh: the gap is lost (Mongo no longer has it) but realtime lives.
    it('drops an expired resume token and reopens fresh instead of replaying it', async () => {
        const scope = new ValueSignal<string[]>(['org-1']);
        const shutdown = new ValueSignal<boolean>(false);
        const T0 = new Timestamp({ t: 1_700_000_000, i: 7 });
        const token = { _data: 'aged-out-token' };
        const factoryCalls: FactoryOptions[] = [];

        // Yields one event (so a token is held), then reports ChangeStreamHistoryLost.
        const first = makeFakeStream({
            nextScript: [
                { resolve: { operationType: 'insert' } },
                {
                    reject: Object.assign(new Error('resume of change stream was not possible'), {
                        code: 286,
                        codeName: 'ChangeStreamHistoryLost',
                    }),
                },
            ],
            resumeToken: token,
            operationTime: T0,
        });
        const second = makeFakeStream({
            nextScript: [{ resolve: '__never__' }],
            resumeToken: token,
            operationTime: T0,
        });
        const streams = [first.stream, second.stream];
        const factory = vi.fn((_scope: string[], options: FactoryOptions) => {
            factoryCalls.push(options);
            return streams[Math.min(factoryCalls.length - 1, streams.length - 1)] as never;
        });

        const sub = new ChangeStreamSubscription<string, never>(scope, shutdown, factory as never, {
            name: 'historylost',
            initialBackoffMs: 5,
            maxBackoffMs: 20,
        });

        const iterator = sub[Symbol.asyncIterator]();
        await iterator.next(); // consume the one event, setting the resume token
        const pending = iterator.next();
        await vi.advanceTimersByTimeAsync(50);

        expect(factoryCalls.length).toBe(2);
        // Fresh watch: neither the doomed token nor a stale operation time.
        expect(factoryCalls[1]).toEqual({});

        shutdown.next(true);
        await vi.advanceTimersByTimeAsync(10);
        await pending;
    });

    it('drops the resume point when the pre-image is gone', async () => {
        const scope = new ValueSignal<string[]>(['org-1']);
        const shutdown = new ValueSignal<boolean>(false);
        const token = { _data: 'points-at-a-delete-with-no-preimage' };
        const factoryCalls: FactoryOptions[] = [];

        const first = makeFakeStream({
            nextScript: [
                { resolve: { operationType: 'insert' } },
                { reject: new Error('PreImageNotFoundForChangeStream: the pre-image was not found') },
            ],
            resumeToken: token,
        });
        const second = makeFakeStream({
            nextScript: [{ resolve: '__never__' }],
            resumeToken: token,
        });
        const streams = [first.stream, second.stream];
        const factory = vi.fn((_scope: string[], options: FactoryOptions) => {
            factoryCalls.push(options);
            return streams[Math.min(factoryCalls.length - 1, streams.length - 1)] as never;
        });

        const sub = new ChangeStreamSubscription<string, never>(scope, shutdown, factory as never, {
            name: 'preimage',
            initialBackoffMs: 5,
            maxBackoffMs: 20,
        });

        const iterator = sub[Symbol.asyncIterator]();
        await iterator.next();
        const pending = iterator.next();
        await vi.advanceTimersByTimeAsync(50);

        expect(factoryCalls.length).toBe(2);
        expect(factoryCalls[1]).toEqual({});

        shutdown.next(true);
        await vi.advanceTimersByTimeAsync(10);
        await pending;
    });
});

describe('ChangeStreamSubscription — reconnect backoff', () => {
    beforeEach(() => {
        vi.useFakeTimers();
    });
    afterEach(() => {
        vi.useRealTimers();
        vi.restoreAllMocks();
    });

    /** A factory whose streams all reject immediately, counting opens. */
    function alwaysFailingFactory() {
        const calls: FactoryOptions[] = [];
        const factory = vi.fn((_scope: string[], options: FactoryOptions) => {
            calls.push(options);
            return makeFakeStream({ nextScript: [{ reject: new Error('still down') }] }).stream as never;
        });
        return { calls, factory };
    }

    it('doubles the pause between consecutive failures instead of retrying at a flat rate', async () => {
        const scope = new ValueSignal<string[]>(['org-1']);
        const shutdown = new ValueSignal<boolean>(false);
        const { calls, factory } = alwaysFailingFactory();

        const sub = new ChangeStreamSubscription<string, never>(scope, shutdown, factory as never, {
            name: 'backoff',
            initialBackoffMs: 5,
            maxBackoffMs: 100,
        });

        const pending = sub[Symbol.asyncIterator]().next();

        // 1st failure (t=0) → 5ms pause → 2nd open at t=5, which fails
        // immediately too, starting a 10ms pause running t=5..15.
        await vi.advanceTimersByTimeAsync(11); // t=11
        expect(calls.length).toBe(2);

        // Still inside the 10ms pause (ends at t=15) — 3rd open must not have
        // happened yet.
        await vi.advanceTimersByTimeAsync(3); // t=14
        expect(calls.length).toBe(2);

        // Past t=15 now — the pause has elapsed and the 3rd open has happened.
        await vi.advanceTimersByTimeAsync(6); // t=20
        expect(calls.length).toBe(3);

        shutdown.next(true);
        await vi.advanceTimersByTimeAsync(10);
        await pending;
    });

    it('caps the pause at maxBackoffMs', async () => {
        const scope = new ValueSignal<string[]>(['org-1']);
        const shutdown = new ValueSignal<boolean>(false);
        const { calls, factory } = alwaysFailingFactory();

        const sub = new ChangeStreamSubscription<string, never>(scope, shutdown, factory as never, {
            name: 'backoff',
            initialBackoffMs: 5,
            maxBackoffMs: 10,
        });

        const pending = sub[Symbol.asyncIterator]().next();

        await vi.advanceTimersByTimeAsync(11); // 5ms pause
        expect(calls.length).toBe(2);
        await vi.advanceTimersByTimeAsync(11); // 10ms pause (capped)
        expect(calls.length).toBe(3);
        await vi.advanceTimersByTimeAsync(11); // still 10ms, not 20ms
        expect(calls.length).toBe(4);

        shutdown.next(true);
        await vi.advanceTimersByTimeAsync(10);
        await pending;
    });

    it('resets the pause to the initial delay once an event flows again', async () => {
        const scope = new ValueSignal<string[]>(['org-1']);
        const shutdown = new ValueSignal<boolean>(false);
        const calls: FactoryOptions[] = [];

        // open #1 fails; open #2 yields an event then fails; open #3 parks.
        const scripts: Array<Array<{ resolve: unknown } | { reject: Error }>> = [
            [{ reject: new Error('blip') }],
            [{ resolve: { operationType: 'insert' } }, { reject: new Error('blip') }],
            [{ resolve: '__never__' }],
        ];
        const factory = vi.fn((_scope: string[], options: FactoryOptions) => {
            const script = scripts[Math.min(calls.length, scripts.length - 1)];
            calls.push(options);
            return makeFakeStream({ nextScript: script }).stream as never;
        });

        const sub = new ChangeStreamSubscription<string, never>(scope, shutdown, factory as never, {
            name: 'backoff-reset',
            initialBackoffMs: 5,
            maxBackoffMs: 100,
        });

        const iterator = sub[Symbol.asyncIterator]();
        // Drives: open #1 → fail → 5ms pause → open #2 → yields the event.
        const firstEvent = iterator.next();
        await vi.advanceTimersByTimeAsync(11);
        expect((await firstEvent).value).toEqual({ operationType: 'insert' });
        expect(calls.length).toBe(2);

        // Open #2 now fails too. Had the backoff kept climbing this would be a
        // 10ms pause; because the yielded event reset it, 6ms must be enough.
        const pending = iterator.next();
        await vi.advanceTimersByTimeAsync(6);
        expect(calls.length).toBe(3);

        shutdown.next(true);
        await vi.advanceTimersByTimeAsync(10);
        await pending;
    });

    it('does not wait out the pause on shutdown', async () => {
        const scope = new ValueSignal<string[]>(['org-1']);
        const shutdown = new ValueSignal<boolean>(false);
        const { calls, factory } = alwaysFailingFactory();

        // A long pause: without cancellation this would add its full length to SIGTERM.
        const sub = new ChangeStreamSubscription<string, never>(scope, shutdown, factory as never, {
            name: 'backoff-cancel',
            initialBackoffMs: 30_000,
            maxBackoffMs: 30_000,
        });

        const pending = sub[Symbol.asyncIterator]().next();
        await vi.advanceTimersByTimeAsync(10); // open #1, fail, enter the pause
        expect(calls.length).toBe(1);

        // No timer advance at all — shutdown alone must end the generator.
        shutdown.next(true);
        const result = await pending;
        expect(result.done).toBe(true);
        expect(calls.length).toBe(1); // never reopened
    });
});

// ---------------------------------------------------------------------------
// Integration: real replica set, real change stream via `coll.watch`.
// ---------------------------------------------------------------------------
describe('ChangeStreamSubscription — integration (real replica set)', () => {
    afterAll(() => stopReplSet());

    it('receives real change-stream events and stops cleanly on shutdown', async () => {
        const { db } = await startReplSet();
        const collectionName = uniqueCollection('css-integration');
        const coll = db.collection(collectionName);

        const scope = new ValueSignal<string[]>(['tenant-1']);
        const shutdown = new ValueSignal<boolean>(false);

        const sub = new ChangeStreamSubscription<string, never>(scope, shutdown, () => coll.watch() as never, {
            name: 'integration',
        });

        const received: unknown[] = [];
        const consume = (async () => {
            for await (const change of sub) {
                received.push(change);
                if (received.length === 3) {
                    // Signal shutdown from inside the loop, right after the 3rd
                    // event, so the iterator's NEXT await (racing shutdown) is
                    // what ends it — proving shutdown terminates a live, running
                    // subscription, not just a parked or errored one.
                    shutdown.next(true);
                }
            }
        })();

        // Give the watch a moment to actually open before inserting.
        await new Promise(resolve => setTimeout(resolve, 200));

        await coll.insertOne({ n: 1 });
        await coll.insertOne({ n: 2 });
        await coll.insertOne({ n: 3 });

        await consume;
        expect(received.length).toBe(3);
    });
});
