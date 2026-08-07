import { describe, it, expect } from 'vitest';
import { mergeAsyncGenerators } from '../../src/core/merge-async-generators.js';

// ---------------------------------------------------------------------------
// Pure tests — no Mongo. Most sources are a hand-rolled "controllable" async
// generator whose items are queued explicitly, exercising mergeAsyncGenerators'
// pull/yield/return semantics without racing real I/O or wall-clock delays.
//
// The teardown-hazard tests below (consumer `break`, source rejection) instead
// use REAL `async function*` sources with a genuinely-pending internal `await`
// — deliberately reproducing the exact condition `controllable()` engineers
// around (see its doc comment), because that condition is precisely what
// mergeAsyncGenerators' teardown must not block on.
// ---------------------------------------------------------------------------

interface Controllable<T> {
    gen: AsyncGenerator<T>;
    push(value: T): void;
    end(): void;
    fail(error: unknown): void;
    nextCalls(): number;
    returned(): boolean;
}

/**
 * A source whose `.next()` resolves from an explicit queue (pushed via
 * `push`/`end`/`fail`) rather than any real async work, and which counts how
 * many times `.next()` was actually called — so tests can assert
 * mergeAsyncGenerators only pulls a source when it has genuinely re-requested
 * an item, never eagerly ahead of what the consumer has taken.
 *
 * Deliberately NOT implemented by wrapping a real `async function*` whose idle
 * state is a bare `await new Promise(...)`: a generator parked on such an
 * internal (non-yield) await cannot process an external `.return()` call
 * until that promise settles — which, for a source that never receives more
 * data, is never. Managing the iterator protocol by hand instead lets
 * `return()` resolve (and unblock any pending `next()`) immediately, exactly
 * like a real change-stream cursor's `.return()` does.
 */
function controllable<T>(): Controllable<T> {
    type QueueItem = { kind: 'value'; value: T } | { kind: 'done' } | { kind: 'error'; error: unknown };
    const queue: QueueItem[] = [];
    let pending: { resolve: (r: IteratorResult<T>) => void; reject: (e: unknown) => void } | undefined;
    let closed = false;
    let returned = false;
    let nextCalls = 0;

    function tryDeliver(): void {
        if (!pending || queue.length === 0) return;
        const item = queue.shift()!;
        const { resolve, reject } = pending;
        pending = undefined;
        if (item.kind === 'error') {
            reject(item.error);
            return;
        }
        if (item.kind === 'done') {
            resolve({ value: undefined, done: true });
            return;
        }
        resolve({ value: item.value, done: false });
    }

    // Built by hand (not `as AsyncGenerator<T>` on the object literal) because a
    // strict lib target requires `[Symbol.asyncDispose]` on that interface — cast
    // at the boundary instead, same as the mongodb driver's own change-stream
    // types are cast where they don't line up exactly with what flows at runtime.
    const gen = {
        next: (): Promise<IteratorResult<T>> => {
            nextCalls += 1;
            if (closed) return Promise.resolve({ value: undefined, done: true });
            return new Promise<IteratorResult<T>>((resolve, reject) => {
                pending = { resolve, reject };
                tryDeliver();
            });
        },
        return: (value?: T): Promise<IteratorResult<T>> => {
            closed = true;
            returned = true;
            if (pending) {
                const { resolve } = pending;
                pending = undefined;
                resolve({ value: value as T, done: true });
            }
            return Promise.resolve({ value: value as T, done: true });
        },
        throw: (error?: unknown): Promise<IteratorResult<T>> => {
            returned = true;
            closed = true;
            if (pending) {
                const { reject } = pending;
                pending = undefined;
                reject(error);
            }
            return Promise.reject(error instanceof Error ? error : new Error(String(error)));
        },
        [Symbol.asyncIterator]() {
            return gen;
        },
    } as unknown as AsyncGenerator<T>;

    return {
        gen,
        push(value) {
            queue.push({ kind: 'value', value });
            tryDeliver();
        },
        end() {
            queue.push({ kind: 'done' });
            tryDeliver();
        },
        fail(error) {
            queue.push({ kind: 'error', error });
            tryDeliver();
        },
        nextCalls: () => nextCalls,
        returned: () => returned,
    };
}

/** Bounded wait so a regression hangs the test instead of the whole suite. */
async function nextValue<T>(gen: AsyncGenerator<T>, ms = 2_000): Promise<IteratorResult<T>> {
    const timeout = new Promise<'timeout'>(resolve => setTimeout(() => resolve('timeout'), ms));
    const result = await Promise.race([gen.next(), timeout]);
    if (result === 'timeout') throw new Error(`expected a result within ${ms}ms but none arrived`);
    return result;
}

/**
 * A REAL `async function*` that yields the resolution of a single externally-
 * supplied promise. When that promise is still pending, `.next()` on this
 * generator is genuinely suspended inside an internal `await` — NOT at a
 * `yield` — exactly the state a real change-stream cursor is in while waiting
 * on the next network event, and exactly the state `controllable()` above is
 * built to avoid. Used only where reproducing that state is the point.
 */
async function* realOnceSource<T>(step: Promise<T>): AsyncGenerator<T> {
    yield await step;
}

/** Bounded wait: rejects instead of hanging the test if `promise` never settles within `ms`. */
async function withTimeout<T>(promise: Promise<T>, ms = 1_000): Promise<T> {
    let timer: ReturnType<typeof setTimeout>;
    const timeout = new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`timed out after ${ms}ms — likely a teardown deadlock`)), ms);
    });
    try {
        return await Promise.race([promise, timeout]);
    } finally {
        clearTimeout(timer!);
    }
}

/**
 * Wraps a real async generator so tests can assert `.return()` was actually
 * INVOKED (fire-and-forget teardown calling it) without needing that call to
 * ever settle — which, for a source parked on a never-resolving promise, it
 * won't.
 */
function trackReturnCalls<T>(gen: AsyncGenerator<T>): { gen: AsyncGenerator<T>; returnCalled(): boolean } {
    let called = false;
    const wrapped = {
        next: () => gen.next(),
        return: (value?: T) => {
            called = true;
            return gen.return(value);
        },
        throw: (error?: unknown) => gen.throw(error),
        [Symbol.asyncIterator]() {
            return wrapped;
        },
    } as unknown as AsyncGenerator<T>;
    return { gen: wrapped, returnCalled: () => called };
}

describe('mergeAsyncGenerators', () => {
    it('yields items from multiple sources as they become ready, not in fixed round-robin order', async () => {
        const a = controllable<string>();
        const b = controllable<string>();
        const merged = mergeAsyncGenerators([a.gen, b.gen]);

        // Only B has data — merged yields B's item first even though A is index 0.
        b.push('b1');
        expect(await nextValue(merged)).toEqual({ value: 'b1', done: false });

        a.push('a1');
        expect(await nextValue(merged)).toEqual({ value: 'a1', done: false });

        b.push('b2');
        expect(await nextValue(merged)).toEqual({ value: 'b2', done: false });

        a.end();
        b.end();
        expect((await nextValue(merged)).done).toBe(true);
    });

    it('pulls at most one item ahead of each source — never re-requests before the previous item is consumed', async () => {
        const a = controllable<string>();
        const b = controllable<string>();
        const merged = mergeAsyncGenerators([a.gen, b.gen]);

        a.push('a1');
        const first = await nextValue(merged);
        expect(first).toEqual({ value: 'a1', done: false });
        // Constructing the merged generator pulls once from every source.
        expect(a.nextCalls()).toBe(1);
        expect(b.nextCalls()).toBe(1);

        // Queue a second item on A before the merge has asked for it again.
        a.push('a2');
        // The merge generator is suspended at its own `yield` (having just handed
        // 'a1' to us) and has not re-pulled A yet — bounded, single-item-ahead pull.
        expect(a.nextCalls()).toBe(1);

        const second = await nextValue(merged);
        expect(second).toEqual({ value: 'a2', done: false });
        expect(a.nextCalls()).toBe(2);

        a.end();
        b.end();
        expect((await nextValue(merged)).done).toBe(true);
    });

    it('a completed source is removed from the pool without ending the merge', async () => {
        const a = controllable<number>();
        const b = controllable<number>();
        const merged = mergeAsyncGenerators([a.gen, b.gen]);

        a.push(1);
        expect(await nextValue(merged)).toEqual({ value: 1, done: false });

        a.end();
        b.push(2);
        // A is drained/removed; B is still live, so the merge keeps delivering.
        expect(await nextValue(merged)).toEqual({ value: 2, done: false });

        b.end();
        expect((await nextValue(merged)).done).toBe(true);
    });

    // -----------------------------------------------------------------------
    // Teardown-hazard regressions. A real async generator's `.return()` cannot
    // settle while the generator is suspended inside its own internal `await`
    // (as opposed to at a `yield`) — that's exactly the state every LOSING
    // source is in after a `Promise.race` (it has an in-flight `next()` that
    // hasn't resolved). If mergeAsyncGenerators' teardown ever awaited those
    // `.return()` calls, both scenarios below would hang forever: a `break`
    // would never complete, and a rejecting source's error would never reach
    // the consumer (it has to unwind through the very `finally` that would be
    // stuck awaiting). Bounded by `withTimeout` so a regression fails fast
    // with a clear message instead of the suite's 60s default.
    // -----------------------------------------------------------------------

    it('consumer break completes promptly even though another source has a still-pending next() (real generators)', async () => {
        const a = realOnceSource(Promise.resolve(1));
        const bNeverResolves = new Promise<number>(() => undefined); // like a live, still-open change stream
        const b = trackReturnCalls(realOnceSource(bNeverResolves));
        const merged = mergeAsyncGenerators([a, b.gen]);

        const consume = (async () => {
            for await (const v of merged) {
                expect(v).toBe(1);
                break;
            }
        })();

        await withTimeout(consume, 1_000);
        // Fire-and-forget teardown still actually fires .return() on B, even
        // though B's own promise can never settle.
        expect(b.returnCalled()).toBe(true);
    });

    it('a source rejection propagates to the consumer promptly even though another source has a still-pending next() (real generators)', async () => {
        const boom = new Error('boom: fatal change-stream error');
        const a = realOnceSource(Promise.reject(boom));
        const bNeverResolves = new Promise<number>(() => undefined);
        const b = trackReturnCalls(realOnceSource(bNeverResolves));
        const merged = mergeAsyncGenerators([a, b.gen]);

        await withTimeout(expect(merged.next()).rejects.toThrow('boom'), 1_000);
        expect(b.returnCalled()).toBe(true);
    });

    it('completes immediately with no sources', async () => {
        const merged = mergeAsyncGenerators([]);
        expect((await nextValue(merged)).done).toBe(true);
    });
});
