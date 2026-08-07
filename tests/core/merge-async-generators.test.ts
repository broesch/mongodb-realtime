import { describe, it, expect } from 'vitest';
import { mergeAsyncGenerators } from '../../src/core/merge-async-generators.js';

// ---------------------------------------------------------------------------
// Pure tests — no Mongo, no real timers. Every source is a hand-rolled
// "controllable" async generator whose items are queued explicitly, so the
// tests exercise mergeAsyncGenerators' pull/yield/return semantics without
// racing real I/O or wall-clock delays.
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

    it('consumer break closes every source via .return()', async () => {
        const a = controllable<number>();
        const b = controllable<number>();
        const merged = mergeAsyncGenerators([a.gen, b.gen]);

        a.push(1);
        for await (const v of merged) {
            expect(v).toBe(1);
            break;
        }

        expect(a.returned()).toBe(true);
        expect(b.returned()).toBe(true);
    });

    it('a source rejecting propagates the rejection to the consumer', async () => {
        const a = controllable<number>();
        const b = controllable<number>();
        const merged = mergeAsyncGenerators([a.gen, b.gen]);

        const boom = new Error('boom');
        a.fail(boom);

        await expect(merged.next()).rejects.toThrow('boom');
    });

    it('a source rejecting still closes every source via .return()', async () => {
        const a = controllable<number>();
        const b = controllable<number>();
        const merged = mergeAsyncGenerators([a.gen, b.gen]);

        a.fail(new Error('boom'));
        await expect(merged.next()).rejects.toThrow('boom');

        expect(a.returned()).toBe(true);
        expect(b.returned()).toBe(true);
    });

    it('completes immediately with no sources', async () => {
        const merged = mergeAsyncGenerators([]);
        expect((await nextValue(merged)).done).toBe(true);
    });
});
