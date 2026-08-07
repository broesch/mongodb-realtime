/**
 * Shared timing helpers for tests driving real (or fake) async generators
 * against a replica set, where events arrive asynchronously and a naive
 * `await gen.next()` would hang the suite on a regression instead of failing
 * fast with a clear message.
 */

export const delay = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));

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
export function trackNext<T>(gen: AsyncGenerator<T>): { next(): Promise<IteratorResult<T>> } {
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

export type Tracker<T> = ReturnType<typeof trackNext<T>>;

/** Bounded wait: resolves 'timeout' if no event arrives within `ms`. */
export async function expectNoEvent<T>(tracker: Tracker<T>, ms = 300): Promise<void> {
    const result = await Promise.race([tracker.next(), delay(ms).then(() => 'timeout' as const)]);
    if (result === 'timeout') return;
    throw new Error(`expected no event within ${ms}ms but got: ${JSON.stringify(result)}`);
}

/** Bounded wait for the next event, so a stuck stream fails fast with a clear message instead of the suite timeout. */
export async function expectEvent<T>(tracker: Tracker<T>, ms = 10_000): Promise<T> {
    const result = await Promise.race([tracker.next(), delay(ms).then(() => 'timeout' as const)]);
    if (result === 'timeout') throw new Error(`expected an event within ${ms}ms but none arrived`);
    if (result.done) throw new Error('generator ended unexpectedly while waiting for an event');
    return result.value;
}

/** Bounded wait for the tracked generator to complete (`done: true`). */
export async function expectDone<T>(tracker: Tracker<T>, ms = 10_000): Promise<void> {
    const result = await Promise.race([tracker.next(), delay(ms).then(() => 'timeout' as const)]);
    if (result === 'timeout') throw new Error(`expected the generator to end within ${ms}ms but it did not`);
    if (!result.done) throw new Error(`expected the generator to end but got a value: ${JSON.stringify(result.value)}`);
}

/** Reads events off `tracker` until one matches `predicate`, bounded by `maxEvents` / a per-read timeout. */
export async function collectUntil<T>(
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
