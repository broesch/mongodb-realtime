/**
 * Round-robin fan-in of N async generators into one. Pulls one item from each
 * source concurrently and yields whichever resolves first, then re-pulls only
 * from that source — so each source is at most one item ahead of what has
 * been consumed downstream (bounded buffering / natural backpressure).
 * Completes when every source completes.
 *
 * If a source rejects, the rejection propagates out to the merged generator's
 * consumer (mirroring a single un-merged watcher's behaviour) — promptly: the
 * teardown below never blocks that propagation. On early return/throw by the
 * consumer, every source's `.return()` is fired (unawaited — see the
 * `finally` block for why) so whatever they hold open (e.g. change streams)
 * is eventually torn down without blocking the consumer's own teardown.
 *
 * Originally ported verbatim from pactsheet.fm's
 * `common/change-stream/watch-entity-changes-all-tenants.ts`; the `finally`
 * below has since diverged from that source (see its comment) — this library
 * is the hardened canonical copy now.
 */
export async function* mergeAsyncGenerators<T>(sources: AsyncGenerator<T>[]): AsyncGenerator<T> {
    type Tagged = { index: number; result: IteratorResult<T> };
    const inflight = new Map<number, Promise<Tagged>>();
    const pull = (index: number): void => {
        const source = sources[index];
        if (!source) return;
        inflight.set(
            index,
            source.next().then(result => ({ index, result }))
        );
    };
    sources.forEach((_, i) => pull(i));

    try {
        while (inflight.size > 0) {
            const { index, result } = await Promise.race(inflight.values());
            if (result.done) {
                inflight.delete(index);
                continue;
            }
            yield result.value;
            pull(index);
        }
    } finally {
        // Consumer broke out (or a source threw): close every source so
        // whatever they hold open is torn down rather than leaked.
        //
        // Fire-and-forget — DO NOT await. Every source that did NOT win the
        // last `Promise.race` still has an in-flight `next()`, i.e. it is
        // suspended inside its OWN internal `await` (waiting on the next real
        // event from a live change stream, say) rather than at a `yield`. An
        // async generator parked mid-await like that cannot process an
        // external `.return()` call until that pending promise settles — for
        // a source still waiting on the next event, that may be arbitrarily
        // far in the future, or never. Awaiting `Promise.allSettled` here
        // would therefore block THIS generator's own `return()`/rejection
        // from ever reaching our consumer: a `break` would hang forever, and
        // a rejecting source's error would never propagate (it would need to
        // pass through this very `finally` first). Firing it unawaited lets
        // this generator finish unwinding NOW; each queued `.return()` still
        // resolves whenever its source's pending step actually completes, so
        // nothing is leaked — it just isn't blocking. (Mirrors fm's
        // `adaptCollection`, which fixed the identical hazard for a single
        // hub consumer's own teardown the same way — see `watchCollection`'s
        // `hubConsumerLoop` in this library.)
        void Promise.allSettled(sources.map(s => s.return(undefined)));
    }
}
