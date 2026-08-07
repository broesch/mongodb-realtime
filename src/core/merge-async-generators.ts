/**
 * Round-robin fan-in of N async generators into one. Pulls one item from each
 * source concurrently and yields whichever resolves first, then re-pulls only
 * from that source — so each source is at most one item ahead of what has
 * been consumed downstream (bounded buffering / natural backpressure).
 * Completes when every source completes.
 *
 * If a source rejects, the rejection propagates out to the merged generator's
 * consumer (mirroring a single un-merged watcher's behaviour). On early
 * return/throw by the consumer, all sources are `.return()`-ed so whatever
 * they hold open (e.g. change streams) is torn down rather than leaked.
 *
 * Ported verbatim from pactsheet.fm's
 * `common/change-stream/watch-entity-changes-all-tenants.ts`.
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
        await Promise.allSettled(sources.map(s => s.return(undefined)));
    }
}
