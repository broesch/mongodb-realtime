import type { Db, Timestamp } from 'mongodb';

/**
 * The current cluster time, for `watch({ startAtOperationTime })`.
 *
 * A change stream cursor only sees writes made after it is established. A
 * test that opens a stream and then writes races the asynchronous open; a
 * stream started at a time taken before the write replays it, however slowly
 * the cursor opens. The start is inclusive: a write made just before it can
 * be replayed as well, so filter the stream to what the test expects.
 */
export async function streamStart(db: Db): Promise<Timestamp> {
    const reply = (await db.command({ ping: 1 })) as { operationTime?: Timestamp };
    if (!reply.operationTime) throw new Error('ping returned no operationTime: not a replica set?');
    return reply.operationTime;
}
