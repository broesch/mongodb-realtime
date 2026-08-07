import { MongoMemoryReplSet } from 'mongodb-memory-server';
import { MongoClient, type Db } from 'mongodb';

let replSet: MongoMemoryReplSet | undefined;
let client: MongoClient | undefined;

/** Boot (once per test file — vitest forks pool gives each file its own process). */
export async function startReplSet(): Promise<{ client: MongoClient; db: Db; uri: string }> {
    if (!replSet) {
        replSet = await MongoMemoryReplSet.create({ replSet: { count: 1, storageEngine: 'wiredTiger' } });
    }
    const uri = replSet.getUri();
    if (!client) {
        client = new MongoClient(uri);
        await client.connect();
    }
    return { client, db: client.db('test'), uri };
}

export async function stopReplSet(): Promise<void> {
    await client?.close();
    client = undefined;
    await replSet?.stop();
    replSet = undefined;
}

/** Unique collection name so tests sharing a db never collide. */
export function uniqueCollection(prefix: string): string {
    return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}
