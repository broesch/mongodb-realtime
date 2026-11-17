import { MongoBinary } from 'mongodb-memory-server';

/**
 * Fetch the mongod binary once, before any test file starts.
 *
 * Every test file boots its own replica set in its own fork (`pool: 'forks'`).
 * On a cold cache (every CI run) they all started the same first download at
 * once and raced on the shared lockfile: some failed with
 * UnableToUnlockLockfileError, others waited past the 60 s test timeout
 * (`tests/helpers/replset.test.ts` boots inside `it`, not `beforeAll`). One
 * download here, outside any test timeout, leaves every fork a warm cache.
 */
export default async function setup(): Promise<void> {
    await MongoBinary.getPath();
}
