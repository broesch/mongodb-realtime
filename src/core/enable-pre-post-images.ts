import type { Db } from 'mongodb';
import { Logger, consoleLogger, prefixed } from '../common/logger.js';

const logger = prefixed(consoleLogger, 'enableChangeStreamPreAndPostImages');

/**
 * Module-level cache of collections on which `changeStreamPreAndPostImages`
 * has been SUCCESSFULLY enabled. Shared across all callers so we never re-run
 * `collMod` once it has taken — but a *failed* enable is never cached, so the
 * next call retries it.
 *
 * Keyed by `${databaseName}::${collectionName}` so the same collection name
 * in two different databases is tracked independently. In practice most apps
 * run a single database, but keying by db keeps the cache correct if that changes.
 */
const enabledCollections = new Set<string>();

function cacheKey(databaseName: string, name: string): string {
    return `${databaseName}::${name}`;
}

/**
 * Test-only: clear the success cache so caching/retry behaviour can be
 * asserted in isolation. Not exported from the barrel (`index.ts`).
 */
export function __resetPrePostImageCacheForTest(): void {
    enabledCollections.clear();
}

/**
 * Enables `changeStreamPreAndPostImages` on the given collections so we can
 * read `fullDocumentBeforeChange` from change streams (needed to derive
 * organizationId on hard deletes for org-scoped fan-out).
 *
 * Single source of truth for pre/post-image enablement. Semantics:
 *   - create-collection-first: a missing collection is created BEFORE
 *     `collMod` runs, because `collMod` throws on a non-existent collection
 *     (the common case on a fresh DB's first boot).
 *   - add-after-success: a collection is recorded as enabled ONLY after the
 *     awaited `collMod` resolves. A failed enable is never cached.
 *   - retry-on-failure: because failures are not cached, a later call
 *     retries the enable — so a collection that didn't exist on first boot
 *     gets enabled once it materialises, with no process restart needed.
 *   - idempotent: once enabled, subsequent calls are a no-op (no extra
 *     `collMod`), so it is cheap to call on every watch startup / event loop.
 */
export async function enablePrePostImages(db: Db, collectionNames: string[], logger$?: Logger): Promise<void> {
    const log = logger$ ? prefixed(logger$, 'enableChangeStreamPreAndPostImages') : logger;
    const databaseName = db.databaseName;
    for (const name of collectionNames) {
        const key = cacheKey(databaseName, name);
        if (enabledCollections.has(key)) continue;
        try {
            // Ensure the collection exists first (collMod fails on missing
            // collections — the fresh-DB first-boot case).
            const existing = await db.listCollections({ name }, { nameOnly: true }).toArray();
            if (existing.length === 0) {
                await db.createCollection(name).catch(() => {
                    // race with another process — ignore
                });
            }
            await db.command({
                collMod: name,
                changeStreamPreAndPostImages: { enabled: true },
            });
            // Only cache AFTER the await resolves — a throw above skips this and
            // leaves the name uncached so the next call retries.
            enabledCollections.add(key);
        } catch (err) {
            // Common reasons: collection still doesn't exist (createCollection
            // raced/failed) or the running MongoDB version doesn't support
            // pre/post images. We log and continue — the watch still works, but
            // hard-deletes won't carry a `fullDocumentBeforeChange` so we can't
            // scope their emit by org. Most domains soft-delete so this only
            // affects the few that hard-delete, and the next call retries.
            log.warn(`Failed to enable changeStreamPreAndPostImages on '${name}': ${(err as Error).message}`);
        }
    }
}
