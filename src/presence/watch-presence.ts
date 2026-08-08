import type { ChangeStreamDocument, Collection, Document, ObjectId } from 'mongodb';
import { ChangeStreamSubscription } from '../core/change-stream-subscription.js';
import { collectionWatchFactory } from '../core/watch-collection.js';
import { consoleLogger, prefixed, type Logger } from '../common/logger.js';
import { ValueSignal, type ReadonlySignal } from '../common/signal.js';
import type { PresenceStore } from './presence-store.js';
import { DEFAULT_ACTIVITY_FIELDS, type PresenceDoc } from './types.js';

export interface PresenceEvent {
    /**
     * `online`/`offline` are 0→1 / 1→0 transitions for the user as a whole.
     * `activity` is a write to one of the watched activity fields (by default
     * `page`) on an already-online connection — it is per-connection, not a
     * transition, so a user with three tabs can produce three of them.
     */
    type: 'online' | 'offline' | 'activity';
    userId: string;
    scopeId: string;
    key: string;
    ownedByThisServer: boolean;
    /** Application metadata from the presence row (`PresenceStore.checkIn({ meta })`). */
    meta?: Record<string, unknown>;
    /** The row's current `page`. Present on `activity` when the row carries one; present on `online`/`offline` when the row carried one. */
    page?: string;
}

/**
 * Watch a `PresenceStore`'s collection and yield `online`/`offline` events on
 * 0→1 / 1→0 transitions, i.e. exactly once per user per side rather than once
 * per connection. Ported from calendar's `online-user.pusher.ts` refinement:
 * announce from the change stream, not the gateway, so a connection landing
 * on ANY node is visible to every node (fm's `presence.gateway.ts` pre-image
 * handling for the delete side).
 *
 * insert → online iff no OTHER row for this user is older (order-stable — see
 * `toPresenceEvent`'s insert branch and `PresenceStore.countOlderUserRows`).
 * A live "total count === 1" check is order-DEPENDENT: two near-simultaneous
 * first connections would each read a total of 2 once both writes have
 * landed, and NEITHER would ever announce — the user is durably online but
 * was never yielded, and the eventual last checkout emits a stray `offline`
 * with no matching `online`. Asking "is any older row present?" instead gives
 * the same answer no matter when it's evaluated: whichever racer's row holds
 * the smallest `_id` announces — exactly one, at any degree of concurrency.
 *
 * delete → `countUserRows(userId) === 0` → offline (fields from the
 * pre-image). This mirrors the reference's `announceOffline` exactly: a
 * plain remaining-count check, NOT order-stable. Two simultaneous
 * last-checkouts can each observe 0 remaining rows (the other racer's delete
 * already landed in the DB by the time this one's count runs, even though
 * its own change-stream event hasn't been processed yet) and both yield
 * `offline` — an accepted, harmless duplicate: a consumer folding these into
 * "is this user online" a second `offline` while already offline is a no-op,
 * whereas a missed `online` (the insert failure mode above) is not.
 *
 * update/replace → `activity` iff the write touched one of `activityFields`
 * (default `['page']`); a `replace` always counts (it rewrites the whole
 * row), an `update` only counts if `updateDescription.updatedFields` names a
 * watched field — a keepAlive writes only `lastSeen` and yields nothing.
 */
export async function* watchPresence(options: {
    store: PresenceStore;
    shutdown: ReadonlySignal<boolean>;
    logger?: Logger;
    /**
     * Fields whose write yields an `activity` event. Defaults to `['page']`.
     * Deliberately NOT "any update": every keepAlive writes `lastSeen`, so an
     * unfiltered update branch would emit an activity event per heartbeat per
     * connection.
     */
    activityFields?: string[];
}): AsyncGenerator<PresenceEvent> {
    const log = prefixed(options.logger ?? consoleLogger, 'watchPresence');
    const { store } = options;
    // `collectionWatchFactory` takes an untyped `Collection<Document>`; `store.collection`
    // is a `Collection<PresenceDoc>`, which TS's invariant `Collection<T>` rejects as an
    // argument even though every `PresenceDoc` is structurally a `Document`.
    const watch = collectionWatchFactory(store.collection as unknown as Collection);

    // Constructed fresh here, not hoisted to a module-level constant: per the
    // constant-scope pattern used by `watchCollection`'s `directLoop`,
    // `ChangeStreamSubscription` never unsubscribes its own scope listener on
    // normal completion, so a shared instance would accumulate one
    // permanently-retained listener per `watchPresence` call for the life of
    // the process. A fresh `[1]` signal per call is cheap and behaves
    // identically since its value is never mutated.
    const scope = new ValueSignal<number[]>([1]);
    const subscription = new ChangeStreamSubscription<number, ChangeStreamDocument<Document>>(
        scope,
        options.shutdown,
        (_scope, resumeOptions) => watch(resumeOptions),
        { name: 'watchPresence', logger: log }
    );

    for await (const change of subscription) {
        // `toPresenceEvent` awaits a per-event count query
        // (`countOlderUserRows`/`countUserRows`) against the store. A
        // transient Mongo rejection there (server-selection timeout,
        // failover) must NOT throw out of this generator — that would kill
        // the whole presence watcher until process restart, taking every
        // consumer's online/offline fan-out down with it over one blip. Log
        // and skip the event instead; the loop continues to the next change.
        let event: PresenceEvent | undefined;
        try {
            event = await toPresenceEvent(change, store, log, options.activityFields ?? DEFAULT_ACTIVITY_FIELDS);
        } catch (err) {
            const doc = change as {
                fullDocument?: { userId?: string };
                fullDocumentBeforeChange?: { userId?: string };
            };
            const userId = doc.fullDocument?.userId ?? doc.fullDocumentBeforeChange?.userId;
            log.warn(
                `Skipping presence event (operationType=${change.operationType}, userId=${userId ?? 'unknown'}): ${(err as Error)?.message ?? String(err)}`
            );
            continue;
        }
        if (event) yield event;
    }
}

async function toPresenceEvent(
    change: ChangeStreamDocument<Document>,
    store: PresenceStore,
    log: Logger,
    activityFields: string[]
): Promise<PresenceEvent | undefined> {
    switch (change.operationType) {
        case 'insert': {
            const doc = change.fullDocument as (PresenceDoc & { _id?: ObjectId }) | undefined;
            if (!doc || !doc.userId || !doc._id) {
                log.warn('Skipping presence insert with no userId or _id — cannot determine online transition');
                return undefined;
            }
            // Order-stable 0→1 check — see the module docstring for why a live
            // total-count check would silently swallow concurrent first connections.
            if ((await store.countOlderUserRows(doc.userId, doc._id)) > 0) return undefined;
            return {
                type: 'online',
                userId: doc.userId,
                scopeId: doc.scopeId,
                key: doc.key,
                ownedByThisServer: doc.serverId === store.serverId,
                meta: doc.meta,
                page: doc.page,
            };
        }
        case 'delete': {
            const before = (change as { fullDocumentBeforeChange?: PresenceDoc }).fullDocumentBeforeChange;
            if (!before) {
                log.warn('Skipping presence delete with no pre-image — cannot determine online/offline transition');
                return undefined;
            }
            if ((await store.countUserRows(before.userId)) !== 0) return undefined;
            return {
                type: 'offline',
                userId: before.userId,
                scopeId: before.scopeId,
                key: before.key,
                ownedByThisServer: before.serverId === store.serverId,
                meta: before.meta,
                page: before.page,
            };
        }
        case 'update':
        case 'replace': {
            const doc = change.fullDocument as PresenceDoc | undefined;
            if (!doc || !doc.userId) return undefined;
            // A `replace` carries no updateDescription but rewrites the whole
            // row, so it counts as having written the activity fields; an
            // `update` must have touched one of them, or this is a keepAlive
            // (lastSeen only) and there is nothing to announce.
            if (change.operationType === 'update') {
                const written = Object.keys(
                    (change as { updateDescription?: { updatedFields?: Record<string, unknown> } }).updateDescription
                        ?.updatedFields ?? {}
                );
                const touched = written.some(field =>
                    activityFields.some(
                        watched =>
                            field === watched || field.startsWith(`${watched}.`) || watched.startsWith(`${field}.`)
                    )
                );
                if (!touched) return undefined;
            }
            return {
                type: 'activity',
                userId: doc.userId,
                scopeId: doc.scopeId,
                key: doc.key,
                ownedByThisServer: doc.serverId === store.serverId,
                meta: doc.meta,
                page: doc.page,
            };
        }
        default:
            return undefined;
    }
}
