import type { ChangeStreamDocument, Collection, Document } from 'mongodb';
import { ChangeStreamSubscription } from '../core/change-stream-subscription.js';
import { collectionWatchFactory } from '../core/watch-collection.js';
import { consoleLogger, prefixed, type Logger } from '../common/logger.js';
import { ValueSignal, type ReadonlySignal } from '../common/signal.js';
import type { PresenceStore } from './presence-store.js';
import type { PresenceDoc } from './types.js';

export interface PresenceEvent {
    type: 'online' | 'offline';
    userId: string;
    scopeId: string;
    key: string;
    ownedByThisServer: boolean;
}

/**
 * Watch a `PresenceStore`'s collection and yield `online`/`offline` events on
 * 0→1 / 1→0 transitions, i.e. exactly once per user per side rather than once
 * per connection. Ported from calendar's `online-user.pusher.ts` refinement:
 * announce from the change stream, not the gateway, so a connection landing
 * on ANY node is visible to every node (fm's `presence.gateway.ts` pre-image
 * handling for the delete side).
 *
 * insert → `countUserRows(userId) === 1` → online (fields from `fullDocument`).
 * delete → `countUserRows(userId) === 0` → offline (fields from the pre-image).
 * update/replace (keepalive/setPage) → no event.
 *
 * Counting AFTER observing the event, rather than racing a live count against
 * the write, is safe because change-stream events for a given document key
 * are delivered in write order — by the time this insert/delete event is
 * observed, `countUserRows` already reflects (at least) that write.
 */
export async function* watchPresence(options: {
    store: PresenceStore;
    shutdown: ReadonlySignal<boolean>;
    logger?: Logger;
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
        const event = await toPresenceEvent(change, store, log);
        if (event) yield event;
    }
}

async function toPresenceEvent(
    change: ChangeStreamDocument<Document>,
    store: PresenceStore,
    log: Logger
): Promise<PresenceEvent | undefined> {
    switch (change.operationType) {
        case 'insert': {
            const doc = change.fullDocument as PresenceDoc | undefined;
            if (!doc) return undefined;
            if ((await store.countUserRows(doc.userId)) !== 1) return undefined;
            return {
                type: 'online',
                userId: doc.userId,
                scopeId: doc.scopeId,
                key: doc.key,
                ownedByThisServer: doc.serverId === store.serverId,
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
            };
        }
        default:
            // update/replace (keepalive, setPage) carry no transition.
            return undefined;
    }
}
