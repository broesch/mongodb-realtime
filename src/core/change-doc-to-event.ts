import type { ChangeStreamDocument } from 'mongodb';
import type { ChangeEvent, ChangeEventConventions } from './change-event.js';
import { DEFAULT_CONVENTIONS } from './change-event.js';

/**
 * Stringifies a dynamically-looked-up field value. A missing/null/undefined
 * field maps to `undefined` (never the string `'null'`/`'undefined'`); any
 * other primitive or object value is stringified via its own `toString()`.
 */
function toStr(value: unknown): string | undefined {
    return (value as { toString(): string } | null | undefined)?.toString();
}

/**
 * `{ before }` when the change document carries a pre-image, else `{}` — so an
 * event without one has no `before` key at all (not `before: undefined`), and
 * consumers comparing events structurally see exactly the pre-0.4 shape.
 */
function preImage(change: ChangeStreamDocument): { before?: Record<string, unknown> } {
    // Only update/replace/delete documents declare the field; an insert has
    // no "before".
    const before = 'fullDocumentBeforeChange' in change ? change.fullDocumentBeforeChange : undefined;
    return before ? { before } : {};
}

/**
 * Maps a raw MongoDB change-stream document to a canonical `ChangeEvent`, or
 * returns `null` when the event should be skipped (a non-delete change with
 * no scope field, or an operation type that carries no document data, e.g.
 * `drop`/`rename`/`invalidate`).
 *
 * Field names are configurable via `conventions` so callers whose schema
 * uses different names for the scope/tenant field, the soft-delete flag, or
 * the owning-user field can reuse the same mapping logic. Ported from
 * pactsheet.fm's `changeDocToEvent`, generalized with configurable field
 * conventions and renamed fields (`organizationId` → `scopeId`, `userId` →
 * `ownerId`).
 *
 * The change document's `fullDocumentBeforeChange` (present when the
 * collection records pre-images and the stream asks for them, which
 * `watchCollection` and the hub both do) travels on the event as `before`,
 * for `update`, `replace` and `delete` — on `changed` and `removed` events
 * alike. The fan-out loops use it to address a removal to whoever could see
 * the record until now, and to detect a record leaving a socket's reach.
 */
export function changeDocToEvent(
    change: ChangeStreamDocument,
    conventions: ChangeEventConventions = {}
): ChangeEvent | null {
    const conv = { ...DEFAULT_CONVENTIONS, ...conventions };

    if (change.operationType === 'delete') {
        const before = change.fullDocumentBeforeChange;
        return {
            type: 'removed',
            id: toStr(change.documentKey._id) ?? '',
            scopeId: toStr(before?.[conv.scopeField]),
            ownerId: toStr(before?.[conv.ownerField]),
            via: 'delete',
            ...preImage(change),
        };
    }

    if (change.operationType !== 'insert' && change.operationType !== 'update' && change.operationType !== 'replace') {
        return null;
    }

    const doc = change.fullDocument;
    if (!doc) return null;

    const scopeId = toStr(doc[conv.scopeField]);
    if (!scopeId) return null;

    if (doc[conv.softDeleteField] === true) {
        return {
            type: 'removed',
            id: toStr(doc._id) ?? '',
            scopeId,
            ownerId: toStr(doc[conv.ownerField]),
            via: 'softDelete',
            ...preImage(change),
        };
    }

    // `updateDescription` exists only on `update` — an `insert`/`replace`
    // wrote the whole document, which `didChange` reports as having written
    // every field.
    const updatedFields =
        change.operationType === 'update' ? Object.keys(change.updateDescription?.updatedFields ?? {}) : undefined;

    return {
        type: 'changed',
        scopeId,
        data: doc,
        operation: change.operationType,
        updatedFields,
        ...preImage(change),
    };
}
