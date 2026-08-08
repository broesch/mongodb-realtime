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
        };
    }

    // `updateDescription` exists only on `update` — an `insert`/`replace`
    // wrote the whole document, which `didChange` reports as having written
    // every field.
    const updatedFields =
        change.operationType === 'update' ? Object.keys(change.updateDescription?.updatedFields ?? {}) : undefined;

    return { type: 'changed', scopeId, data: doc, operation: change.operationType, updatedFields };
}
