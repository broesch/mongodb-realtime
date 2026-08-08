/**
 * Canonical realtime event emitted for a single MongoDB change-stream
 * document. Consumers (socket.io fan-out, NestJS gateways, application code)
 * work against this shape instead of raw driver change documents.
 */
/** The change-stream operation that produced a `changed` event. */
export type ChangeOperation = 'insert' | 'update' | 'replace';

export interface EntityChanged<TTenant = unknown> {
    type: 'changed';
    /** Stringified value of the configured scope field (e.g. organizationId / tenantId). */
    scopeId: string;
    /** The full document as read off the change stream. */
    data: unknown;
    /**
     * Which operation produced this event. Consumers that emit different
     * application events for a create than for an edit (a near-universal
     * need) branch on this; without it `insert` and `update` are
     * indistinguishable.
     */
    operation: ChangeOperation;
    /**
     * Top-level and dotted keys written by an `update`, from the change
     * document's `updateDescription.updatedFields`. `undefined` for `insert`
     * and `replace`, which write the whole document — prefer {@link didChange}
     * over reading this directly, so that case is handled correctly.
     */
    updatedFields?: string[];
    tenant?: TTenant;
}

export interface EntityRemoved<TTenant = unknown> {
    type: 'removed';
    /**
     * Stringified value of the configured scope field, when it could be
     * determined (e.g. from a pre-image on a hard delete). `undefined` when
     * no pre-image was available.
     */
    scopeId?: string;
    id: string;
    /** Stringified value of the configured owner field, when available. */
    ownerId?: string;
    /**
     * How the removal was expressed: a hard `delete`, or an update that set
     * the configured soft-delete field. Consumers that must distinguish
     * "gone from the database" from "flagged deleted" branch on this.
     * NOTE: every later write to an already-soft-deleted row ALSO yields a
     * `removed` event with `via: 'softDelete'` — the event alone cannot say
     * whether this was the flagging write. Consumers needing exactly-once
     * removal handling should de-duplicate by `id`.
     */
    via: 'delete' | 'softDelete';
    tenant?: TTenant;
}

export type ChangeEvent<TTenant = unknown> = EntityChanged<TTenant> | EntityRemoved<TTenant>;

/**
 * Did this event write `field`?
 *
 * The subtlety this exists to hide: a `replace` rewrites the entire document
 * but carries NO `updateDescription`, so a consumer testing
 * `updatedFields.includes('status')` directly would treat a whole-document
 * rewrite as "status untouched" and silently drop the event. Same for
 * `insert`. Both are reported as having written every field.
 *
 * A dotted subpath write (`{'address.city': 'X'}`) counts as a write of its
 * parent (`address`), because that is what a consumer watching `address`
 * means, and the reverse also holds: a write of the parent (`{'address':
 * {...}}`) counts as a write of a watched dotted subpath (`address.city`),
 * because `$set`-ing the whole parent rewrites every subfield underneath it —
 * `didChange(event, 'address.city')` must see that just as clearly as
 * `didChange(event, 'address')` does. A field that merely shares a prefix
 * (`statusCode` vs `status`) matches in neither direction.
 */
export function didChange(event: ChangeEvent, field: string): boolean {
    if (event.type !== 'changed') return false;
    if (event.operation !== 'update') return true;
    return (event.updatedFields ?? []).some(
        written => written === field || written.startsWith(`${field}.`) || field.startsWith(`${written}.`)
    );
}

/**
 * Field-name conventions used to derive a `ChangeEvent` from a raw change
 * document. All fields are optional on input; `DEFAULT_CONVENTIONS` supplies
 * the fallback names used by the pactsheet.fm origin of this logic.
 */
export interface ChangeEventConventions {
    /** Field holding the tenant/organization the document is scoped to. */
    scopeField?: string;
    /** Boolean field marking a document as soft-deleted. */
    softDeleteField?: string;
    /** Field holding the id of the user that owns/authored the document. */
    ownerField?: string;
}

export const DEFAULT_CONVENTIONS: Required<ChangeEventConventions> = {
    scopeField: 'organizationId',
    softDeleteField: 'deleted',
    ownerField: 'userId',
};
