/**
 * Canonical realtime event emitted for a single MongoDB change-stream
 * document. Consumers (socket.io fan-out, NestJS gateways, application code)
 * work against this shape instead of raw driver change documents.
 */
export interface EntityChanged<TTenant = unknown> {
    type: 'changed';
    /** Stringified value of the configured scope field (e.g. organizationId / tenantId). */
    scopeId: string;
    /** The full document as read off the change stream. */
    data: unknown;
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
    tenant?: TTenant;
}

export type ChangeEvent<TTenant = unknown> = EntityChanged<TTenant> | EntityRemoved<TTenant>;

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
