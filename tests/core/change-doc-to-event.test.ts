import { describe, it, expect } from 'vitest';
import type { ChangeStreamDocument } from 'mongodb';
import { changeDocToEvent } from '../../src/core/change-doc-to-event.js';
import { didChange } from '../../src/core/change-event.js';
import type { ChangeEvent, EntityChanged } from '../../src/core/change-event.js';

describe('changeDocToEvent', () => {
    it('insert with fullDocument.organizationId maps to a changed event', () => {
        const fullDocument = {
            _id: { toString: () => 'id1' },
            organizationId: { toString: () => 'org1' },
            name: 'Widget',
        };
        const change = {
            operationType: 'insert',
            fullDocument,
        } as unknown as ChangeStreamDocument;

        expect(changeDocToEvent(change)).toEqual({
            type: 'changed',
            scopeId: 'org1',
            data: fullDocument,
            operation: 'insert',
        });
    });

    it('update with fullDocument.deleted === true maps to a removed event (soft delete)', () => {
        const change = {
            operationType: 'update',
            fullDocument: {
                _id: { toString: () => 'id2' },
                organizationId: { toString: () => 'org1' },
                userId: { toString: () => 'u1' },
                deleted: true,
            },
        } as unknown as ChangeStreamDocument;

        expect(changeDocToEvent(change)).toEqual({
            type: 'removed',
            id: 'id2',
            scopeId: 'org1',
            ownerId: 'u1',
            via: 'softDelete',
        });
    });

    it("operationType 'delete' with a pre-image maps to removed using scopeId/ownerId from fullDocumentBeforeChange", () => {
        const change = {
            operationType: 'delete',
            documentKey: { _id: { toString: () => 'id3' } },
            fullDocumentBeforeChange: {
                organizationId: { toString: () => 'org1' },
                userId: { toString: () => 'u1' },
            },
        } as unknown as ChangeStreamDocument;

        expect(changeDocToEvent(change)).toEqual({
            type: 'removed',
            id: 'id3',
            scopeId: 'org1',
            ownerId: 'u1',
            via: 'delete',
            before: { organizationId: { toString: expect.any(Function) }, userId: { toString: expect.any(Function) } },
        });
    });

    it("operationType 'delete' without a pre-image maps to removed with scopeId/ownerId undefined", () => {
        const change = {
            operationType: 'delete',
            documentKey: { _id: { toString: () => 'id4' } },
        } as unknown as ChangeStreamDocument;

        expect(changeDocToEvent(change)).toEqual({
            type: 'removed',
            id: 'id4',
            scopeId: undefined,
            ownerId: undefined,
            via: 'delete',
        });
    });

    it('a changed document with no scope field is skipped (returns null)', () => {
        const change = {
            operationType: 'insert',
            fullDocument: {
                _id: { toString: () => 'id5' },
                name: 'Widget',
            },
        } as unknown as ChangeStreamDocument;

        expect(changeDocToEvent(change)).toBeNull();
    });

    it('update with no fullDocument returns null', () => {
        const change = {
            operationType: 'update',
        } as unknown as ChangeStreamDocument;

        expect(changeDocToEvent(change)).toBeNull();
    });

    it('honors custom conventions for a changed event (tenantId scope field)', () => {
        const fullDocument = {
            _id: { toString: () => 'id7' },
            tenantId: { toString: () => 'tenant1' },
            ownerId: { toString: () => 'u7' },
        };
        const change = {
            operationType: 'insert',
            fullDocument,
        } as unknown as ChangeStreamDocument;

        expect(
            changeDocToEvent(change, { scopeField: 'tenantId', softDeleteField: 'archived', ownerField: 'ownerId' })
        ).toEqual({
            type: 'changed',
            scopeId: 'tenant1',
            data: fullDocument,
            operation: 'insert',
        });
    });

    it('honors custom conventions for a soft-delete event (archived flag + ownerId field)', () => {
        const change = {
            operationType: 'update',
            fullDocument: {
                _id: { toString: () => 'id8' },
                tenantId: { toString: () => 'tenant1' },
                ownerId: { toString: () => 'u8' },
                archived: true,
            },
        } as unknown as ChangeStreamDocument;

        expect(
            changeDocToEvent(change, { scopeField: 'tenantId', softDeleteField: 'archived', ownerField: 'ownerId' })
        ).toEqual({
            type: 'removed',
            id: 'id8',
            scopeId: 'tenant1',
            ownerId: 'u8',
            via: 'softDelete',
        });
    });
});

describe('changeDocToEvent — operation and updatedFields', () => {
    it('insert carries operation:insert and no updatedFields', () => {
        const change = {
            operationType: 'insert',
            fullDocument: { _id: { toString: () => 'id1' }, organizationId: { toString: () => 'org1' } },
        } as unknown as ChangeStreamDocument;

        const event = changeDocToEvent(change);
        expect(event).toMatchObject({ type: 'changed', operation: 'insert' });
        expect((event as { updatedFields?: string[] }).updatedFields).toBeUndefined();
    });

    it('update carries operation:update and the top-level updatedFields keys', () => {
        const change = {
            operationType: 'update',
            fullDocument: { _id: { toString: () => 'id2' }, organizationId: { toString: () => 'org1' } },
            updateDescription: { updatedFields: { status: 'cancelled', 'nested.leaf': 1 }, removedFields: [] },
        } as unknown as ChangeStreamDocument;

        expect(changeDocToEvent(change)).toMatchObject({
            type: 'changed',
            operation: 'update',
            updatedFields: ['status', 'nested.leaf'],
        });
    });

    it('replace carries operation:replace and no updatedFields (the whole document was written)', () => {
        const change = {
            operationType: 'replace',
            fullDocument: { _id: { toString: () => 'id3' }, organizationId: { toString: () => 'org1' } },
        } as unknown as ChangeStreamDocument;

        const event = changeDocToEvent(change);
        expect(event).toMatchObject({ type: 'changed', operation: 'replace' });
        expect((event as { updatedFields?: string[] }).updatedFields).toBeUndefined();
    });

    it("hard delete is via:'delete'; soft delete is via:'softDelete'", () => {
        const hard = {
            operationType: 'delete',
            documentKey: { _id: { toString: () => 'id4' } },
            fullDocumentBeforeChange: { organizationId: { toString: () => 'org1' } },
        } as unknown as ChangeStreamDocument;
        const soft = {
            operationType: 'update',
            fullDocument: {
                _id: { toString: () => 'id5' },
                organizationId: { toString: () => 'org1' },
                deleted: true,
            },
        } as unknown as ChangeStreamDocument;

        expect(changeDocToEvent(hard)).toMatchObject({ type: 'removed', via: 'delete' });
        expect(changeDocToEvent(soft)).toMatchObject({ type: 'removed', via: 'softDelete' });
    });
});

describe('didChange', () => {
    const changed = (extra: Partial<EntityChanged>): ChangeEvent => ({
        type: 'changed',
        scopeId: 'org1',
        data: {},
        operation: 'update',
        ...extra,
    });

    it('is true when the field is in updatedFields', () => {
        expect(didChange(changed({ updatedFields: ['status'] }), 'status')).toBe(true);
    });

    it('is false when an update wrote other fields only', () => {
        expect(didChange(changed({ updatedFields: ['externalEventIds'] }), 'status')).toBe(false);
    });

    it('is false when an update carries no updatedFields at all', () => {
        expect(didChange(changed({}), 'status')).toBe(false);
    });

    it('is true for insert and replace, which write the whole document', () => {
        expect(didChange(changed({ operation: 'insert' }), 'status')).toBe(true);
        expect(didChange(changed({ operation: 'replace' }), 'status')).toBe(true);
    });

    it('matches a dotted subpath write as a write of its parent', () => {
        expect(didChange(changed({ updatedFields: ['address.city'] }), 'address')).toBe(true);
    });

    it('matches a parent write as a write of a watched dotted subpath', () => {
        expect(didChange(changed({ updatedFields: ['address'] }), 'address.city')).toBe(true);
    });

    it('does not match a field that merely shares a prefix, in either direction', () => {
        expect(didChange(changed({ updatedFields: ['statusCode'] }), 'status')).toBe(false);
        expect(didChange(changed({ updatedFields: ['status'] }), 'statusCode')).toBe(false);
    });

    it('is false for removed events, which wrote no fields', () => {
        expect(didChange({ type: 'removed', id: 'x', via: 'delete' }, 'status')).toBe(false);
    });
});

describe('changeDocToEvent — pre-image as event.before', () => {
    const before = { _id: 'id1', organizationId: 'org1', userId: 'u1', propertyId: 'p-old' };

    it('update carries fullDocumentBeforeChange as before', () => {
        const fullDocument = { _id: 'id1', organizationId: 'org1', propertyId: 'p-new' };
        const change = {
            operationType: 'update',
            fullDocument,
            fullDocumentBeforeChange: before,
            updateDescription: { updatedFields: { propertyId: 'p-new' } },
        } as unknown as ChangeStreamDocument;

        expect(changeDocToEvent(change)).toMatchObject({ type: 'changed', data: fullDocument, before });
    });

    it('replace carries fullDocumentBeforeChange as before', () => {
        const change = {
            operationType: 'replace',
            fullDocument: { _id: 'id1', organizationId: 'org1' },
            fullDocumentBeforeChange: before,
        } as unknown as ChangeStreamDocument;

        expect((changeDocToEvent(change) as EntityChanged).before).toBe(before);
    });

    it('hard delete carries the whole pre-image as before, not only scopeId/ownerId', () => {
        const change = {
            operationType: 'delete',
            documentKey: { _id: 'id1' },
            fullDocumentBeforeChange: before,
        } as unknown as ChangeStreamDocument;

        expect(changeDocToEvent(change)).toEqual({
            type: 'removed',
            id: 'id1',
            scopeId: 'org1',
            ownerId: 'u1',
            via: 'delete',
            before,
        });
    });

    it('soft delete carries the pre-image (the state before the deleted flag was set) as before', () => {
        const change = {
            operationType: 'update',
            fullDocument: { _id: 'id1', organizationId: 'org1', userId: 'u1', deleted: true },
            fullDocumentBeforeChange: before,
        } as unknown as ChangeStreamDocument;

        expect(changeDocToEvent(change)).toMatchObject({ type: 'removed', via: 'softDelete', before });
    });

    it('insert has no before key at all', () => {
        const change = {
            operationType: 'insert',
            fullDocument: { _id: 'id1', organizationId: 'org1' },
        } as unknown as ChangeStreamDocument;

        expect(changeDocToEvent(change)).not.toHaveProperty('before');
    });

    it('an update or delete without a pre-image (null or absent) has no before key', () => {
        const update = {
            operationType: 'update',
            fullDocument: { _id: 'id1', organizationId: 'org1' },
            fullDocumentBeforeChange: null,
        } as unknown as ChangeStreamDocument;
        const hard = {
            operationType: 'delete',
            documentKey: { _id: 'id1' },
            fullDocumentBeforeChange: null,
        } as unknown as ChangeStreamDocument;

        expect(changeDocToEvent(update)).not.toHaveProperty('before');
        expect(changeDocToEvent(hard)).not.toHaveProperty('before');
    });
});
