import { describe, it, expect } from 'vitest';
import type { ChangeStreamDocument } from 'mongodb';
import { changeDocToEvent } from '../../src/core/change-doc-to-event.js';

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
        });
    });
});
