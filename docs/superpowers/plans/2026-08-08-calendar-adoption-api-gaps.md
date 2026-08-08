# Calendar Adoption API Gaps Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the API gaps that block `calendar`'s `apps/api` from consuming `mongodb-realtime` at the canonical-event and fan-out layers, so the app keeps zero hand-rolled change-stream, presence, or socket-binding code.

**Architecture:** All changes are additive to the existing four subpaths — no new modules, no new runtime dependencies. The through-line is that the canonical `ChangeEvent` currently discards the two things every real consumer branches on (which operation fired, which fields were written), which forces consumers down to raw change documents and makes the rest of the library dead weight to them. Task 1 restores that information; Tasks 2–4 make fan-out and presence expressive enough to carry app-semantic events on top of it; Tasks 5–7 finish the NestJS binding so an app needs no glue of its own; Tasks 8–9 are ship-safe hardening already triaged in `docs/follow-ups-0.1.x.md` that touches the same files. Tasks 11–12 add the ongoing-compatibility guardrails: a consumer-driven contract suite encoding calendar's five pusher compositions, and a checked-in public-API report that turns any surface change into a reviewable diff.

**Tech Stack:** TypeScript 5.x, tsup (dual ESM/CJS + dts), vitest + `mongodb-memory-server` (`MongoMemoryReplSet`), pnpm.

## Global Constraints

- **Zero runtime `dependencies`.** `mongodb` is the required peer; `socket.io` and `@nestjs/common` are optional peers imported **type-only** and only from `src/socketio/` and `src/nestjs/` respectively.
- **No rxjs, no lodash, no `@nestjs` imports in `src/` outside `src/nestjs/`.** Reactive wiring uses `ReadonlySignal`/`ValueSignal` from `src/common/signal.ts`.
- Package manager: **pnpm** (never npm).
- Relative ESM imports carry explicit `.js` extensions.
- Every `src/` file ≤ ~450 lines; one responsibility per file; sibling `types.ts` per domain folder; barrel `index.ts` per subpath.
- Tests import from `src/` (not `dist/`), run against a real `MongoMemoryReplSet` via `tests/helpers/replset.ts` when they touch Mongo, and use unique per-test collection names. Socket.io tests use `tests/socketio/harness.ts` (real server, real client sockets) rather than a mocked `Server`.
- Commands: `pnpm test`, `pnpm lint`, `pnpm typecheck`, `pnpm build`. A single file runs via `pnpm exec vitest run <path>`.
- Commit after every green task: conventional commits, body ends with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- **Version:** these are additive API changes plus one deliberate breaking change to a 0.x type (Task 1). Bump to `0.2.0` in Task 10, not per-task.
- **Reference consumer** (read it, don't guess at what the app needs): `/Users/benja/projects/calendar/apps/api/src` — specifically `change-stream/`, `realtime/`, `realtime-emitter/`, and the five pushers (`calendar/calendar-event.pusher.ts`, `booking/booking.pusher.ts`, `optimization-result/optimization-result.pusher.ts`, `notification/notification.pusher.ts`, `realtime/online-user.pusher.ts`). Each pusher's comments state the bug its field-gating prevents — those comments are the requirements for Task 1.

## File Structure

| File | Change | Responsibility after this plan |
|---|---|---|
| `src/core/change-event.ts` | Modify | Canonical event shape — now carries `operation`, `updatedFields`, `via`, plus the `didChange` predicate |
| `src/core/change-doc-to-event.ts` | Modify | Raw change doc → canonical event, preserving operation and written fields |
| `src/core/change-stream-hub.ts` | Modify | Per-key DB-level cursor; filters ignored collections server-side; `close()` is terminal |
| `src/core/change-stream-subscription.ts` | Modify | Resumable subscription; `resolveDb` no longer depends on driver-copy identity |
| `src/socketio/push-mapped.ts` | Create | Fan-out loop where the consumer picks event name, payload and rooms per event |
| `src/socketio/rooms.ts` | Modify | Room naming + `toRooms` normalisation |
| `src/socketio/types.ts` | Modify | `roomOf` widened to `string \| string[]`; `PushMappedOptions`/`MappedEmission` |
| `src/socketio/push-scoped.ts`, `push-owner-scoped.ts`, `push-scoped-filtered.ts` | Modify | Emit to one or many rooms in a single `to()` call |
| `src/presence/types.ts` | Modify | `PresenceEvent` gains `meta`/`page`; `activity` event type |
| `src/presence/watch-presence.ts` | Modify | Yields `activity` on tracked-field writes, threads `meta` |
| `src/nestjs/change-stream-event-pusher.ts` | Modify | Lifecycle base: `enabled` gate, `watchOne` convenience, early-stop log, both-path latch |
| `src/nestjs/preflight.ts` | Modify | Returns a usable result; actionable pre-image remediation copy |
| `src/nestjs/socket-server-handle.ts` | Create | Late-bound socket.io `Server` handle for gateways → pushers |
| `src/nestjs/realtime-module.ts` | Modify | Provides `REALTIME_ENABLED` and `SOCKET_SERVER` |
| `src/nestjs/index.ts` | Modify | Barrel export for the new handle |

---

### Task 1: `ChangeEvent` preserves operation and written fields

The canonical event collapses `insert`/`update`/`replace` into `changed` and discards `updateDescription`. Every consumer that must answer "was this a create or an edit?" or "was field X actually written?" is forced back onto raw change documents. This task restores both, and adds the predicate that gets the `replace` case right (a replace carries no `updateDescription` but rewrites every field, so it must count as having written any field — getting this wrong is a silent missed-event bug).

**Breaking:** `operation` and `via` are **required**, not optional — an optional field consumers can't rely on defeats the purpose. Existing hand-constructed `ChangeEvent` literals in tests must be updated; Step 7 covers that.

**Files:**
- Modify: `src/core/change-event.ts`
- Modify: `src/core/change-doc-to-event.ts:33-62`
- Test: `tests/core/change-doc-to-event.test.ts`, `tests/core/watch-collection.test.ts`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `type ChangeOperation = 'insert' | 'update' | 'replace'`
  - `EntityChanged.operation: ChangeOperation`, `EntityChanged.updatedFields?: string[]`
  - `EntityRemoved.via: 'delete' | 'softDelete'`
  - `didChange(event: ChangeEvent, field: string): boolean`
  - Tasks 4, 5 and the calendar pushers all consume `didChange`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/core/change-doc-to-event.test.ts`:

```ts
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
    const changed = (extra: Partial<EntityChanged>): ChangeEvent =>
        ({ type: 'changed', scopeId: 'org1', data: {}, operation: 'update', ...extra }) as ChangeEvent;

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

    it('does not match a field that merely shares a prefix', () => {
        expect(didChange(changed({ updatedFields: ['statusCode'] }), 'status')).toBe(false);
    });

    it('is false for removed events, which wrote no fields', () => {
        expect(didChange({ type: 'removed', id: 'x', via: 'delete' }, 'status')).toBe(false);
    });
});
```

Add to that file's imports:

```ts
import { changeDocToEvent } from '../../src/core/change-doc-to-event.js';
import { didChange } from '../../src/core/change-event.js';
import type { ChangeEvent, EntityChanged } from '../../src/core/change-event.js';
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pnpm exec vitest run tests/core/change-doc-to-event.test.ts`
Expected: FAIL — `didChange is not a function`, and the `toMatchObject` assertions fail on missing `operation`/`via`/`updatedFields`.

- [ ] **Step 3: Extend the event types**

In `src/core/change-event.ts`, replace the `EntityChanged` and `EntityRemoved` interfaces and append `didChange`:

```ts
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
```

Then append to the same file:

```ts
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
 * means; a field that merely shares a prefix (`statusCode` vs `status`) does
 * not.
 */
export function didChange(event: ChangeEvent, field: string): boolean {
    if (event.type !== 'changed') return false;
    if (event.operation !== 'update') return true;
    return (event.updatedFields ?? []).some(written => written === field || written.startsWith(`${field}.`));
}
```

- [ ] **Step 4: Populate the new fields in `changeDocToEvent`**

In `src/core/change-doc-to-event.ts`, replace the body from the `delete` branch to the end of the function:

```ts
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
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `pnpm exec vitest run tests/core/change-doc-to-event.test.ts`
Expected: PASS.

- [ ] **Step 6: Add the end-to-end passthrough test**

The fields must survive `watchCollection`'s mapping, not just `changeDocToEvent`. Append to `tests/core/watch-collection.test.ts`, following that file's existing replica-set setup and collection-naming pattern:

```ts
it('carries operation and updatedFields through to the yielded ChangeEvent', async () => {
    const name = uniqueCollectionName();
    const coll = db.collection(name);
    const shutdown = new ValueSignal(false);
    const events: ChangeEvent[] = [];

    const watcher = watchCollection({
        source: { watch: collectionWatchFactory(coll), db, collection: name },
        shutdown,
    });
    const pump = (async () => {
        for await (const event of watcher) {
            events.push(event);
            if (events.length === 2) break;
        }
    })();

    await delay(300); // let the change stream open before writing
    const { insertedId } = await coll.insertOne({ organizationId: 'org1', status: 'pending' });
    await coll.updateOne({ _id: insertedId }, { $set: { status: 'ready' } });

    await pump;
    shutdown.next(true);

    expect(events[0]).toMatchObject({ type: 'changed', operation: 'insert' });
    expect(events[1]).toMatchObject({ type: 'changed', operation: 'update', updatedFields: ['status'] });
});
```

Reuse whatever the file already imports for `db`, `delay` and its unique-collection-name helper; add `ChangeEvent` to its type imports if absent.

- [ ] **Step 7: Update existing literals and run the full suite**

`operation` and `via` are required, so every hand-constructed `ChangeEvent` literal now fails typecheck. Run `pnpm typecheck` and fix each reported literal: add `operation: 'insert'` to `changed` literals and `via: 'delete'` to `removed` literals unless the test's intent calls for another value. Expect hits in `tests/socketio/push-scoped.test.ts`, `push-owner-scoped.test.ts`, `push-scoped-filtered.test.ts`, and any `toEqual` on a whole event in `tests/core/change-doc-to-event.test.ts` (`toEqual` is exact — those need the new fields spelled out, or relaxing to `toMatchObject`).

Run: `pnpm typecheck && pnpm test && pnpm lint`
Expected: all PASS.

- [ ] **Step 8: Commit**

```bash
git add src/core/change-event.ts src/core/change-doc-to-event.ts tests/
git commit -m "feat(core): carry operation, updatedFields and removal cause on ChangeEvent"
```

---

### Task 2: `pushMapped` — consumer-chosen event name, payload and rooms

The three existing fan-out loops emit a fixed pair of event names (`{ changed, removed }`) with the document as payload, to one derived room. Apps with semantic events — "booking created" vs "booking cancelled" vs "meeting rescheduled", chosen from the document's own state, addressed to a set of participants plus their organization — cannot express that through any of them. This adds the general loop: the consumer returns what to emit, or `null` to skip. Skipping subsumes per-event filtering, so a consumer no longer needs a separate filter hook.

**Files:**
- Create: `src/socketio/push-mapped.ts`
- Modify: `src/socketio/types.ts` (append `MappedEmission`, `PushMappedOptions`)
- Modify: `src/socketio/index.ts` (export the new module)
- Test: `tests/socketio/push-mapped.test.ts`

**Interfaces:**
- Consumes: `ChangeEvent` from Task 1 (a `select` callback typically branches on `event.operation` / `didChange`).
- Produces:
  - `interface MappedEmission { name: string; payload: unknown; rooms: string | string[] }`
  - `pushMapped<TTenant>(server, watcher, select, options): Promise<void>` where `select: (event: ChangeEvent<TTenant>) => MappedEmission | null | Promise<MappedEmission | null>`
  - Task 3's `toRooms` is used here too — implement `toRooms` in this task (Step 3) and Task 3 reuses it.

- [ ] **Step 1: Write the failing test**

Create `tests/socketio/push-mapped.test.ts`:

```ts
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import type { Server } from 'socket.io';
import { pushMapped } from '../../src/socketio/push-mapped.js';
import type { ChangeEvent } from '../../src/core/change-event.js';
import type { Logger } from '../../src/common/logger.js';
import { ValueSignal } from '../../src/common/signal.js';
import { scopeRoom, userRoom } from '../../src/socketio/rooms.js';
import { startHarness, stopHarness, connectClient, joinRoom, receivedEvents, waitForCount, delay } from './harness.js';

async function* gen(events: ChangeEvent[]): AsyncGenerator<ChangeEvent> {
    for (const event of events) yield event;
}

function mockLogger(): { logger: Logger; calls: { level: string; message: string }[] } {
    const calls: { level: string; message: string }[] = [];
    return {
        calls,
        logger: {
            debug: message => calls.push({ level: 'debug', message }),
            log: message => calls.push({ level: 'log', message }),
            warn: message => calls.push({ level: 'warn', message }),
            error: message => calls.push({ level: 'error', message }),
        },
    };
}

const changed = (data: unknown): ChangeEvent => ({
    type: 'changed',
    scopeId: 'org1',
    data,
    operation: 'insert',
});

let server: Server;
let port: number;

beforeEach(async () => {
    const started = await startHarness();
    server = started.server;
    port = started.port;
});

afterEach(async () => {
    await stopHarness();
});

describe('pushMapped', () => {
    it('emits under the name and payload the selector returns', async () => {
        const client = await connectClient(port);
        await joinRoom(client, scopeRoom('org1'));
        const received = receivedEvents(client, 'booking:created');

        await pushMapped(
            server,
            gen([changed({ id: 'b1' })]),
            () => ({ name: 'booking:created', payload: { bookingId: 'b1' }, rooms: scopeRoom('org1') }),
            { shutdown: new ValueSignal(true) }
        );

        await waitForCount(received, 1);
        expect(received[0]).toEqual({ bookingId: 'b1' });
    });

    it('a socket in two of the target rooms receives exactly one copy', async () => {
        const client = await connectClient(port);
        await joinRoom(client, userRoom('u1'));
        await joinRoom(client, scopeRoom('org1'));
        const received = receivedEvents(client, 'booking:created');

        await pushMapped(
            server,
            gen([changed({ id: 'b1' })]),
            () => ({ name: 'booking:created', payload: {}, rooms: [userRoom('u1'), scopeRoom('org1')] }),
            { shutdown: new ValueSignal(true) }
        );

        await waitForCount(received, 1);
        await delay(200);
        expect(received).toHaveLength(1);
    });

    it('a selector returning null skips the event entirely', async () => {
        const client = await connectClient(port);
        await joinRoom(client, scopeRoom('org1'));
        const received = receivedEvents(client, 'booking:created');

        await pushMapped(server, gen([changed({ id: 'b1' })]), () => null, { shutdown: new ValueSignal(true) });

        await delay(300);
        expect(received).toHaveLength(0);
    });

    it('a throwing selector skips that one event and keeps the loop running', async () => {
        const client = await connectClient(port);
        await joinRoom(client, scopeRoom('org1'));
        const received = receivedEvents(client, 'e');
        const { logger, calls } = mockLogger();

        await pushMapped(
            server,
            gen([changed({ id: 'bad' }), changed({ id: 'good' })]),
            event => {
                if ((event as { data: { id: string } }).data.id === 'bad') throw new Error('boom');
                return { name: 'e', payload: 'ok', rooms: scopeRoom('org1') };
            },
            { shutdown: new ValueSignal(true), logger }
        );

        await waitForCount(received, 1);
        expect(received).toEqual(['ok']);
        expect(calls.some(c => c.level === 'error' && c.message.includes('boom'))).toBe(true);
    });

    it('drops an emission addressed to no rooms rather than broadcasting', async () => {
        const client = await connectClient(port);
        await joinRoom(client, scopeRoom('org1'));
        const received = receivedEvents(client, 'e');
        const { logger, calls } = mockLogger();

        await pushMapped(server, gen([changed({})]), () => ({ name: 'e', payload: 'x', rooms: [] }), {
            shutdown: new ValueSignal(true),
            logger,
        });

        await delay(300);
        expect(received).toHaveLength(0);
        expect(calls.some(c => c.level === 'warn' && c.message.includes('no rooms'))).toBe(true);
    });

    it('logs an error when the watcher returns before shutdown was signalled', async () => {
        const { logger, calls } = mockLogger();

        await pushMapped(server, gen([]), () => null, { shutdown: new ValueSignal(false), logger });

        expect(calls.some(c => c.level === 'error' && c.message.includes('returned before shutdown'))).toBe(true);
    });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pnpm exec vitest run tests/socketio/push-mapped.test.ts`
Expected: FAIL — cannot resolve `../../src/socketio/push-mapped.js`.

- [ ] **Step 3: Add `toRooms` to `src/socketio/rooms.ts`**

```ts
/**
 * Normalises a room specifier to a list, dropping empty entries. A single
 * `to(rooms)` call with the full list is not the same as one `to()` per room:
 * socket.io deduplicates recipients WITHIN a call, so a socket that belongs
 * to two of the target rooms receives one copy from the former and two from
 * the latter.
 */
export function toRooms(rooms: string | string[]): string[] {
    return (Array.isArray(rooms) ? rooms : [rooms]).filter(Boolean);
}
```

- [ ] **Step 4: Add the types**

Append to `src/socketio/types.ts`:

```ts
/** What {@link pushMapped}'s selector asks the loop to emit for one change event. */
export interface MappedEmission {
    /** socket.io event name. */
    name: string;
    payload: unknown;
    /**
     * One or more rooms. Emitted in a SINGLE `server.to(rooms)` call, so a
     * socket belonging to several of them receives exactly one copy.
     */
    rooms: string | string[];
}

export interface PushMappedOptions {
    /** Signals a graceful shutdown in progress; suppresses the "watcher returned early" error log. */
    shutdown: ReadonlySignal<boolean>;
    logger?: Logger;
    /** Log-message prefix; identifies which pusher an operator-visible log line came from. */
    name?: string;
}
```

- [ ] **Step 5: Implement `pushMapped`**

Create `src/socketio/push-mapped.ts`:

```ts
import type { Server } from 'socket.io';
import type { ChangeEvent } from '../core/change-event.js';
import { consoleLogger, prefixed } from '../common/logger.js';
import { toRooms } from './rooms.js';
import type { MappedEmission, PushMappedOptions } from './types.js';

/**
 * General fan-out loop: the consumer decides, per event, what to emit and to
 * whom — or that nothing should be emitted at all.
 *
 * `pushScoped`/`pushOwnerScoped`/`pushScopedFiltered` cover the case where
 * one collection maps to one pair of event names addressed to one derived
 * room. This covers the rest: application-semantic event names chosen from
 * the document's own state, hand-built DTOs, and recipient sets that aren't a
 * single room. Returning `null` from `select` skips the event, which is how a
 * consumer expresses "this write isn't interesting" (e.g. an update that
 * didn't touch the field this pusher exists to announce — see `didChange`).
 *
 * Free function, no lifecycle: same contract as `pushScoped`. A `select` that
 * throws or rejects skips that one event and is logged; it never breaks the
 * loop, because one malformed document must not stop fan-out for every
 * subsequent change.
 *
 * SECURITY NOTE: this loop has NO per-socket permission gate — routing is by
 * room membership alone. Do not use it for entities where recipients inside
 * the same room differ in what they may see; that is `pushScopedFiltered`'s
 * job (per-socket `canReceive`, two-variant PII rendering).
 */
export async function pushMapped<TTenant>(
    server: Server,
    watcher: AsyncGenerator<ChangeEvent<TTenant>>,
    select: (event: ChangeEvent<TTenant>) => MappedEmission | null | Promise<MappedEmission | null>,
    options: PushMappedOptions
): Promise<void> {
    const log = prefixed(options.logger ?? consoleLogger, options.name ?? 'pushMapped');

    for await (const event of watcher) {
        let emission: MappedEmission | null;
        try {
            emission = await Promise.resolve(select(event));
        } catch (err) {
            log.error(`select failed for a ${event.type} event — skipping emit: ${(err as Error)?.message ?? String(err)}`);
            continue;
        }
        if (!emission) continue;

        const rooms = toRooms(emission.rooms);
        if (rooms.length === 0) {
            // Emitting with an empty room list would broadcast to every
            // connected socket — the opposite of what a caller that resolved
            // no recipients meant.
            log.warn(`Dropping ${emission.name} — select resolved no rooms to address`);
            continue;
        }
        server.to(rooms).emit(emission.name, emission.payload);
    }

    if (!options.shutdown.getValue()) {
        log.error(
            'Change-stream watcher returned before shutdown — gateway will no longer fan out events. Restart the service or check the watcher for an uncaught rejection.'
        );
    }
}
```

- [ ] **Step 6: Export it**

In `src/socketio/index.ts`, add `export * from './push-mapped.js';`.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `pnpm exec vitest run tests/socketio/push-mapped.test.ts && pnpm typecheck && pnpm lint`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/socketio/ tests/socketio/push-mapped.test.ts
git commit -m "feat(socketio): add pushMapped for consumer-chosen event name, payload and rooms"
```

---

### Task 3: `roomOf` may address several rooms

Same recipient-dedupe need as Task 2, for the three preset loops: a consumer whose `roomOf` wants to reach a user's own room *and* their organization's room currently cannot, because the signature returns one string.

**Files:**
- Modify: `src/socketio/types.ts` (`PushScopedOptions.roomOf`)
- Modify: `src/socketio/push-scoped.ts:24-33,45,56`
- Modify: `src/socketio/push-scoped-filtered.ts:69-75,93,97`
- Modify: `src/socketio/push-owner-scoped.ts` (leave `roomFor` returning one room; only guard the empty case)
- Test: `tests/socketio/push-scoped.test.ts`

**Interfaces:**
- Consumes: `toRooms` from Task 2 Step 3.
- Produces: `PushScopedOptions.roomOf?: (event: ChangeEvent<TTenant>) => string | string[]`.

- [ ] **Step 1: Write the failing test**

Append to `tests/socketio/push-scoped.test.ts`:

```ts
it('roomOf returning several rooms delivers one copy to a socket in two of them', async () => {
    const client = await connectClient(port);
    await joinRoom(client, scopeRoom('org1'));
    await joinRoom(client, 'extra-room');
    const received = receivedEvents(client, 'doc:changed');

    const event: ChangeEvent = { type: 'changed', scopeId: 'org1', data: { v: 1 }, operation: 'insert' };
    await pushScoped(server, gen([event]), { changed: 'doc:changed' }, {
        shutdown: new ValueSignal(true),
        roomOf: () => [scopeRoom('org1'), 'extra-room'],
    });

    await waitForCount(received, 1);
    await delay(200);
    expect(received).toHaveLength(1);
});

it('drops an event whose roomOf resolves to no rooms rather than broadcasting', async () => {
    const client = await connectClient(port);
    await joinRoom(client, scopeRoom('org1'));
    const received = receivedEvents(client, 'doc:changed');
    const { logger, calls } = mockLogger();

    const event: ChangeEvent = { type: 'changed', scopeId: 'org1', data: { v: 1 }, operation: 'insert' };
    await pushScoped(server, gen([event]), { changed: 'doc:changed' }, {
        shutdown: new ValueSignal(true),
        roomOf: () => [],
        logger,
    });

    await delay(300);
    expect(received).toHaveLength(0);
    expect(calls.some(c => c.level === 'warn' && c.message.includes('no rooms'))).toBe(true);
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pnpm exec vitest run tests/socketio/push-scoped.test.ts`
Expected: FAIL — typecheck rejects `() => [...]` for `roomOf`, and the empty-room case broadcasts instead of dropping.

- [ ] **Step 3: Widen the type**

In `src/socketio/types.ts`, change `PushScopedOptions.roomOf` to:

```ts
    /**
     * Room(s) an event fans out to. Default:
     * `namespacedRoom(tenantNamespace?.(tenant), scopeRoom(scopeId))`.
     * Returning a list emits once to all of them in a single `to()` call, so
     * a socket in several receives one copy; returning an empty list drops
     * the event (emitting to no rooms would broadcast to everyone).
     */
    roomOf?: (event: ChangeEvent<TTenant>) => string | string[];
```

- [ ] **Step 4: Guard the emit sites**

In `src/socketio/push-scoped.ts`, import `toRooms` from `./rooms.js`, and replace each of the two `server.to(roomOf(event)).emit(...)` calls with a resolved-and-checked list. Add this helper above the `for await` loop:

```ts
    const roomsFor = (event: ChangeEvent<TTenant>, eventName: string): string[] | null => {
        const rooms = toRooms(roomOf(event));
        if (rooms.length === 0) {
            log.warn(`Dropping ${eventName} — roomOf resolved no rooms to address`);
            return null;
        }
        return rooms;
    };
```

then in the removed branch:

```ts
            const rooms = roomsFor(event, events.removed);
            if (!rooms) continue;
            server.to(rooms).emit(events.removed, event.id);
```

and in the changed branch, after `payload` is resolved:

```ts
            const rooms = roomsFor(event, events.changed);
            if (!rooms) continue;
            server.to(rooms).emit(events.changed, payload);
```

In `src/socketio/push-scoped-filtered.ts`, add the identical `toRooms` import and `roomsFor` helper (its `roomOf` default and log-prefix variable are already named the same), replace its removed-branch emit the same way, and change its changed path — which additionally feeds `server.in(...)`:

```ts
        const rooms = roomsFor(event, events.changed);
        if (!rooms) continue;
        const sockets = await server.in(rooms).fetchSockets();
```

In `src/socketio/push-owner-scoped.ts`, `roomFor` derives exactly one room from an owner id and stays `string` — no change beyond leaving it as is.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `pnpm exec vitest run tests/socketio/ && pnpm typecheck && pnpm lint`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/socketio/ tests/socketio/push-scoped.test.ts
git commit -m "feat(socketio): allow roomOf to address several rooms in one emit"
```

---

### Task 4: Presence activity events and metadata passthrough

`watchPresence` yields only `online`/`offline`; every `update`/`replace` returns `undefined`, so a page/route change written by `PresenceStore.setPage` is unobservable — the store can record it but nothing can announce it. Separately, `PresenceDoc.meta` exists and is written by `checkIn`, but `PresenceEvent` drops it, so an app that wants to announce *who* came online (an email, a display name) has to keep its own presence collection alongside this one.

**Files:**
- Modify: `src/presence/types.ts`
- Modify: `src/presence/watch-presence.ts`
- Test: `tests/presence/watch-presence.test.ts`

**Interfaces:**
- Consumes: `PresenceStore` (unchanged), `didChange`'s dotted-prefix semantics are **not** reused here — presence reads `updateDescription` directly because it watches raw change docs, not `ChangeEvent`s.
- Produces:
  - `PresenceEvent.type` widened to `'online' | 'offline' | 'activity'`
  - `PresenceEvent.meta?: Record<string, unknown>`, `PresenceEvent.page?: string`
  - `watchPresence(options)` gains `activityFields?: string[]` (default `['page']`)

- [ ] **Step 1: Write the failing test**

Append to `tests/presence/watch-presence.test.ts`, following that file's existing store/replica-set setup:

```ts
it('yields an activity event carrying the new page when setPage writes it', async () => {
    const shutdown = new ValueSignal(false);
    const events: PresenceEvent[] = [];
    const pump = (async () => {
        for await (const event of watchPresence({ store, shutdown })) {
            events.push(event);
            if (events.some(e => e.type === 'activity')) break;
        }
    })();

    await delay(300);
    await store.checkIn({ userId: 'u1', clientId: 'c1', scopeId: 'org1' });
    await store.setPage('u1', 'c1', '/calendar');

    await pump;
    shutdown.next(true);

    const activity = events.find(e => e.type === 'activity');
    expect(activity).toMatchObject({ type: 'activity', userId: 'u1', scopeId: 'org1', page: '/calendar' });
});

it('does not yield an activity event for a keepAlive, which only writes lastSeen', async () => {
    const shutdown = new ValueSignal(false);
    const events: PresenceEvent[] = [];
    const pump = (async () => {
        for await (const event of watchPresence({ store, shutdown })) {
            events.push(event);
        }
    })();

    await delay(300);
    await store.checkIn({ userId: 'u2', clientId: 'c2', scopeId: 'org1' });
    await store.keepAlive('u2', 'c2');
    await delay(500);

    shutdown.next(true);
    await pump;

    expect(events.filter(e => e.type === 'activity')).toHaveLength(0);
});

it('threads the row meta onto online and offline events', async () => {
    const shutdown = new ValueSignal(false);
    const events: PresenceEvent[] = [];
    const pump = (async () => {
        for await (const event of watchPresence({ store, shutdown })) {
            events.push(event);
            if (events.length === 2) break;
        }
    })();

    await delay(300);
    await store.checkIn({ userId: 'u3', clientId: 'c3', scopeId: 'org1', meta: { email: 'a@b.c' } });
    await store.checkOut('u3', 'c3');

    await pump;
    shutdown.next(true);

    expect(events[0]).toMatchObject({ type: 'online', meta: { email: 'a@b.c' } });
    expect(events[1]).toMatchObject({ type: 'offline', meta: { email: 'a@b.c' } });
});
```

Add `PresenceEvent` to the file's type imports if it isn't already there.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pnpm exec vitest run tests/presence/watch-presence.test.ts`
Expected: FAIL — no `activity` event is ever yielded (the update branch returns `undefined`), and `meta` is absent from online/offline events.

- [ ] **Step 3: Widen the event type**

In `src/presence/types.ts`, append:

```ts
/** Fields whose write produces an `activity` event, when the caller doesn't name their own. */
export const DEFAULT_ACTIVITY_FIELDS = ['page'];
```

In `src/presence/watch-presence.ts`, replace the `PresenceEvent` interface:

```ts
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
    /** The row's current `page`. Always present on `activity`; present on `online` when the row carried one. */
    page?: string;
}
```

- [ ] **Step 4: Implement the activity branch**

In `src/presence/watch-presence.ts`, add `activityFields` to the options and thread it into `toPresenceEvent`:

```ts
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
```

and at the call site inside the loop:

```ts
            event = await toPresenceEvent(change, store, log, options.activityFields ?? DEFAULT_ACTIVITY_FIELDS);
```

Then in `toPresenceEvent`, add the parameter, thread `meta`/`page` into the two existing branches, and replace the `default` branch:

```ts
async function toPresenceEvent(
    change: ChangeStreamDocument<Document>,
    store: PresenceStore,
    log: Logger,
    activityFields: string[]
): Promise<PresenceEvent | undefined> {
```

In the `insert` branch's return, add `meta: doc.meta, page: doc.page`. In the `delete` branch's return, add `meta: before.meta, page: before.page`. Then:

```ts
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
                    activityFields.some(watched => field === watched || field.startsWith(`${watched}.`))
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
```

Import `DEFAULT_ACTIVITY_FIELDS` and `PresenceDoc` from `./types.js` as needed, and update the module docstring's "update/replace (keepalive/setPage) → no event" line to describe the new behaviour.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `pnpm exec vitest run tests/presence/ && pnpm typecheck && pnpm lint`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/presence/ tests/presence/watch-presence.test.ts
git commit -m "feat(presence): yield activity events on tracked-field writes and thread row meta"
```

---

### Task 5: `ChangeStreamEventPusher` gains an enabled gate, a one-collection helper, and a complete latch

Three gaps in the NestJS lifecycle base, all of which every consumer currently reimplements:

1. No way to boot with realtime off (a dev/CI affordance), so each subclass writes its own `if (!enabled) return`.
2. No convenience for the overwhelmingly common shape — watch one collection, handle each change — so each subclass wires a `ChangeStreamSubscription` by hand.
3. The shutdown latch is only resolved on the *rejection* path (`docs/follow-ups-0.1.x.md`), so a `pushChangeEvents()` that returns normally without calling `loopEnded()` hangs shutdown forever; and a loop that stops early while the app is still running is silently invisible.

**Files:**
- Modify: `src/nestjs/change-stream-event-pusher.ts`
- Test: `tests/nestjs/change-stream-event-pusher.test.ts`

**Interfaces:**
- Consumes: `ChangeStreamSubscription`, `ValueSignal` (both existing).
- Produces:
  - `ChangeStreamEventPusherOptions.enabled?: boolean` (default `true`)
  - `protected watchOne<TChange>(options: { collectionName: string; watch: (resumeOptions: ResumeOptions) => ChangeStream<Document, ChangeStreamDocument<Document>>; handler: (change: ChangeStreamDocument<Document>) => void | Promise<void> }): Promise<void>`
  - Task 6 supplies `enabled` from the `REALTIME_ENABLED` token.

- [ ] **Step 1: Write the failing tests**

Append to `tests/nestjs/change-stream-event-pusher.test.ts`:

```ts
describe('ChangeStreamEventPusher — enabled gate and latching', () => {
    it('never invokes pushChangeEvents when constructed disabled, and shutdown still returns', async () => {
        let invoked = false;
        class Pusher extends ChangeStreamEventPusher {
            protected async pushChangeEvents(): Promise<void> {
                invoked = true;
            }
        }
        const pusher = new Pusher({ enabled: false });

        pusher.onApplicationBootstrap();
        await expect(pusher.onApplicationShutdown()).resolves.toBeUndefined();
        expect(invoked).toBe(false);
    });

    it('shutdown returns even when the loop resolves without calling loopEnded', async () => {
        class Pusher extends ChangeStreamEventPusher {
            protected async pushChangeEvents(): Promise<void> {
                // Deliberately does NOT call this.loopEnded().
            }
        }
        const pusher = new Pusher();

        pusher.onApplicationBootstrap();
        await expect(pusher.onApplicationShutdown()).resolves.toBeUndefined();
    });

    it('logs an error when the loop ends while the app is still running', async () => {
        const { logger, calls } = mockLogger();
        class Pusher extends ChangeStreamEventPusher {
            protected async pushChangeEvents(): Promise<void> {
                this.markWatching();
            }
        }
        const pusher = new Pusher({ logger });

        pusher.onApplicationBootstrap();
        await delay(50);

        expect(calls.some(c => c.level === 'error' && c.message.includes('before shutdown was requested'))).toBe(true);
    });

    it('does not log that error when the loop ends during shutdown', async () => {
        const { logger, calls } = mockLogger();
        class Pusher extends ChangeStreamEventPusher {
            protected async pushChangeEvents(): Promise<void> {
                this.markWatching();
                await new Promise<void>(resolve => {
                    const check = (): void => {
                        if (this.shutdown.getValue()) resolve();
                        else setTimeout(check, 5);
                    };
                    check();
                });
            }
        }
        const pusher = new Pusher({ logger });

        pusher.onApplicationBootstrap();
        await delay(20);
        await pusher.onApplicationShutdown();

        expect(calls.some(c => c.level === 'error' && c.message.includes('before shutdown was requested'))).toBe(false);
    });
});
```

Reuse the file's existing `mockLogger`/`delay` helpers; add them following `tests/socketio/push-scoped.test.ts`'s definitions if the file lacks them. `markWatching` is a protected hook added in Step 3 so a test (and `watchOne`) can declare "a watch is live" without opening a real stream.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pnpm exec vitest run tests/nestjs/change-stream-event-pusher.test.ts`
Expected: FAIL — `enabled` is not an option, `markWatching` is undefined, and the no-`loopEnded` case hangs until vitest's timeout.

- [ ] **Step 3: Extend the base class**

In `src/nestjs/change-stream-event-pusher.ts`, add to the options interface:

```ts
export interface ChangeStreamEventPusherOptions {
    logger?: Logger;
    /**
     * When `false`, `pushChangeEvents()` is never invoked and the shutdown
     * latch is resolved immediately — the pusher is inert but its lifecycle
     * still completes. Wire this from a preflight result (see
     * `assertChangeStreamPreflight` and the `REALTIME_ENABLED` token) so an
     * app can boot on a deployment without change streams instead of
     * crash-looping.
     */
    enabled?: boolean;
}
```

Add the fields and the `markWatching` hook:

```ts
    protected readonly enabled: boolean;
    private watching = false;

    /**
     * Declares that a watch is live. Subclasses driving their own loop call
     * this once the stream is open (`watchOne` does it for you) so the base
     * class can tell "the loop ended early" — an operator-visible failure —
     * from "the loop never started", which is normal when realtime is off.
     */
    protected markWatching(): void {
        this.watching = true;
    }
```

Set `this.enabled = options?.enabled !== false;` in the constructor, and replace `onApplicationBootstrap`:

```ts
    onApplicationBootstrap(): void {
        if (!this.enabled) {
            this.logger.warn('Realtime is disabled — not watching for changes. No live events will be delivered.');
            this.loopEnded();
            return;
        }

        // Fire-and-forget: the change-stream loop runs in the background for
        // the lifetime of the host. `void` marks the promise as intentionally
        // unawaited.
        void this.pushChangeEvents()
            .then(() => {
                if (this.watching && !this.shutdown.getValue()) {
                    this.logger.error(
                        `${this.constructor?.name ?? 'pusher'} stopped watching before shutdown was requested — ` +
                            'no further events will be delivered. Restart the service or check the change stream ' +
                            'for an uncaught rejection.'
                    );
                }
            })
            .catch((err: unknown) => {
                this.logger.error(
                    `Change-stream pusher crashed during startup: ${(err as Error)?.message ?? String(err)}`,
                    (err as Error)?.stack
                );
            })
            // Latch BOTH paths. Previously only the rejection path resolved
            // it, so a loop that returned normally without calling
            // `loopEnded()` in its own `finally` hung `onApplicationShutdown`
            // forever. `loopEnded` is idempotent.
            .finally(() => this.loopEnded());
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pnpm exec vitest run tests/nestjs/change-stream-event-pusher.test.ts`
Expected: PASS.

- [ ] **Step 5: Write the failing test for `watchOne`**

Append to the same test file — this one needs a real replica set, so follow `tests/core/watch-collection.test.ts`'s `beforeAll`/`afterAll` setup:

```ts
it('watchOne opens a stream, delivers each change to the handler, and stops on shutdown', async () => {
    const name = uniqueCollectionName();
    const coll = db.collection(name);
    const seen: string[] = [];

    class Pusher extends ChangeStreamEventPusher {
        protected async pushChangeEvents(): Promise<void> {
            await this.watchOne({
                collectionName: name,
                watch: resumeOptions => coll.watch([], { fullDocument: 'updateLookup', ...resumeOptions }),
                handler: change => {
                    seen.push(change.operationType);
                },
            });
        }
    }
    const pusher = new Pusher();

    pusher.onApplicationBootstrap();
    await delay(400);
    await coll.insertOne({ v: 1 });
    await delay(400);

    await pusher.onApplicationShutdown();
    expect(seen).toContain('insert');
});
```

- [ ] **Step 6: Run it to verify it fails**

Run: `pnpm exec vitest run tests/nestjs/change-stream-event-pusher.test.ts`
Expected: FAIL — `this.watchOne is not a function`.

- [ ] **Step 7: Implement `watchOne`**

Add to `src/nestjs/change-stream-event-pusher.ts` (and import `ChangeStreamSubscription`, `ResumeOptions`, `ValueSignal`, plus `ChangeStream`/`ChangeStreamDocument`/`Document` as types from `mongodb`):

```ts
    /**
     * Watch one collection and run `handler` for every raw change document.
     *
     * The common shape by a wide margin, and the one the canonical
     * `watchCollection` + `pushScoped` pipeline does NOT cover: a consumer
     * that needs the raw change document (to branch on `operationType`, or to
     * read `updateDescription` for something `ChangeEvent` doesn't model)
     * would otherwise hand-roll the subscription, the constant scope signal
     * and the loop. Prefer `watchCollection` + a push loop when the canonical
     * event is enough.
     *
     * Latching: `watchOne` deliberately does NOT resolve the shutdown latch
     * itself — the base class latches when `pushChangeEvents()` settles. A
     * pusher running several watches must therefore `await Promise.all` of
     * its `watchOne` calls, so shutdown drains ALL of them; a per-call latch
     * here would let the first-to-finish release shutdown early.
     *
     * The scope signal is constructed fresh per call — `ChangeStreamSubscription`
     * never unsubscribes its own scope listener on normal completion, so a
     * shared instance would retain one listener per call for the process's
     * life. Its value is never mutated, so a fresh `[1]` is equivalent.
     */
    protected async watchOne(options: {
        /** Used in log messages only. */
        collectionName: string;
        /** Opens the stream; MUST spread the passed resume options into the driver call. */
        watch: (resumeOptions: ResumeOptions) => ChangeStream<Document, ChangeStreamDocument<Document>>;
        handler: (change: ChangeStreamDocument<Document>) => void | Promise<void>;
    }): Promise<void> {
        const scope = new ValueSignal<number[]>([1]);
        const subscription = new ChangeStreamSubscription<number, ChangeStreamDocument<Document>>(
            scope,
            this.shutdown,
            (_scope, resumeOptions) => options.watch(resumeOptions),
            { name: `${this.constructor?.name ?? 'pusher'}:${options.collectionName}`, logger: this.logger }
        );

        this.markWatching();
        for await (const change of subscription) {
            // One malformed document must not stop fan-out for every
            // subsequent change, so a throwing handler is logged and
            // skipped rather than breaking the loop.
            try {
                await options.handler(change);
            } catch (err) {
                this.logger.warn(
                    `Handler failed for a ${change.operationType} change on ${options.collectionName} — skipping: ${(err as Error)?.message ?? String(err)}`
                );
            }
        }
        // NO loopEnded() here — see the doc comment: the base's bootstrap
        // `.finally` latches when pushChangeEvents() settles, which for a
        // multi-watch pusher is after ALL its awaited watchOne calls end.
    }
```

- [ ] **Step 8: Run the tests to verify they pass**

Run: `pnpm exec vitest run tests/nestjs/ && pnpm typecheck && pnpm lint`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add src/nestjs/change-stream-event-pusher.ts tests/nestjs/change-stream-event-pusher.test.ts
git commit -m "feat(nestjs): add enabled gate, watchOne helper and both-path shutdown latch to the pusher base"
```

---

### Task 6: Preflight returns a usable result, with actionable remediation

`assertChangeStreamPreflight` either throws or returns `void`, so an app that boots with `requireChangeStreams: false` has no way to tell its pushers "don't bother" — it must re-derive that itself. Its pre-image failure message also names the problem without naming the fix, unlike its replica-set message.

**Files:**
- Modify: `src/nestjs/preflight.ts`
- Modify: `src/nestjs/realtime-module.ts` (add `REALTIME_ENABLED`)
- Test: `tests/nestjs/preflight.test.ts`, `tests/nestjs/realtime-module.test.ts`

**Interfaces:**
- Consumes: `ChangeStreamEventPusherOptions.enabled` from Task 5.
- Produces:
  - `interface PreflightResult { ok: boolean; reason?: 'not-a-replica-set' }`
  - `assertChangeStreamPreflight(db, options?): Promise<PreflightResult>`
  - `REALTIME_ENABLED = 'MONGODB_REALTIME_ENABLED'` exported from `realtime-module.ts`; `RealtimeModuleOptions.enabled?: boolean`

- [ ] **Step 1: Write the failing tests**

Append to `tests/nestjs/preflight.test.ts`:

```ts
it('returns ok:true on a replica set', async () => {
    await expect(assertChangeStreamPreflight(db)).resolves.toEqual({ ok: true });
});

it('returns ok:false with a reason on a standalone when not required', async () => {
    // Stubbed: a real MongoMemoryReplSet always reports a replica set, so the
    // standalone branch is unreachable against a live server.
    const standalone = {
        admin: () => ({ command: () => Promise.resolve({}) }),
    } as unknown as Db;

    await expect(assertChangeStreamPreflight(standalone, { requireChangeStreams: false })).resolves.toEqual({
        ok: false,
        reason: 'not-a-replica-set',
    });
});

it('the pre-image failure message names the exact remediation commands', async () => {
    // `enablePrePostImages` runs before the verification and would succeed
    // against a real server, so the post-enable verification failure is only
    // reachable with a stub whose collMod silently does nothing — which is
    // exactly the real-world case this error exists for (an older MongoDB
    // that accepts the command and ignores the flag).
    const stub = {
        admin: () => ({ command: () => Promise.resolve({ setName: 'rs0' }) }),
        command: () => Promise.resolve({ ok: 1 }),
        listCollections: () => ({
            toArray: () => Promise.resolve([{ name: 'widgets', options: {} }]),
        }),
    } as unknown as Db;

    await expect(assertChangeStreamPreflight(stub, { collections: ['widgets'] })).rejects.toThrow(/collMod/);
});
```

Add `import type { Db } from 'mongodb';` to that file's imports if absent.

Append to `tests/nestjs/realtime-module.test.ts`:

```ts
it('provides REALTIME_ENABLED, defaulting to true', async () => {
    const moduleRef = await Test.createTestingModule({
        imports: [RealtimeModule.forRoot({ db })],
    }).compile();

    expect(moduleRef.get(REALTIME_ENABLED)).toBe(true);
});

it('provides REALTIME_ENABLED as false when disabled', async () => {
    const moduleRef = await Test.createTestingModule({
        imports: [RealtimeModule.forRoot({ db, enabled: false })],
    }).compile();

    expect(moduleRef.get(REALTIME_ENABLED)).toBe(false);
});
```

Match the existing file's import style for `Test`/`RealtimeModule`, and add `REALTIME_ENABLED` to its imports.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pnpm exec vitest run tests/nestjs/preflight.test.ts tests/nestjs/realtime-module.test.ts`
Expected: FAIL — preflight resolves `undefined`, and `REALTIME_ENABLED` is not exported.

- [ ] **Step 3: Return a result from the preflight**

In `src/nestjs/preflight.ts`, add above `assertChangeStreamPreflight`:

```ts
export interface PreflightResult {
    /**
     * Whether change streams are usable on this deployment. `false` only ever
     * comes back with `requireChangeStreams: false` — the default throws
     * instead. Feed it to `ChangeStreamEventPusher`'s `enabled` option (or the
     * `REALTIME_ENABLED` provider) so pushers stay inert rather than opening
     * streams that cannot work.
     */
    ok: boolean;
    reason?: 'not-a-replica-set';
}
```

Change the signature to `Promise<PreflightResult>`, return `{ ok: false, reason: 'not-a-replica-set' }` from the `!requireChangeStreams` early return, and `return { ok: true }` at the end.

- [ ] **Step 4: Make the pre-image error actionable**

Replace the thrown pre-image error's message with:

```ts
                throw new Error(
                    [
                        `Change stream pre-images are not enabled on the "${name}" collection after`,
                        'enablePrePostImages ran, so hard deletes there cannot carry the scope/owner',
                        'fields needed to route them — deleted entities would be dropped rather than',
                        'announced.',
                        '',
                        'If the collection already exists, enable pre-images in place:',
                        `  mongosh --eval 'db.runCommand({ collMod: "${name}", changeStreamPreAndPostImages: { enabled: true } })'`,
                        '',
                        'If you are provisioning a FRESH database, create it with pre-images enabled',
                        'instead, before first run — an ODM that auto-creates the collection would',
                        'otherwise create it without them and the next restart would hit this again:',
                        `  mongosh --eval 'db.createCollection("${name}", { changeStreamPreAndPostImages: { enabled: true } })'`,
                        '',
                        'Or pass { requireChangeStreams: false } to boot without realtime.',
                    ].join('\n')
                );
```

- [ ] **Step 5: Provide `REALTIME_ENABLED`**

In `src/nestjs/realtime-module.ts`, add the token beside the existing four:

```ts
export const REALTIME_ENABLED = 'MONGODB_REALTIME_ENABLED';
```

add to `RealtimeModuleOptions`:

```ts
    /**
     * Master switch, surfaced as the `REALTIME_ENABLED` token for pushers to
     * gate on. Default `true`. Typically wired from
     * `assertChangeStreamPreflight`'s `ok`, so one preflight decides for every
     * pusher whether realtime is live.
     */
    enabled?: boolean;
```

add to `buildProviders`'s array (and to the equivalent inline array in `forRootAsync`, reading from the options token):

```ts
        {
            provide: REALTIME_ENABLED,
            useFactory: (): boolean => options.enabled !== false,
        },
```

and add `REALTIME_ENABLED` to both `exports` arrays.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `pnpm exec vitest run tests/nestjs/ && pnpm typecheck && pnpm lint`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/nestjs/preflight.ts src/nestjs/realtime-module.ts tests/nestjs/
git commit -m "feat(nestjs): return a preflight result and provide REALTIME_ENABLED"
```

---

### Task 7: Late-bound socket.io server handle

The README's NestJS example injects `'SOCKET_SERVER'` and leaves the app to provide it. Every Nest consumer hits the same problem — the gateway constructs the `Server` in `afterInit`, so a pusher cannot inject it directly, and whether `afterInit` has run by the time `onApplicationBootstrap` fires is not something an app should have to reason about. This ships the handle.

**Files:**
- Create: `src/nestjs/socket-server-handle.ts`
- Modify: `src/nestjs/realtime-module.ts`
- Modify: `src/nestjs/index.ts`
- Test: `tests/nestjs/socket-server-handle.test.ts`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `class SocketServerHandle { setServer(server: Server): void; isReady(): boolean; get(): Server; whenReady(): Promise<Server> }`
  - `SOCKET_SERVER = 'MONGODB_REALTIME_SOCKET_SERVER'` provided and exported by `RealtimeModule`.

- [ ] **Step 1: Write the failing test**

Create `tests/nestjs/socket-server-handle.test.ts`:

```ts
import { describe, it, expect } from 'vitest';
import type { Server } from 'socket.io';
import { SocketServerHandle } from '../../src/nestjs/socket-server-handle.js';

const fakeServer = {} as Server;

describe('SocketServerHandle', () => {
    it('is not ready before a server is set', () => {
        expect(new SocketServerHandle().isReady()).toBe(false);
    });

    it('get() throws with an actionable message before a server is set', () => {
        expect(() => new SocketServerHandle().get()).toThrow(/afterInit/);
    });

    it('get() returns the server once set', () => {
        const handle = new SocketServerHandle();
        handle.setServer(fakeServer);
        expect(handle.isReady()).toBe(true);
        expect(handle.get()).toBe(fakeServer);
    });

    it('whenReady() resolves for a server set after the wait began', async () => {
        const handle = new SocketServerHandle();
        const waiting = handle.whenReady();
        handle.setServer(fakeServer);
        await expect(waiting).resolves.toBe(fakeServer);
    });

    it('whenReady() resolves immediately for a server already set', async () => {
        const handle = new SocketServerHandle();
        handle.setServer(fakeServer);
        await expect(handle.whenReady()).resolves.toBe(fakeServer);
    });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pnpm exec vitest run tests/nestjs/socket-server-handle.test.ts`
Expected: FAIL — cannot resolve `../../src/nestjs/socket-server-handle.js`.

- [ ] **Step 3: Implement the handle**

Create `src/nestjs/socket-server-handle.ts`:

```ts
import type { Server } from 'socket.io';

/**
 * Late-bound holder for the socket.io `Server`.
 *
 * A Nest gateway constructs the server itself and only surfaces it in
 * `afterInit`, so a pusher cannot inject it directly — and the ordering of
 * `afterInit` against `onApplicationBootstrap` is not something an
 * application should have to depend on. The gateway calls `setServer` in
 * `afterInit`; a pusher awaits `whenReady()` at the top of its loop and is
 * correct either way round.
 */
export class SocketServerHandle {
    private server: Server | null = null;
    private resolveReady!: (server: Server) => void;
    private readonly ready: Promise<Server> = new Promise<Server>(resolve => {
        this.resolveReady = resolve;
    });

    /** Call from the gateway's `afterInit(server)`. Idempotent; a re-set replaces the handle for later readers. */
    setServer(server: Server): void {
        this.server = server;
        // Resolving an already-resolved promise is a no-op, so a re-set never
        // rejects or double-resolves an in-flight `whenReady()`.
        this.resolveReady(server);
    }

    isReady(): boolean {
        return this.server !== null;
    }

    /** The server, or a throw naming the fix. Prefer `whenReady()` anywhere an await is possible. */
    get(): Server {
        if (!this.server) {
            throw new Error(
                'Socket.io server is not available yet — call SocketServerHandle.setServer(server) from your ' +
                    "gateway's afterInit(server) hook, or await whenReady() instead of get()."
            );
        }
        return this.server;
    }

    /** Resolves once the gateway has handed the server over; already-resolved if it has. */
    whenReady(): Promise<Server> {
        return this.ready;
    }
}
```

- [ ] **Step 4: Provide it from the module**

In `src/nestjs/realtime-module.ts`, add the token, import the class, add the provider to both `buildProviders` and `forRootAsync`'s inline array, and add the token to both `exports`:

```ts
export const SOCKET_SERVER = 'MONGODB_REALTIME_SOCKET_SERVER';
```

```ts
        {
            provide: SOCKET_SERVER,
            useFactory: (): SocketServerHandle => new SocketServerHandle(),
        },
```

In `src/nestjs/index.ts`, add `export * from './socket-server-handle.js';`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `pnpm exec vitest run tests/nestjs/ && pnpm typecheck && pnpm lint`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/nestjs/ tests/nestjs/socket-server-handle.test.ts
git commit -m "feat(nestjs): ship a late-bound SocketServerHandle and SOCKET_SERVER token"
```

---

### Task 8: Hub filters ignored collections server-side and refuses use after close

`ignoreCollections` is applied in the reader loop, after every change document for the whole database has already been produced (with `fullDocument: 'updateLookup'`) and shipped to the client. In a database shared with a job queue or any other high-write collection, that is the dominant cost of running the hub at all. Moving the filter into the `$match` stage stops those documents at the server.

Also folds in the triaged follow-up: `close()` is terminal but unguarded, so a post-close `subscribe()` silently opens an untracked cursor or returns a generator that hangs forever.

**Files:**
- Modify: `src/core/change-stream-hub.ts`
- Test: `tests/core/change-stream-hub.test.ts`

**Interfaces:**
- Consumes: nothing.
- Produces: no signature change — `ChangeStreamHub.subscribe` now throws after `close()`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/core/change-stream-hub.test.ts`, following its existing replica-set and unique-collection setup:

```ts
it('does not deliver changes from an ignored collection', async () => {
    const watched = uniqueCollectionName();
    const ignored = uniqueCollectionName();
    const hub = new ChangeStreamHub({ ignoreCollections: [ignored] });
    const consumer = hub.subscribe('k1', db, watched);
    const seen: string[] = [];
    const pump = (async () => {
        for await (const change of consumer) {
            seen.push((change as { ns?: { coll?: string } }).ns?.coll ?? '');
            break;
        }
    })();

    await delay(400);
    await db.collection(ignored).insertOne({ v: 1 });
    await db.collection(watched).insertOne({ v: 1 });

    await pump;
    await hub.close();

    expect(seen).toEqual([watched]);
});

it('subscribe() after close() throws instead of returning a dead generator', async () => {
    const hub = new ChangeStreamHub();
    hub.subscribe('k1', db, uniqueCollectionName());
    await hub.close();

    expect(() => hub.subscribe('k2', db, uniqueCollectionName())).toThrow(/closed/i);
});
```

The first test passes today (the reader-loop filter already produces this behaviour) — it is the regression pin for the pipeline change, not a new capability. The second fails.

- [ ] **Step 2: Run the tests to verify the second fails**

Run: `pnpm exec vitest run tests/core/change-stream-hub.test.ts`
Expected: the ignored-collection test PASSES; the post-close test FAILS (no throw).

- [ ] **Step 3: Move the filter into the pipeline**

In `src/core/change-stream-hub.ts`, inside `ensureStream`'s subscription factory, replace the pipeline argument to `db.watch`:

```ts
                db.watch(
                    [
                        {
                            $match: {
                                operationType: {
                                    $in: ['insert', 'update', 'replace', 'delete'],
                                },
                                // Filter ignored collections HERE, not in the
                                // reader loop: a DB-level watch produces a
                                // change document (with an `updateLookup`
                                // full document) for every write in the whole
                                // database, so filtering after the fact still
                                // pays the server-side lookup and the
                                // transport for collections nobody consumes.
                                // The reader-loop check below stays as the
                                // backstop for collections with no fanout.
                                ...(ignored.length > 0 ? { 'ns.coll': { $nin: ignored } } : {}),
                            },
                        },
                    ],
                    {
                        fullDocument: 'updateLookup',
                        fullDocumentBeforeChange: 'whenAvailable',
                        ...options,
                    }
                ) as never,
```

with `const ignored = [...this.ignoreCollections];` computed at the top of `ensureStream`. Leave the reader loop's `this.ignoreCollections.has(coll)` check in place — it costs nothing and keeps behaviour correct for a hub whose ignore set is read before the stream opens.

- [ ] **Step 4: Guard `close()`**

Add the field and the check:

```ts
    private closed = false;
```

At the top of `subscribe`:

```ts
        if (this.closed) {
            // Terminal by design: after close() the reader loops are gone, so
            // a new subscription would either open an untracked cursor
            // (new key) or hand back a generator parked forever on a closed
            // fanout (existing key). Fail loudly instead.
            throw new Error('ChangeStreamHub is closed — subscribe() is not valid after close()');
        }
```

and set `this.closed = true;` as the first statement of `close()`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `pnpm exec vitest run tests/core/change-stream-hub.test.ts && pnpm typecheck && pnpm lint`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/core/change-stream-hub.ts tests/core/change-stream-hub.test.ts
git commit -m "perf(core): filter ignored collections server-side; make hub close terminal"
```

---

### Task 9: `resolveDb` no longer depends on driver-copy identity

`resolveDb` narrows a change stream's `parent` with `instanceof` against the library's own `mongodb` import. A consumer whose driver copy is not deduplicated with the library's — which is the norm when an ODM vendors its own, as mongoose does — fails every check and silently loses operation-time capture, so a stream that fails before yielding anything reopens from scratch and drops the changes in between. Nothing errors; realtime just quietly develops gaps. This is the triaged follow-up marked "do before adoption".

**Files:**
- Modify: `src/core/change-stream-subscription.ts:419-425`
- Test: `tests/core/change-stream-subscription.test.ts`

**Interfaces:**
- Consumes: nothing.
- Produces: no signature change.

- [ ] **Step 1: Write the failing test**

Append to `tests/core/change-stream-subscription.test.ts`:

```ts
describe('resolveDb duck-typing', () => {
    // A parent from a DIFFERENT copy of the driver fails every `instanceof`
    // check against this package's copy, which is exactly what happens when an
    // ODM vendors its own mongodb and the package manager doesn't dedupe.
    const helloCalls: Record<string, unknown>[] = [];
    const foreignDb = {
        admin: () => ({
            command: (cmd: Record<string, unknown>) => {
                helloCalls.push(cmd);
                return Promise.resolve({ operationTime: { toString: () => 'ts1' } });
            },
        }),
    };

    it('captures operation time from a foreign-copy Db parent', async () => {
        helloCalls.length = 0;
        const scope = new ValueSignal<number[]>([1]);
        const shutdown = new ValueSignal(false);
        const subscription = new ChangeStreamSubscription<number, unknown>(
            scope,
            shutdown,
            () => makeFakeStream({ parent: foreignDb }) as never,
            { name: 'foreign' }
        );

        const iterator = subscription[Symbol.asyncIterator]();
        const first = iterator.next();
        await delay(50);
        shutdown.next(true);
        await first.catch(() => undefined);

        expect(helloCalls).toContainEqual({ hello: 1 });
    });

    it('captures operation time from a foreign-copy Collection parent', async () => {
        helloCalls.length = 0;
        const foreignCollection = { db: foreignDb };
        const scope = new ValueSignal<number[]>([1]);
        const shutdown = new ValueSignal(false);
        const subscription = new ChangeStreamSubscription<number, unknown>(
            scope,
            shutdown,
            () => makeFakeStream({ parent: foreignCollection }) as never,
            { name: 'foreign-coll' }
        );

        const iterator = subscription[Symbol.asyncIterator]();
        const first = iterator.next();
        await delay(50);
        shutdown.next(true);
        await first.catch(() => undefined);

        expect(helloCalls).toContainEqual({ hello: 1 });
    });
});
```

Reuse the file's existing fake-stream builder; if it doesn't accept a `parent`, extend it to pass one through (a `next()` that never settles until closed, a no-op `close()`, and the supplied `parent`).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pnpm exec vitest run tests/core/change-stream-subscription.test.ts`
Expected: FAIL — `helloCalls` is empty; every `instanceof` misses and `resolveDb` returns `undefined`.

- [ ] **Step 3: Add the duck-typed fallback**

Replace `resolveDb` in `src/core/change-stream-subscription.ts`:

```ts
    /**
     * Resolve a `Db` handle from a change stream's `parent`, which the driver
     * types as `MongoClient | Db | Collection`. We only need it to run the
     * `hello` admin command, so any database on the same client works.
     *
     * `instanceof` is tried first (exact, and cheap), then a structural
     * fallback. The fallback is load-bearing rather than defensive: a consumer
     * whose driver copy isn't deduplicated with ours — the normal case when an
     * ODM vendors its own `mongodb` — fails every `instanceof` even though the
     * object is a perfectly good `Db`. Without the fallback that degrades
     * SILENTLY: no operation-time capture, so a stream that fails before
     * yielding its first event reopens from scratch and the changes in between
     * are lost, with nothing logged.
     */
    private resolveDb(stream: ChangeStream<Document, TChange>): Db | undefined {
        const parent: unknown = stream.parent;
        if (parent instanceof DbClass) return parent;
        if (parent instanceof Collection) return parent.db;
        if (parent instanceof MongoClient) return parent.db();

        const candidate = parent as
            | { admin?: unknown; db?: unknown }
            | null
            | undefined;
        if (!candidate) return undefined;
        // Db: has admin(). Collection: has a .db property that has admin().
        // MongoClient: has a .db() method returning one.
        if (typeof candidate.admin === 'function') return candidate as unknown as Db;
        if (typeof candidate.db === 'function') {
            const db = (candidate as { db: () => unknown }).db();
            return typeof (db as { admin?: unknown })?.admin === 'function' ? (db as Db) : undefined;
        }
        if (typeof (candidate.db as { admin?: unknown } | undefined)?.admin === 'function') {
            return candidate.db as Db;
        }
        return undefined;
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pnpm exec vitest run tests/core/ && pnpm typecheck && pnpm lint`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/core/change-stream-subscription.ts tests/core/change-stream-subscription.test.ts
git commit -m "fix(core): duck-type resolveDb so a vendored driver copy still captures operation time"
```

---

### Task 10: Documentation, version bump, follow-up reconciliation

**Files:**
- Modify: `README.md`
- Modify: `docs/follow-ups-0.1.x.md`
- Modify: `package.json` (version)
- Test: `tests/build-artifacts.test.ts` (verify the new subpath surface still builds and exports)

**Interfaces:**
- Consumes: every task above.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Update the README's feature list and examples**

- Under **Change event conventions**, document `operation`, `updatedFields`, `via` and `didChange`, with the `replace`-writes-everything caveat spelled out.
- Under **Features**, add `pushMapped` to the socket.io bullet and note that `roomOf` may return several rooms.
- Update the **Presence** bullet and the presence example for the `activity` event type and `meta` passthrough.
- Replace the NestJS example's `@Inject('SOCKET_SERVER')` with the real `SOCKET_SERVER` / `SocketServerHandle` (`const io = await this.serverHandle.whenReady()`), and show `REALTIME_ENABLED` wired into a pusher's `enabled` option, sourced from `assertChangeStreamPreflight`'s `ok`.
- Add a short **Upgrading to 0.2** section listing the one breaking change: `EntityChanged.operation` and `EntityRemoved.via` are required, so hand-constructed event literals need updating.
- One-line clarification beside the preflight docs: `assertChangeStreamPreflight` only *asserts* (throws) under the default `requireChangeStreams: true`; with `false` it reports through the returned `PreflightResult` instead.

- [ ] **Step 2: Reconcile the follow-ups doc**

In `docs/follow-ups-0.1.x.md`, strike the three items delivered here — the `ChangeStreamHub.close()` guard (Task 8), the `ChangeStreamEventPusher` resolve-path latch (Task 5), and the `resolveDb` duck-type fallback (Task 9) — noting the task that closed each. Leave the remaining test-adds and cosmetics.

- [ ] **Step 3: Bump the version**

Set `"version": "0.2.0"` in `package.json`.

- [ ] **Step 4: Verify the build and the whole suite**

Run: `pnpm build && pnpm test && pnpm lint && pnpm typecheck`
Expected: all PASS, including `tests/build-artifacts.test.ts`.

- [ ] **Step 5: Commit**

```bash
git add README.md docs/follow-ups-0.1.x.md package.json
git commit -m "docs: document 0.2 API additions and reconcile follow-ups"
```

---

### Task 11: Calendar consumer contract suite

Consumer-driven compatibility guardrail: an executable statement of the compositions calendar's five pushers rely on, living in the library's CI so "did this change break calendar?" is answered on every PR — before a release tag exists. These tests deliberately test COMPOSITIONS (the way calendar wires library pieces together), not individual units; unit behavior is already pinned by the per-module suites. A change that fails a contract test must be coordinated with the calendar repo, not just "fixed" here.

**Files:**
- Create: `tests/contracts/calendar/README.md` (contract concept, coordination rule, mapping test→calendar pusher)
- Create: `tests/contracts/calendar/calendar-event.contract.test.ts`, `booking.contract.test.ts`, `optimization-result.contract.test.ts`, `notification.contract.test.ts`, `presence.contract.test.ts`
- Test: they are the tests.

**Interfaces:**
- Consumes: everything Tasks 1–7 produced (`didChange`, `pushMapped`, multi-room `roomOf`, `activity` events, `watchOne`); the socketio harness; the presence store + replica set.
- Produces: nothing — terminal guardrail.

- [ ] **Step 1: Write the README**

`tests/contracts/calendar/README.md`: state that each file encodes one calendar pusher's composition (name the calendar source file it mirrors, e.g. `apps/api/src/booking/booking.pusher.ts`); that these run in library CI as the consumer's contract; and the coordination rule above. Note the fm contract suite will join as `tests/contracts/fm/` when fm migrates.

- [ ] **Step 2: Write the booking contract (the richest composition — full code)**

`tests/contracts/calendar/booking.contract.test.ts` — uses the socketio harness; watchers are hand-made generators (no Mongo needed):

```ts
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import type { Server } from 'socket.io';
import { pushMapped } from '../../../src/socketio/push-mapped.js';
import { scopeRoom, userRoom } from '../../../src/socketio/rooms.js';
import { didChange, type ChangeEvent } from '../../../src/core/change-event.js';
import { ValueSignal } from '../../../src/common/signal.js';
import { startHarness, stopHarness, connectClient, joinRoom, receivedEvents, waitForCount, delay } from '../../socketio/harness.js';

/**
 * CONTRACT: calendar's booking pusher (apps/api/src/booking/booking.pusher.ts).
 * A booking insert announces 'booking:created'; an update announces
 * 'booking:cancelled' only when the write actually touched `status` and the
 * document now says cancelled; recipients are every participant's user room
 * PLUS the org room, with a socket in several of those rooms receiving
 * exactly one copy.
 */
type BookingDoc = { id: string; status: string; participants: string[]; organizationId: string };

const select = (event: ChangeEvent) => {
    if (event.type !== 'changed') return null;
    const doc = event.data as BookingDoc;
    const rooms = [...doc.participants.map(userRoom), scopeRoom(doc.organizationId)];
    if (event.operation === 'insert') return { name: 'booking:created', payload: { id: doc.id }, rooms };
    if (didChange(event, 'status') && doc.status === 'cancelled')
        return { name: 'booking:cancelled', payload: { id: doc.id }, rooms };
    return null; // an update that didn't flip status is not announced
};

const changed = (doc: BookingDoc, operation: 'insert' | 'update' | 'replace', updatedFields?: string[]): ChangeEvent =>
    ({ type: 'changed', scopeId: doc.organizationId, data: doc, operation, updatedFields }) as ChangeEvent;

async function* gen(events: ChangeEvent[]): AsyncGenerator<ChangeEvent> {
    for (const event of events) yield event;
}

let server: Server;
let port: number;
beforeEach(async () => ({ server, port } = await startHarness()));
afterEach(async () => stopHarness());

describe('calendar contract: booking pusher composition', () => {
    const doc: BookingDoc = { id: 'b1', status: 'cancelled', participants: ['u1', 'u2'], organizationId: 'org1' };

    it('a participant who is also in the org room receives exactly one copy', async () => {
        const participant = await connectClient(port);
        await joinRoom(participant, userRoom('u1'));
        await joinRoom(participant, scopeRoom('org1'));
        const received = receivedEvents(participant, 'booking:cancelled');

        await pushMapped(server, gen([changed(doc, 'update', ['status'])]), select, {
            shutdown: new ValueSignal(true),
        });

        await waitForCount(received, 1);
        await delay(200);
        expect(received).toEqual([{ id: 'b1' }]);
    });

    it('an org member who is not a participant still receives the announcement', async () => {
        const orgMember = await connectClient(port);
        await joinRoom(orgMember, scopeRoom('org1'));
        const received = receivedEvents(orgMember, 'booking:created');

        await pushMapped(server, gen([changed(doc, 'insert')]), select, { shutdown: new ValueSignal(true) });

        await waitForCount(received, 1);
        expect(received).toEqual([{ id: 'b1' }]);
    });

    it('an update that did not touch status is silent, even though the doc says cancelled', async () => {
        const orgMember = await connectClient(port);
        await joinRoom(orgMember, scopeRoom('org1'));
        const received = receivedEvents(orgMember, 'booking:cancelled');

        await pushMapped(server, gen([changed(doc, 'update', ['externalEventIds'])]), select, {
            shutdown: new ValueSignal(true),
        });

        await delay(300);
        expect(received).toHaveLength(0);
    });

    it('a replace that leaves the doc cancelled IS announced (replace counts as writing every field)', async () => {
        const orgMember = await connectClient(port);
        await joinRoom(orgMember, scopeRoom('org1'));
        const received = receivedEvents(orgMember, 'booking:cancelled');

        await pushMapped(server, gen([changed(doc, 'replace')]), select, { shutdown: new ValueSignal(true) });

        await waitForCount(received, 1);
        expect(received).toEqual([{ id: 'b1' }]);
    });
});
```

- [ ] **Step 3: Write the remaining four contracts**

Same harness/generator style; each file carries its CONTRACT header comment naming the calendar source it mirrors:

- `calendar-event.contract.test.ts` — "calendar events are personal": select routes to `userRoom(doc.userId)` ONLY; assert a socket in `scopeRoom(doc.organizationId)` receives nothing while the owner's socket receives the event; an insert and an update both route the same way.
- `optimization-result.contract.test.ts` — select yields only when `didChange(event, 'status') && doc.status === 'ready'` (or insert with status ready), payload is a hand-built DTO WITHOUT the `slots` field; assert a doc containing `slots: [...]` arrives with `payload.slots === undefined` and non-ready updates are silent.
- `notification.contract.test.ts` — uses `pushOwnerScoped` (not pushMapped): changed → owner's room via `ownerIdOf`; removed with `ownerId` → bare id to owner's room; removed WITHOUT `ownerId` → dropped with a warn (spy logger), never broadcast.
- `presence.contract.test.ts` — replica-set test (reuse `tests/presence/watch-presence.test.ts`'s setup pattern): two `PresenceStore`s with different serverIds; a user connecting twice announces `online` once; last checkout announces `offline`; `setPage` yields `activity` with the page; `meta` from checkIn appears on the online event. (This is calendar's online-user pusher composition end-to-end.)

- [ ] **Step 4: Run the suite, verify green**

Run: `pnpm exec vitest run tests/contracts/ && pnpm lint && pnpm typecheck`
Expected: PASS. (CI picks these up automatically — `pnpm test` runs the whole tests/ tree.)

- [ ] **Step 5: Commit**

```bash
git add tests/contracts/
git commit -m "test(contracts): calendar consumer contract suite"
```

---

### Task 12: Public-API report tripwire, final gate, tag

Any change to the exported surface of the four subpaths must show up as a reviewable diff, so accidental breaking changes cannot merge silently and deliberate ones require touching a checked-in report (plus the right semver bump). Uses `@microsoft/api-extractor` (devDependency — runtime deps stay zero) over tsc-emitted declarations.

**Files:**
- Create: `tsconfig.api.json`, `scripts/api-report.mjs`, `etc/` (four committed `*.api.md` reports)
- Modify: `package.json` (scripts + devDependency), `.github/workflows/ci.yml` (api:check step), `.gitignore` (`.api-tmp/`)

**Interfaces:**
- Consumes: the final surface of Tasks 1–11.
- Produces: `pnpm api:update` / `pnpm api:check`.

- [ ] **Step 1: Install and configure**

`pnpm add -D @microsoft/api-extractor`

`tsconfig.api.json`:
```json
{
    "extends": "./tsconfig.json",
    "compilerOptions": { "noEmit": false, "declaration": true, "emitDeclarationOnly": true, "outDir": ".api-tmp" },
    "include": ["src"]
}
```

Add `.api-tmp/` to `.gitignore`.

- [ ] **Step 2: Write the report script**

`scripts/api-report.mjs`:
```js
// Generates (or, with --verify, checks) the public-API reports for the four
// subpath entry points. The committed etc/*.api.md files are the reviewable
// record of the package's surface: a PR that changes them is a PR that
// changes the public API, and must say so (and bump semver accordingly).
import { execSync } from 'node:child_process';
import { Extractor, ExtractorConfig } from '@microsoft/api-extractor';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const verify = process.argv.includes('--verify');

execSync('pnpm exec tsc -p tsconfig.api.json', { cwd: root, stdio: 'inherit' });

const entries = [
    { name: 'mongodb-realtime', dts: '.api-tmp/index.d.ts' },
    { name: 'mongodb-realtime-presence', dts: '.api-tmp/presence/index.d.ts' },
    { name: 'mongodb-realtime-socketio', dts: '.api-tmp/socketio/index.d.ts' },
    { name: 'mongodb-realtime-nestjs', dts: '.api-tmp/nestjs/index.d.ts' },
];

let failed = false;
for (const entry of entries) {
    const config = ExtractorConfig.prepare({
        configObject: {
            projectFolder: root,
            mainEntryPointFilePath: path.join(root, entry.dts),
            compiler: { tsconfigFilePath: path.join(root, 'tsconfig.api.json') },
            apiReport: { enabled: true, reportFileName: `${entry.name}.api.md`, reportFolder: path.join(root, 'etc') },
            docModel: { enabled: false },
            dtsRollup: { enabled: false },
            tsdocMetadata: { enabled: false },
        },
        configObjectFullPath: undefined,
        packageJsonFullPath: path.join(root, 'package.json'),
    });
    const result = Extractor.invoke(config, { localBuild: !verify, showVerboseMessages: false });
    if (verify && (result.apiReportChanged || !result.succeeded)) failed = true;
    if (!verify && !result.succeeded) failed = true;
}
process.exit(failed ? 1 : 0);
```

Adjust `messages` settings in the config object as needed until a clean run reports no errors (api-extractor warns about e.g. forgotten exports; route non-actionable categories to `logLevel: "none"` rather than suppressing real ones). The behavioral gate is Step 4, not exact config.

- [ ] **Step 3: Wire the scripts and CI**

`package.json` scripts: `"api:update": "node scripts/api-report.mjs"`, `"api:check": "node scripts/api-report.mjs --verify"`. In `.github/workflows/ci.yml`, add `- run: pnpm api:check` after the build step.

- [ ] **Step 4: Generate, commit, and prove the tripwire trips**

Run `pnpm api:update`; inspect `etc/*.api.md` (each should list that subpath's exported symbols with signatures). Then the sanity cycle: temporarily rename any exported function in `src/`, run `pnpm api:check` — MUST exit non-zero; revert; `pnpm api:check` — exits zero. Record both outputs in the task report.

- [ ] **Step 5: Full gate and tag**

Run: `pnpm lint && pnpm typecheck && pnpm test && pnpm build && pnpm api:check`
Expected: all PASS.

```bash
git add -A
git commit -m "chore(api): checked-in public-API reports with CI verification"
git tag v0.2.0
```

---

## Out of scope

Deliberately not in this plan, and why:

- **The calendar-side migration itself.** Once this lands, `apps/api` deletes its `change-stream/`, `realtime-emitter/`, `PresenceService`, `OnlineUser` schema and `server-id.provider.ts`, and rewrites its five pushers onto `watchOne` + `pushMapped`. That is a plan in the calendar repo, not this one.
- **`ChangeEvent` carrying `removedFields`.** `updateDescription.removedFields` exists, but no consumer branches on it yet. Add it when one does.
- **The remaining cosmetics in `docs/follow-ups-0.1.x.md`** (double log prefix in direct mode, the "mapData failed" wording for `variants.render` throws, `forRoot`/`forRootAsync` provider duplication, the `logger$` parameter name). None block adoption.
- **Multi-tenant concerns** (`tenantNamespace`, `runInContext`, `tenantOf`). Calendar is single-DB; these paths are exercised by the fm adoption, not this one.
