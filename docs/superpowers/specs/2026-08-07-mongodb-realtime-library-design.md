# mongodb-realtime — shared MongoDB change-stream realtime library

**Date:** 2026-08-07
**Status:** Design approved, awaiting implementation plan
**Repo (new):** `github.com/broesch/mongodb-realtime`
**Consumers (in order):** smart-calendar (Phase-4 realtime branch), pactsheet/fm (later migration), future apps

## 1. Problem

Three applications carry hand-copied implementations of the same MongoDB-change-stream realtime architecture (multi-node fan-out over socket.io without Redis, presence via a TTL'd `onlineUsers` collection, per-node connected-org/tenant tracking driving watch scopes):

| | Original pactsheet (`pactsheet/code`) | fm (`pactsheet/fm`) | Calendar (branch `claude/vibrant-thompson-344b45`) |
|---|---|---|---|
| `ChangeStreamSubscription` (scope-driven restartable watch) | ✅ oldest, no error handling | ✅ most mature (resume token + `startAtOperationTime` + fatal classification after 8829f95c) | ✅ independent rewrite, drifted |
| `ChangeStreamEventPusher` lifecycle base | ✅ | ✅ (39 gateway subclasses) | ✅ |
| Presence (`onlineUsers` TTL, `serverId`, check-in/keepalive) | ✅ (heartbeat broken: queries a field the schema lacks) | ✅ | ✅ reimplemented |
| `orgsOnServer` / `connectedTenants$` (per-node scope tracking) | ✅ | ✅ | ❌ |
| Per-tenant DB-level watch hub (cursor reduction) | ❌ | ✅ (`TenantChangeStreamHub`) | ❌ (5 pushers = 5 cursors) |

The copies have already diverged: each has fixes the others lack, and known bugs (old pactsheet's dead heartbeat) persist because there is no single source. Every future app repeats the copy.

`mongodb-task-queue` was considered as a home and rejected: it is a work-distribution package (exactly one worker claims a message) while realtime is broadcast (every subscriber sees the event); its only change-stream code is an insert-only single-event wake latch with no resume logic; and adding socket.io to its dependency story would undermine its zero-dependency scope. Its *scaffolding*, however, is the template for the new repo.

## 2. Goals / non-goals

**Goals**

- One canonical, tested implementation of: resumable change-stream subscription, DB-level watch hub with demux, presence store + connected-scope registry, socket.io fan-out loops, NestJS lifecycle binding.
- Support both tenancy models in production today: db-per-tenant (fm) and single-DB-with-org-field (calendar). The library treats "tenant" as an opaque generic tag.
- Multi-node correctness without Redis: MongoDB change streams are the only cross-node bus; a `serverId` per node; presence state in a shared collection.
- Zero runtime dependencies. `mongodb` wide peer (`^5.7 || ^6 || ^7`); `socket.io` and `@nestjs/common` optional peers, type-only imports.
- Framework-agnostic core; NestJS supported via a thin subpath, socket.io via a thin subpath.

**Non-goals**

- No app policy: event names, DTOs, permission logic, PII redaction rules, room-join policy, and tenancy resolution stay in the consuming apps (passed in as hooks).
- No npm publishing initially — consumed as a git dependency (`github:broesch/mongodb-realtime`), like calendar already consumes mongodb-task-queue. npm later if/when the API stabilizes.
- No queue functionality (that is mongodb-task-queue's domain) and no socket.io adapter.
- No migration of fm in this project (see §10).

## 3. Repo & package shape

One package, four subpath exports (mirrors mongodb-task-queue's layout):

```
mongodb-realtime           → src/index.ts        core stream engine
mongodb-realtime/presence  → src/presence/…      presence + connected-scope registry
mongodb-realtime/socketio  → src/socketio/…      room naming + fan-out loops
mongodb-realtime/nestjs    → src/nestjs/…        DynamicModule, tokens, lifecycle base
```

Tooling copied from mongodb-task-queue: tsup (4 entries, `format: ['cjs','esm']`, `dts: true`), vitest, flat eslint + prettier, `src/<domain>/{Thing,types}.ts` + barrels, `files: ["dist"]`, `engines.node >= 20`. GitHub Actions CI: Node 20/22/24, `lint → test → build`.

Deliberate differences from mongodb-task-queue:

- **Tests run against `MongoMemoryReplSet`** (change streams require a replica set; mongo_queue's CI never exercises its change-stream path — this package must from day one).
- **Optional peers declared correctly**: `socket.io` and `@nestjs/common` in `peerDependencies` *and* marked optional in `peerDependenciesMeta` (mongo_queue marks `@nestjs/common` optional without declaring it, so the marker is a no-op).
- **No rxjs**: the scope-signal contract is a structural interface (`getValue()`, `next(v)`, `subscribe(fn) → {unsubscribe()}`) implemented internally as a small `ValueSignal<T>`. rxjs `BehaviorSubject` satisfies it structurally, so apps already on rxjs pass one in directly.
- Structural `Logger` interface (`debug/log/warn/error`), defaulting to a console logger; injectable everywhere (mongo_queue convention).

## 4. Core engine (`mongodb-realtime`)

Raw-driver based throughout. Mongoose interops without special support: `model.watch()` returns a driver `ChangeStream`, `model.db.db` is a `Db`.

### 4.1 `ChangeStreamSubscription<TScope, TChange>`

Port of fm's class (the most hardened copy), folding in calendar's additions. An `AsyncIterable<TChange>` wrapping a driver `ChangeStream` built by a caller-supplied factory `(scope: TScope[], options: ResumeOptions) => ChangeStream`.

Behavior (all existing, consolidated):

- **Scope-driven**: parks (no open cursor) while the scope signal is empty; opens on first non-empty scope; closes + reopens with resume on scope change. Scope dedup via an injectable comparator (default: structural deep-equal for JSON-ish values, replacing fm's lodash `isEqual`).
- **Resume ladder**: `startAfter: resumeToken` when ≥1 event was yielded → `startAtOperationTime` captured via `hello` right after each open (covers a stream that dies before its first event) → fresh open. A stream opened from operation time that fails before yielding drops the time (aged past the oplog window) and degrades to fresh.
- **Error classification**: fatal errors (40573 "not a replica set", 40574, missing-pre-image class) rethrow to the caller instead of looping; resume-token-expired (286 / `ChangeStreamHistoryLost`) drops the token and reopens fresh; everything else is transient → capped exponential backoff 1s→30s (cancellable by shutdown).
- **Shutdown race** on every await (`next()` vs scope-change vs shutdown), so teardown can never deadlock — including the empty-scope park branch.

### 4.2 `Fanout<T>`

fm's single-producer/multi-consumer async fan-out queue, near-verbatim: independent per-consumer generators, bounded buffer (default 10 000, drop-with-warning when full), `close()` completes all consumers, consumer `break` self-deregisters. Logger injected.

### 4.3 `ChangeStreamHub`

Generalization of fm's `TenantChangeStreamHub`. One DB-level `db.watch()` per string **key**, demuxed by `ns.coll` into per-collection `Fanout`s.

- `subscribe(key, db, collection) → AsyncGenerator<ChangeStreamDocument>`; lazy stream creation, shared per key.
- **Connection-driven parking**: an optional `connectedKeys` signal (from `ConnectionRegistry`, §5.2, or anything else) opens a key's watch only while that key is connected; disconnect arms a configurable grace timer (default 45 s) before parking; reconnect within the window cancels it. Reopen resumes from token/operation time. Without the signal, streams are always-open.
- **Ignored collections** configurable (default: the presence collection name + any caller-supplied noise collections).
- **Crash-evict + self-heal**: a terminally-dead reader is evicted and its fanouts closed; the *caller* resubscribes (§4.5 wraps this with backoff).
- Single-DB apps use one constant key. This gives calendar cursor reduction it lacks today (5 pushers each currently own a cursor).

### 4.4 `enablePrePostImages(db, collections)`

fm's helper on a raw `Db`: create-collection-first, cache-on-success keyed by `${dbName}::${collection}`, retry-on-failure, never throws. Needed so hard deletes carry `fullDocumentBeforeChange` for scoped removed-event routing.

### 4.5 `ChangeEvent`, `changeDocToEvent`, `watchCollection`, `mergeAsyncGenerators`

- **`ChangeEvent<TTenant>`** = `{ type: 'changed', scopeId, data, tenant? }` | `{ type: 'removed', scopeId?, id, ownerId?, tenant? }`. `TTenant` is opaque — the library never defines what a tenant is.
- **`changeDocToEvent(change, conventions)`** with `conventions = { scopeField: 'organizationId', softDeleteField: 'deleted', ownerField: 'userId' }` (defaults match all three apps). Soft delete (`softDeleteField: true` on an update) yields `removed`; hard delete recovers `scopeId`/`ownerId` from the pre-image; a changed doc without a scope id is skipped.
- **`watchCollection(opts)`** — the composed entry point. Two source modes: hub-backed (`{hub, key, db, collection}`) or direct (`{watchFactory}` e.g. wrapping `model.watch`). Enables pre/post images, maps to `ChangeEvent`s, self-heals an evicted hub consumer with capped backoff (fm's `adaptCollection` logic incl. the non-awaited detach on teardown), and races shutdown throughout.
- **`mergeAsyncGenerators(sources)`** — fm's bounded round-robin fan-in; multi-DB callers map N tenants to N `watchCollection`s, tag each (`tagTenant` helper), and merge.

## 5. Presence (`mongodb-realtime/presence`)

### 5.1 `PresenceStore`

Owns the presence collection (name configurable, default `onlineUsers`). Document: `{ userId, clientId, serverId, scopeId, lastSeen, meta?, page? }` — one row per socket connection.

- `ensureIndexes()`: unique `{userId, clientId, serverId}`, TTL on `lastSeen` (configurable, default 300 s), plus pre/post images via §4.4.
- `checkIn(...)` — atomic `findOneAndUpdate` upsert keyed on the unique triple, `returnDocument: 'before'`; the DB arbitrates new-vs-rejoin (fm's audit-fixed race-free version). Returns `{ isNewConnection }`.
- `keepAlive(clientId)`, `checkOut(userId, clientId)`, `setPage(...)` (optional feature), `clearServer()` for orderly shutdown; TTL is the safety net for killed nodes.
- The store owns both schema and queries, eliminating old-pactsheet's bug class (heartbeat querying a field the schema doesn't have).

### 5.2 `ConnectionRegistry`

In-memory, per-node. Tracks connected clients and derives, as signals (§3):

- `connectedUsers$` — `{clientId, userId, scopeId, key, meta}` list;
- `connectedScopes$` — distinct `scopeId`s (fm's `orgsOnServer`), for scope-driven subscriptions;
- `connectedKeys$` — distinct `key`s (fm's `connectedTenants$`), feeding `ChangeStreamHub` parking.

Emits only on real set changes (sorted-compare dedup). Copy-on-write updates (no in-place array mutation — a latent old-pactsheet defect).

### 5.3 `watchPresence(opts)`

Watches the presence collection through the core engine and yields high-level events: `{ type: 'online' | 'offline', userId, scopeId, ownedByThisServer }`.

- **Transition detection**: `online` only on a user's 0→1 row transition, `offline` only at 1→0 (calendar's refinement; multi-tab churn stays silent). Keepalive updates yield nothing.
- `ownedByThisServer` (pre-image `serverId === myServerId`) supports run-exactly-once-per-cluster teardown (old pactsheet's ownership-token pattern).
- Transport-agnostic: yields events; the app emits them (typically to the scope's room via §6).

## 6. Socket.io helpers (`mongodb-realtime/socketio`)

`socket.io` optional peer, type-only imports.

### 6.1 Rooms

`userRoom(id)` → `user:<id>`, `scopeRoom(id)` → `org:<id>` (prefixes configurable), `namespacedRoom(ns, id)` → `<ns>::<base>` (fm's tenant-namespacing; also fixes calendar-main's raw-id user/org room collision). Consumers derive join and emit through the same helpers so they cannot disagree.

### 6.2 Fan-out loops

fm's three pusher loops as plain async functions `(server, watcher, options) => Promise<void>`:

- **`pushScoped`** — emit `changed` payload / `removed` bare id to the event's scope room. A removed event with no resolvable scope is **dropped, never broadcast**.
- **`pushScopedFiltered`** — fetches the room's sockets and runs hooks per recipient:
  - `canReceive(socket, data, prepared)` — per-socket permission gate (app policy);
  - `prepareEvent(data)` — once-per-event shared resolution threaded to every `canReceive`;
  - `mapData(data)` — once-per-event payload mapping, **or** `variants: { render → {full, redacted}, pick(socket) → 'full'|'redacted' }` — the two-variant PII pattern, mutually exclusive with `mapData` (asserted at call time). Deliberately not a generic per-socket transformer: a recipient can only ever receive one of two pre-rendered payloads.
  - `runInContext(ctx, fn)` — optional; generalizes fm's CLS seam (per-tenant DB reads inside hooks) without the library depending on nestjs-cls. Applied around once-per-event hooks (event's tenant) and per-recipient checks (socket's tenant).
  - Removed events bypass filtering and emit the bare id to the scope room (an id leaks nothing; every client holding the entity must evict it).
- **`pushOwnerScoped`** — per-user entities (notifications): route by `ownerId`; removed events route via the pre-image owner or are dropped.

Failure semantics preserved from fm: a throwing mapper/hook skips that one event (never kills the loop); a `canReceive` failure skips that one recipient; a watcher that completes before shutdown logs an operator-visible error; every loop resolves a shutdown latch in `finally`.

## 7. NestJS binding (`mongodb-realtime/nestjs`)

Thin, factory-wired, no decorators (mongo_queue pattern — keeps `@nestjs/common` type-only):

- **`ChangeStreamEventPusher`** abstract base: `onApplicationBootstrap` fires `pushChangeEvents()` un-awaited with a crash latch (`.catch` resolves the shutdown latch so `onApplicationShutdown` can't hang — fm's version); `onApplicationShutdown` signals the shutdown signal and awaits drain. Apps subclass per domain and call the §6.2 loops.
- **`RealtimeModule.forRoot/forRootAsync`**: provides `ChangeStreamHub`, `PresenceStore`, `ConnectionRegistry` (+ generated `serverId`) behind string injection tokens.
- **`assertChangeStreamPreflight(db, opts)`** boot-time provider: replica-set probe (`hello` → `setName`) + pre/post-image assertion for declared collections, with an env escape hatch (calendar's Phase-4 preflight). Fails fast with actionable messages instead of silently degrading.
- README must state consumers should call `app.enableShutdownHooks()` (old pactsheet shipped without it — graceful close was dead code).

## 8. What stays in the apps

Event name enums and DTOs; permission logic (bodies of `canReceive`); PII redaction rules (bodies of `variants.render/pick`); room-join policy at check-in; tenancy resolution (fm: `TenantModels` + CLS; calendar: JWT claim); every concrete pusher's routing decisions; auth (WS strategies/guards). The library is mechanism; policy stays home.

## 9. Testing

Vitest + `MongoMemoryReplSet` (single-node replica set; change streams work, boots fast).

- **Core**: resume-after-transient-error; died-before-first-event → operation-time resume; token-expiry → fresh reopen; fatal error → rethrow; scope park/reopen with no event loss; hub demux, grace-window parking, crash-evict + caller self-heal; fanout backpressure/drop; merge fairness + teardown. fm has ~2 500 lines of spec for exactly these paths — they port alongside the code.
- **Presence**: concurrent check-in races (DB arbitration), TTL expiry → offline event, transition detection under multi-tab churn, `clearServer`.
- **Socketio**: loops against a real in-process socket.io server — scope isolation, dropped unroutable removed events, hook-failure skip semantics, two-variant delivery.
- **NestJS**: lifecycle (crash latch, shutdown drain) with a minimal test module.

CI: GitHub Actions, Node 20/22/24, `lint → test → build`.

## 10. Adoption plan

1. **Build the library** from fm's code (canonical source) + calendar's refinements, with tests. Tag `v0.1.0`.
2. **Calendar adopts first** (separate follow-up project): its Phase-4 branch `claude/vibrant-thompson-344b45` replaces `apps/api/src/change-stream/` and the presence stack with library imports (git dep, like its existing mongodb-task-queue dep). Smaller surface, single-DB, unmerged — lowest risk, validates the API.
3. **fm migrates later** (separate project, not scheduled here): replaces `backend/app/src/common/change-stream/` + `online-user.service.ts`; its 39 gateways keep their `pushChangeEvents` bodies, swapping base class and helper imports. fm's e2e suite is the acceptance gate. Until then fm's copy is intentionally frozen — divergence risk is accepted short-term and the library is the only place new fixes land.

## 11. Risks & mitigations

- **API wrong on first contact** → calendar adoption is step 2, before any fm work; breaking changes are cheap on a git dep with one consumer.
- **Divergence window while fm is unmigrated** → new stream-engine fixes land in the library only; fm backports only if urgent (its copy is mature and stable).
- **Replica-set-only features** (change streams, pre/post images) → preflight (§7) fails fast with actionable messages; standalone Mongo is unsupported by design.
- **Optional-peer typing drift** (socket.io / nestjs majors) → type-only imports, wide peer ranges, CI matrix.

## 12. Open items (deferred, not blocking)

- npm publishing decision — revisit after both consumers are on the library.
- A `mongodb-task-queue` follow-up: its `ChangeStreamWatcher` wake latch could later be rebuilt on this library's engine; explicitly out of scope now.
- Calendar's `BookingPusher` PII broadcast (booker email/name to the whole org room) — an app bug to fix during calendar adoption, noted here so it isn't lost.
