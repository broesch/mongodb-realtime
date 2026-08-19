# mongodb-realtime v0.3.0 — Keyed Multi-Database Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every part of the library able to span N databases keyed by a
string, so a multi-tenant consumer (pactsheet/fm) can use it without writing
its own fan-in and per-tenant presence adapters.

**Architecture:** The core is already keyed — `ChangeStreamHub.subscribe(key,
db, collection)` already accepts a different `Db` per key. Three sites still
assume one database: `PresenceStore`, `RealtimeModule`, and the missing fan-in
helper. All three are fixed by one idea: a `DbResolver` (`keys()` +
`dbFor(key)`), with a bare `Db` normalised to the single key `'__default__'`.
Every existing single-database call site keeps working unchanged.

**Tech Stack:** TypeScript 6, tsup (cjs+esm dual output), vitest on
`MongoMemoryReplSet`, api-extractor for the checked-in public API report.

## Global Constraints

- Package version at the end of this plan: `0.3.0`.
- Zero runtime dependencies. Peers: `mongodb` (`^5.7.0 || ^6.0.0 || ^7.0.0`),
  optional `socket.io` (`^4.6.0`), optional `@nestjs/common` (`>=10 <12`).
- No rxjs. Signals are the structural `ReadonlySignal` / `ValueSignal`.
- `pnpm api:check` must pass before every commit that changes an exported
  signature. Run `pnpm api:update` and commit the regenerated reports in
  `etc/` as part of the same commit. `ae-forgotten-export` must stay
  unsilenced — if it fires, add the missing re-export to the subpath's
  `index.ts` rather than suppressing the rule.
- `tests/contracts/calendar/` pins calendar's five pusher compositions against
  its live consumer code. It must stay green at **every** commit in this plan —
  it is the proof that single-database behaviour is untouched.
- Every task ends with `pnpm test` green (all files), not just the new test.
- Node `>=20`. Do not use `Date.now()`-free-style restrictions here — that
  constraint applies to Workflow scripts, not to this library.
- Indentation is 4 spaces; formatting is enforced by prettier via eslint. Run
  `pnpm lint` before committing.

---

## File Structure

**Created:**
- `src/core/db-resolver.ts` — `DEFAULT_KEY`, `DbResolver`, `toDbResolver`.
- `src/core/watch-across-targets.ts` — N-target fan-in over `watchCollection`.
- `src/mongoose/index.ts` — `MongooseModelLike`, `modelTarget`, `watchModel`.
- `src/nestjs/nest-logger.ts` — `nestLogger(context)` adapter.
- `tests/core/db-resolver.test.ts`
- `tests/core/watch-across-targets.test.ts`
- `tests/mongoose/watch-model.test.ts`
- `tests/nestjs/nest-logger.test.ts`
- `tests/contracts/fm/README.md`
- `tests/contracts/fm/entity-fanout.contract.test.ts`
- `tests/contracts/fm/multi-tenant-watch.contract.test.ts`
- `tests/contracts/fm/presence.contract.test.ts`

**Modified:**
- `src/presence/types.ts` — `PresenceStoreOptions.db` widened.
- `src/presence/presence-store.ts` — keyed throughout.
- `src/presence/watch-presence.ts` — multi-key merge.
- `src/presence/connection-registry.ts` — use the shared `DEFAULT_KEY`.
- `src/nestjs/realtime-module.ts` — collapse the two factories, widen `db`.
- `src/index.ts`, `src/presence/index.ts`, `src/nestjs/index.ts` — exports.
- `package.json` — `exports` map, `mongoose` devDependency, version.
- `tsup.config.ts` — the new `mongoose` entry.

---

## Task 1: Collapse `forRoot` / `forRootAsync` onto `buildProviders`

`forRootAsync` duplicates all seven providers inline (logged in
`docs/follow-ups-0.1.x.md`). Threading a resolver through two copies would
entrench the duplication, so this lands first — a pure refactor with no
behaviour change.

**Files:**
- Modify: `src/nestjs/realtime-module.ts:111-249`
- Test: `tests/nestjs/realtime-module.test.ts`

**Interfaces:**
- Produces: `buildProviders(optionsToken: string | RealtimeModuleOptions): Provider[]`
  — when given a string it is treated as a DI token to inject the options
  from; when given an object the options are captured directly.
- Produces: `REALTIME_EXPORTS: string[]` — the shared export list.

- [ ] **Step 1: Write the failing test**

Append to `tests/nestjs/realtime-module.test.ts`:

```typescript
import { CHANGE_STREAM_HUB, CONNECTION_REGISTRY, PRESENCE_STORE, REALTIME_ENABLED, RealtimeModule, SERVER_ID, SOCKET_SERVER, RealtimeLifecycle } from '../../src/nestjs/realtime-module.js';

describe('RealtimeModule provider parity', () => {
    it('forRoot and forRootAsync expose the identical provider and export sets', async () => {
        const { db } = await startReplSet();
        const sync = RealtimeModule.forRoot({ db });
        const async = RealtimeModule.forRootAsync({ useFactory: () => ({ db }) });

        const tokensOf = (mod: { providers?: unknown[] }): unknown[] =>
            (mod.providers ?? [])
                .map(p => (p as { provide?: unknown }).provide)
                .filter(t => t !== 'MONGODB_REALTIME_MODULE_OPTIONS');

        expect(new Set(tokensOf(sync))).toEqual(
            new Set([
                SERVER_ID,
                REALTIME_ENABLED,
                SOCKET_SERVER,
                CONNECTION_REGISTRY,
                PRESENCE_STORE,
                CHANGE_STREAM_HUB,
                RealtimeLifecycle,
            ])
        );
        expect(new Set(tokensOf(async))).toEqual(new Set(tokensOf(sync)));
        expect(sync.exports).toEqual(async.exports);
    });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pnpm vitest run tests/nestjs/realtime-module.test.ts -t 'identical provider'`
Expected: FAIL — `forRootAsync`'s inline list and `forRoot`'s `buildProviders`
output are separate arrays whose export lists are separate literals; the test
fails on the first mismatch it finds (or passes only by coincidence, in which
case it still guards the refactor below).

- [ ] **Step 3: Rewrite `buildProviders` to serve both entry points**

Replace `buildProviders` and both static methods in
`src/nestjs/realtime-module.ts`:

```typescript
const OPTIONS_TOKEN = 'MONGODB_REALTIME_MODULE_OPTIONS';

const REALTIME_EXPORTS = [
    SERVER_ID,
    REALTIME_ENABLED,
    SOCKET_SERVER,
    CONNECTION_REGISTRY,
    PRESENCE_STORE,
    CHANGE_STREAM_HUB,
    RealtimeLifecycle,
];

/**
 * The seven providers, built once for both `forRoot` and `forRootAsync`.
 *
 * `source` is either the options object itself (sync) or the DI token the
 * options are provided under (async). Each factory that needs the options
 * takes them as its FIRST injected argument in the async case and closes over
 * them in the sync case, so the two shapes stay one code path.
 */
function buildProviders(source: RealtimeModuleOptions | string): Provider[] {
    const async = typeof source === 'string';
    const optionsInject = async ? [source] : [];
    // In sync mode the factory receives no options argument, so fall back to
    // the captured object.
    const opts = (args: any[]): RealtimeModuleOptions =>
        async ? (args[0] as RealtimeModuleOptions) : (source as RealtimeModuleOptions);

    return [
        { provide: SERVER_ID, useFactory: (): string => randomUUID() },
        {
            provide: REALTIME_ENABLED,
            useFactory: (...args: any[]): boolean => opts(args).enabled !== false,
            inject: optionsInject,
        },
        { provide: SOCKET_SERVER, useFactory: (): SocketServerHandle => new SocketServerHandle() },
        { provide: CONNECTION_REGISTRY, useFactory: (): ConnectionRegistry => new ConnectionRegistry() },
        {
            provide: PRESENCE_STORE,
            useFactory: (...args: any[]): PresenceStore => {
                const o = opts(args);
                const serverId = args[optionsInject.length] as string;
                return new PresenceStore({ ...o.presence, db: o.db, serverId });
            },
            inject: [...optionsInject, SERVER_ID],
        },
        {
            provide: CHANGE_STREAM_HUB,
            useFactory: (...args: any[]): ChangeStreamHub => {
                const o = opts(args);
                const registry = args[optionsInject.length] as ConnectionRegistry;
                return new ChangeStreamHub({
                    ...o.hub,
                    ignoreCollections: hubIgnoreCollections(o),
                    connectedKeys: o.wireHubParking !== false ? registry.connectedKeys : o.hub?.connectedKeys,
                });
            },
            inject: [...optionsInject, CONNECTION_REGISTRY],
        },
        {
            provide: RealtimeLifecycle,
            useFactory: (presenceStore: PresenceStore, hub: ChangeStreamHub): RealtimeLifecycle =>
                new RealtimeLifecycle(presenceStore, hub),
            inject: [PRESENCE_STORE, CHANGE_STREAM_HUB],
        },
    ];
}

export class RealtimeModule {
    static forRoot(options: RealtimeModuleOptions): DynamicModule {
        return { module: RealtimeModule, providers: buildProviders(options), exports: REALTIME_EXPORTS };
    }

    static forRootAsync(options: {
        imports?: unknown[];
        inject?: any[];
        useFactory: (...args: any[]) => RealtimeModuleOptions | Promise<RealtimeModuleOptions>;
    }): DynamicModule {
        return {
            module: RealtimeModule,
            imports: options.imports as DynamicModule['imports'],
            providers: [
                {
                    provide: OPTIONS_TOKEN,
                    useFactory: async (...args: any[]) => options.useFactory(...args),
                    inject: options.inject ?? [],
                },
                ...buildProviders(OPTIONS_TOKEN),
            ] as Provider[],
            exports: REALTIME_EXPORTS,
        };
    }
}
```

- [ ] **Step 4: Run the full suite**

Run: `pnpm test`
Expected: PASS, including every existing `realtime-module.test.ts` case and
all of `tests/contracts/calendar/`.

- [ ] **Step 5: Lint and commit**

```bash
pnpm lint
git add src/nestjs/realtime-module.ts tests/nestjs/realtime-module.test.ts
git commit -m "refactor(nestjs): build forRoot and forRootAsync providers from one builder"
```

---

## Task 2: `DbResolver`

**Files:**
- Create: `src/core/db-resolver.ts`
- Modify: `src/index.ts`
- Test: `tests/core/db-resolver.test.ts`

**Interfaces:**
- Produces: `DEFAULT_KEY = '__default__'`
- Produces: `interface DbResolver { keys(): string[]; dbFor(key: string): Db }`
- Produces: `toDbResolver(source: Db | DbResolver): DbResolver`

- [ ] **Step 1: Write the failing test**

Create `tests/core/db-resolver.test.ts`:

```typescript
import { DEFAULT_KEY, toDbResolver, type DbResolver } from '../../src/core/db-resolver.js';
import { startReplSet, stopReplSet } from '../helpers/replset.js';

describe('toDbResolver', () => {
    afterAll(() => stopReplSet());

    it('normalises a bare Db to the single default key', async () => {
        const { db } = await startReplSet();
        const resolver = toDbResolver(db);

        expect(resolver.keys()).toEqual([DEFAULT_KEY]);
        expect(resolver.dbFor(DEFAULT_KEY)).toBe(db);
        // A bare Db has exactly one database, so any key resolves to it —
        // callers of a single-db store must not have to know the key name.
        expect(resolver.dbFor('anything')).toBe(db);
    });

    it('passes a resolver through unchanged', async () => {
        const { client } = await startReplSet();
        const a = client.db('tenant-a');
        const b = client.db('tenant-b');
        const resolver: DbResolver = {
            keys: () => ['a', 'b'],
            dbFor: key => (key === 'a' ? a : b),
        };

        expect(toDbResolver(resolver)).toBe(resolver);
        expect(toDbResolver(resolver).dbFor('b')).toBe(b);
    });

    it('duck-types a Db apart from a resolver without instanceof', async () => {
        const { db } = await startReplSet();
        // A driver Db has neither `keys` nor `dbFor`; the discriminator must
        // not rely on `instanceof Db`, which fails across duplicated driver
        // copies (mongoose vendors its own).
        expect(typeof (db as unknown as DbResolver).dbFor).toBe('undefined');
        expect(toDbResolver(db).keys()).toEqual([DEFAULT_KEY]);
    });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pnpm vitest run tests/core/db-resolver.test.ts`
Expected: FAIL — `Cannot find module '../../src/core/db-resolver.js'`.

- [ ] **Step 3: Implement**

Create `src/core/db-resolver.ts`:

```typescript
import type { Db } from 'mongodb';

/**
 * The key used when a consumer has exactly one database, or when a connection
 * registers without naming one. Shared by `DbResolver`, `ConnectionRegistry`
 * and `PresenceStore` so "no key" means the same string everywhere — the hub's
 * parking compares `connectedKeys` against subscription keys, and a mismatch
 * there silently parks a watch while clients are connected.
 */
export const DEFAULT_KEY = '__default__';

/**
 * Resolves a key to the database it names. Lets one library instance span N
 * databases (one per tenant, per region, per shard — the library does not care
 * what the key means) while a single-database consumer passes a bare `Db` and
 * never sees a key at all.
 *
 * `keys()` is called at boot and whenever a store needs to act across every
 * database (index creation, shutdown cleanup). It is NOT re-polled per
 * operation, so a consumer that provisions databases at runtime must supply a
 * resolver whose `keys()` reflects the current set each time it is called.
 */
export interface DbResolver {
    keys(): string[];
    dbFor(key: string): Db;
}

function isDbResolver(source: Db | DbResolver): source is DbResolver {
    // Structural, not `instanceof`: a consumer whose driver copy is duplicated
    // (mongoose vendors its own mongodb) would fail an identity check against
    // this package's copy. A driver `Db` has neither of these methods.
    const candidate = source as Partial<DbResolver>;
    return typeof candidate.dbFor === 'function' && typeof candidate.keys === 'function';
}

/**
 * Normalises either accepted form to a resolver. A bare `Db` becomes a
 * single-key resolver that answers with that database for ANY key, so a
 * single-database consumer never has to learn or pass {@link DEFAULT_KEY}.
 */
export function toDbResolver(source: Db | DbResolver): DbResolver {
    if (isDbResolver(source)) return source;
    return {
        keys: () => [DEFAULT_KEY],
        dbFor: () => source,
    };
}
```

Add to `src/index.ts` after the `enablePrePostImages` line:

```typescript
export * from './core/db-resolver.js';
```

- [ ] **Step 4: Run the tests**

Run: `pnpm vitest run tests/core/db-resolver.test.ts`
Expected: PASS (3 tests).

- [ ] **Step 5: Regenerate the API report, lint, commit**

```bash
pnpm api:update && pnpm lint && pnpm test
git add src/core/db-resolver.ts src/index.ts tests/core/db-resolver.test.ts etc/
git commit -m "feat(core): add DbResolver so one instance can span N databases"
```

---

## Task 3: Use `DEFAULT_KEY` in `ConnectionRegistry`

Small, but it removes the second hardcoded `'__default__'` literal so the two
sides of hub parking can never drift.

**Files:**
- Modify: `src/presence/connection-registry.ts:36`
- Test: `tests/presence/connection-registry.test.ts`

**Interfaces:**
- Consumes: `DEFAULT_KEY` from Task 2.

- [ ] **Step 1: Write the failing test**

Append to `tests/presence/connection-registry.test.ts`:

```typescript
import { DEFAULT_KEY } from '../../src/core/db-resolver.js';

it('defaults a keyless connection to the shared DEFAULT_KEY constant', () => {
    const registry = new ConnectionRegistry();
    registry.add({ clientId: 'c1', userId: 'u1', scopeId: 's1' });

    expect(registry.connectedKeys.getValue()).toEqual([DEFAULT_KEY]);
    expect(registry.byClientId('c1')?.key).toBe(DEFAULT_KEY);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pnpm vitest run tests/presence/connection-registry.test.ts -t 'DEFAULT_KEY'`
Expected: FAIL — `Cannot find module '../../src/core/db-resolver.js'` only if
Task 2 was skipped; otherwise this passes on the literal. Either way it pins
the constant against a future edit, so keep it.

- [ ] **Step 3: Replace the literal**

In `src/presence/connection-registry.ts`, add the import and replace the
literal in `add`:

```typescript
import { DEFAULT_KEY } from '../core/db-resolver.js';
```

```typescript
            key: entry.key ?? DEFAULT_KEY,
```

- [ ] **Step 4: Run the tests**

Run: `pnpm vitest run tests/presence/connection-registry.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
pnpm lint && pnpm test
git add src/presence/connection-registry.ts tests/presence/connection-registry.test.ts
git commit -m "refactor(presence): use the shared DEFAULT_KEY constant"
```

---

## Task 4: `watchAcrossTargets`

**Files:**
- Create: `src/core/watch-across-targets.ts`
- Modify: `src/index.ts`
- Test: `tests/core/watch-across-targets.test.ts`

**Interfaces:**
- Consumes: `watchCollection`, `collectionWatchFactory` (`src/core/watch-collection.ts`),
  `mergeAsyncGenerators` (`src/core/merge-async-generators.ts`).
- Produces:

```typescript
interface WatchTarget<TTenant> { key: string; db: Db; collection: string; tenant?: TTenant }
function watchAcrossTargets<TTenant = unknown>(options: {
    targets: Array<WatchTarget<TTenant>>;
    shutdown: ReadonlySignal<boolean>;
    hub?: ChangeStreamHub;
    conventions?: ChangeEventConventions;
    name?: string;
    logger?: Logger;
    initialBackoffMs?: number;
    maxBackoffMs?: number;
}): AsyncGenerator<ChangeEvent<TTenant>>
```

- [ ] **Step 1: Write the failing test**

Create `tests/core/watch-across-targets.test.ts`:

```typescript
import { randomUUID } from 'node:crypto';
import { ValueSignal } from '../../src/common/signal.js';
import { watchAcrossTargets } from '../../src/core/watch-across-targets.js';
import type { ChangeEvent } from '../../src/core/change-event.js';
import { startReplSet, stopReplSet, uniqueCollection } from '../helpers/replset.js';

describe('watchAcrossTargets', () => {
    afterAll(() => stopReplSet());

    it('merges events from every target and tags each with its tenant', async () => {
        const { client } = await startReplSet();
        const dbA = client.db(`wat-a-${randomUUID().slice(0, 8)}`);
        const dbB = client.db(`wat-b-${randomUUID().slice(0, 8)}`);
        const collection = uniqueCollection('widgets');
        await dbA.createCollection(collection);
        await dbB.createCollection(collection);

        const shutdown = new ValueSignal(false);
        const watcher = watchAcrossTargets<{ slug: string }>({
            targets: [
                { key: 'a', db: dbA, collection, tenant: { slug: 'a' } },
                { key: 'b', db: dbB, collection, tenant: { slug: 'b' } },
            ],
            shutdown,
            name: 'widgets',
        });

        const seen: ChangeEvent<{ slug: string }>[] = [];
        const collecting = (async () => {
            for await (const event of watcher) {
                seen.push(event);
                if (seen.length === 2) break;
            }
        })();

        // Give both change streams time to open before writing.
        await new Promise(resolve => setTimeout(resolve, 1_000));
        await dbA.collection(collection).insertOne({ organizationId: 'org-a', name: 'from-a' });
        await dbB.collection(collection).insertOne({ organizationId: 'org-b', name: 'from-b' });

        await collecting;
        shutdown.next(true);

        const bySlug = new Map(seen.map(e => [e.tenant?.slug, e]));
        expect(bySlug.get('a')?.type).toBe('changed');
        expect((bySlug.get('a') as { scopeId: string }).scopeId).toBe('org-a');
        expect((bySlug.get('b') as { scopeId: string }).scopeId).toBe('org-b');
    });

    it('yields nothing and completes immediately when given no targets', async () => {
        const shutdown = new ValueSignal(false);
        const seen: unknown[] = [];
        for await (const event of watchAcrossTargets({ targets: [], shutdown })) {
            seen.push(event);
        }
        expect(seen).toEqual([]);
    });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pnpm vitest run tests/core/watch-across-targets.test.ts`
Expected: FAIL — `Cannot find module '../../src/core/watch-across-targets.js'`.

- [ ] **Step 3: Implement**

Create `src/core/watch-across-targets.ts`:

```typescript
import type { Db } from 'mongodb';
import type { Logger } from '../common/logger.js';
import type { ReadonlySignal } from '../common/signal.js';
import type { ChangeEvent, ChangeEventConventions } from './change-event.js';
import type { ChangeStreamHub } from './change-stream-hub.js';
import { mergeAsyncGenerators } from './merge-async-generators.js';
import { collectionWatchFactory, watchCollection } from './watch-collection.js';

/** One database's copy of a collection to watch, and the tenant its events belong to. */
export interface WatchTarget<TTenant = unknown> {
    /** Hub subscription key — the database's identity. See `DbResolver`. */
    key: string;
    db: Db;
    collection: string;
    /** Stamped onto every event from this target, for downstream fan-out. */
    tenant?: TTenant;
}

export interface WatchAcrossTargetsOptions<TTenant> {
    targets: Array<WatchTarget<TTenant>>;
    shutdown: ReadonlySignal<boolean>;
    /**
     * When supplied every target subscribes through the shared hub under its
     * own `key`, so the process holds one DB-level cursor per key rather than
     * one per (key, collection). Without it each target opens a dedicated
     * collection-level change stream.
     */
    hub?: ChangeStreamHub;
    conventions?: ChangeEventConventions;
    name?: string;
    logger?: Logger;
    initialBackoffMs?: number;
    maxBackoffMs?: number;
}

/**
 * Watch the same collection across N databases and merge the results into one
 * stream of canonical `ChangeEvent`s, each tagged with its target's tenant.
 *
 * This is the multi-database counterpart of `watchCollection`: it is exactly
 * `mergeAsyncGenerators` over one `watchCollection` per target, and exists so
 * every multi-tenant consumer does not re-derive the same composition (plus
 * the subtleties `mergeAsyncGenerators` encodes about non-blocking teardown).
 *
 * An empty `targets` list yields nothing and completes immediately, rather
 * than hanging — a consumer whose tenant registry is empty at boot gets a
 * finished generator, not a stalled one.
 */
export async function* watchAcrossTargets<TTenant = unknown>(
    options: WatchAcrossTargetsOptions<TTenant>
): AsyncGenerator<ChangeEvent<TTenant>> {
    const streams = options.targets.map(target =>
        watchCollection<TTenant>({
            source: options.hub
                ? { hub: options.hub, key: target.key, db: target.db, collection: target.collection }
                : {
                      watch: collectionWatchFactory(target.db.collection(target.collection)),
                      db: target.db,
                      collection: target.collection,
                  },
            shutdown: options.shutdown,
            conventions: options.conventions,
            tenant: target.tenant,
            name: options.name ? `${options.name}[${target.key}]` : `watch:${target.collection}[${target.key}]`,
            logger: options.logger,
            initialBackoffMs: options.initialBackoffMs,
            maxBackoffMs: options.maxBackoffMs,
        })
    );

    yield* mergeAsyncGenerators(streams);
}
```

Add to `src/index.ts`:

```typescript
export * from './core/watch-across-targets.js';
```

- [ ] **Step 4: Run the tests**

Run: `pnpm vitest run tests/core/watch-across-targets.test.ts`
Expected: PASS (2 tests).

- [ ] **Step 5: Regenerate the API report, lint, commit**

```bash
pnpm api:update && pnpm lint && pnpm test
git add src/core/watch-across-targets.ts src/index.ts tests/core/watch-across-targets.test.ts etc/
git commit -m "feat(core): add watchAcrossTargets for multi-database fan-in"
```

---

## Task 5: Keyed `PresenceStore`

**Files:**
- Modify: `src/presence/types.ts:16-27`
- Modify: `src/presence/presence-store.ts` (whole file)
- Test: `tests/presence/presence-store.test.ts`

**Interfaces:**
- Consumes: `DbResolver`, `toDbResolver`, `DEFAULT_KEY` from Task 2.
- Produces (all `key` parameters optional, defaulting to `DEFAULT_KEY`, so
  every existing single-database call site is source-compatible):

```typescript
class PresenceStore {
    readonly serverId: string;
    collectionFor(key?: string): Collection<PresenceDoc>;
    ensureIndexes(): Promise<void>;                       // loops every key
    checkIn(entry: { userId; clientId; scopeId; key?; meta? }): Promise<{ isNewConnection: boolean }>;
    keepAlive(userId: string, clientId: string, key?: string): Promise<void>;
    setPage(userId: string, clientId: string, page: string, key?: string): Promise<void>;
    checkOut(userId: string, clientId: string, key?: string): Promise<void>;
    clearServer(): Promise<void>;                          // loops every key
    countUserRows(userId: string, key?: string): Promise<number>;
    countOlderUserRows(userId: string, beforeId: ObjectId, key?: string): Promise<number>;
    keys(): string[];
}
```

**Breaking change:** the public readonly `collection` property becomes the
`collectionFor(key?)` method. Its only in-repo consumer is `watchPresence`
(Task 6). `pnpm api:check` will flag the removal — that is expected; regenerate
the report.

- [ ] **Step 1: Write the failing test**

Append to `tests/presence/presence-store.test.ts`:

```typescript
import { DEFAULT_KEY, type DbResolver } from '../../src/core/db-resolver.js';

describe('PresenceStore across keys', () => {
    it('routes each key to its own database and keeps counts per key', async () => {
        const { client } = await startReplSet();
        const dbA = client.db(`presence-a-${randomUUID().slice(0, 8)}`);
        const dbB = client.db(`presence-b-${randomUUID().slice(0, 8)}`);
        const resolver: DbResolver = {
            keys: () => ['a', 'b'],
            dbFor: key => (key === 'a' ? dbA : dbB),
        };
        const collectionName = uniqueCollection('presence');
        const store = new PresenceStore({ db: resolver, serverId: randomUUID(), collectionName });
        await store.ensureIndexes();

        await store.checkIn({ userId: 'u1', clientId: 'c1', scopeId: 's1', key: 'a' });
        await store.checkIn({ userId: 'u1', clientId: 'c2', scopeId: 's1', key: 'b' });

        // Same user id in two databases must not be conflated.
        expect(await store.countUserRows('u1', 'a')).toBe(1);
        expect(await store.countUserRows('u1', 'b')).toBe(1);
        expect(await store.collectionFor('a').countDocuments({})).toBe(1);
        expect(await store.collectionFor('b').countDocuments({})).toBe(1);
    });

    it('ensureIndexes and clearServer act on every key', async () => {
        const { client } = await startReplSet();
        const dbA = client.db(`presence-c-${randomUUID().slice(0, 8)}`);
        const dbB = client.db(`presence-d-${randomUUID().slice(0, 8)}`);
        const resolver: DbResolver = { keys: () => ['a', 'b'], dbFor: k => (k === 'a' ? dbA : dbB) };
        const collectionName = uniqueCollection('presence');
        const store = new PresenceStore({ db: resolver, serverId: randomUUID(), collectionName });
        await store.ensureIndexes();

        for (const key of ['a', 'b']) {
            const indexes = await store.collectionFor(key).indexes();
            expect(indexes.find(i => i.unique === true)?.key).toEqual({ userId: 1, clientId: 1, serverId: 1 });
        }

        await store.checkIn({ userId: 'u1', clientId: 'c1', scopeId: 's1', key: 'a' });
        await store.checkIn({ userId: 'u2', clientId: 'c2', scopeId: 's1', key: 'b' });
        await store.clearServer();

        expect(await store.collectionFor('a').countDocuments({})).toBe(0);
        expect(await store.collectionFor('b').countDocuments({})).toBe(0);
    });

    it('a bare Db still works with no key argument anywhere', async () => {
        const { db } = await startReplSet();
        const store = new PresenceStore({ db, serverId: randomUUID(), collectionName: uniqueCollection('presence') });
        await store.ensureIndexes();

        const { isNewConnection } = await store.checkIn({ userId: 'u9', clientId: 'c9', scopeId: 's9' });
        expect(isNewConnection).toBe(true);
        await store.keepAlive('u9', 'c9');
        await store.setPage('u9', 'c9', '/home');
        expect(await store.countUserRows('u9')).toBe(1);
        expect(store.collectionFor().collectionName).toBe(store.collectionFor(DEFAULT_KEY).collectionName);
        await store.checkOut('u9', 'c9');
        expect(await store.countUserRows('u9')).toBe(0);
    });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pnpm vitest run tests/presence/presence-store.test.ts -t 'across keys'`
Expected: FAIL — `store.collectionFor is not a function`, and the
`PresenceStoreOptions.db` type rejects a `DbResolver`.

- [ ] **Step 3: Widen the options type**

In `src/presence/types.ts`, add the import and widen `db`:

```typescript
import type { DbResolver } from '../core/db-resolver.js';
```

```typescript
export interface PresenceStoreOptions {
    /**
     * One database, or a resolver spanning several keyed databases. A bare
     * `Db` behaves exactly as before: every method's optional `key` argument
     * can be omitted and resolves to that single database.
     */
    db: Db | DbResolver;
    serverId: string;
    collectionName?: string; // default 'onlineUsers'
    ttlSeconds?: number; // default 300
    logger?: Logger;
}
```

- [ ] **Step 4: Make the store keyed**

Rewrite the body of `src/presence/presence-store.ts`, keeping every doc comment
already on `checkIn` (the `includeResultMetadata` explanation is load-bearing)
and `countOlderUserRows`:

```typescript
import type { Collection, ObjectId } from 'mongodb';
import { DEFAULT_KEY, toDbResolver, type DbResolver } from '../core/db-resolver.js';
import { enablePrePostImages } from '../core/enable-pre-post-images.js';
import { consoleLogger, prefixed, type Logger } from '../common/logger.js';
import { DEFAULT_PRESENCE_COLLECTION, type PresenceDoc, type PresenceStoreOptions } from './types.js';

const DEFAULT_TTL_SECONDS = 300;

export class PresenceStore {
    readonly serverId: string;
    private readonly resolver: DbResolver;
    private readonly collectionName: string;
    private readonly ttlSeconds: number;
    private readonly logger: Logger;

    constructor(options: PresenceStoreOptions) {
        this.resolver = toDbResolver(options.db);
        this.serverId = options.serverId;
        this.collectionName = options.collectionName ?? DEFAULT_PRESENCE_COLLECTION;
        this.ttlSeconds = options.ttlSeconds ?? DEFAULT_TTL_SECONDS;
        this.logger = options.logger
            ? prefixed(options.logger, 'PresenceStore')
            : prefixed(consoleLogger, 'PresenceStore');
    }

    /** Every key this store spans. One entry (`DEFAULT_KEY`) for a bare `Db`. */
    keys(): string[] {
        return this.resolver.keys();
    }

    /** The presence collection in the database `key` names. */
    collectionFor(key: string = DEFAULT_KEY): Collection<PresenceDoc> {
        return this.resolver.dbFor(key).collection<PresenceDoc>(this.collectionName);
    }

    /** Idempotent: safe to call on every startup. Runs for every key. */
    async ensureIndexes(): Promise<void> {
        for (const key of this.resolver.keys()) {
            const collection = this.collectionFor(key);
            await collection.createIndex({ userId: 1, clientId: 1, serverId: 1 }, { unique: true });
            await collection.createIndex({ lastSeen: 1 }, { expireAfterSeconds: this.ttlSeconds });
            await enablePrePostImages(this.resolver.dbFor(key), [this.collectionName], this.logger);
        }
    }

    async checkIn(entry: {
        userId: string;
        clientId: string;
        scopeId: string;
        key?: string;
        meta?: Record<string, unknown>;
    }): Promise<{ isNewConnection: boolean }> {
        const key = entry.key ?? DEFAULT_KEY;
        // The DB arbitrates new-vs-rejoin: with upsert + returnDocument 'before', exactly one of any
        // set of concurrent racers for the same key observes `null` (fm audit-2026-05-21 B1d).
        //
        // includeResultMetadata: false is load-bearing, not cosmetic: the peer range's floor,
        // mongodb@5.7.0, defaults findOneAndUpdate to returning a `ModifyResult` wrapper
        // (`{ ok, value, lastErrorObject }`), which is truthy even for a brand-new upsert — so
        // `before === null` would silently always be false on that floor. The option was added in
        // exactly 5.7 (hence the floor) to make the direct-document return shape used below
        // deterministic across the whole declared 5.7–7.x range.
        const before = await this.collectionFor(key).findOneAndUpdate(
            { userId: entry.userId, clientId: entry.clientId, serverId: this.serverId },
            {
                $set: {
                    userId: entry.userId,
                    clientId: entry.clientId,
                    serverId: this.serverId,
                    scopeId: entry.scopeId,
                    key,
                    meta: entry.meta ?? {},
                    lastSeen: new Date(),
                },
            },
            { upsert: true, returnDocument: 'before', includeResultMetadata: false }
        );
        return { isNewConnection: before === null };
    }

    async keepAlive(userId: string, clientId: string, key?: string): Promise<void> {
        await this.collectionFor(key).updateOne(
            { userId, clientId, serverId: this.serverId },
            { $set: { lastSeen: new Date() } }
        );
    }

    async setPage(userId: string, clientId: string, page: string, key?: string): Promise<void> {
        await this.collectionFor(key).updateOne(
            { userId, clientId, serverId: this.serverId },
            { $set: { page } }
        );
    }

    async checkOut(userId: string, clientId: string, key?: string): Promise<void> {
        await this.collectionFor(key).deleteOne({ userId, clientId, serverId: this.serverId });
    }

    /** Orderly shutdown: drop every row this server owns, in every key's database. */
    async clearServer(): Promise<void> {
        for (const key of this.resolver.keys()) {
            await this.collectionFor(key).deleteMany({ serverId: this.serverId });
        }
    }

    /** Count of this user's rows across all servers, within one key's database. */
    async countUserRows(userId: string, key?: string): Promise<number> {
        return this.collectionFor(key).countDocuments({ userId });
    }

    /**
     * Count of this user's rows (across all servers, within one key's
     * database) whose `_id` sorts before `beforeId` — i.e. rows inserted
     * earlier. Order-stable 0→1 online detection (`watchPresence`) asks "is
     * any older row present?" instead of a live total count: with N concurrent
     * first-time check-ins, every racer's row lands with a distinct `_id`, and
     * this answers identically regardless of when it's evaluated relative to
     * the other inserts — exactly the row with the smallest `_id` sees zero
     * older rows.
     */
    async countOlderUserRows(userId: string, beforeId: ObjectId, key?: string): Promise<number> {
        return this.collectionFor(key).countDocuments({ userId, _id: { $lt: beforeId } });
    }
}
```

- [ ] **Step 5: Run the presence tests**

Run: `pnpm vitest run tests/presence/presence-store.test.ts`
Expected: FAIL in the pre-existing `ensureIndexes` test — it reads
`store.collection.indexes()`. Change that one line to
`store.collectionFor().indexes()`, and the same for any other `store.collection`
reference in the file. Re-run: PASS.

- [ ] **Step 6: Regenerate the API report, lint, commit**

```bash
pnpm api:update && pnpm lint
pnpm vitest run tests/presence
git add src/presence/types.ts src/presence/presence-store.ts tests/presence/presence-store.test.ts etc/
git commit -m "feat(presence)!: key PresenceStore by database, replacing collection with collectionFor"
```

`pnpm test` is expected to fail at this point — `watchPresence` still reads
`store.collection`. Task 6 fixes it; do not skip ahead to a green full suite
before committing this task.

---

## Task 6: Multi-key `watchPresence`

**Files:**
- Modify: `src/presence/watch-presence.ts` (whole file)
- Test: `tests/presence/watch-presence.test.ts`

**Interfaces:**
- Consumes: `PresenceStore.collectionFor/keys/countUserRows/countOlderUserRows`
  from Task 5; `mergeAsyncGenerators` from core.
- Produces:

```typescript
function watchPresence(options: {
    store: PresenceStore;
    shutdown: ReadonlySignal<boolean>;
    keys?: string[];          // default: store.keys()
    logger?: Logger;
    activityFields?: string[];
}): AsyncGenerator<PresenceEvent>
```

- [ ] **Step 1: Write the failing test**

Append to `tests/presence/watch-presence.test.ts`:

```typescript
import type { DbResolver } from '../../src/core/db-resolver.js';

it('watches every key and reports transitions per database', async () => {
    const { client } = await startReplSet();
    const dbA = client.db(`wp-a-${randomUUID().slice(0, 8)}`);
    const dbB = client.db(`wp-b-${randomUUID().slice(0, 8)}`);
    const resolver: DbResolver = { keys: () => ['a', 'b'], dbFor: k => (k === 'a' ? dbA : dbB) };
    const store = new PresenceStore({
        db: resolver,
        serverId: randomUUID(),
        collectionName: uniqueCollection('presence'),
    });
    await store.ensureIndexes();

    const shutdown = new ValueSignal(false);
    const seen: PresenceEvent[] = [];
    const collecting = (async () => {
        for await (const event of watchPresence({ store, shutdown })) {
            seen.push(event);
            if (seen.length === 2) break;
        }
    })();

    await new Promise(resolve => setTimeout(resolve, 1_000));
    // The SAME user id in two databases: each is its own 0->1 transition.
    await store.checkIn({ userId: 'u1', clientId: 'c1', scopeId: 's1', key: 'a' });
    await store.checkIn({ userId: 'u1', clientId: 'c2', scopeId: 's2', key: 'b' });

    await collecting;
    shutdown.next(true);

    expect(seen.every(e => e.type === 'online')).toBe(true);
    expect(new Set(seen.map(e => e.key))).toEqual(new Set(['a', 'b']));
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pnpm vitest run tests/presence/watch-presence.test.ts`
Expected: FAIL — the whole file errors, because `watchPresence` still reads
the removed `store.collection`.

- [ ] **Step 3: Implement the per-key split**

In `src/presence/watch-presence.ts`, add the imports:

```typescript
import { mergeAsyncGenerators } from '../core/merge-async-generators.js';
import { DEFAULT_KEY } from '../core/db-resolver.js';
```

Replace the exported `watchPresence` with a dispatcher plus the existing
single-key loop renamed and given a `key`:

```typescript
export async function* watchPresence(options: {
    store: PresenceStore;
    shutdown: ReadonlySignal<boolean>;
    /**
     * Which keys to watch. Defaults to every key the store spans. One
     * dedicated change stream is opened per key and the results are merged;
     * a single-database store therefore opens exactly one, as before.
     */
    keys?: string[];
    logger?: Logger;
    activityFields?: string[];
}): AsyncGenerator<PresenceEvent> {
    const keys = options.keys ?? options.store.keys();
    if (keys.length === 1) {
        // Avoid the merge wrapper entirely in the overwhelmingly common
        // single-database case, so its teardown semantics are not in the path.
        yield* watchPresenceKey(options.store, keys[0]!, options);
        return;
    }
    yield* mergeAsyncGenerators(keys.map(key => watchPresenceKey(options.store, key, options)));
}

async function* watchPresenceKey(
    store: PresenceStore,
    key: string,
    options: { shutdown: ReadonlySignal<boolean>; logger?: Logger; activityFields?: string[] }
): AsyncGenerator<PresenceEvent> {
    const label = key === DEFAULT_KEY ? 'watchPresence' : `watchPresence[${key}]`;
    const log = prefixed(options.logger ?? consoleLogger, label);
    const watch = collectionWatchFactory(store.collectionFor(key) as unknown as Collection);

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
        { name: label, logger: log }
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
            event = await toPresenceEvent(change, store, key, log, options.activityFields ?? DEFAULT_ACTIVITY_FIELDS);
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
```

Then give `toPresenceEvent` the key and thread it into both count calls:

```typescript
async function toPresenceEvent(
    change: ChangeStreamDocument<Document>,
    store: PresenceStore,
    key: string,
    log: Logger,
    activityFields: string[]
): Promise<PresenceEvent | undefined> {
```

Inside it, change exactly two call sites:

```typescript
            if ((await store.countOlderUserRows(doc.userId, doc._id, key)) > 0) return undefined;
```

```typescript
            if ((await store.countUserRows(before.userId, key)) !== 0) return undefined;
```

Leave the three `key: doc.key` / `key: before.key` fields on the returned
events as they are — the row's stored key equals the watching key by
construction, and reading it from the document keeps the event faithful to
what is persisted.

- [ ] **Step 4: Run the full suite**

Run: `pnpm test`
Expected: PASS — including every pre-existing `watch-presence.test.ts` case
(single-key stores take the no-merge branch) and all of
`tests/contracts/calendar/`.

- [ ] **Step 5: Regenerate the API report, lint, commit**

```bash
pnpm api:update && pnpm lint && pnpm test
git add src/presence/watch-presence.ts tests/presence/watch-presence.test.ts etc/
git commit -m "feat(presence): watch every key and merge, keeping single-db on the direct path"
```

---

## Task 7: Accept a `DbResolver` in `RealtimeModule`

**Files:**
- Modify: `src/nestjs/realtime-module.ts:19-24`
- Test: `tests/nestjs/realtime-module.test.ts`

**Interfaces:**
- Consumes: `DbResolver` (Task 2), keyed `PresenceStore` (Task 5).
- Produces: `RealtimeModuleOptions.db: Db | DbResolver`.

`RealtimeLifecycle` needs **no** change: `ensureIndexes()` and `clearServer()`
already loop every key internally as of Task 5.

- [ ] **Step 1: Write the failing test**

Append to `tests/nestjs/realtime-module.test.ts`:

```typescript
import type { DbResolver } from '../../src/core/db-resolver.js';

it('provides a PresenceStore spanning every key when given a resolver', async () => {
    const { client } = await startReplSet();
    const dbA = client.db(`rm-a-${randomUUID().slice(0, 8)}`);
    const dbB = client.db(`rm-b-${randomUUID().slice(0, 8)}`);
    const resolver: DbResolver = { keys: () => ['a', 'b'], dbFor: k => (k === 'a' ? dbA : dbB) };

    const module = RealtimeModule.forRoot({ db: resolver });
    const storeProvider = (module.providers ?? []).find(
        p => (p as { provide?: unknown }).provide === PRESENCE_STORE
    ) as { useFactory: (serverId: string) => PresenceStore };

    const store = storeProvider.useFactory('server-1');
    expect(store.keys()).toEqual(['a', 'b']);
    expect(store.collectionFor('a').dbName).toBe(dbA.databaseName);
    expect(store.collectionFor('b').dbName).toBe(dbB.databaseName);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pnpm vitest run tests/nestjs/realtime-module.test.ts -t 'spanning every key'`
Expected: FAIL — `Type 'DbResolver' is not assignable to type 'Db'` at the
`forRoot` call.

- [ ] **Step 3: Widen the option**

In `src/nestjs/realtime-module.ts`, import the type and widen the field:

```typescript
import type { DbResolver } from '../core/db-resolver.js';
```

```typescript
export interface RealtimeModuleOptions {
    /**
     * One database, or a resolver spanning several keyed databases. With a
     * resolver, the presence store keys every operation and the hub's
     * `connectedKeys` parking compares against those same keys. A bare `Db`
     * behaves exactly as before.
     */
    db: Db | DbResolver;
    ...
}
```

No factory changes are needed — `options.db` flows straight into
`new PresenceStore({ ... db: o.db ... })`, which now accepts both.

- [ ] **Step 4: Run the tests**

Run: `pnpm vitest run tests/nestjs`
Expected: PASS.

- [ ] **Step 5: Re-export the type from the nestjs subpath**

Add to `src/nestjs/index.ts` alongside the other self-describing re-exports:

```typescript
export type { DbResolver } from '../core/db-resolver.js';
```

- [ ] **Step 6: Regenerate the API report, lint, commit**

```bash
pnpm api:update && pnpm lint && pnpm test
git add src/nestjs/realtime-module.ts src/nestjs/index.ts tests/nestjs/realtime-module.test.ts etc/
git commit -m "feat(nestjs): accept a DbResolver as the module's database"
```

---

## Task 8: `nestLogger`

Both consumers hand-roll this adapter today (calendar in
`apps/api/src/realtime-infra/watch-model.ts`). It belongs in the `nestjs`
subpath, not the `mongoose` one — it has nothing to do with the ORM.

**Files:**
- Create: `src/nestjs/nest-logger.ts`
- Modify: `src/nestjs/index.ts`
- Test: `tests/nestjs/nest-logger.test.ts`

**Interfaces:**
- Produces: `nestLogger(context: string, impl?: NestLoggerLike): Logger`

- [ ] **Step 1: Write the failing test**

Create `tests/nestjs/nest-logger.test.ts`:

```typescript
import { nestLogger } from '../../src/nestjs/nest-logger.js';

describe('nestLogger', () => {
    it('forwards each level to the injected Nest logger', () => {
        const calls: Array<[string, unknown[]]> = [];
        const fake = {
            debug: (...args: unknown[]) => calls.push(['debug', args]),
            log: (...args: unknown[]) => calls.push(['log', args]),
            warn: (...args: unknown[]) => calls.push(['warn', args]),
            error: (...args: unknown[]) => calls.push(['error', args]),
            verbose: (...args: unknown[]) => calls.push(['verbose', args]),
        };

        const logger = nestLogger('Ctx', fake);
        logger.debug('d');
        logger.log('l');
        logger.warn('w');
        logger.error('e', 'stack-trace');

        expect(calls).toEqual([
            ['debug', ['d']],
            ['log', ['l']],
            ['warn', ['w']],
            ['error', ['e', 'stack-trace']],
        ]);
    });

    it('maps verbose to the Nest verbose level when present, else debug', () => {
        const calls: string[] = [];
        const withoutVerbose = {
            debug: () => calls.push('debug'),
            log: () => {},
            warn: () => {},
            error: () => {},
        };
        nestLogger('Ctx', withoutVerbose).verbose?.('v');
        expect(calls).toEqual(['debug']);
    });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pnpm vitest run tests/nestjs/nest-logger.test.ts`
Expected: FAIL — `Cannot find module '../../src/nestjs/nest-logger.js'`.

- [ ] **Step 3: Implement**

Create `src/nestjs/nest-logger.ts`:

```typescript
import { Logger as NestLogger } from '@nestjs/common';
import type { Logger } from '../common/logger.js';

/**
 * The subset of a NestJS logger this adapter drives. Declared structurally so
 * a test (or a consumer with a custom logger) can pass a stand-in without
 * constructing a real `NestLogger`.
 */
export interface NestLoggerLike {
    debug(message: string, ...rest: unknown[]): void;
    log(message: string, ...rest: unknown[]): void;
    warn(message: string, ...rest: unknown[]): void;
    error(message: string, ...rest: unknown[]): void;
    verbose?(message: string, ...rest: unknown[]): void;
}

/**
 * Adapts a NestJS logger to this library's structural `Logger`, so
 * change-stream and fan-out logs land in the same stream and format as the
 * rest of the host application instead of going to bare console.
 *
 * Both consumers wrote this by hand before it lived here.
 */
export function nestLogger(context: string, impl?: NestLoggerLike): Logger {
    const target: NestLoggerLike = impl ?? new NestLogger(context);
    return {
        debug: message => target.debug(message),
        log: message => target.log(message),
        warn: message => target.warn(message),
        error: (message, stack) =>
            stack === undefined ? target.error(message) : target.error(message, stack),
        // Nest's default logger has `verbose`; a custom one may not. Fall back
        // to `debug` rather than dropping the line.
        verbose: message => (target.verbose ? target.verbose(message) : target.debug(message)),
    };
}
```

Add to `src/nestjs/index.ts`:

```typescript
export * from './nest-logger.js';
```

- [ ] **Step 4: Run the tests**

Run: `pnpm vitest run tests/nestjs/nest-logger.test.ts`
Expected: PASS (2 tests).

Note: if `Logger` in `src/common/logger.ts` has no `verbose` member, drop the
`verbose` property from the returned object and the second test — check
`src/common/logger.ts` first and match its interface exactly.

- [ ] **Step 5: Regenerate the API report, lint, commit**

```bash
pnpm api:update && pnpm lint && pnpm test
git add src/nestjs/nest-logger.ts src/nestjs/index.ts tests/nestjs/nest-logger.test.ts etc/
git commit -m "feat(nestjs): ship the NestJS logger adapter both consumers hand-rolled"
```

---

## Task 9: `mongodb-realtime/mongoose` subpath

**Files:**
- Create: `src/mongoose/index.ts`
- Modify: `package.json` (exports map, `mongoose` devDependency)
- Modify: `tsup.config.ts` (new entry)
- Test: `tests/mongoose/watch-model.test.ts`

**Interfaces:**
- Consumes: `watchAcrossTargets`, `WatchTarget` (Task 4); `DEFAULT_KEY` (Task 2).
- Produces:

```typescript
interface MongooseModelLike { modelName: string; db: { db?: Db | null }; collection: { name: string } }
function modelTarget<TTenant>(model: MongooseModelLike, options?: { key?: string; tenant?: TTenant }): WatchTarget<TTenant>
function watchModel<TTenant>(options: {
    models: MongooseModelLike | Array<{ model: MongooseModelLike; key?: string; tenant?: TTenant }>;
    shutdown: ReadonlySignal<boolean>;
    hub?: ChangeStreamHub;
    conventions?: ChangeEventConventions;
    name?: string;
    logger?: Logger;
}): AsyncGenerator<ChangeEvent<TTenant>>
```

The model type is **structural**, so the library takes no mongoose peer
dependency. `mongoose` is added as a devDependency only, so one test exercises
the real `model.db.db` / `model.collection.name` shape — that seam is exactly
what would break silently on a mongoose major bump.

- [ ] **Step 1: Add mongoose as a devDependency**

```bash
pnpm add -D mongoose
```

- [ ] **Step 2: Write the failing test**

Create `tests/mongoose/watch-model.test.ts`:

```typescript
import { randomUUID } from 'node:crypto';
import mongoose from 'mongoose';
import { ValueSignal } from '../../src/common/signal.js';
import { modelTarget, watchModel } from '../../src/mongoose/index.js';
import { DEFAULT_KEY } from '../../src/core/db-resolver.js';
import type { ChangeEvent } from '../../src/core/change-event.js';
import { startReplSet, stopReplSet, uniqueCollection } from '../helpers/replset.js';

describe('watchModel', () => {
    const connections: mongoose.Connection[] = [];

    afterAll(async () => {
        await Promise.all(connections.map(c => c.close()));
        await stopReplSet();
    });

    async function connect(dbName: string): Promise<mongoose.Connection> {
        const { uri } = await startReplSet();
        const connection = mongoose.createConnection(uri, { dbName });
        await connection.asPromise();
        connections.push(connection);
        return connection;
    }

    it('modelTarget reads the driver Db and collection name off a real mongoose model', async () => {
        const dbName = `wm-${randomUUID().slice(0, 8)}`;
        const connection = await connect(dbName);
        const collectionName = uniqueCollection('widgets');
        const model = connection.model('Widget', new mongoose.Schema({ organizationId: String }), collectionName);

        const target = modelTarget(model as never);
        expect(target.key).toBe(DEFAULT_KEY);
        expect(target.collection).toBe(collectionName);
        expect(target.db.databaseName).toBe(dbName);
    });

    it('watches two tenants\' models and tags each event', async () => {
        const collectionName = uniqueCollection('widgets');
        const schema = new mongoose.Schema({ organizationId: String, name: String });
        const connA = await connect(`wm-a-${randomUUID().slice(0, 8)}`);
        const connB = await connect(`wm-b-${randomUUID().slice(0, 8)}`);
        const modelA = connA.model('WidgetA', schema, collectionName);
        const modelB = connB.model('WidgetB', schema, collectionName);
        await modelA.createCollection();
        await modelB.createCollection();

        const shutdown = new ValueSignal(false);
        const seen: ChangeEvent<{ slug: string }>[] = [];
        const collecting = (async () => {
            for await (const event of watchModel<{ slug: string }>({
                models: [
                    { model: modelA as never, key: 'a', tenant: { slug: 'a' } },
                    { model: modelB as never, key: 'b', tenant: { slug: 'b' } },
                ],
                shutdown,
                name: 'widgets',
            })) {
                seen.push(event);
                if (seen.length === 2) break;
            }
        })();

        await new Promise(resolve => setTimeout(resolve, 1_000));
        await modelA.create({ organizationId: 'org-a', name: 'a' });
        await modelB.create({ organizationId: 'org-b', name: 'b' });

        await collecting;
        shutdown.next(true);

        expect(new Set(seen.map(e => e.tenant?.slug))).toEqual(new Set(['a', 'b']));
    });

    it('throws a named error when a model has no driver database handle', () => {
        const detached = { modelName: 'Detached', db: { db: null }, collection: { name: 'x' } };
        expect(() => modelTarget(detached)).toThrow(/Detached/);
    });
});
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pnpm vitest run tests/mongoose/watch-model.test.ts`
Expected: FAIL — `Cannot find module '../../src/mongoose/index.js'`.

- [ ] **Step 4: Implement**

Create `src/mongoose/index.ts`:

```typescript
import type { Db } from 'mongodb';
import type { Logger } from '../common/logger.js';
import type { ReadonlySignal } from '../common/signal.js';
import type { ChangeEvent, ChangeEventConventions } from '../core/change-event.js';
import type { ChangeStreamHub } from '../core/change-stream-hub.js';
import { DEFAULT_KEY } from '../core/db-resolver.js';
import { watchAcrossTargets, type WatchTarget } from '../core/watch-across-targets.js';

/**
 * The part of a mongoose `Model` this adapter reads. Declared STRUCTURALLY so
 * this package takes no mongoose dependency — not even a peer. Any object with
 * these three members works, which also makes the adapter trivially fakeable.
 */
export interface MongooseModelLike {
    modelName: string;
    db: { db?: Db | null };
    collection: { name: string };
}

/**
 * Convert a mongoose model into a {@link WatchTarget}.
 *
 * The collection name comes from the model rather than being hardcoded, so a
 * schema-level `collection:` rename cannot silently detach the watcher.
 *
 * Throws when the model's connection has no driver handle — a model on an
 * unconnected connection would otherwise produce a target that fails later,
 * inside the change-stream loop, with a far less obvious message.
 */
export function modelTarget<TTenant = unknown>(
    model: MongooseModelLike,
    options: { key?: string; tenant?: TTenant } = {}
): WatchTarget<TTenant> {
    const db = model.db.db;
    if (!db) {
        throw new Error(
            `Mongoose connection for ${model.modelName} has no database handle; cannot open a change stream.`
        );
    }
    return {
        key: options.key ?? DEFAULT_KEY,
        db,
        collection: model.collection.name,
        tenant: options.tenant,
    };
}

/**
 * Watch one mongoose model — or the same model across N tenant connections —
 * and yield canonical `ChangeEvent`s.
 *
 * Hub mode when `hub` is supplied: every collection under a given key shares
 * ONE database-level cursor, so the process holds O(keys) change-stream
 * cursors instead of one per watcher, and each cursor parks entirely while
 * nobody is connected on that key. `hub` stays optional so callers can migrate
 * one watcher at a time.
 */
export async function* watchModel<TTenant = unknown>(options: {
    models: MongooseModelLike | Array<{ model: MongooseModelLike; key?: string; tenant?: TTenant }>;
    shutdown: ReadonlySignal<boolean>;
    hub?: ChangeStreamHub;
    conventions?: ChangeEventConventions;
    name?: string;
    logger?: Logger;
}): AsyncGenerator<ChangeEvent<TTenant>> {
    const entries = Array.isArray(options.models) ? options.models : [{ model: options.models }];
    const targets = entries.map(entry =>
        modelTarget<TTenant>(entry.model, { key: entry.key, tenant: (entry as { tenant?: TTenant }).tenant })
    );

    yield* watchAcrossTargets<TTenant>({
        targets,
        shutdown: options.shutdown,
        hub: options.hub,
        conventions: options.conventions,
        name: options.name,
        logger: options.logger,
    });
}
```

- [ ] **Step 5: Wire the new entry point**

In `tsup.config.ts`, add to `entry`:

```typescript
        'mongoose/index': 'src/mongoose/index.ts',
```

In `package.json`, add to `exports` after the `./nestjs` block:

```json
    "./mongoose": {
      "types": "./dist/mongoose/index.d.ts",
      "import": "./dist/mongoose/index.mjs",
      "require": "./dist/mongoose/index.js"
    }
```

Check `scripts/api-report.mjs` for a hardcoded list of entry points; if it has
one, add `mongoose` to it so `pnpm api:check` covers the new subpath.

- [ ] **Step 6: Run the tests and the build-artifact check**

Run: `pnpm build && pnpm vitest run tests/mongoose tests/build-artifacts.test.ts`
Expected: PASS. `build-artifacts.test.ts` verifies the dual cjs/esm layout —
if it enumerates subpaths, add `mongoose` to its list.

- [ ] **Step 7: Regenerate the API report, lint, commit**

```bash
pnpm api:update && pnpm lint && pnpm test
git add src/mongoose tests/mongoose package.json pnpm-lock.yaml tsup.config.ts etc/ scripts/
git commit -m "feat(mongoose): add a structural mongoose adapter subpath"
```

---

## Task 10: fm consumer contract tests

Mirrors `tests/contracts/calendar/`: pins the compositions fm will depend on,
written **before** fm migrates, so a later library change that breaks fm fails
here rather than in fm's e2e suite.

**Files:**
- Create: `tests/contracts/fm/README.md`
- Create: `tests/contracts/fm/entity-fanout.contract.test.ts`
- Create: `tests/contracts/fm/multi-tenant-watch.contract.test.ts`
- Create: `tests/contracts/fm/presence.contract.test.ts`

**Interfaces:**
- Consumes: everything built in Tasks 2–9, plus `pushScoped`,
  `pushScopedFiltered`, `pushOwnerScoped` and the socket.io test harness at
  `tests/socketio/harness.ts`.

- [ ] **Step 1: Read the calendar contract suite first**

Run: `cat tests/contracts/calendar/README.md tests/contracts/calendar/presence.contract.test.ts`

Match its structure, naming and level of assertion. Do not invent a new style.

- [ ] **Step 2: Write the README**

Create `tests/contracts/fm/README.md`:

```markdown
# fm consumer contracts

These tests pin the library compositions that pactsheet/fm depends on, in the
exact shapes fm uses them. They are consumer-driven contract tests: fm does not
run them, this repo does, so a change here that would break fm fails in this
repo's CI instead of in fm's (CI-only) backend e2e suite.

Source of truth for the shapes: fm's `backend/app/src/common/change-stream/`
and its 38 gateway pushers, as of the 2026-08-19 adoption design
(`docs/superpowers/specs/2026-08-19-mongodb-realtime-adoption-design.md` in the
fm repo).

**Re-sync these if fm's compositions change.** In particular:
- `pushScopedFiltered` with `variants` + `prepareEvent` + `runInContext` +
  `tenantOf` all supplied at once — fm's hierarchy gateways use every hook
  together, which no other consumer does.
- Multi-tenant `watchAcrossTargets`/`watchModel` in hub mode with per-tenant
  event tagging.
- Presence keyed by tenant database.
```

- [ ] **Step 3: Write the entity fan-out contract**

Create `tests/contracts/fm/entity-fanout.contract.test.ts`:

```typescript
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import type { RemoteSocket, Server } from 'socket.io';
import { pushOwnerScoped } from '../../../src/socketio/push-owner-scoped.js';
import { pushScopedFiltered } from '../../../src/socketio/push-scoped-filtered.js';
import { namespacedRoom, scopeRoom, userRoom } from '../../../src/socketio/rooms.js';
import { ValueSignal } from '../../../src/common/signal.js';
import type { ChangeEvent } from '../../../src/core/change-event.js';
import {
    connectClient,
    joinRoomWithData,
    receivedEvents,
    startHarness,
    stopHarness,
    waitForCount,
    delay,
} from '../../socketio/harness.js';

/** fm's tenant shape, reduced to what the fan-out reads. */
interface Tenant {
    dbName: string;
}

const TENANT: Tenant = { dbName: 'tenant_a' };
const ORG = 'org1';
const ORG_ROOM = namespacedRoom(TENANT.dbName, scopeRoom(ORG));
const PII_ROOM = `${ORG_ROOM}:pii`;

async function* gen(events: ChangeEvent<Tenant>[]): AsyncGenerator<ChangeEvent<Tenant>> {
    for (const event of events) yield event;
}

function changed(data: unknown): ChangeEvent<Tenant> {
    return { type: 'changed', scopeId: ORG, data, operation: 'update', tenant: TENANT };
}

/** fm's `tenantFanout(cls)`, in the exact shape fm ships. */
function tenantFanout(observed: unknown[]) {
    return {
        tenantNamespace: (tenant: Tenant) => tenant.dbName,
        runInContext: <R>(ctx: { tenant?: unknown }, fn: () => Promise<R>): Promise<R> => {
            observed.push(ctx.tenant);
            // fm branches on the VALUE here; the library wraps on hook PRESENCE.
            return fn();
        },
        tenantOf: (socket: RemoteSocket<never, unknown>) =>
            (socket.data as { tenant?: Tenant } | undefined)?.tenant,
    };
}

let server: Server;
let port: number;

beforeEach(async () => {
    const started = await startHarness();
    server = started.server;
    port = started.port;
});

afterEach(() => stopHarness());

describe('fm entity fan-out contract', () => {
    it('runs prepareEvent once per event and canReceive once per socket, each in context', async () => {
        const allowed = await connectClient(port);
        const denied = await connectClient(port);
        await joinRoomWithData(allowed, ORG_ROOM, { tenant: TENANT, allow: true });
        await joinRoomWithData(denied, ORG_ROOM, { tenant: TENANT, allow: false });

        const allowedEvents = receivedEvents(allowed, 'thing:changed');
        const deniedEvents = receivedEvents(denied, 'thing:changed');

        const contexts: unknown[] = [];
        const preparedSeen: unknown[] = [];
        let prepareCalls = 0;
        let canReceiveCalls = 0;

        await pushScopedFiltered<Tenant>(
            server,
            gen([changed({ _id: '1', name: 'a' })]),
            { changed: 'thing:changed', removed: 'thing:removed' },
            {
                shutdown: new ValueSignal(true),
                ...tenantFanout(contexts),
                canReceive: (socket, _data, prepared) => {
                    canReceiveCalls += 1;
                    preparedSeen.push(prepared);
                    return (socket.data as { allow?: boolean }).allow === true;
                },
                prepareEvent: () => {
                    prepareCalls += 1;
                    return { scope: 'property-1' };
                },
                mapData: data => ({ ...(data as object), mapped: true }),
            }
        );

        await waitForCount(allowedEvents, 1);
        await delay(100);

        expect(prepareCalls).toBe(1);
        expect(canReceiveCalls).toBe(2);
        // The once-per-event value reaches every recipient.
        expect(preparedSeen).toEqual([{ scope: 'property-1' }, { scope: 'property-1' }]);
        // Once for the event block, once per recipient socket.
        expect(contexts).toEqual([TENANT, TENANT, TENANT]);
        expect(allowedEvents).toEqual([{ _id: '1', name: 'a', mapped: true }]);
        expect(deniedEvents).toEqual([]);
    });

    it('reports ctx.tenant undefined for a socket that has no tenant', async () => {
        // fm's adapter branches on ctx.tenant so a tenantless socket does not
        // enter CLS. This pins that the library reports the socket's own value
        // faithfully rather than substituting the event's tenant.
        const client = await connectClient(port);
        await joinRoomWithData(client, ORG_ROOM, {});
        const events = receivedEvents(client, 'thing:changed');

        const contexts: unknown[] = [];
        await pushScopedFiltered<Tenant>(
            server,
            gen([changed({ _id: '1' })]),
            { changed: 'thing:changed' },
            {
                shutdown: new ValueSignal(true),
                ...tenantFanout(contexts),
                canReceive: () => true,
            }
        );

        await waitForCount(events, 1);
        // First entry is the once-per-event block (the EVENT has a tenant);
        // the second is this socket's check, which does not.
        expect(contexts).toEqual([TENANT, undefined]);
    });

    it('renders both PII variants once and delivers the one pick selects', async () => {
        const withPii = await connectClient(port);
        const withoutPii = await connectClient(port);
        await joinRoomWithData(withPii, ORG_ROOM, { tenant: TENANT });
        await joinRoomWithData(withPii, PII_ROOM, { tenant: TENANT });
        await joinRoomWithData(withoutPii, ORG_ROOM, { tenant: TENANT });

        const fullEvents = receivedEvents(withPii, 'contact:changed');
        const redactedEvents = receivedEvents(withoutPii, 'contact:changed');

        let renderCalls = 0;
        await pushScopedFiltered<Tenant>(
            server,
            gen([changed({ _id: '1', email: 'a@b.c' })]),
            { changed: 'contact:changed' },
            {
                shutdown: new ValueSignal(true),
                ...tenantFanout([]),
                canReceive: () => true,
                variants: {
                    render: data => {
                        renderCalls += 1;
                        return {
                            full: data,
                            redacted: { ...(data as object), email: null, piiRedacted: true },
                        };
                    },
                    pick: socket => (socket.rooms.has(PII_ROOM) ? 'full' : 'redacted'),
                },
            }
        );

        await waitForCount(fullEvents, 1);
        await waitForCount(redactedEvents, 1);

        expect(renderCalls).toBe(1);
        expect(fullEvents).toEqual([{ _id: '1', email: 'a@b.c' }]);
        expect(redactedEvents).toEqual([{ _id: '1', email: null, piiRedacted: true }]);
    });

    it('rejects mapData and variants supplied together', async () => {
        await expect(
            pushScopedFiltered<Tenant>(
                server,
                gen([]),
                { changed: 'thing:changed' },
                {
                    shutdown: new ValueSignal(true),
                    ...tenantFanout([]),
                    canReceive: () => true,
                    mapData: data => data,
                    variants: { render: data => ({ full: data, redacted: data }), pick: () => 'full' },
                }
            )
        ).rejects.toThrow(/mutually exclusive/);
    });

    it('emits a removed event to the whole room without consulting canReceive', async () => {
        // fm relies on this: a delete carries no document, so a scope-derived
        // canReceive would deny every recipient and the deleted row would
        // linger in their lists until a refresh.
        const client = await connectClient(port);
        await joinRoomWithData(client, ORG_ROOM, { tenant: TENANT });
        const events = receivedEvents(client, 'thing:removed');

        let canReceiveCalls = 0;
        await pushScopedFiltered<Tenant>(
            server,
            gen([{ type: 'removed', id: '42', scopeId: ORG, via: 'delete', tenant: TENANT }]),
            { changed: 'thing:changed', removed: 'thing:removed' },
            {
                shutdown: new ValueSignal(true),
                ...tenantFanout([]),
                canReceive: () => {
                    canReceiveCalls += 1;
                    return false;
                },
            }
        );

        await waitForCount(events, 1);
        expect(events).toEqual(['42']);
        expect(canReceiveCalls).toBe(0);
    });

    it('routes per-owner events by ownerIdOf, and removals by event.ownerId', async () => {
        const owner = await connectClient(port);
        const other = await connectClient(port);
        const ownerRoom = namespacedRoom(TENANT.dbName, userRoom('user-1'));
        const otherRoom = namespacedRoom(TENANT.dbName, userRoom('user-2'));
        await joinRoomWithData(owner, ownerRoom, { tenant: TENANT });
        await joinRoomWithData(other, otherRoom, { tenant: TENANT });

        const ownerChanged = receivedEvents(owner, 'notification');
        const ownerRemoved = receivedEvents(owner, 'notification:removed');
        const otherChanged = receivedEvents(other, 'notification');

        await pushOwnerScoped<Tenant>(
            server,
            gen([
                changed({ _id: '1', userId: 'user-1' }),
                { type: 'removed', id: '1', ownerId: 'user-1', via: 'delete', tenant: TENANT },
            ]),
            { changed: 'notification', removed: 'notification:removed' },
            {
                shutdown: new ValueSignal(true),
                tenantNamespace: (tenant: Tenant) => tenant.dbName,
                ownerIdOf: data => (data as { userId?: string }).userId,
            }
        );

        await waitForCount(ownerChanged, 1);
        await waitForCount(ownerRemoved, 1);
        await delay(100);

        expect(ownerChanged).toEqual([{ _id: '1', userId: 'user-1' }]);
        expect(ownerRemoved).toEqual(['1']);
        expect(otherChanged).toEqual([]);
    });
});
```

- [ ] **Step 4: Write the multi-tenant watch contract**

Create `tests/contracts/fm/multi-tenant-watch.contract.test.ts`. Read
`tests/core/watch-across-targets.test.ts` (Task 4) and
`tests/core/change-stream-hub.test.ts`'s parking case first, and mirror their
setup exactly — two `client.db(...)` databases, `createCollection`, a
`ValueSignal(false)` shutdown, and a 1 s settle before the first write — rather
than inventing a third style. Add a `ChangeStreamHub` with a short `graceMs`
so the parking case does not wait the default 45 seconds.

Two cases:

```typescript
it('hub mode isolates tenants: each target sees only its own database\'s events', async () => {
    // Two dbs sharing a collection name, one hub, connectedKeys = ValueSignal(['a','b']).
    // Insert into db A only. Assert exactly one event arrives, its tenant is A,
    // and nothing is delivered carrying tenant B.
});

it('parks a key absent from connectedKeys and resumes losslessly when it returns', async () => {
    // connectedKeys starts ['a'], hub graceMs 100. Set it to [], wait past the
    // grace window, insert into A, then set ['a'] again. Assert the insert made
    // during the park is still delivered on resume — the resume token covers it.
});
```

- [ ] **Step 5: Write the presence contract**

Create `tests/contracts/fm/presence.contract.test.ts`. Reuse the two-database
`DbResolver` setup from Task 5's tests and the collect-until-N pattern from
Task 6's.

Four cases:

```typescript
it('never conflates the same userId across two tenant databases', async () => {
    // checkIn u1 in 'a' and in 'b'. Assert countUserRows('u1','a') === 1,
    // countUserRows('u1','b') === 1, and each collection holds exactly one row.
});

it('announces online once per user per tenant, not once per connection', async () => {
    // checkIn (u1,c1,'a') then (u1,c2,'a'). Collect from watchPresence and
    // assert exactly one 'online' event, with key 'a'.
});

it('announces offline only when the user\'s last row in that tenant goes', async () => {
    // checkOut c1 -> no event. checkOut c2 -> one 'offline' carrying scopeId
    // and meta recovered from the pre-image.
});

it('clearServer removes only this server\'s rows, in every tenant database', async () => {
    // Two stores with different serverIds writing through the same resolver.
    // clearServer on one leaves the other's rows intact in both databases.
});
```

- [ ] **Step 6: Run the contract suite**

Run: `pnpm vitest run tests/contracts`
Expected: PASS — both `calendar/` and the new `fm/`.

- [ ] **Step 7: Commit**

```bash
pnpm lint && pnpm test
git add tests/contracts/fm
git commit -m "test(contracts): pin fm's consumer compositions before it migrates"
```

---

## Task 11: Release 0.3.0

**Files:**
- Modify: `package.json` (version)
- Modify: `docs/follow-ups-0.1.x.md`
- Modify: `README.md`

- [ ] **Step 1: Update the follow-ups doc**

Strike through the now-done entry and add the adoption notes this plan created:

```markdown
- ~~`forRootAsync` duplicates `forRoot`'s 5-provider wiring inline; refactor to a shared parameterized builder.~~ **Done — Task 1 (0.3.0).**
```

Add under **Adoption notes**:

```markdown
- `PresenceStore.collection` was replaced by `collectionFor(key?)` in 0.3.0. A
  consumer reading the property directly must switch; `watchPresence` users are
  unaffected.
- `watchPresence` opens one cursor per key. A consumer with many keys and a
  large `keys()` should expect O(keys) presence cursors — the hub does NOT
  cover presence, by design (see `hubIgnoreCollections`).
```

- [ ] **Step 2: Document the new surface in the README**

Add a section covering `DbResolver`, `watchAcrossTargets`, the keyed presence
API and the `mongodb-realtime/mongoose` subpath, following the README's
existing per-subpath structure.

- [ ] **Step 3: Bump the version**

```bash
npm version 0.3.0 --no-git-tag-version
```

- [ ] **Step 4: Full verification**

```bash
pnpm lint && pnpm typecheck && pnpm build && pnpm api:check && pnpm test
```
Expected: all green, including both contract suites.

- [ ] **Step 5: Commit and tag**

```bash
git add package.json README.md docs/follow-ups-0.1.x.md
git commit -m "chore(release): 0.3.0 — keyed multi-database support"
git tag v0.3.0
```

Do **not** push. Pushing main and the tag is an explicit, separate instruction
from the repo owner — and the first push runs the `api:check` CI gate.

---

## Notes for the executor

- The `pnpm test` suite boots a `MongoMemoryReplSet` per test file and the
  first run downloads a mongod binary. Allow several minutes on a cold cache;
  `hookTimeout` is already 120 s.
- Task 5 deliberately leaves the full suite red at commit time (only
  `tests/presence` is required to pass). Task 6 restores it. Do not merge the
  two tasks to avoid the red — the split is what keeps each diff reviewable.
- If `pnpm api:check` reports `ae-forgotten-export`, the fix is to add the
  missing type to the relevant subpath's `index.ts` re-export block, never to
  silence the rule.
