# mongodb-realtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `mongodb-realtime` library (spec: `docs/superpowers/specs/2026-08-07-mongodb-realtime-library-design.md`) — resumable change-stream subscriptions, a per-key DB-level watch hub, presence + connected-scope registry, socket.io fan-out loops, and a thin NestJS binding — by porting pactsheet/fm's hardened implementation into a standalone zero-dependency package.

**Architecture:** One package, four subpath exports (`.`, `./presence`, `./socketio`, `./nestjs`). Core is raw-`mongodb`-driver based and transport-agnostic; socket.io and NestJS are optional peers touched only from their subpaths via type-only imports. Reactive scope wiring uses a structural signal interface (rxjs `BehaviorSubject`-compatible) implemented internally, so the package has zero runtime dependencies.

**Tech Stack:** TypeScript 5.x, tsup (dual ESM/CJS + dts), vitest + `mongodb-memory-server` (`MongoMemoryReplSet` — change streams need a replica set), pnpm, GitHub Actions.

## Global Constraints

- Package name `mongodb-realtime`, version `0.1.0`, MIT, `"type": "module"`, `engines.node >= 20`.
- **Zero runtime `dependencies`.** `peerDependencies`: `mongodb ^5.7.0 || ^6.0.0 || ^7.0.0` (required), `socket.io ^4.6.0` (optional), `@nestjs/common >=10 <12` (optional). Optional peers MUST appear in **both** `peerDependencies` and `peerDependenciesMeta` (mongo_queue's marker-without-declaration bug must not be repeated).
- **No rxjs, no lodash, no @nestjs imports in `src/` outside `src/nestjs/`**, and `src/nestjs/` + `src/socketio/` may import their peer **types only** (`import type`). Exception: none. `mongodb` value imports allowed everywhere (it's the required peer).
- Package manager: **pnpm** (never npm).
- Every `src/` file ≤ ~450 lines; one responsibility per file; sibling `types.ts` per domain folder; barrel `index.ts` per subpath.
- Tests import from `src/`, run against a real `MongoMemoryReplSet` shared per test file, and use unique per-test collection names (`` `c-${Date.now()}-${Math.random().toString(36).slice(2)}` ``).
- Commit after every green task: conventional commits, body ends with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- **Port sources (absolute paths, read them — do not reinvent):**
  - FM = `/Users/benja/projects/pactsheet/fm/backend/app/src/common` (branch `integration`, includes the 8829f95c hardening)
  - CAL = `/Users/benja/projects/calendar/.claude/worktrees/vibrant-thompson-344b45/apps/api/src` (calendar Phase-4 branch)
  - MQ = `/Users/benja/projects/mongo_queue` (scaffolding template only)
- Standard port adaptations, applied to every ported file (listed once here, not repeated per task):
  1. `import { Logger } from '@nestjs/common'` → `import type { Logger } from '../common/logger.js'`, logger received via options (default `consoleLogger`). `logger.verbose(...)` → `logger.debug(...)` (our Logger has no verbose).
  2. `BehaviorSubject<X>` (rxjs) → `ReadonlySignal<X>` / `ValueSignal<X>` from `src/common/signal.ts`; `.pipe(skip(1)).subscribe(fn)` → `subscribeToChanges(signal, fn)`.
  3. `isEqual` (lodash) → `deepEqual` from `src/common/deep-equal.ts`.
  4. mongoose types (`Model`, `Document`, `Connection`) → raw driver (`Collection`, `Document` from `mongodb`, `Db`).
  5. Relative ESM imports get explicit `.js` extensions (MQ convention).
  6. Keep fm's comments where the logic ports; rewrite them only where the surrounding code changed.

## File Structure

```
src/
  common/            logger.ts, signal.ts, deep-equal.ts, delay-unless-stopped.ts, types.ts
  core/              fanout.ts, change-stream-subscription.ts, change-stream-hub.ts,
                     enable-pre-post-images.ts, change-event.ts, change-doc-to-event.ts,
                     watch-collection.ts, merge-async-generators.ts, types.ts
  index.ts           (barrel: common + core)
  presence/          presence-store.ts, connection-registry.ts, watch-presence.ts, types.ts, index.ts
  socketio/          rooms.ts, push-scoped.ts, push-scoped-filtered.ts, push-owner-scoped.ts, types.ts, index.ts
  nestjs/            change-stream-event-pusher.ts, realtime-module.ts, preflight.ts, index.ts
tests/
  helpers/replset.ts            (shared MongoMemoryReplSet boot/teardown)
  common/  core/  presence/  socketio/  nestjs/   (mirror src/)
```

---

### Task 1: Repo scaffold

**Files:**
- Create: `package.json`, `tsconfig.json`, `tsup.config.ts`, `vitest.config.ts`, `eslint.config.mjs`, `.prettierrc.json`, `.gitignore`, `.github/workflows/ci.yml`, `src/index.ts`, `src/presence/index.ts`, `src/socketio/index.ts`, `src/nestjs/index.ts`, `tests/helpers/replset.ts`, `tests/helpers/replset.test.ts`

**Interfaces:**
- Produces: `startReplSet(): Promise<{ client: MongoClient; db: Db; uri: string }>` and `stopReplSet(): Promise<void>` from `tests/helpers/replset.ts` — every later test file uses these in `beforeAll`/`afterAll`.

- [ ] **Step 1: Write `package.json`**

```json
{
  "name": "mongodb-realtime",
  "version": "0.1.0",
  "description": "MongoDB change-stream realtime infrastructure: resumable subscriptions, per-key watch hubs, presence, and socket.io fan-out — multi-node without Redis",
  "type": "module",
  "license": "MIT",
  "engines": { "node": ">=20" },
  "main": "./dist/index.js",
  "module": "./dist/index.mjs",
  "types": "./dist/index.d.ts",
  "exports": {
    ".": { "types": "./dist/index.d.ts", "import": "./dist/index.mjs", "require": "./dist/index.js" },
    "./presence": { "types": "./dist/presence/index.d.ts", "import": "./dist/presence/index.mjs", "require": "./dist/presence/index.js" },
    "./socketio": { "types": "./dist/socketio/index.d.ts", "import": "./dist/socketio/index.mjs", "require": "./dist/socketio/index.js" },
    "./nestjs": { "types": "./dist/nestjs/index.d.ts", "import": "./dist/nestjs/index.mjs", "require": "./dist/nestjs/index.js" }
  },
  "files": ["dist"],
  "scripts": {
    "build": "tsup",
    "test": "vitest run",
    "test:watch": "vitest",
    "lint": "eslint .",
    "typecheck": "tsc --noEmit",
    "prepublishOnly": "pnpm build"
  },
  "peerDependencies": {
    "mongodb": "^5.7.0 || ^6.0.0 || ^7.0.0",
    "socket.io": "^4.6.0",
    "@nestjs/common": ">=10 <12"
  },
  "peerDependenciesMeta": {
    "socket.io": { "optional": true },
    "@nestjs/common": { "optional": true }
  }
}
```

Then: `pnpm add -D typescript tsup vitest mongodb mongodb-memory-server socket.io socket.io-client @nestjs/common eslint @eslint/js typescript-eslint prettier eslint-config-prettier reflect-metadata`

- [ ] **Step 2: Write build/test/lint config**

`tsconfig.json`:
```json
{
  "compilerOptions": {
    "target": "ES2022", "module": "NodeNext", "moduleResolution": "NodeNext",
    "strict": true, "declaration": true, "skipLibCheck": true, "noEmit": true,
    "forceConsistentCasingInFileNames": true, "isolatedModules": true,
    "types": ["node"]
  },
  "include": ["src", "tests", "tsup.config.ts", "vitest.config.ts"]
}
```
(add `pnpm add -D @types/node`)

`tsup.config.ts`:
```ts
import { defineConfig } from 'tsup';

export default defineConfig({
  entry: {
    index: 'src/index.ts',
    'presence/index': 'src/presence/index.ts',
    'socketio/index': 'src/socketio/index.ts',
    'nestjs/index': 'src/nestjs/index.ts',
  },
  format: ['cjs', 'esm'],
  dts: true,
  sourcemap: true,
  clean: true,
  target: 'node20',
  external: ['mongodb', 'socket.io', '@nestjs/common'],
});
```

`vitest.config.ts`:
```ts
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    testTimeout: 60_000,
    hookTimeout: 120_000, // replica-set boot on first run downloads a mongod binary
    pool: 'forks',        // isolate replica sets per test file
  },
});
```

`eslint.config.mjs`, `.prettierrc.json`: copy from `/Users/benja/projects/mongo_queue/eslint.config.mjs` and MQ's `package.json` `prettier` block (4-space, single quotes, 120 cols, `arrowParens: 'avoid'`) into `.prettierrc.json`; in the eslint config change nothing except the `ignores` to `['dist/**', 'docs/**']`.

`.gitignore`:
```
node_modules/
dist/
*.tsbuildinfo
```

`.github/workflows/ci.yml`:
```yaml
name: CI
on:
  push: { branches: [main] }
  pull_request: { branches: [main] }
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix: { node: [20, 22, 24] }
    steps:
      - uses: actions/checkout@v4
      - uses: pnpm/action-setup@v4
        with: { version: 10 }
      - uses: actions/setup-node@v4
        with: { node-version: '${{ matrix.node }}', cache: pnpm }
      - run: pnpm install --frozen-lockfile
      - run: pnpm lint
      - run: pnpm typecheck
      - run: pnpm test
      - run: pnpm build
```

Four empty barrels so tsup/tsc have entries: each of `src/index.ts`, `src/presence/index.ts`, `src/socketio/index.ts`, `src/nestjs/index.ts` contains only `export {};` for now (replaced as tasks land).

- [ ] **Step 3: Write the replica-set test helper and its failing smoke test**

`tests/helpers/replset.ts`:
```ts
import { MongoMemoryReplSet } from 'mongodb-memory-server';
import { MongoClient, type Db } from 'mongodb';

let replSet: MongoMemoryReplSet | undefined;
let client: MongoClient | undefined;

/** Boot (once per test file — vitest forks pool gives each file its own process). */
export async function startReplSet(): Promise<{ client: MongoClient; db: Db; uri: string }> {
    if (!replSet) {
        replSet = await MongoMemoryReplSet.create({ replSet: { count: 1, storageEngine: 'wiredTiger' } });
    }
    const uri = replSet.getUri();
    if (!client) {
        client = new MongoClient(uri);
        await client.connect();
    }
    return { client, db: client.db('test'), uri };
}

export async function stopReplSet(): Promise<void> {
    await client?.close();
    client = undefined;
    await replSet?.stop();
    replSet = undefined;
}

/** Unique collection name so tests sharing a db never collide. */
export function uniqueCollection(prefix: string): string {
    return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}
```

`tests/helpers/replset.test.ts`:
```ts
import { startReplSet, stopReplSet, uniqueCollection } from './replset.js';

describe('replset helper', () => {
    afterAll(() => stopReplSet());

    it('boots a replica set that supports change streams', async () => {
        const { db } = await startReplSet();
        const coll = db.collection(uniqueCollection('smoke'));
        const stream = coll.watch();
        const nextPromise = stream.next();
        await coll.insertOne({ hello: 'world' });
        const change = await nextPromise;
        expect(change.operationType).toBe('insert');
        await stream.close();
    });
});
```

- [ ] **Step 4: Run everything**

Run: `pnpm lint && pnpm typecheck && pnpm test && pnpm build`
Expected: all PASS (first test run downloads a mongod binary — slow once). `dist/` contains `index.{js,mjs,d.ts}` and the three subpath dirs.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "chore: scaffold package (tsup, vitest + MongoMemoryReplSet, eslint, CI)"
```

---

### Task 2: Common utilities — Logger, ValueSignal, deepEqual, delayUnlessStopped

**Files:**
- Create: `src/common/logger.ts`, `src/common/signal.ts`, `src/common/deep-equal.ts`, `src/common/delay-unless-stopped.ts`, `src/common/index.ts`; replace `src/index.ts`
- Test: `tests/common/signal.test.ts`, `tests/common/deep-equal.test.ts`, `tests/common/delay-unless-stopped.test.ts`

**Interfaces:**
- Produces (used by every later task):
  - `interface Logger { debug(msg: string): void; log(msg: string): void; warn(msg: string): void; error(msg: string, stack?: string): void }`; `consoleLogger: Logger`; `noopLogger: Logger`; `prefixed(base: Logger, prefix: string): Logger`
  - `interface ReadonlySignal<T> { getValue(): T; subscribe(fn: (value: T) => void): { unsubscribe(): void } }` — **subscribe emits the current value synchronously** (rxjs `BehaviorSubject` satisfies this structurally)
  - `interface Signal<T> extends ReadonlySignal<T> { next(value: T): void }`
  - `class ValueSignal<T> implements Signal<T>` — `constructor(initial: T)`
  - `subscribeToChanges<T>(signal: ReadonlySignal<T>, fn: (value: T) => void): { unsubscribe(): void }` — like subscribe but skips the initial synchronous emission (replaces rxjs `.pipe(skip(1))`)
  - `deepEqual(a: unknown, b: unknown): boolean` — structural equality for JSON-ish values (primitives, arrays, plain objects, Date; identity for everything else)
  - `delayUnlessStopped(ms: number, stop: Promise<unknown>): Promise<'elapsed' | 'stopped'>`

- [ ] **Step 1: Write failing tests**

`tests/common/signal.test.ts`:
```ts
import { ValueSignal, subscribeToChanges } from '../../src/common/signal.js';

describe('ValueSignal', () => {
    it('holds and returns the current value', () => {
        const s = new ValueSignal(1);
        expect(s.getValue()).toBe(1);
        s.next(2);
        expect(s.getValue()).toBe(2);
    });

    it('subscribe emits the current value synchronously, then updates', () => {
        const s = new ValueSignal('a');
        const seen: string[] = [];
        const sub = s.subscribe(v => seen.push(v));
        s.next('b');
        expect(seen).toEqual(['a', 'b']);
        sub.unsubscribe();
        s.next('c');
        expect(seen).toEqual(['a', 'b']);
    });

    it('subscribeToChanges skips the initial emission', () => {
        const s = new ValueSignal(0);
        const seen: number[] = [];
        subscribeToChanges(s, v => seen.push(v));
        expect(seen).toEqual([]);
        s.next(1);
        expect(seen).toEqual([1]);
    });

    it('a listener that unsubscribes during notification does not break other listeners', () => {
        const s = new ValueSignal(0);
        const seen: number[] = [];
        const sub = s.subscribe(() => sub.unsubscribe());
        s.subscribe(v => seen.push(v));
        s.next(1);
        expect(seen).toEqual([0, 1]);
    });
});
```

`tests/common/deep-equal.test.ts`:
```ts
import { deepEqual } from '../../src/common/deep-equal.js';

describe('deepEqual', () => {
    it.each([
        [1, 1, true], ['a', 'b', false], [null, null, true], [null, undefined, false],
        [[1, 2], [1, 2], true], [[1, 2], [2, 1], false],
        [{ a: 1, b: [2] }, { b: [2], a: 1 }, true], [{ a: 1 }, { a: 1, b: 2 }, false],
        [new Date(0), new Date(0), true], [new Date(0), new Date(1), false],
    ])('deepEqual(%j, %j) === %s', (a, b, expected) => {
        expect(deepEqual(a, b)).toBe(expected);
    });

    it('falls back to identity for class instances', () => {
        class X { constructor(public v: number) {} }
        const x = new X(1);
        expect(deepEqual(x, x)).toBe(true);
        expect(deepEqual(new X(1), new X(1))).toBe(false);
    });
});
```

`tests/common/delay-unless-stopped.test.ts`:
```ts
import { delayUnlessStopped } from '../../src/common/delay-unless-stopped.js';

describe('delayUnlessStopped', () => {
    it('resolves "elapsed" after the delay', async () => {
        await expect(delayUnlessStopped(10, new Promise(() => {}))).resolves.toBe('elapsed');
    });

    it('resolves "stopped" immediately when stop settles first', async () => {
        const start = Date.now();
        await expect(delayUnlessStopped(5_000, Promise.resolve())).resolves.toBe('stopped');
        expect(Date.now() - start).toBeLessThan(1_000);
    });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pnpm vitest run tests/common -r .`
Expected: FAIL — modules not found.

- [ ] **Step 3: Implement**

`src/common/logger.ts`:
```ts
/** Structural logger contract (same shape as mongodb-task-queue's). */
export interface Logger {
    debug(message: string): void;
    log(message: string): void;
    warn(message: string): void;
    error(message: string, stack?: string): void;
}

export const consoleLogger: Logger = {
    debug: () => undefined, // debug is opt-in; default logger stays quiet
    log: message => console.log(message),
    warn: message => console.warn(message),
    error: (message, stack) => console.error(stack ? `${message}\n${stack}` : message),
};

export const noopLogger: Logger = {
    debug: () => undefined,
    log: () => undefined,
    warn: () => undefined,
    error: () => undefined,
};

/** Wrap a logger so every message carries a `[prefix]` tag (replaces per-class nest Logger names). */
export function prefixed(base: Logger, prefix: string): Logger {
    const tag = (message: string) => `[${prefix}] ${message}`;
    return {
        debug: message => base.debug(tag(message)),
        log: message => base.log(tag(message)),
        warn: message => base.warn(tag(message)),
        error: (message, stack) => base.error(tag(message), stack),
    };
}
```

`src/common/signal.ts`:
```ts
export interface ReadonlySignal<T> {
    getValue(): T;
    /** Emits the current value synchronously on subscribe (BehaviorSubject-compatible). */
    subscribe(listener: (value: T) => void): { unsubscribe(): void };
}

export interface Signal<T> extends ReadonlySignal<T> {
    next(value: T): void;
}

/** Minimal BehaviorSubject replacement so the library has no rxjs dependency. */
export class ValueSignal<T> implements Signal<T> {
    private readonly listeners = new Set<(value: T) => void>();

    constructor(private value: T) {}

    getValue(): T {
        return this.value;
    }

    next(value: T): void {
        this.value = value;
        // Copy so a listener unsubscribing (or subscribing) mid-notification is safe.
        for (const listener of [...this.listeners]) {
            if (this.listeners.has(listener)) listener(value);
        }
    }

    subscribe(listener: (value: T) => void): { unsubscribe(): void } {
        this.listeners.add(listener);
        listener(this.value);
        return { unsubscribe: () => this.listeners.delete(listener) };
    }
}

/** Subscribe skipping the initial synchronous emission — replaces rxjs `.pipe(skip(1)).subscribe(fn)`. */
export function subscribeToChanges<T>(
    signal: ReadonlySignal<T>,
    listener: (value: T) => void,
): { unsubscribe(): void } {
    let first = true;
    return signal.subscribe(value => {
        if (first) {
            first = false;
            return;
        }
        listener(value);
    });
}
```

`src/common/deep-equal.ts`:
```ts
/** Structural equality for JSON-ish values; identity fallback for class instances. */
export function deepEqual(a: unknown, b: unknown): boolean {
    if (Object.is(a, b)) return true;
    if (a instanceof Date && b instanceof Date) return a.getTime() === b.getTime();
    if (Array.isArray(a) && Array.isArray(b)) {
        return a.length === b.length && a.every((item, i) => deepEqual(item, b[i]));
    }
    if (isPlainObject(a) && isPlainObject(b)) {
        const keysA = Object.keys(a);
        const keysB = Object.keys(b);
        return keysA.length === keysB.length && keysA.every(key => key in b && deepEqual(a[key], b[key]));
    }
    return false;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
    if (typeof value !== 'object' || value === null) return false;
    const proto: unknown = Object.getPrototypeOf(value);
    return proto === Object.prototype || proto === null;
}
```

`src/common/delay-unless-stopped.ts`: port `/Users/benja/projects/pactsheet/fm/backend/app/src/common/change-stream/delay-unless-stopped.ts` verbatim (it has no framework deps — timer `unref()`, early-resolve on stop, cleared timer).

`src/common/index.ts` re-exports all four modules; `src/index.ts` becomes `export * from './common/index.js';`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pnpm vitest run tests/common -r .` then `pnpm lint && pnpm typecheck`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(common): Logger, ValueSignal, deepEqual, delayUnlessStopped"
```

---

### Task 3: `Fanout<T>`

**Files:**
- Create: `src/core/fanout.ts`; extend `src/index.ts` (add `export * from './core/fanout.js';` — same pattern for every later core file, not repeated below)
- Test: `tests/core/fanout.test.ts`

**Interfaces:**
- Produces: `class Fanout<T>` — `constructor(options?: { maxQueue?: number; label?: string; logger?: Logger })` (default maxQueue 10_000), `push(item: T): void`, `close(): void`, `get size(): number`, `consumer(): AsyncGenerator<T>`.

- [ ] **Step 1: Port the class with failing tests first**

Port `/Users/benja/projects/pactsheet/fm/backend/app/src/common/change-stream/fanout.ts` applying the standard adaptations; change the constructor from positional `(maxQueue, label)` to the options object above. Logic is unchanged: per-consumer queue + pending resolve, drop-with-warning at maxQueue, `close()` completes all consumers, consumer `finally` self-deregisters.

Write `tests/core/fanout.test.ts` — port the cases from `/Users/benja/projects/pactsheet/fm/backend/app/src/common/change-stream/fanout.spec.ts` (adapt imports/vitest); ensure these cases exist (add any missing):
```ts
// - two consumers each receive every pushed item
// - items pushed while a consumer is not awaiting are buffered and delivered in order
// - pushing beyond maxQueue drops the overflow and logs one warning (inject a spy Logger)
// - close() terminates all consumer generators (for-await loops end)
// - a consumer that `break`s out of for-await is removed (size drops)
```

- [ ] **Step 2: Run tests to verify they fail** — `pnpm vitest run tests/core/fanout.test.ts -r .` → FAIL (module missing), then implement, then:

- [ ] **Step 3: Run tests to verify they pass** — same command → PASS; `pnpm lint && pnpm typecheck` → PASS.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(core): Fanout multi-consumer async queue"
```

---

### Task 4: `enablePrePostImages`

**Files:**
- Create: `src/core/enable-pre-post-images.ts`
- Test: `tests/core/enable-pre-post-images.test.ts`

**Interfaces:**
- Produces: `enablePrePostImages(db: Db, collectionNames: string[], logger?: Logger): Promise<void>` and `__resetPrePostImageCacheForTest(): void` (exported from the file, NOT from the barrel).

- [ ] **Step 1: Write failing tests**

```ts
import { startReplSet, stopReplSet, uniqueCollection } from '../helpers/replset.js';
import { enablePrePostImages, __resetPrePostImageCacheForTest } from '../../src/core/enable-pre-post-images.js';

describe('enablePrePostImages', () => {
    beforeEach(() => __resetPrePostImageCacheForTest());
    afterAll(() => stopReplSet());

    it('creates a missing collection and enables pre/post images', async () => {
        const { db } = await startReplSet();
        const name = uniqueCollection('ppi');
        await enablePrePostImages(db, [name]);
        const info = await db.listCollections({ name }).toArray();
        expect(info[0]?.options?.changeStreamPreAndPostImages).toEqual({ enabled: true });
    });

    it('is idempotent (second call is a cached no-op) and caches per db+collection', async () => {
        const { db } = await startReplSet();
        const name = uniqueCollection('ppi');
        await enablePrePostImages(db, [name]);
        await enablePrePostImages(db, [name]); // must not throw / re-run collMod
    });

    it('delete events carry fullDocumentBeforeChange once enabled', async () => {
        const { db } = await startReplSet();
        const name = uniqueCollection('ppi');
        await enablePrePostImages(db, [name]);
        const coll = db.collection(name);
        const { insertedId } = await coll.insertOne({ organizationId: 'org1' });
        const stream = coll.watch([], { fullDocumentBeforeChange: 'whenAvailable' });
        const nextPromise = stream.next();
        await coll.deleteOne({ _id: insertedId });
        const change = await nextPromise;
        expect(change.operationType).toBe('delete');
        expect((change as { fullDocumentBeforeChange?: { organizationId?: string } }).fullDocumentBeforeChange?.organizationId).toBe('org1');
        await stream.close();
    });
});
```

- [ ] **Step 2: Run to verify FAIL**, then port `/Users/benja/projects/pactsheet/fm/backend/app/src/common/change-stream/enable-pre-post-images.ts`: signature takes raw `Db` (drop the mongoose `Connection` unwrap and the `if (!db)` guard), cache key stays `${db.databaseName}::${name}`, create-collection-first, cache-only-on-success, never throws.

- [ ] **Step 3: Run to verify PASS**; `pnpm lint && pnpm typecheck`.

- [ ] **Step 4: Commit** — `git add -A && git commit -m "feat(core): enablePrePostImages collMod helper"`

---

### Task 5: `ChangeEvent` + `changeDocToEvent` with configurable conventions

**Files:**
- Create: `src/core/change-event.ts`, `src/core/change-doc-to-event.ts`
- Test: `tests/core/change-doc-to-event.test.ts`

**Interfaces:**
- Produces:
```ts
export interface EntityChanged<TTenant = unknown> { type: 'changed'; scopeId: string; data: unknown; tenant?: TTenant }
export interface EntityRemoved<TTenant = unknown> { type: 'removed'; scopeId?: string; id: string; ownerId?: string; tenant?: TTenant }
export type ChangeEvent<TTenant = unknown> = EntityChanged<TTenant> | EntityRemoved<TTenant>;
export interface ChangeEventConventions { scopeField?: string; softDeleteField?: string; ownerField?: string }
export const DEFAULT_CONVENTIONS: Required<ChangeEventConventions>; // organizationId / deleted / userId
export function changeDocToEvent(change: ChangeStreamDocument, conventions?: ChangeEventConventions): ChangeEvent | null;
```

- [ ] **Step 1: Write failing tests** — feed hand-built change docs (plain objects cast to `ChangeStreamDocument`), no Mongo needed:

```ts
// - insert/update with fullDocument.organizationId → { type: 'changed', scopeId, data: fullDocument }
// - update with fullDocument.deleted === true → { type: 'removed', id, scopeId, ownerId? } (soft delete)
// - operationType 'delete' with fullDocumentBeforeChange → removed with scopeId+ownerId from the pre-image
// - operationType 'delete' without pre-image → removed with scopeId undefined
// - changed doc with NO scope field → null (skipped)
// - update with no fullDocument → null
// - custom conventions { scopeField: 'tenantId', softDeleteField: 'archived', ownerField: 'ownerId' } are honored
```
Write each as a concrete `it(...)` with an inline change-doc object and exact `toEqual` assertion (values are strings like `'org1'`, `'u1'`; `_id`/ids as objects with a `toString()` — assert the stringified form).

- [ ] **Step 2: FAIL**, then implement by porting `/Users/benja/projects/pactsheet/fm/backend/app/src/common/change-stream/change-doc-to-event.ts`, replacing the hard-coded `organizationId`/`deleted`/`userId` reads with `conventions` lookups (`doc[conv.scopeField]` etc., each `?.toString()`), and renaming the event fields per the Interfaces block (`organizationId` → `scopeId`, `userId` → `ownerId`).

- [ ] **Step 3: PASS**; lint+typecheck. **Step 4: Commit** — `git add -A && git commit -m "feat(core): canonical ChangeEvent with configurable field conventions"`

---

### Task 6: `ChangeStreamSubscription`

**Files:**
- Create: `src/core/change-stream-subscription.ts` (includes `isFatalChangeStreamError` / `isUnresumableChangeStreamError`)
- Test: `tests/core/change-stream-subscription.test.ts`

**Interfaces:**
- Consumes: `ReadonlySignal`, `ValueSignal`, `subscribeToChanges`, `deepEqual`, `delayUnlessStopped`, `Logger`, `prefixed`.
- Produces:
```ts
export type ResumeOptions = ChangeStreamOptions; // driver ChangeStreamOptions minus writeConcern
export interface ChangeStreamSubscriptionOptions<TScope> {
    name?: string; logger?: Logger;
    initialBackoffMs?: number; maxBackoffMs?: number;   // defaults 1_000 / 30_000
    isEqual?: (a: TScope[], b: TScope[]) => boolean;    // default deepEqual
}
export class ChangeStreamSubscription<TScope, TChange extends ChangeStreamDocument<Document>>
    implements AsyncIterable<TChange> {
    constructor(
        scope: ReadonlySignal<TScope[]>,
        shutdown: ReadonlySignal<boolean>,
        factory: (scope: TScope[], options: ResumeOptions) => ChangeStream<Document, TChange>,
        options?: ChangeStreamSubscriptionOptions<TScope>,
    );
}
export function isFatalChangeStreamError(error: unknown): boolean;
export function isUnresumableChangeStreamError(error: unknown): boolean;
```

- [ ] **Step 1: Port the implementation**

Source: `/Users/benja/projects/pactsheet/fm/backend/app/src/common/change-stream/change-stream-subscription.ts` (437 lines, post-8829f95c — read it fully first). Beyond the standard adaptations:

1. **Generics collapse from 3 to 2.** fm's `<T, T2 extends MongooseDocument, T3 extends ChangeStreamInsertDocument<T2> | ...>` becomes `<TScope, TChange extends ChangeStreamDocument<Document>>` with `Document` from `mongodb`. The factory type becomes the `ResumeOptions` signature above. Delete the mongoose import entirely.
2. Constructor takes the single `options` object (name folded in) per the Interfaces block.
3. `subscribeToNewData`/`subscribeToShutdown`: replace `.pipe(skip(1)).subscribe(...)` with `subscribeToChanges(...)`; the returned handle already has `unsubscribe()`.
4. `hasDataChanged` uses `this.options.isEqual ?? deepEqual`.
5. Everything else ports as-is **including all comments**: empty-scope park branch (with its do-not-reintroduce-preloop-await warning), resume ladder (`startAfter` → `startAtOperationTime` → fresh), `captureOperationTime` via `hello` + `resolveDb` (`instanceof` checks against driver `Db`/`Collection`/`MongoClient` value imports), fatal rethrow, unresumable → drop resume point + fresh reopen, transient → capped exponential backoff via `delayUnlessStopped` OUTSIDE the finally, backoff reset on successful yield, `safeClose`.

- [ ] **Step 2: Write the test suite**

Port applicable cases from `/Users/benja/projects/pactsheet/fm/backend/app/src/common/change-stream/change-stream-subscription.spec.ts` (620 lines — mix of fake-factory unit tests and the 8829f95c hardening cases). Two groups:

*Unit (fake factory, no Mongo):* build a `FakeChangeStream` (`next()` resolves from a controllable queue or rejects on demand; `close()` records; `resumeToken` settable) and cover, at minimum:
```ts
// - yields events from the stream; resumeToken carried across an error-triggered reopen ({ startAfter })
// - scope change closes + reopens the stream, factory called with the new scope
// - scope change to [] parks (factory NOT called again) until scope refills
// - identical scope re-emission (deepEqual) does NOT restart the stream
// - shutdown while parked, while awaiting next(), and during backoff each end iteration promptly
// - fatal error (code 40573) rethrows out of the iterator after closing the stream
// - unresumable error (code 286) drops the token: next factory call gets {} (no startAfter/startAtOperationTime)
// - transient errors back off 1s→2s→…; use options { initialBackoffMs: 5, maxBackoffMs: 20 } to keep tests fast
// - error before any event with a captured operationTime → factory called with { startAtOperationTime }
```
*Integration (replica set):* one test — open on a real collection via `coll.watch` factory, insert 3 docs, receive 3 events, `shutdown.next(true)`, iterator completes.

- [ ] **Step 3: Run to verify green** — `pnpm vitest run tests/core/change-stream-subscription.test.ts -r .` (write tests first where practical; the port is a unit, so red→green happens per test case as you fill the file). Then `pnpm lint && pnpm typecheck`.

- [ ] **Step 4: Commit** — `git add -A && git commit -m "feat(core): resumable scope-driven ChangeStreamSubscription"`

---

### Task 7: `ChangeStreamHub`

**Files:**
- Create: `src/core/change-stream-hub.ts`
- Test: `tests/core/change-stream-hub.test.ts`

**Interfaces:**
- Consumes: `ChangeStreamSubscription`, `Fanout`, `ValueSignal`, `subscribeToChanges`, `Logger`.
- Produces:
```ts
export interface ChangeStreamHubOptions {
    connectedKeys?: ReadonlySignal<string[]>; // omit → every stream always-open
    graceMs?: number;                          // default 45_000
    ignoreCollections?: string[];              // default []
    maxQueue?: number;                         // per-collection Fanout buffer, default 10_000
    logger?: Logger;
}
export class ChangeStreamHub {
    constructor(options?: ChangeStreamHubOptions);
    subscribe(key: string, db: Db, collectionName: string): AsyncGenerator<ChangeStreamDocument>;
    close(): Promise<void>; // replaces fm's onApplicationShutdown
}
```

- [ ] **Step 1: Port**

Source: `/Users/benja/projects/pactsheet/fm/backend/app/src/common/change-stream/tenant-change-stream-hub.ts` (read fully). Beyond standard adaptations:
1. Drop `@Injectable`/`OnApplicationShutdown`; `close()` carries the shutdown body (clear grace timers, `shutdown.next(true)` per stream, close fanouts, `await Promise.allSettled(readers)`).
2. Replace the `OnlineUserService` constructor dep: subscribe to `options.connectedKeys` when provided (`subscribeToChanges` + initial `getValue()` seed); when absent, `reconcile()` treats every key as connected (streams always open — `ensureStream` seeds `scope$` with `[1]` unconditionally).
3. `IGNORE_COLLECTIONS` const → `new Set(options.ignoreCollections ?? [])`.
4. Keep: lazy `ensureStream` keyed by `key`, DB-level `db.watch` with the 4-op `$match` + `fullDocument: 'updateLookup'` + `fullDocumentBeforeChange: 'whenAvailable'`, demux by `ns.coll` into per-collection `Fanout`s, grace-timer parking with `unref()`, `evictIfCrashed` self-heal, all comments.

- [ ] **Step 2: Write tests (replica set)**

```ts
// - two collections on one key: each subscriber receives only its own collection's changes (demux)
// - two subscribers on the same collection both receive each change (fanout)
// - ignoreCollections: changes there are never delivered
// - with connectedKeys signal: key absent → insert delivered only after key connects (parked stream opens);
//   use graceMs: 50 and verify disconnect+reconnect within grace does NOT lose events, while
//   disconnect past grace parks (no cursor) and a reconnect resumes delivery
// - close() ends all consumer generators
```
For the parking assertions, observe delivery (or its absence within a bounded wait) rather than internal state; a 300 ms `expectNoEvent(gen)` helper (`Promise.race([gen.next(), delay(300).then(() => 'timeout')])`) is sufficient and used in fm's hub spec (`tenant-change-stream-hub.spec.ts`) — port its structure.

- [ ] **Step 3: Green + lint + typecheck.**  **Step 4: Commit** — `git add -A && git commit -m "feat(core): per-key DB-level ChangeStreamHub with connection-driven parking"`

---

### Task 8: `mergeAsyncGenerators`, `tagTenant`, `watchCollection`

**Files:**
- Create: `src/core/merge-async-generators.ts`, `src/core/watch-collection.ts`
- Test: `tests/core/merge-async-generators.test.ts`, `tests/core/watch-collection.test.ts`

**Interfaces:**
- Consumes: everything from Tasks 3–7.
- Produces:
```ts
export function mergeAsyncGenerators<T>(sources: AsyncGenerator<T>[]): AsyncGenerator<T>;
export function tagTenant<TTenant>(source: AsyncGenerator<ChangeEvent<TTenant>>, tenant: TTenant | undefined): AsyncGenerator<ChangeEvent<TTenant>>;
export type WatchSource =
    | { hub: ChangeStreamHub; key: string; db: Db; collection: string }
    | { watch: (options: ResumeOptions) => ChangeStream<Document, ChangeStreamDocument<Document>>; db: Db; collection: string };
export interface WatchCollectionOptions<TTenant> {
    source: WatchSource;
    shutdown: ReadonlySignal<boolean>;
    conventions?: ChangeEventConventions;
    tenant?: TTenant;
    name?: string; logger?: Logger;
    initialBackoffMs?: number; maxBackoffMs?: number; // hub-consumer reconnect backoff, defaults 1_000 / 30_000
}
export function watchCollection<TTenant = unknown>(options: WatchCollectionOptions<TTenant>): AsyncGenerator<ChangeEvent<TTenant>>;
```

- [ ] **Step 1: Port `mergeAsyncGenerators` + write its tests**

Source: the `mergeAsyncGenerators` function inside `/Users/benja/projects/pactsheet/fm/backend/app/src/common/change-stream/watch-entity-changes-all-tenants.ts` — ports verbatim (no framework deps). Tests (pure, no Mongo): items from N sources interleave as they resolve; each source at most one item ahead (bounded pull); a source completing removes it; consumer `break` calls `.return()` on all sources; a source rejection propagates.

- [ ] **Step 2: Implement `watchCollection` + `tagTenant`**

Compose from the same fm file's logic, restructured (read `watchEntityChanges` in `watch-entity-changes.ts` and `adaptCollection` in `watch-entity-changes-all-tenants.ts` first):

```ts
export async function* watchCollection<TTenant = unknown>(
    options: WatchCollectionOptions<TTenant>,
): AsyncGenerator<ChangeEvent<TTenant>> {
    const log = prefixed(options.logger ?? consoleLogger, options.name ?? `watch:${options.source.collection}`);
    await enablePrePostImages(options.source.db, [options.source.collection], log);
    const inner = 'hub' in options.source
        ? hubConsumerLoop(options.source, options, log)   // fm adaptCollection: shutdown race, done→resubscribe
                                                          // with capped backoff, unawaited .return() detach
        : directLoop(options.source, options, log);       // fm watchEntityChanges: ChangeStreamSubscription with
                                                          // a constant non-empty ValueSignal([1]) scope
    for await (const change of inner) {
        const event = changeDocToEvent(change, options.conventions);
        if (event) yield options.tenant === undefined ? event : { ...event, tenant: options.tenant };
    }
}
```
`directLoop` builds the subscription factory as fm's `watchEntityChanges` does: caller's `watch(options)` merged with the 4-op `$match` pipeline is the CALLER's job (document this in the jsdoc: "your `watch` factory must spread the passed resume options into the driver call and should set `fullDocument: 'updateLookup'`, `fullDocumentBeforeChange: 'whenAvailable'`"); provide and export a helper that builds a correct factory from a `Collection`:
```ts
export function collectionWatchFactory(coll: Collection): (options: ResumeOptions) => ChangeStream<Document, ChangeStreamDocument<Document>> {
    return options =>
        coll.watch(
            [{ $match: { operationType: { $in: ['insert', 'update', 'replace', 'delete'] } } }],
            { fullDocument: 'updateLookup', fullDocumentBeforeChange: 'whenAvailable', ...options },
        ) as ChangeStream<Document, ChangeStreamDocument<Document>>;
}
```
`hubConsumerLoop` ports fm's `adaptCollection` exactly (whenShutdown latch, `Promise.race` next-vs-stop, `done` + not-shutdown → resubscribe after `delayUnlessStopped(backoff)`, backoff reset only after a real event, final unawaited `consumer.return()` — keep fm's deadlock-explaining comments).

- [ ] **Step 3: `watchCollection` tests (replica set)**

```ts
// direct mode: insert → 'changed' event with scopeId; set deleted:true → 'removed'; hard delete → 'removed'
//   with scopeId recovered from pre-image; doc without organizationId → no event
// hub mode: same assertions through a ChangeStreamHub
// tenant tagging: options.tenant = { dbName: 'x' } appears on every yielded event
// multi-tenant merge: two dbs, two watchCollection generators tagged differently, merged via
//   mergeAsyncGenerators — events from both arrive with correct tags
// shutdown: shutdown.next(true) ends the generator in both modes
```

- [ ] **Step 4: Green + lint + typecheck.**  **Step 5: Commit** — `git add -A && git commit -m "feat(core): watchCollection composition + mergeAsyncGenerators"`

---

### Task 9: `PresenceStore`

**Files:**
- Create: `src/presence/types.ts`, `src/presence/presence-store.ts`, `src/presence/index.ts` (barrel, grows over Tasks 10–11)
- Test: `tests/presence/presence-store.test.ts`

**Interfaces:**
- Produces:
```ts
export interface PresenceDoc {
    userId: string; clientId: string; serverId: string;
    scopeId: string; key: string;             // key = tenant key, '__default__' when single-DB
    lastSeen: Date; page?: string; meta?: Record<string, unknown>;
}
export interface PresenceStoreOptions {
    db: Db; serverId: string;
    collectionName?: string;                   // default 'onlineUsers'
    ttlSeconds?: number;                       // default 300
    logger?: Logger;
}
export class PresenceStore {
    constructor(options: PresenceStoreOptions);
    readonly serverId: string;
    readonly collection: Collection<PresenceDoc>;
    ensureIndexes(): Promise<void>;            // unique {userId, clientId, serverId}; TTL lastSeen; pre/post images
    checkIn(entry: { userId: string; clientId: string; scopeId: string; key?: string; meta?: Record<string, unknown> }): Promise<{ isNewConnection: boolean }>;
    keepAlive(userId: string, clientId: string): Promise<void>;
    setPage(userId: string, clientId: string, page: string): Promise<void>;
    checkOut(userId: string, clientId: string): Promise<void>;
    clearServer(): Promise<void>;              // delete all rows with this serverId (orderly shutdown)
    countUserRows(userId: string): Promise<number>;
}
```

- [ ] **Step 1: Write failing tests**

```ts
// - ensureIndexes creates the unique index and a TTL index on lastSeen with expireAfterSeconds
//   (read db.collection(...).indexes() and assert) and enables pre/post images
// - checkIn first call → { isNewConnection: true }; same (userId, clientId) again → { isNewConnection: false }
// - 10 concurrent checkIns for the same (userId, clientId) → exactly ONE isNewConnection: true
//   (Promise.all over 10 calls; count the trues)
// - checkIn stamps userId/clientId/serverId/scopeId/key/lastSeen on the row; key defaults '__default__'
// - keepAlive advances lastSeen (compare Dates before/after)
// - checkOut removes exactly that (userId, clientId, serverId) row — a second client's row survives
// - clearServer removes only this serverId's rows (seed a row with a different serverId directly)
// - countUserRows counts rows across servers for one user
```

- [ ] **Step 2: FAIL**, then implement. `checkIn` is fm's audit-fixed atomic pattern (`/Users/benja/projects/pactsheet/fm/backend/app/src/common/online-user.service.ts` `checkIn`, DB parts only — the in-memory parts land in Task 10):

```ts
async checkIn(entry): Promise<{ isNewConnection: boolean }> {
    // The DB arbitrates new-vs-rejoin: with upsert + returnDocument 'before', exactly one of any
    // set of concurrent racers for the same key observes `null` (fm audit-2026-05-21 B1d).
    const before = await this.collection.findOneAndUpdate(
        { userId: entry.userId, clientId: entry.clientId, serverId: this.serverId },
        {
            $set: {
                userId: entry.userId,
                clientId: entry.clientId,
                serverId: this.serverId,
                scopeId: entry.scopeId,
                key: entry.key ?? '__default__',
                meta: entry.meta ?? {},
                lastSeen: new Date(),
            },
        },
        { upsert: true, returnDocument: 'before' },
    );
    return { isNewConnection: before === null };
}
```
`ensureIndexes`: `createIndex({ userId: 1, clientId: 1, serverId: 1 }, { unique: true })`, `createIndex({ lastSeen: 1 }, { expireAfterSeconds: this.ttlSeconds })`, then `enablePrePostImages(this.db, [this.collectionName], this.logger)`. `keepAlive`/`setPage`: `updateOne` on the exact triple `$set: { lastSeen: new Date() }` / `{ page }`. `checkOut`: `deleteOne` on the triple. `clearServer`: `deleteMany({ serverId: this.serverId })`. `countUserRows`: `countDocuments({ userId })`.

- [ ] **Step 3: Green + lint + typecheck.**  **Step 4: Commit** — `git add -A && git commit -m "feat(presence): PresenceStore with atomic check-in and TTL"`

---

### Task 10: `ConnectionRegistry`

**Files:**
- Create: `src/presence/connection-registry.ts`
- Test: `tests/presence/connection-registry.test.ts`

**Interfaces:**
- Produces:
```ts
export interface ConnectionEntry { clientId: string; userId: string; scopeId: string; key: string; meta?: unknown }
export class ConnectionRegistry {
    readonly connectedUsers: ReadonlySignal<ConnectionEntry[]>;
    readonly connectedScopes: ReadonlySignal<string[]>;  // distinct scopeIds, sorted (fm orgsOnServer)
    readonly connectedKeys: ReadonlySignal<string[]>;    // distinct keys, sorted (fm connectedTenants$)
    add(entry: { clientId: string; userId: string; scopeId: string; key?: string; meta?: unknown }): void; // key defaults '__default__'
    remove(clientId: string): ConnectionEntry | undefined;
    byScope(scopeId: string): ConnectionEntry[];
    byClientId(clientId: string): ConnectionEntry | undefined;
}
```

- [ ] **Step 1: Write failing tests**

```ts
// - add exposes the entry via connectedUsers/byScope/byClientId; key defaults '__default__'
// - add with an existing clientId REPLACES that entry (no duplicates)
// - remove returns the removed entry and updates all three signals; unknown clientId → undefined, no emission
// - connectedScopes/connectedKeys are distinct + sorted
// - derived signals emit ONLY on real set changes: adding a second user in the same scope does not
//   re-emit connectedScopes (count emissions via subscribeToChanges)
// - updates are copy-on-write: the array captured from an earlier emission is not mutated by later adds
```

- [ ] **Step 2: FAIL**, then implement — port the in-memory logic of fm's `online-user.service.ts` (`_userOnServer`, `recomputeOrgs`, `recomputeTenants`, `socketCheckout`) as three `ValueSignal`s, with the sorted-compare dedup fm already uses and strictly copy-on-write list updates (never `push`/`splice` on the current value — old pactsheet's in-place-mutation defect).

- [ ] **Step 3: Green + lint + typecheck.**  **Step 4: Commit** — `git add -A && git commit -m "feat(presence): ConnectionRegistry with connectedScopes/connectedKeys signals"`

---

### Task 11: `watchPresence`

**Files:**
- Create: `src/presence/watch-presence.ts`
- Test: `tests/presence/watch-presence.test.ts`

**Interfaces:**
- Consumes: `PresenceStore` (`collection`, `serverId`, `countUserRows`), `watchCollection`'s building blocks (`ChangeStreamSubscription`, `collectionWatchFactory`), `ReadonlySignal`.
- Produces:
```ts
export interface PresenceEvent { type: 'online' | 'offline'; userId: string; scopeId: string; key: string; ownedByThisServer: boolean }
export function watchPresence(options: { store: PresenceStore; shutdown: ReadonlySignal<boolean>; logger?: Logger }): AsyncGenerator<PresenceEvent>;
```

- [ ] **Step 1: Write failing tests (replica set)**

```ts
// - first checkIn for a user yields { type: 'online', ownedByThisServer: true } (0→1 transition)
// - a second checkIn for the SAME user (different clientId) yields nothing (already online)
// - checkOut of one of two rows yields nothing; checkOut of the last row yields
//   { type: 'offline', scopeId, ownedByThisServer: true } (1→0 transition, fields from the pre-image)
// - a row inserted by ANOTHER store (different serverId) still yields online here, with
//   ownedByThisServer: false — the cross-node presence property
// - keepAlive updates yield nothing
// - shutdown ends the generator
```
Drive with two `PresenceStore`s (different `serverId`) on the same db + collection name. Collect events by running the generator in a background task pushing into an array; assert with a small `waitFor(() => arr.length === n)` polling helper (define it in the test file).

- [ ] **Step 2: FAIL**, then implement**

Watch the store's collection via `ChangeStreamSubscription` (constant `[1]` scope, `collectionWatchFactory(store.collection)`); for each change:
- `insert` → if `await store.countUserRows(userId) === 1` yield `online` (fields from `fullDocument`).
- `delete` → read `fullDocumentBeforeChange`; skip if absent (warn); if `await store.countUserRows(userId) === 0` yield `offline`; `ownedByThisServer = before.serverId === store.serverId`.
- `update`/`replace` (keepalive/page) → no yield.
(Transition detection = calendar's `online-user.pusher.ts` refinement; count-after-event is safe because change streams are ordered per document key.)

- [ ] **Step 3: Green + lint + typecheck.**  **Step 4: Commit** — `git add -A && git commit -m "feat(presence): watchPresence with 0/1 transition detection and server ownership"`

---

### Task 12: socketio — rooms + `pushScoped` + `pushOwnerScoped`

**Files:**
- Create: `src/socketio/rooms.ts`, `src/socketio/types.ts`, `src/socketio/push-scoped.ts`, `src/socketio/push-owner-scoped.ts`, `src/socketio/index.ts`
- Test: `tests/socketio/rooms.test.ts`, `tests/socketio/push-scoped.test.ts`, `tests/socketio/push-owner-scoped.test.ts`

**Interfaces:**
- Consumes: `ChangeEvent`, `Logger`, `ReadonlySignal`. `socket.io` types via `import type { Server, Socket, RemoteSocket } from 'socket.io'` ONLY.
- Produces:
```ts
export function userRoom(id: string): string;                  // `user:${id}`
export function scopeRoom(id: string): string;                 // `org:${id}`
export function namespacedRoom(ns: string | undefined, room: string): string; // ns ? `${ns}::${room}` : room
export interface PushEvents { changed: string; removed?: string }
export interface PushScopedOptions<TTenant> {
    shutdown: ReadonlySignal<boolean>;
    roomOf?: (event: ChangeEvent<TTenant>) => string;          // default: namespacedRoom(tenantNamespace?.(tenant), scopeRoom(scopeId))
    tenantNamespace?: (tenant: TTenant) => string | undefined; // how to derive the room namespace from the opaque tenant
    mapData?: (data: unknown) => unknown | Promise<unknown>;
    logger?: Logger; name?: string;
}
export function pushScoped<TTenant>(server: Server, watcher: AsyncGenerator<ChangeEvent<TTenant>>, events: PushEvents, options: PushScopedOptions<TTenant>): Promise<void>;
export interface PushOwnerScopedOptions<TTenant> extends Omit<PushScopedOptions<TTenant>, 'roomOf'> {
    ownerIdOf: (data: unknown) => string | null | undefined;   // changed events; removed events use event.ownerId
}
export function pushOwnerScoped<TTenant>(server: Server, watcher: AsyncGenerator<ChangeEvent<TTenant>>, events: PushEvents, options: PushOwnerScopedOptions<TTenant>): Promise<void>;
```

- [ ] **Step 1: rooms — failing test then trivial implementation** (three pure functions above; test each mapping incl. `namespacedRoom(undefined, r) === r`). 

- [ ] **Step 2: `pushScoped` — write failing tests**

Test harness (put in `tests/socketio/harness.ts`, reused by Task 13): boot a real `Server` on an ephemeral port (`new Server(httpServer)`), connect `socket.io-client` clients, have the server join them to rooms on a `join` message. Feed the pusher a hand-made `async function*` watcher (no Mongo needed).
```ts
// - changed event → payload emitted to scopeRoom(scopeId); a client in another scope's room receives nothing
// - tenantNamespace: tenant-tagged event lands in the namespaced room only
// - removed event → bare id emitted to the scope room under events.removed
// - removed event WITHOUT scopeId → dropped (no emission to anyone) + warning logged
// - mapData transforms the payload; a THROWING mapData skips that event, loop continues with the next
// - watcher completing while shutdown.getValue() === false logs the operator-visible error;
//   completing after shutdown.next(true) does not
```

- [ ] **Step 3: Implement `pushScoped`** — port fm's `pushOrgScoped` loop body (`change-stream-event-pusher.ts`) as a free function: `for await` over the watcher; removed → drop-if-unroutable warn else `server.to(room).emit(events.removed, event.id)`; changed → guarded `await mapData` (catch → log + continue) then emit; after the loop, the not-shutdown error log. No latch here — lifecycle latching is the NestJS base class's job (Task 14).

- [ ] **Step 4: `pushOwnerScoped` — failing tests then implementation** — port fm's `pushUserScoped`: changed → `ownerIdOf(data)`, missing → warn + skip, else emit to `userRoom(ownerId)` (namespaced via `tenantNamespace`); removed → `event.ownerId`, missing → warn + drop (never broadcast), else bare id to the owner's room; same guarded-mapData semantics. Tests mirror pushScoped's, plus: removed with no ownerId is dropped.

- [ ] **Step 5: Green + lint + typecheck.**  **Step 6: Commit** — `git add -A && git commit -m "feat(socketio): rooms + pushScoped/pushOwnerScoped fan-out loops"`

---

### Task 13: socketio — `pushScopedFiltered`

**Files:**
- Create: `src/socketio/push-scoped-filtered.ts`
- Test: `tests/socketio/push-scoped-filtered.test.ts`

**Interfaces:**
- Produces:
```ts
export interface FanoutVariants { render: (data: unknown) => { full: unknown; redacted: unknown }; pick: (socket: RemoteSocket<never, unknown>) => 'full' | 'redacted' }
export interface PushScopedFilteredOptions<TTenant> extends PushScopedOptions<TTenant> {
    canReceive: (socket: RemoteSocket<never, unknown>, data: unknown, prepared?: unknown) => boolean | Promise<boolean>;
    prepareEvent?: (data: unknown) => unknown | Promise<unknown>;
    variants?: FanoutVariants;                 // mutually exclusive with mapData — throws at call time if both
    runInContext?: <R>(ctx: { tenant?: unknown }, fn: () => Promise<R>) => Promise<R>; // generalizes fm's CLS seam
    tenantOf?: (socket: RemoteSocket<never, unknown>) => unknown; // socket's tenant for per-recipient context
}
export function pushScopedFiltered<TTenant>(server: Server, watcher: AsyncGenerator<ChangeEvent<TTenant>>, events: PushEvents, options: PushScopedFilteredOptions<TTenant>): Promise<void>;
```

- [ ] **Step 1: Write failing tests** (reuse `tests/socketio/harness.ts`; attach `socket.data` server-side on join so `canReceive`/`pick`/`tenantOf` can read it):

```ts
// - only sockets whose canReceive returns true receive the changed payload
// - a THROWING canReceive skips that recipient only (others still receive) + warning
// - prepareEvent runs once per event; its result is passed to every canReceive (spy call counts)
// - removed events BYPASS filtering: bare id reaches every socket in the scope room
// - variants: render called once; each passing socket gets full or redacted per pick; passing
//   both mapData and variants throws synchronously
// - runInContext wraps the once-per-event block with the EVENT's tenant and each canReceive with
//   the SOCKET's tenant (record ctx.tenant values seen by a spy)
```

- [ ] **Step 2: Implement** — port fm's `pushOrgScopedFiltered` body: mutual-exclusion assert up front; removed → bare id to room (bypass); changed → `fetchSockets()` on the room, once-per-event block (`variants.render` XOR `mapData`, then `prepareEvent`) inside `runInContext({ tenant: event.tenant })` when provided; per-socket `canReceive` inside `runInContext({ tenant: tenantOf?.(socket) })` when both provided, `Promise.all` over sockets, per-socket try/catch → warn + skip. Keep fm's comments about why `variants.pick` deliberately can only choose between two pre-rendered values.

- [ ] **Step 3: Green + lint + typecheck.**  **Step 4: Commit** — `git add -A && git commit -m "feat(socketio): permission-filtered fan-out with two-variant PII rendering"`

---

### Task 14: NestJS subpath — `ChangeStreamEventPusher`, preflight, `RealtimeModule`

**Files:**
- Create: `src/nestjs/change-stream-event-pusher.ts`, `src/nestjs/preflight.ts`, `src/nestjs/realtime-module.ts`, `src/nestjs/index.ts`
- Test: `tests/nestjs/change-stream-event-pusher.test.ts`, `tests/nestjs/preflight.test.ts`, `tests/nestjs/realtime-module.test.ts`

**Interfaces:**
- Consumes: core + presence. `@nestjs/common` via `import type { DynamicModule, OnApplicationBootstrap, OnApplicationShutdown } from '@nestjs/common'` ONLY (no decorators — MQ's `src/nestjs/index.ts` is the pattern; read it).
- Produces:
```ts
export abstract class ChangeStreamEventPusher implements OnApplicationBootstrap, OnApplicationShutdown {
    protected readonly shutdown: ValueSignal<boolean>;
    protected readonly logger: Logger;                    // set via constructor(options?: { logger?: Logger })
    protected abstract pushChangeEvents(): Promise<void>;
    protected loopEnded(): void;                          // resolves the shutdown latch (call in finally of each loop)
    onApplicationBootstrap(): void;
    onApplicationShutdown(): Promise<void>;
}
export interface PreflightOptions { collections?: string[]; requireChangeStreams?: boolean; logger?: Logger }
export function assertChangeStreamPreflight(db: Db, options?: PreflightOptions): Promise<void>;
export const CHANGE_STREAM_HUB = 'MONGODB_REALTIME_HUB';
export const PRESENCE_STORE = 'MONGODB_REALTIME_PRESENCE_STORE';
export const CONNECTION_REGISTRY = 'MONGODB_REALTIME_CONNECTION_REGISTRY';
export const SERVER_ID = 'MONGODB_REALTIME_SERVER_ID';
export interface RealtimeModuleOptions { db: Db; presence?: Omit<PresenceStoreOptions, 'db' | 'serverId'>; hub?: ChangeStreamHubOptions; wireHubParking?: boolean /* default true: hub.connectedKeys = registry.connectedKeys */ }
export class RealtimeModule {
    static forRoot(options: RealtimeModuleOptions): DynamicModule;
    static forRootAsync(options: { imports?: unknown[]; inject?: unknown[]; useFactory: (...args: unknown[]) => RealtimeModuleOptions | Promise<RealtimeModuleOptions> }): DynamicModule;
}
```

- [ ] **Step 1: `ChangeStreamEventPusher`** — failing tests: bootstrap runs `pushChangeEvents`; a rejecting `pushChangeEvents` logs the error AND `onApplicationShutdown` still resolves (the crash latch — fm's load-bearing `.catch`); orderly path: shutdown signals the ValueSignal and awaits until `loopEnded()` is called. Implement by porting fm's `change-stream-event-pusher.ts` lifecycle part only (the push loops moved to `socketio/` in Tasks 12–13); `shutdownResolve` becomes the `loopEnded()` method. Test with plain `new (class extends ChangeStreamEventPusher …)` — no Nest test harness needed since the class only implements the lifecycle interfaces.

- [ ] **Step 2: `assertChangeStreamPreflight`** — port `/Users/benja/projects/pactsheet/fm/backend/app/src/common/change-stream/change-stream-preflight.ts` (98 lines): `hello` probe → throw a named, actionable error on a non-replica-set unless `requireChangeStreams === false` (then warn + return); when collections are supplied, run `enablePrePostImages` and verify via `listCollections` that each reports `changeStreamPreAndPostImages.enabled`. Tests on the replica set: passes with collections; a spy-`hello` unit test asserting the standalone message names the fix ("start mongod with --replSet").

- [ ] **Step 3: `RealtimeModule`** — MQ's factory-provider pattern: `forRoot` returns a `DynamicModule` whose providers create `serverId = randomUUID()`, `new ConnectionRegistry()`, `new PresenceStore({ db, serverId, ...presence })`, and `new ChangeStreamHub({ ...hub, connectedKeys: wireHubParking !== false ? registry.connectedKeys : hub?.connectedKeys })`, exported under the four tokens plus a lifecycle service that runs `presenceStore.ensureIndexes()` on init and `presenceStore.clearServer()` + `hub.close()` on destroy. Test by instantiating the module object and invoking the factories directly (assert provider wiring + lifecycle calls), no full Nest app.

- [ ] **Step 4: Green + lint + typecheck.**  **Step 5: Commit** — `git add -A && git commit -m "feat(nestjs): lifecycle base, preflight, RealtimeModule"`

---

### Task 15: README, barrel audit, tag

**Files:**
- Create: `README.md`
- Modify: the four `src/**/index.ts` barrels (audit only)

- [ ] **Step 1: Barrel audit** — every public symbol named in a task's Interfaces block is exported from its subpath barrel; `__resetPrePostImageCacheForTest` is NOT. `pnpm build`, then from a scratch dir verify all four subpaths resolve in both module systems:
`node -e "import('mongodb-realtime').then(m => console.log(Object.keys(m)))"` (after `pnpm link` or a tarball install via `pnpm pack`).

- [ ] **Step 2: README** — structure mirrors MQ's README: headline ("Realtime infrastructure on MongoDB change streams — resumable subscriptions, per-key watch hubs, presence, socket.io fan-out. No Redis."), requirements (**replica set required**; call `app.enableShutdownHooks()` in NestJS apps — spec §7), a quick-start per subpath (single-DB app: hub + watchCollection + pushScoped; multi-tenant app: N tagged watchCollections + mergeAsyncGenerators; presence: store + registry + watchPresence), the conventions table (`scopeField`/`softDeleteField`/`ownerField` defaults), and a "what stays in your app" section copied in spirit from spec §8.

- [ ] **Step 3: Full gate** — `pnpm lint && pnpm typecheck && pnpm test && pnpm build` all green.

- [ ] **Step 4: Commit + tag**

```bash
git add -A && git commit -m "docs: README and public-API audit" && git tag v0.1.0
```

---

## Self-Review (performed)

- **Spec coverage:** §3 repo shape → Task 1; §4.1–4.5 → Tasks 6, 3, 7, 4, 5+8; §5.1–5.3 → Tasks 9–11; §6 → Tasks 12–13; §7 → Task 14; §9 testing → embedded per task (replica set from Task 1); §10 step 1 (build + tag v0.1.0) → Task 15. §10 steps 2–3 (calendar/fm adoption) are separate follow-up projects per the spec — intentionally no tasks here.
- **Type consistency:** `ReadonlySignal`/`ValueSignal`/`subscribeToChanges` (Task 2) used by 6, 7, 8, 10, 11, 12, 14; `ChangeEvent`/`scopeId`/`ownerId` naming (Task 5) used by 8, 12, 13; `PresenceStore.countUserRows`/`serverId`/`collection` (Task 9) used by 11; `collectionWatchFactory` (Task 8) used by 11; `PushScopedOptions` (Task 12) extended by 13; hub `close()` (Task 7) used by 14.
- **Placeholders:** test-case lists are given as concrete behavioral assertions with harness instructions and port sources; implementation steps carry either full code or an exact port source path plus enumerated deltas. No TBDs.
