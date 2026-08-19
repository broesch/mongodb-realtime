# Follow-ups (0.1.x / 0.2.x)

All ship-safe (triaged by the final whole-branch review); scheduled for a future point release or the adoption projects.

## API hardening
- ~~`ChangeStreamHub.close()` is terminal but unguarded: a post-close `subscribe()` on a new key opens an untracked cursor; on an existing key it hands back a dead generator that hangs. Add a `closed` flag that throws on `subscribe()` (~5 lines).~~ **Done — Task 8 (0.2.0):** `close()` now sets a terminal flag; `subscribe()` throws after close instead of opening an untracked cursor or handing back a dead generator.
- ~~`ChangeStreamEventPusher`: chain `.then(() => this.loopEnded())` on the bootstrap promise so a subclass loop that *resolves* without calling `loopEnded()` cannot hang shutdown (today only the rejection path is latched; README documents the subclass obligation).~~ **Done — Task 5 (0.2.0):** the bootstrap chain now calls `loopEnded()` from a `.finally()`, latching on either settlement; `loopEnded()` is idempotent so a subclass's own call in its loop's `finally` no longer conflicts.
- ~~`resolveDb`'s `instanceof` checks against the library's own `mongodb` copy degrade silently (no operation-time capture) when a consumer has a duplicated driver copy — **do the duck-type fallback before fm migrates** (mongoose vendors its own driver; pnpm may not dedupe).~~ **Done — Task 9 (0.2.0):** added a structural (duck-type) fallback after the `instanceof` checks, recognizing Db-/MongoClient-/Collection-shaped parents from any driver copy.

## Adoption notes (carry into calendar/fm migration plans)
- `pushScopedFiltered`'s per-socket `runInContext` wrap keys on hook presence, not tenant value: fm's adapter must be `(ctx, fn) => ctx.tenant ? cls.run(...) : fn()` to preserve no-CLS-for-tenantless-sockets (JSDoc on the option documents this).
- `watchPresence` deliberately skips (not retries) an event whose count query rejects transiently — a single online/offline announcement can be lost on a DB blip; the row state stays correct.
- Watchers churned dynamically against one process-lifetime shutdown signal accumulate signal listeners (`whenShutdown`, subscription internals never unsubscribe on external teardown). Both current consumers build watchers once at boot; revisit before any tenant-onboarding-without-restart design.
- `PresenceStore.collection` was replaced by `collectionFor(key?)` in 0.3.0. A consumer reading the property directly must switch; `watchPresence` users are unaffected.
- `watchPresence` opens one cursor per key. A consumer with many keys and a large `keys()` should expect O(keys) presence cursors — the hub does NOT cover presence, by design (see `hubIgnoreCollections`).
- The `mongoose` devDependency exists specifically as a canary for the `model.db.db`/`model.collection.name` seam `mongodb-realtime/mongoose` reads — a mongoose major bump is exactly what would break it silently. It's currently an unpinned caret range (`^9.9.3`), so a future install could jump a major before anyone deliberately re-verifies that seam; consider pinning it, or add a deliberate re-verification step when it moves.

## Test adds
- Scripted-hub fake locking in `hubConsumerLoop`'s backoff growth / reset-only-after-real-event / no-resubscribe-on-shutdown (probe-verified in review, not test-pinned).
- `openedFromOperationTime` second-failure drop branch; injected `isEqual` option; `currentData` defensive copy.
- Widen the hub parking test's 300 ms `expectNoEvent` and apply the `collectUntil` prime-event fix in watch-collection tests on first CI flake.
- `tests/contracts/fm/multi-tenant-watch.contract.test.ts` writes only to database A, so a regression where target B's stream never opens at all would still pass, and B→A leakage is unexercised. A symmetric insert into B, asserted to arrive tagged `{ slug: 'b' }`, would close both gaps.
- The two presence transition tests in `tests/contracts/fm/presence.contract.test.ts` populate only key `'a'`, so the "per tenant" half of their names is undiscriminated — a regression making the 0→1/1→0 transition global across databases would pass. A same-user check-in on key `'b'` asserted to yield a second `online` would pin it.
- `tests/contracts/fm/entity-fanout.contract.test.ts`'s per-owner test has no negative counterpart for the *removed* direction (it asserts the non-owner receives no `changed` event, but never checks for a leaked `removed`), so a regression fanning owner-scoped removals to every user room would not be caught.
- `tests/contracts/fm/multi-tenant-watch.contract.test.ts` uses `graceMs: 300`, tighter than the deliberately-generous `400` used in `tests/core/change-stream-hub.test.ts` (which carries a comment about scheduling jitter). Risk is a false red on a loaded CI, not a missed bug.
- `tests/contracts/fm/multi-tenant-watch.contract.test.ts` calls `await hub.close()` after its assertions without a `try/finally`, so a failing assertion leaks the hub's open cursors into `afterAll`. `presence.contract.test.ts` in the same commit does use `try/finally`.

## Cosmetics
- Once-block error log in `pushScopedFiltered` says "mapData failed" for `variants.render`/`prepareEvent` throws (and "canReceive check failed" for `pick` throws).
- Direct-mode double log prefix `[watch:c] [ChangeStreamSubscription]`.
- ~~`forRootAsync` duplicates `forRoot`'s 5-provider wiring inline; refactor to a shared parameterized builder.~~ **Done — Task 1 (0.3.0).**
- Dead `primeResult` block in the tenant-tag test burns ~5 s per run.
- `enable-pre-post-images.ts` param named `logger$`.

## Out of repo
- mongo_queue has the same latent CJS-under-`"type":"module"` bug this repo fixed in Task 1 (separate fix task exists).

## Rot watch (0.2.0 final review)
- `@microsoft/api-extractor@7.58.12` bundles its own `typescript@5.9.3`, one minor behind this project's `typescript@^6.0.3` — `api:check` is green today because `.api-tmp/` generation only exercises the declaration surface, but bump `@microsoft/api-extractor` to whatever release first bundles a TS 6.x compiler once one exists, so the checked-in-report step stays on the same compiler as `typecheck`.
- Checked during this review: `PushScopedOptions` (and `PushScopedFilteredOptions`/`PushOwnerScopedOptions`, which extend it) already carry the same optional `name` log-prefix field as `PushMappedOptions` (`src/socketio/types.ts`) — no asymmetry found, no action needed.
