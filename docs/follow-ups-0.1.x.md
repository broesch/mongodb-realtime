# Follow-ups for 0.1.x (from the v0.1.0 final review)

All ship-safe (triaged by the final whole-branch review); scheduled for 0.1.x or the adoption projects.

## API hardening
- `ChangeStreamHub.close()` is terminal but unguarded: a post-close `subscribe()` on a new key opens an untracked cursor; on an existing key it hands back a dead generator that hangs. Add a `closed` flag that throws on `subscribe()` (~5 lines).
- `ChangeStreamEventPusher`: chain `.then(() => this.loopEnded())` on the bootstrap promise so a subclass loop that *resolves* without calling `loopEnded()` cannot hang shutdown (today only the rejection path is latched; README documents the subclass obligation).
- `resolveDb`'s `instanceof` checks against the library's own `mongodb` copy degrade silently (no operation-time capture) when a consumer has a duplicated driver copy — **do the duck-type fallback before fm migrates** (mongoose vendors its own driver; pnpm may not dedupe).

## Adoption notes (carry into calendar/fm migration plans)
- `pushScopedFiltered`'s per-socket `runInContext` wrap keys on hook presence, not tenant value: fm's adapter must be `(ctx, fn) => ctx.tenant ? cls.run(...) : fn()` to preserve no-CLS-for-tenantless-sockets (JSDoc on the option documents this).
- `watchPresence` deliberately skips (not retries) an event whose count query rejects transiently — a single online/offline announcement can be lost on a DB blip; the row state stays correct.
- Watchers churned dynamically against one process-lifetime shutdown signal accumulate signal listeners (`whenShutdown`, subscription internals never unsubscribe on external teardown). Both current consumers build watchers once at boot; revisit before any tenant-onboarding-without-restart design.

## Test adds
- Scripted-hub fake locking in `hubConsumerLoop`'s backoff growth / reset-only-after-real-event / no-resubscribe-on-shutdown (probe-verified in review, not test-pinned).
- `openedFromOperationTime` second-failure drop branch; injected `isEqual` option; `currentData` defensive copy.
- Widen the hub parking test's 300 ms `expectNoEvent` and apply the `collectUntil` prime-event fix in watch-collection tests on first CI flake.

## Cosmetics
- Once-block error log in `pushScopedFiltered` says "mapData failed" for `variants.render`/`prepareEvent` throws (and "canReceive check failed" for `pick` throws).
- Direct-mode double log prefix `[watch:c] [ChangeStreamSubscription]`.
- `forRootAsync` duplicates `forRoot`'s 5-provider wiring inline; refactor to a shared parameterized builder.
- Dead `primeResult` block in the tenant-tag test burns ~5 s per run.
- `enable-pre-post-images.ts` param named `logger$`.

## Out of repo
- mongo_queue has the same latent CJS-under-`"type":"module"` bug this repo fixed in Task 1 (separate fix task exists).
