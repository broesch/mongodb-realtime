# fm consumer contracts

These tests pin the library compositions that pactsheet/fm depends on, in the
exact shapes fm uses them. They are consumer-driven contract tests: fm does
not run them, this repo does, so a change here that would break fm fails in
this repo's CI instead of in fm's (CI-only) backend e2e suite.

Written **before** fm migrates onto this library — unlike `tests/contracts/
calendar/`, there is no live pusher to diff against yet. Source of truth for
the shapes is fm's `backend/app/src/common/change-stream/` and its 38 gateway
pushers, as of the 2026-08-19 adoption design
(`docs/superpowers/specs/2026-08-19-mongodb-realtime-adoption-design.md` in
the fm repo).

## What these test

| Contract file                              | Composition it pins                                                                    |
| ------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `entity-fanout.contract.test.ts`            | `pushScopedFiltered` (hooks used together) and `pushOwnerScoped`, as fm's hierarchy and per-owner gateways wire them |
| `multi-tenant-watch.contract.test.ts`       | `watchAcrossTargets` / `ChangeStreamHub` in hub mode, with per-tenant event tagging and connection-driven parking |
| `presence.contract.test.ts`                 | `PresenceStore` / `watchPresence` keyed by tenant database                             |

**Re-sync these if fm's compositions change.** In particular:

- `pushScopedFiltered` with `variants` + `prepareEvent` + `runInContext` +
  `tenantOf` all supplied at once — fm's hierarchy gateways use every hook
  together, which no other consumer does.
- Multi-tenant `watchAcrossTargets`/`watchModel` in hub mode with per-tenant
  event tagging.
- Presence keyed by tenant database.

## Coordination rule

**A change that fails a contract test must be coordinated with the fm repo,
not just "fixed" here.** These tests encode compositions fm's adoption design
commits to; if a library change makes one fail, that is a real (or intended)
break in what fm is about to build against. Before adjusting the test to
match new behavior:

1. Confirm the composition change is intentional, not a regression.
2. Coordinate the corresponding change in fm's adoption design (or its own
   gateway code, once migrated) — do not treat a red contract test as
   something to silence unilaterally on the library side.
3. Only then update the contract test (and this README's description of the
   rule it pins) to reflect the newly agreed behavior.

## Relationship to `tests/contracts/calendar/`

Same pattern, same coordination rule — see `../calendar/README.md` for the
full rationale. The one structural difference: calendar's suite is checked
against a live pusher today (so most of it is unconditionally binding, with
a couple of explicitly forward-looking blocks); this suite is entirely
forward-looking, since fm has not migrated yet. Every case here is still
real, executable coverage of the library's actual behavior — it is just
pinning a composition fm has designed to depend on rather than one it
currently runs in production.
