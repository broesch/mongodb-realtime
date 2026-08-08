# Calendar consumer contracts

These tests are a consumer-driven compatibility guardrail (CDCT-style):
an executable statement of what the `calendar` app's five change-stream
pushers actually rely on from this library. They run in this repo's CI, on
every PR, so "did this change break calendar?" is answered _before_ a
release tag exists — not discovered later when calendar upgrades its
dependency.

## What these test — and what they don't

Each file exercises one calendar pusher's **composition**: the specific way
that pusher wires the library's primitives together (`pushMapped` /
`pushOwnerScoped` selector logic, room routing, `didChange` gating,
`watchPresence`/`PresenceStore` semantics). They deliberately do **not**
re-test individual unit behavior — `didChange`, room-name helpers, the
per-socket dedupe guarantee of a single `to(rooms)` call, `PresenceStore`'s
order-stable 0/1 transition detection, etc. are already pinned by the
per-module suites elsewhere in `tests/`. A contract test failing means a
_composition_ calendar depends on broke, even if every unit test is green.

| Contract file                          | Calendar source it mirrors                                       |
| -------------------------------------- | ---------------------------------------------------------------- |
| `calendar-event.contract.test.ts`      | `apps/api/src/calendar/calendar-event.pusher.ts`                 |
| `booking.contract.test.ts`             | `apps/api/src/booking/booking.pusher.ts`                         |
| `optimization-result.contract.test.ts` | `apps/api/src/optimization-result/optimization-result.pusher.ts` |
| `notification.contract.test.ts`        | `apps/api/src/notification/notification.pusher.ts`               |
| `presence.contract.test.ts`            | `apps/api/src/realtime/online-user.pusher.ts`                    |

Each file's header comment names its calendar source and states the exact
routing/gating rule it pins, quoting the calendar pusher's own comments
where that rule is explained (e.g. "Calendar events are personal — never
broadcast them to the org room").

## Coordination rule

**A change that fails a contract test must be coordinated with the calendar
repo, not just "fixed" here.** These tests encode calendar's actual runtime
behavior; if a library change makes one fail, that is a real (or intended)
break in what calendar consumes. Before adjusting the test to match new
behavior:

1. Confirm the composition change is intentional, not a regression.
2. Coordinate the corresponding change in calendar's pusher (or its own test
   suite) — do not treat a red contract test as something to silence
   unilaterally on the library side.
3. Only then update the contract test (and this README's description of the
   rule it pins) to reflect the newly agreed behavior.

## `fm` joins later

`fm` (pactsheet.fm) is a second consumer of this library, migrating onto it
separately (see the library's own planning docs). Once that migration lands,
its own consumer contract suite will live alongside this one as
`tests/contracts/fm/`, following the same one-file-per-pusher pattern and the
same coordination rule.
