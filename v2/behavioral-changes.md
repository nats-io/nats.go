# Behavioral Default Changes in v2

This file lists silent behavior changes — places where v2 changes a default or runtime behavior that does **not** appear as a signature change. Users moving from v1 to v2 should review this list even if their code compiles unchanged.

| # | Area | v1 behavior | v2 behavior | Origin |
| - | ---- | ----------- | ----------- | ------ |
| 1 | Reconnect attempts | 60, then give up | unlimited (`-1` is the new default) | item #24 |
| 2 | Callbacks after `Close()` | fire unless `NoCallbacksAfterClientClose()` set | suppressed by default; `ClosedHandler` still fires correctly before close completes | item #6 |
| 3 | `UserJWT` / credentials validation | `UserJWT` invokes the user JWT callback at option-evaluation time as a smoke test | lazy — validation happens on first connect, consistent with all other options | item #22 / auth-design.md |
| 4 | Sub-level error handler delivery | (new in v2) | when `SubErrorHandler` is set, the connection-level `ErrorHandler` does NOT fire for that subscription's errors | item #9 / errors-design.md |
| 5 | `Conn` stats access | promoted fields on `Conn` (e.g. `nc.OutBytes`) — mutable and racy | only via `nc.Stats() Statistics` snapshot; promoted access removed | item #7 |
| 6 | Drain completion | fire-and-forget; users hack timing | observable via `sub.Drained() <-chan struct{}` | item #3 / subscriptions-design.md |
| 7 | `Subscription` stats access | five separate getters (`Pending`, `Delivered`, `Dropped`, `MaxPending`, `PendingLimits`) | single `sub.Stats() SubStats` atomic snapshot | item #3 / subscriptions-design.md |
| 8 | Pending limits mutability | `SetPendingLimits` at runtime | set once via the `PendingLimits` option at subscribe time | item #3 / subscriptions-design.md |
| 9 | `Token` vs `TokenHandler` | runtime mutex (`ErrTokenAlreadySet`) | two independent options; last-wins with a warning to the new `ErrorHandler` if both present | item #22 / auth-design.md |
| 10 | Internal `Statistics` type | embedded into `Conn` (promoted, mutable) | private `stats Statistics` field; type stays public for snapshot reads | item #7 |

## Adding to this list

Implementation-time discoveries that change a v1 default or observable behavior should be appended here. Cross-link to the proposal item that introduced the change.
