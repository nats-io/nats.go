# v2 Follow-up Notes

Items captured but deferred to implementation/release time. Not blocking the v2 design discussion.

## Migration guide (priority — AI-agent-friendly format)

A detailed migration guide is the single most important post-design deliverable. Target audience includes coding agents performing drop-in migrations on user codebases. Format requirements:

- Per-item migration steps with explicit before/after code blocks
- Tabular old → new mappings wherever applicable (auth, TLS, subscriptions, Msg, errors, JetStream all already have these in their design docs)
- Stable section anchors so an agent can be pointed at a specific section
- No "etc." or "and similar" — full enumeration

## Implementation-time inventories (not blocking design)

| Topic | Notes |
| ----- | ----- |
| Item #12 handler renames | Enumerate every `*CB` / `*Cb` / non-`*Handler` callback type and produce a complete rename list before starting work |
| Item #13 acronym renames | `ErrJsonParse` → `ErrJSONParse` is named explicitly; sweep for `JsAlreadyExistsErr`, `JsError`, etc. and apply the same rule consistently |
| Item #18 `ObjectStore.List()` iterator | Pin the return type to `iter.Seq2[*ObjectInfo, error]` to match item #15's `Messages()` shape (not a custom Lister) |
| Subject / queue validator exposure | Recent fix (PR #2076) tightened `keyValid` for consecutive dots. Decide whether validators are exposed publicly so users can validate strings before publishing |
| Header canonical-form semantics | Today some operations canonicalize MIME-style, some don't. Pick one behavior and document |

## Release-time operations

| Topic | Notes |
| ----- | ----- |
| `testing.go` migration timing | Lands as part of the consolidated `tests/` module refactor on `main` before v2 is cut. See modules-design.md migration sequence. |
| Multi-module release flow | modules-design.md says "manual coordination" — document the actual tag order for v2.0 (core first, then jetstream, then kv/object, micro) and the per-release pre-checks |
| Pre-release benchmark gate | Item #2 requires confirming jetstream pull throughput parity (within ~5%) when ChanSubscribe is replaced with SubscribeSync + iterator |

## Tracked elsewhere

| Topic | Where it lives |
| ----- | -------------- |
| Header API review (Set/Add/Del consistency, canonical-form, reserved Nats-* slots) | Active in v1: [nats-io/nats.go#2068](https://github.com/nats-io/nats.go/issues/2068). Pull conclusions into v2 once that work lands. |

## Future considerations (non-breaking; can land post-v2.0)

| Topic | Notes |
| ----- | ----- |
| Wire-protocol parser as a public module | Extract `parser.go` into `nats.go/v2/protocol` (or similar) so embedded NATS use cases (proxies, sidecars, tools) can parse without depending on the full client. No demand evidence today; defer until users ask. Additive, non-breaking. |
| Synchronous error return on Subscribe | Considered and dropped for v2.0 (PING/PONG flush would cost ~1 RTT per subscribe, especially painful on reconnect re-subscribe storms). Not protocol-native — the server doesn't ack successful SUBs. |
| Server-version feature checks cleanup | The Go client has few of these (unlike JS); not worth a dedicated pass. Address opportunistically during other work. |

## Things explicitly out of scope for v2.0

(Already documented in their respective files; listed here for visibility.)

- Context-aware subscription lifecycle (`SubscribeContext`) — see `subscriptions-design.md` Out-of-Scope section
- Pluggable slow-consumer policy — see `subscriptions-design.md` Out-of-Scope
- Tracing / metrics hooks — not a v2 client responsibility. Item #14 (`Header.Keys()`) makes `nats.Header` directly usable as an OpenTelemetry `TextMapCarrier`; manual span injection/extraction by the user is the supported path. Automatic span creation / interceptors / first-party OTel sub-package — out of scope and may belong in `orbit.go` if ever needed.
- Generic / typed subscriptions — see `subscriptions-design.md` Out-of-Scope
- Release scripting / migration tooling automation — see `modules-design.md` Out-of-Scope
