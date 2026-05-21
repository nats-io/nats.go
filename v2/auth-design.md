# Auth Options Cleanup

## Context

Today's auth API has 10+ public options spread across five conceptual modes (user/pass, token, JWT-with-NKey-signature, NKey-only, TLS client cert). Pain points:

1. **Discoverability** — no narrative; IDE autocomplete shows ten options with no grouping.
2. **Asymmetry** — credentials have `File`, `Bytes`, `JWTAndSeed`, and `Callback` helpers; NKey-only has just `Callback`.
3. **Runtime mutex** — `Token` and `TokenHandler` are mutually exclusive at runtime via `ErrTokenAlreadySet`.
4. **Eager side-effect** — `UserJWT` immediately calls the user JWT callback at option-evaluation time as a "smoke test." Surprising for true callback semantics (HSM-backed, vault-backed).
5. **Mandatory `nkeys` dep** — users on user/pass or token still pull `github.com/nats-io/nkeys` because JWT/NKey options live in the core package.

Custom auth is already supported today via the callback-based options (`UserJWT(userCB, sigCB)`, `Nkey(pubKey, sigCB)`). v2 does **not** introduce a new `Authenticator` interface — the callback shape continues to work, and a new abstraction would add migration cost without unlocking new capability.

TLS-as-auth (`ClientCert`, `Secure`) is out of scope for this refactor — it's a transport-layer concern and has its own consolidation story.

## Design

Keep callback-based options. Rename for discoverability. Move JWT/NKey to a sub-package. Fix asymmetry. Drop the eager side-effect.

### Core `nats` package (no `nkeys` dependency)

```go
func AuthUserPass(user, pass string) Option
func AuthUserPassFunc(cb UserPassHandler) Option
func AuthToken(t string) Option
func AuthTokenFunc(cb AuthTokenHandler) Option
```

### `nats/auth/creds` sub-package (pulls `nkeys`)

```go
func AuthCredentials(file string) Option
func AuthCredentialsBytes(b []byte) Option
func AuthJWTAndSeed(jwt, seed string) Option
func AuthJWT(jwtCB UserJWTHandler, sigCB SignatureHandler) Option
func AuthNKey(pubKey string, sigCB SignatureHandler) Option
func AuthNKeyBytes(seed []byte) Option   // NEW — fills today's asymmetry
```

### Migration mapping

| v1 option                        | v2 replacement                       |
|----------------------------------|--------------------------------------|
| `UserInfo(u, p)`                 | `AuthUserPass(u, p)`                 |
| `UserInfoHandler(cb)`            | `AuthUserPassFunc(cb)`               |
| `Token(t)`                       | `AuthToken(t)`                       |
| `TokenHandler(cb)`               | `AuthTokenFunc(cb)`                  |
| `UserCredentials(file)`          | `creds.AuthCredentials(file)`        |
| `UserCredentials(jwt, seed)`     | `creds.AuthJWT(...)` or open Q below |
| `UserCredentialBytes(b)`         | `creds.AuthCredentialsBytes(b)`      |
| `UserJWTAndSeed(j, s)`           | `creds.AuthJWTAndSeed(j, s)`         |
| `UserJWT(jwtCB, sigCB)`          | `creds.AuthJWT(jwtCB, sigCB)`        |
| `Nkey(pub, sigCB)`               | `creds.AuthNKey(pub, sigCB)`         |
| —                                | `creds.AuthNKeyBytes(seed)` (new)    |

## Key decisions

- **No `Authenticator` interface.** Callback-based options remain. Custom auth (Vault, HSM, etc.) uses the existing callback shape via `creds.AuthJWT(jwtCB, sigCB)`.
- **Eager smoke test dropped.** `creds.AuthJWT` does not pre-invoke `jwtCB` at option-evaluation time. Validation errors surface on first connect, consistent with other options.
- **Token static/handler mutex removed.** `AuthToken` and `AuthTokenFunc` are independent options. Passing both is last-wins; a warning is delivered via the new `ErrorHandler` if both are present at connect time.
- **`nkeys` becomes opt-in.** Production binaries that use only user/pass or token auth no longer pull `nkeys`. The `creds` sub-package owns the dependency.
- **No backward-compat shims for v1 names.** Users update imports/option names as part of v2 migration. Documented in migration guide.

## Open questions

- **Two-file credentials.** Today's `UserCredentials(userOrChainedFile, seedFiles ...string)` allows separate JWT and seed files. Confirm whether to expose a `AuthCredentialsWithSeed(jwtFile, seedFile string)` helper or have such users drop down to `AuthJWT(jwtFn, sigFn)`.
- **Handler shape unification.** Should `AuthTokenFunc` and `AuthUserPassFunc` share a generic handler signature (e.g., `func() (Credentials, error)`)? Probably no — the return shapes diverge and forcing unification reintroduces the abstraction we just rejected.
- **Naming verbosity.** `AuthCredentials` vs `Credentials`. The `Auth` prefix groups everything together under IDE autocomplete but is verbose. Decision: keep `Auth` prefix for grouping consistency.
