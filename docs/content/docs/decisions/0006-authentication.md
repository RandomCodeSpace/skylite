---
title: "0006: Authentication"
description: Clients authenticate to the query endpoint with OIDC JWT bearer tokens only, verified with go-jose against configured issuers, and are authorised by deny-by-default role rules.
weight: 6
---

**Status:** Accepted

**Date:** 2026-11-15

## Context

The [architecture page](/docs/architecture/#client-authentication) requires authentication and authorisation on the query endpoint and left open how clients authenticate. The owner decided: clients present OIDC JWT bearer tokens, and nothing else. No static API tokens, no mTLS client identity, and no trust in identity headers injected by a mesh or an ingress.

Two deployments have to work:

- **Kubernetes service-account tokens.** A pod mounts a projected, audience-bound token issued by its cluster's service-account issuer and sends it to skylite.
- **Microsoft Entra ID.** A service or user obtains an access token for skylite's app registration and sends it to skylite.

There is no server yet. This record fixes skylite's side of authentication and authorisation, and the package `internal/auth` implements it. The HTTPS query API will wire it in.

## Decision

### Trusted issuers

Server configuration lists the trusted issuers. Each one has an issuer string, which a token's `iss` claim must equal exactly, and an audience, which the token's `aud` claim must contain. A token whose `iss` names no configured issuer is rejected before any network request is made. Keys are looked up only among the keys of the issuer the token names.

skylite gets an issuer's public keys in one of two ways:

- **Discovery.** The issuer must be an `https` URL with no user info, and must not contain `?`, `#` or white space, so an empty query or fragment is refused too. skylite fetches `<issuer>/.well-known/openid-configuration`, with any trailing `/` removed from the issuer first. The document's `issuer` must equal the configured issuer exactly and its `jwks_uri` must be an `https` URL. skylite then fetches the JWKS from `jwks_uri`. Each response must have status 200. skylite follows no redirect: a redirect response is a failed fetch, so no request ever goes to a URL that a redirect names.
- **JWKS file.** For clusters with no route to the issuer, the configuration names a JWKS file on disk, and skylite makes no network request for that issuer. The issuer string can then be any non-empty value. The file is re-read under the same refresh rules as a fetched key set, so a key set mounted from a ConfigMap rotates when the ConfigMap changes. The file must hold a new key before the issuer starts signing with it: a token signed with a key the file does not yet hold is rejected.

The HTTP client is supplied by the server. As built, it trusts the system roots, so an issuer whose certificate chains to a private CA, such as a Kubernetes API server, needs the JWKS file in this version.

### Token checks

| Check | Rule |
|---|---|
| Size | At most 16 KiB (16,384 bytes), checked before parsing. |
| Form | JWS compact serialization with exactly three segments. JSON serialization, JWE and nested tokens are rejected. |
| Algorithm | `RS256`, `ES256` or `EdDSA` (Ed25519), checked before any key lookup. `none`, every HMAC algorithm and every other algorithm are rejected. The list is fixed, not configurable. |
| Key id | `kid` is required. A token without one is rejected. |
| Keys | RSA keys of at least 2048 bits, ECDSA keys on P-256, and Ed25519 keys. A JWK with a `use` other than `sig`, with a `key_ops` that does not include `verify`, without a `kid`, or of any other type, symmetric keys included, is ignored. Private key material in a JWKS is discarded. A JWK that names an `alg` is used only for tokens with that `alg`. When two keys share a `kid`, the first one accepted is used and the other is ignored. |
| `iss` | Required; equal to a configured issuer. |
| `aud` | Required; must contain the issuer's configured audience. A string or an array is accepted. |
| `sub` | Required and non-empty. |
| `exp` | Required. |
| `nbf`, `iat` | Optional; checked when present. A token is not accepted before `nbf` or with an `iat` in the future. |
| Clock skew | 60 seconds, applied to `exp`, `nbf` and `iat`. |

### Key cache

- Keys are fetched on first use. Building the verifier makes no network request, so the server starts while an issuer is unreachable and rejects that issuer's tokens until a fetch succeeds.
- One refresh, discovery and JWKS together, has a 10-second timeout. A discovery document or JWKS larger than 1 MiB is rejected, whether fetched or read from disk.
- Fetched keys are used for 1 hour. After that, the next token from that issuer triggers a refresh. If another request is already refreshing, the token is checked against the cached key without waiting. Entra ID's documentation calls checking for new keys every 24 hours reasonable ([access-tokens#validate-the-signature](https://learn.microsoft.com/en-us/entra/identity-platform/access-tokens#validate-the-signature)); 1 hour is within that.
- A token with an unknown key id triggers a refresh, but skylite makes at most one fetch attempt per issuer per minute, whether the attempt succeeds or fails. A stream of tokens with made-up key ids therefore costs at most one fetch per issuer per minute, and those tokens are rejected. A key that an issuer starts using within a minute of the last attempt is rejected until the minute has passed. Microsoft's key rollover guidance asks for a refresh on an unknown key "no more frequently than 5 mins" ([signing-key-rollover#best-practices-for-keys-metadata-caching-and-validation](https://learn.microsoft.com/en-us/entra/identity-platform/signing-key-rollover#best-practices-for-keys-metadata-caching-and-validation)); skylite's one minute fetches from Entra ID more often than that, in exchange for picking up a new key sooner.
- A failed refresh keeps the previous keys. Known keys are used for up to 24 hours after the last successful fetch. After that, every token from that issuer is rejected until a fetch succeeds. A token with an unknown key id is always rejected while the issuer is unreachable, so the cache fails closed.
- A key set with no usable key counts as a failed fetch, so it does not replace the cached keys.
- skylite starts no goroutine for this. A refresh runs in the request that needs it and has its own timeout, so it is not cancelled when that request gives up, because other requests may be waiting on it. A request that waits for a running refresh, because its key is not cached, waits until that refresh ends, at most the refresh timeout, even if the request itself is cancelled first.

### Principal and logging

A principal is the pair (issuer, subject). The verified token's claims travel with it, for the authorisation rules, and so does its expiry: the token's `exp` plus the clock skew above, which is the time from which verification refuses the token. The server must not use a principal after that time, for example on a long-lived connection.

A token's string form is not unique: the same signed content can be written as more than one string. The token string must never be used as a key for revocation or replay detection.

A refused request is logged at info level with the stable error code, the configured issuer (only if the token named one), the subject (only once the signature has verified), the key id (only if it is at most 64 bytes long), and the first 12 hex digits of the SHA-256 hash of the token. A 403 also logs the database and the role required. A failed key refresh is logged at warning level with the issuer and the error. That error names the request URL and the status or the cause of the failure, never a token, and never the URL that a redirect names. Every value that comes from a remote document, a token or the request, such as a discovery document's `issuer` or `jwks_uri`, a URL, a parser error, a subject or a database name, is cut to 200 bytes before it is put in an error or a log line.

A token, any segment of a token, its signature and the raw `Authorization` header are never logged, never returned to the client and never put in an error. Verification errors carry only a code and the fields listed above. They do not wrap the parsing library's errors.

### HTTP behaviour

The request must carry one `Authorization` header with the `Bearer` scheme, then one or more spaces, as [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110#section-11.4) allows, then exactly one token. Anything after the token is refused. The scheme is matched case-insensitively. Responses follow [RFC 6750](https://www.rfc-editor.org/rfc/rfc6750#section-3) and say nothing more about the cause:

| Situation | Status | `WWW-Authenticate` | Body |
|---|---|---|---|
| No bearer token | 401 | `Bearer` | `missing_token` |
| The token fails any check, or the header appears more than once | 401 | `Bearer error="invalid_token"` | `invalid_token` |
| Authenticated, but without the role | 403 | `Bearer error="insufficient_scope"` | `forbidden` |

The log records the specific reason: `token_too_large`, `malformed_token`, `unsupported_algorithm`, `untrusted_issuer`, `unknown_key`, `keys_unavailable`, `invalid_signature`, `missing_claim`, `invalid_audience`, `token_expired`, `token_not_yet_valid` or `issued_in_future`.

### Authorisation

Access is denied by default. Server configuration holds rules, and each rule grants one role on a database name or a pattern to principals of one issuer. A rule matches principals either by subject or by a claim: a top-level claim that equals the rule's value or, if it is an array, contains it, for example a `groups` or `roles` entry. A request is allowed if any rule grants the required role or a higher one. Configuration that names an unconfigured issuer, sets both or neither of subject and claim, has a database that is not a valid name or pattern, or names an unknown role is rejected at start-up.

A database name is 1 to 63 characters of lower-case ASCII letters, digits, `-` and `_`, and starts with a letter or a digit. This is a requirement on the server, which must refuse any other name, and on storage naming, which must not map two names to one database. `internal/auth` denies any name outside that set before it reads a rule. A pattern is a name in which `*` may also appear, matching zero or more characters, and `?`, matching exactly one. It is 1 to 63 characters long and starts with a letter, a digit, `*` or `?`, so every pattern matches some valid name. Character classes, escapes and every other character are configuration errors, so a malformed pattern is never a rule that silently matches nothing or everything. Subjects, claim values, names and patterns are compared exactly, byte for byte, with no case or Unicode folding.

There are three roles, in increasing order: `read`, then `write`, which includes read, then `admin`, which includes write and is reserved for schema changes and administrative endpoints. Which role each endpoint and statement requires is decided with the query API.

Deferred: row-level and statement-level policy, rules on nested claims, rules on the Entra ID `scp` claim, which is a space-separated string that no rule matches in this version, token exchange, revocation lists and token introspection, resolving Entra ID group overage, and multi-tenant Entra ID endpoints.

### Library

`internal/auth` uses [go-jose](https://github.com/go-jose/go-jose) v4 (`github.com/go-jose/go-jose/v4` [v4.1.9](https://github.com/go-jose/go-jose/releases/tag/v4.1.9), released 2026-11-05) for JWS parsing, the algorithm allowlist, decoding each JWK, signature verification and standard claim validation. skylite's own code fetches the discovery document and the JWKS, splits the JWKS's `keys` array and decodes and filters each key on its own, so that one unusable key does not reject the whole set, runs the cache described above, and checks that the required claims are present. It does not parse or verify tokens itself.

Checks made before adding it:

- Licence: [Apache-2.0](https://github.com/go-jose/go-jose/blob/v4.1.9/LICENSE).
- Dependencies: none. Its `go.mod` has no `require` block.
- Maintenance: actively maintained, with v4.1.9 released ten days before this record. It is also the JOSE library under `github.com/coreos/go-oidc/v3`.
- Advisories: the go-jose repository's advisory list names none affecting v4.1.9. The OSV database returns none either, but on the date of this record it had not indexed the newest five, so the repository's list is the evidence. The newest fixes it includes are the five advisories published on 2026-10-30 and fixed in v4.1.7: GHSA-p9j4-2j95-j4rv, GHSA-27pc-rm86-68pq, GHSA-r39w-98wm-9pp6, GHSA-c67m-wr4h-2ccg and GHSA-v6h2-qj3f-2h6v. No advisory names a fix in a later release. `govulncheck` reports no vulnerabilities in this module.
- Pure Go: it builds with `CGO_ENABLED=0`.

## Alternatives considered

- **`github.com/coreos/go-oidc/v3`, the default candidate.** Rejected because its key set and verifier do not meet the rules above, and replacing those parts would leave it doing one discovery request. Read from v3.21.0:
  - Its remote key set fetches the JWKS again for every token whose key id is not cached. Concurrent fetches are merged, but nothing limits how often they happen, so made-up key ids cause one fetch after another ([jwks.go#L155-L188](https://github.com/coreos/go-oidc/blob/v3.21.0/oidc/jwks.go#L155-L188)).
  - Cached keys never expire, and a failed fetch keeps them with no time limit. The fetch runs in a goroutine under the key set's own context, not a per-fetch timeout ([jwks.go#L199-L236](https://github.com/coreos/go-oidc/blob/v3.21.0/oidc/jwks.go#L199-L236)).
  - The JWKS and the discovery document are read with `io.ReadAll` and no size limit ([jwks.go#L302-L315](https://github.com/coreos/go-oidc/blob/v3.21.0/oidc/jwks.go#L302-L315), [oidc.go#L300-L312](https://github.com/coreos/go-oidc/blob/v3.21.0/oidc/oidc.go#L300-L312)).
  - The verifier applies no skew to `exp` and a fixed 5 minutes to `nbf` ([verify.go#L262-L284](https://github.com/coreos/go-oidc/blob/v3.21.0/oidc/verify.go#L262-L284)), and it does not require `sub`.

  It would also add `golang.org/x/oauth2` as a dependency.
- **Hand-rolled JWT and JWKS handling.** Rejected. Token parsing and signature verification are where algorithm confusion, `alg: none` and key-type mistakes occur, and a maintained library already handles them.
- **Trusting identity headers injected by a mesh or an ingress.** Rejected by the owner. Any client that reaches skylite without going through the component that sets the headers could forge them. It would also tie skylite to one mesh or ingress, and a misconfigured route would bypass authentication without anything showing it.
- **Static API tokens.** Rejected by the owner. They are long-lived shared secrets that someone has to store, rotate and revoke, and both target deployments already issue short-lived, audience-bound tokens.
- **mTLS client identity.** Rejected by the owner. In the target deployments, TLS from the client ends at the ingress or the sidecar, so skylite does not see the client's certificate.

## Deployments

None of the following was run against a real Kubernetes cluster or a real Entra ID tenant. The vendor documentation cited is the basis, and everything below is **unverified** until it has been run.

### Kubernetes service-account tokens

- The client pod mounts a projected `serviceAccountToken` volume whose `audience` equals the audience configured in skylite. The kubelet refreshes the token when it is older than 80% of its lifetime or older than 24 hours, and the client must read the file again when it changes ([configure-service-account#launch-a-pod-using-service-account-token-projection](https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/#launch-a-pod-using-service-account-token-projection)).
- The token's claims include `iss`, `sub` in the form `system:serviceaccount:<namespace>:<name>`, `aud`, `exp`, `iat`, `nbf` and a nested `kubernetes.io` object ([service-accounts-admin#schema-for-service-account-private-claims](https://kubernetes.io/docs/reference/access-authn-authz/service-accounts-admin/#schema-for-service-account-private-claims)). A rule for one service account is a subject rule. A rule for a whole namespace would need the nested claim, which is deferred.
- The API server serves discovery at `/.well-known/openid-configuration` and the JWKS at `/openid/v1/jwks`. They are "OIDC compatible, but not strictly OIDC compliant". By default, the role that may read them is bound to the `system:serviceaccounts` group, and `--service-account-jwks-uri` can point the `jwks_uri` at a public endpoint ([configure-service-account#service-account-issuer-discovery](https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/#service-account-issuer-discovery)). skylite sends no credentials with its fetches. It is inferred, not tested, that the default binding therefore does not cover skylite's request to the API server, and the API server's certificate is usually signed by the cluster's own CA. Inside the cluster, use the JWKS file, exported with `kubectl get --raw /openid/v1/jwks` into a ConfigMap. Use discovery where the issuer and `jwks_uri` are served publicly with a certificate the system roots trust.
- skylite requires a `kid` in the token header, and expects the API server to set one. Unverified.

### Microsoft Entra ID

- skylite's app registration must request v2.0 access tokens: `requestedAccessTokenVersion` set to `2` in the app manifest ([access-tokens#token-formats](https://learn.microsoft.com/en-us/entra/identity-platform/access-tokens#token-formats)). In v2.0 tokens, `iss` ends in `/v2.0` and `aud` is the API's client ID ([access-token-claims-reference#payload-claims](https://learn.microsoft.com/en-us/entra/identity-platform/access-token-claims-reference#payload-claims)). Configure the issuer as `https://login.microsoftonline.com/<tenant-id>/v2.0` and the audience as the API's client ID.
- Discovery is at the authority URL plus `/.well-known/openid-configuration` ([v2-protocols-oidc#find-your-apps-openid-configuration-document-uri](https://learn.microsoft.com/en-us/entra/identity-platform/v2-protocols-oidc#find-your-apps-openid-configuration-document-uri)), and the tenant-specific document's issuer has to match the token's `iss` exactly ([access-tokens#v10-and-v20-tokens](https://learn.microsoft.com/en-us/entra/identity-platform/access-tokens#v10-and-v20-tokens)). skylite therefore requires the tenant ID, not a domain name, and does not support the multi-tenant `common` and `organizations` documents, whose issuer is a template, or v1.0 tokens, whose issuer is on another host.
- Tokens are signed with asymmetric algorithms such as RS256 and carry a `kid` that selects the key from the JWKS ([access-tokens#validate-the-signature](https://learn.microsoft.com/en-us/entra/identity-platform/access-tokens#validate-the-signature)). Applications that use custom signing keys, which need an `appid` query parameter on the JWKS URL, are not supported.
- `sub` is a pairwise identifier per application, `roles` holds app roles, including those granted to applications calling with client credentials, and `groups` holds group object IDs ([access-token-claims-reference#payload-claims](https://learn.microsoft.com/en-us/entra/identity-platform/access-token-claims-reference#payload-claims)). Above 200 groups, Entra ID omits `groups` and adds an overage claim instead ([access-token-claims-reference#groups-overage-claim](https://learn.microsoft.com/en-us/entra/identity-platform/access-token-claims-reference#groups-overage-claim)). skylite does not resolve the overage, so such a user matches no group rule. Prefer claim rules on `roles`.

## Verification

The tests in `internal/auth` run a TLS issuer inside the test process that serves discovery and a JWKS and signs tokens with keys generated in the test. The clock is injected. They cover each accepted algorithm, every check in the table above, and each fixed number of this record at its boundary: the 60-second skew, the 16,384-byte token, the 2048-bit RSA minimum, the 1-hour cache, the 24-hour stale limit, the one-minute refresh interval, the 1 MiB document, fetched and from a file, and the 64-byte key id and 200-byte value caps in errors and logs. They cover the fetch rules: status 200, an `https` `jwks_uri`, no redirect followed, the trailing `/`, a failed fetch counting as an attempt, a refresh that the caller's cancellation does not stop and whose deadline is 10 seconds, a stale key that does not wait for a running refresh, and a refresh lock released after a panic. They cover unknown key ids under concurrency, key rotation, an unreachable issuer, a discovery document that names another issuer, the JWKS file, the authorisation matrix with exact matching and the database name rules, configuration validation, the middleware's status codes and headers, and the fields of each log line. They also check that errors, responses and logs contain no part of the token.

Not covered: a real Kubernetes issuer, a real Entra ID tenant, a slow issuer reaching the 10-second timeout, and performance.

## Consequences

- One mechanism covers both deployments, and the server has no secret of its own to store.
- A key that an issuer starts using within a minute of skylite's last fetch is rejected for up to that minute. An issuer that stays unreachable for more than 24 hours stops all authentication through it.
- Clusters whose issuer skylite cannot reach have to keep a JWKS file up to date.
- `github.com/go-jose/go-jose/v4` becomes a direct dependency.
- The HTTPS query API has to wire in `internal/auth`, define the configuration format and decide each endpoint's required role. The user-facing configuration reference is written when that exists.
- The [architecture page](/docs/architecture/#client-authentication) question is updated in the same pull request to state this decision.
