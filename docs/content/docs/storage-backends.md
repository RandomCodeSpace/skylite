---
title: Storage backends
description: The storage backend packages that exist, how each maps the storage contract onto its service, and what has been tested.
weight: 25
status: current
---

Every durable byte in skylite is to go through one Go interface, `storage.Store` in `internal/storage`. Its package documentation is the contract: create-if-absent, compare-and-swap on a version token and an unconditional delete, with no unconditional put; range reads; listing; and a rule for writes whose outcome is unknown. Package `internal/storage/storagetest` is the conformance suite that every backend must pass, and each backend carries its own tests for what the suite cannot check. Package `internal/storage/localfs` is the reference backend on a local directory.

The backends are library packages. The skylite server cannot be configured to use any of them yet.

## Azure Blob Storage

Package `internal/storage/azureblob` stores each object as a block blob in one container of an Azure Blob Storage account with a flat namespace (not Data Lake Storage). The blob name is the key.

### How the contract maps onto the service

| Contract | Blob service request |
|---|---|
| `Create` | one Put Blob with `If-None-Match: *` |
| `Replace` | one Put Blob with `If-Match: <version token>` |
| `Delete` | Delete Blob; 404 `BlobNotFound` counts as success |
| `Get`, `GetRange` | one Get Blob, with `x-ms-range` for a range |
| `Head`, `GetRange` of length 0 | Get Blob Properties |
| `List` | List Blobs, flat, with the prefix, following `NextMarker` |

**Version tokens.** The token is the blob's ETag, quotes included, and `Replace` sends it unchanged in `If-Match`. Azure documents that a blob's ETag changes on every write. The contract also needs an ETag never to repeat for a key, also after the blob is deleted and created again. Azure does not document that, and the backend relies on it as an assumption. A version that is not a quoted entity tag, `*` included, fails `Replace` without a request.

**Conditions.** A write's condition is checked by the service inside its one Put Blob request, so among concurrent writers exactly one succeeds. 409 `BlobAlreadyExists` and 412 are precondition failures, and so is 404 `BlobNotFound` for `Replace`.

**Upload path.** Every write is one Put Blob request; the backend never stages blocks. Azurite counts a blob that has only uncommitted blocks as existing for `If-None-Match: *`, and the real service's behaviour there is not documented, so an interrupted staged upload could block a later `Create`. Put Blob needs the body's length, so a write reads its whole body first. Up to 4 MiB stays in memory; a longer body is written to a spool file in a directory the caller chooses. The file is unlinked as soon as it is created and closed when the write returns, so it never outlives the write. A body longer than 5,000 MiB, the most one Put Blob accepts, fails with `ErrInvalidArgument` before anything is sent.

**Unknown outcomes and retries.** The SDK's retries are turned off for `Create`, `Replace` and `Delete`, so a retry can never meet a write's own effect and turn its success into a precondition failure. A mutation returns `ErrOutcomeUnknown` when its request may have reached the service and no definite answer came back: a lost response, a reset connection, the context ending while the request is in flight, a 5xx, a 408, or a 2xx or 3xx other than the one the request expects (201 for Put Blob, 202 for Delete Blob), which only a non-conforming proxy sends. Any other 4xx is a definite error. A mutation also returns a definite error when the service cannot have applied the request: net/http obtained no connection for it, the SDK failed before calling its transport (for example, the credential got no token), or the body was not read to its end. A credential that calls its token endpoint with the call's context shares the call's connection trace, so a failure there can come back as `ErrOutcomeUnknown`. Once a call returns, its request body refuses further reads, so a request still being sent cannot complete later. Reads keep the client's retries.

**Listing.** The service may return fewer entries than requested, so `List` keeps requesting until it has the number asked for or the listing ends. List Blobs has no start-after parameter on a flat namespace, so `List` reads the keys under the prefix up to `startAfter` and skips them, up to 5,000 per request.

**Errors.** An error from the service keeps only its HTTP status and Azure error code. Other errors keep their text with every URL query string removed, since a SAS URL carries its signature there.

### Configuration

`azureblob.New(client, spoolDir)` takes a `*container.Client` from the Azure SDK for Go (`github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container`) and the spool directory (empty means the system temporary directory). The package does not choose a credential: a caller builds the client with a shared key, a SAS URL, or `container.NewClient` and any `azcore.TokenCredential`, which is how Entra ID, including workload identity, can be wired in later. The container must already exist, nothing else may write to it, and the client's transport must be the SDK's default net/http transport or an `*http.Client`.

The transport must also close an idle connection before the service, or a load balancer or proxy in between, does: set `http.Transport.IdleConnTimeout` below the shortest idle timeout on the path. The SDK's default transport keeps idle connections for 90 seconds, and Azurite closes them after 5. A mutation is never retried, so a write sent on a pooled connection that the other end is closing fails, often with `ErrOutcomeUnknown`. The tests set 2 seconds.

### Limits

- The spool file relies on Linux or macOS, where an open file can be unlinked.
- A `List` with `startAfter` costs requests in proportion to the keys before `startAfter` under the prefix.
- Blobs with snapshots cannot be deleted; the backend never creates snapshots.
- The SDK is pinned at azblob v1.8.0. Version 1.8.1 and later add Apache Arrow (`arrow-go`) to the dependency graph, so Dependabot is told to ignore them.

### What was tested

Tested against Azurite 3.37.0 (`mcr.microsoft.com/azure-storage/azurite`, pinned by digest in the Makefile), with Azurite's well-known development account and a container SAS:

- the full conformance suite, `storagetest.Run`;
- this backend's own tests, with a fault-injecting HTTP transport between the SDK and Azurite: responses lost after the service applied a `Create`, a `Replace` and a `Delete`; a refused connection and a request that failed before its body was read; a connection cut in the middle of the body; a request still being sent when its call returned; a response slower than the context's deadline; 500, 503 and 408 before and after the effect; 400, 403 and a 409 with another code than `BlobAlreadyExists`; a 200 after the effect; that a successful write sends one request and returns the token of its response; a deleted container; writes on connections idle for about as long as Azurite keeps them; read retries; listings in short pages, including one longer than `storage.MaxListLimit`, and one whose second page keeps failing; a 416 for a blob that has grown; the size limit; the spool directory; and redaction of the SAS signature from the errors of every call;
- against a local HTTP server, a token credential that fails before any request is sent.

`make integration` runs them in CI. It keeps Azurite's data on the container's disk: Azurite frees the data of deleted blobs only every 10 minutes, and the suite writes more than its in-memory store holds on a CI runner.

Not tested: the real Azure Blob Storage service, working real credentials (Entra ID, workload identity, managed identity, account keys of a real account), real latency and throttling, and the assumption that an ETag never repeats for a key. Azurite derives its ETags from the time in milliseconds multiplied by a random factor, so on the emulator two writes in the same millisecond can, rarely, share one.
