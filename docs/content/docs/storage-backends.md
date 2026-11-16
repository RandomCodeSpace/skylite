---
title: Storage backends
description: The storage interface's backends, how each maps the interface onto its service, and what has been tested.
weight: 25
status: current
---

Every durable byte in skylite is to go through one interface, the `storage.Store` of package `internal/storage`. Its package documentation is the contract: create-if-absent and compare-and-swap as the only writes, version tokens that never repeat for a key, strongly consistent reads and listings, and a rule for writes whose outcome is unknown. Package `internal/storage/storagetest` is the conformance suite every backend must pass.

The backends below are library packages. The server cannot be configured to use any of them yet. A backend for Google Cloud Storage is still to come.

| Package | Service | Status |
|---|---|---|
| `internal/storage/localfs` | a local directory (Linux and macOS) | reference backend |
| `internal/storage/s3` | Amazon S3 and S3-compatible stores | tested against the RustFS emulator only; one conformance subtest is skipped there (see below) |
| `internal/storage/azureblob` | Azure Blob Storage | tested against the Azurite emulator only |

## Amazon S3 and S3-compatible stores

Package `internal/storage/s3` implements the interface on one S3 bucket with the AWS SDK for Go v2 (`github.com/aws/aws-sdk-go-v2/service/s3`).

### How the contract maps onto S3

| Contract | S3 |
|---|---|
| Create | One `PutObject` with `If-None-Match: *`. A `412` is a precondition failure. |
| Replace | One `PutObject` with `If-Match: <token>`. A `412`, or a `404` for a missing key, is a precondition failure. An empty token, or one that is not a quoted entity tag (such as `*`), fails without a request. |
| Delete | One `DeleteObject`, which S3 answers with success for a missing key too. |
| Version token | The object's ETag. |
| Get, GetRange | One ranged `GetObject`; the size and token come from the same response as the body. |
| Head | One `HeadObject`. |
| List | `ListObjectsV2` with the prefix, `StartAfter` and `MaxKeys`, repeated from the last key returned until the page is full or the listing ends. |

**Version tokens.** S3 derives an ETag from the stored bytes, so two writes of the same content would share one, which the contract forbids. The backend therefore appends a 32-byte trailer to every object it writes: the bytes `SKYLS3T1` and 24 random bytes, new for every write. The same 24 bytes are stored, in hex, in the object's metadata as `x-amz-meta-skylite-id`. The trailer makes every write's bytes, and so its ETag, unique, and the token a Replace is conditioned on names that one write. Through the interface the trailer is invisible: sizes exclude it and no read returns it. An object without that metadata, or shorter than the trailer, is rejected with an error rather than misread. Bucket versioning is not needed.

**Upload path.** A write first copies the caller's body to a spool file in `SpoolDir`, then sends it with a single `PutObject`. Nothing is sent before the body has ended, and S3 never stores part of a `PutObject`, so a failed or cancelled write leaves nothing behind in the bucket. The spool file is unlinked as soon as it is created on Linux and macOS, so even a killed process leaves no file; elsewhere it is removed when the call returns. Memory per write is bounded by a 32 KiB copy buffer and the SDK's own per-request buffers: writing a 64 MiB body allocated about 0.2 MiB in the backend's test. Disk use per write is the body size plus 32 bytes. Multipart upload is not used.

**Limits.** A body may be at most 5 GiB minus the 32-byte trailer (5,368,709,088 bytes), S3's limit for one `PutObject`. A larger body fails with `storage.ErrInvalidArgument` and no effect. Keys are used as object names unchanged. The bucket must exist, and nothing else may write under it.

**Retries and unknown outcomes.** The SDK's standard retryer runs for every operation, with up to three attempts, its client-side retry quota turned off, and S3's `409 ConditionalRequestConflict` added to the errors it retries. The backend classifies every attempt of a write or a Delete: an HTTP answer below 500 other than success had no effect, and so did a connection that failed before the whole request was written; a 5xx, a success that could not be decoded, or a connection lost after the whole request was written may have had an effect. The SDK may retry a write or a Delete only while every attempt so far had no effect, so no attempt of a call is still pending once the call has returned, unless its outcome is reported unknown. A write that failed definitely returns a definite error. If its attempt may have had an effect, the backend reads the object's metadata: if it carries this write's random identity, the write succeeded, at that object's token; otherwise the error wraps `storage.ErrOutcomeUnknown`, never a precondition failure. A Delete whose attempt may have had an effect returns an error wrapping `storage.ErrOutcomeUnknown`. Reads are retried as the SDK decides.

### Configuration

| `Config` field | Meaning |
|---|---|
| `Bucket` | The bucket. It must exist. Required. |
| `Region` | The bucket's region, for request signing. Required. |
| `Credentials` | An `aws.CredentialsProvider` that signs every request. Required. |
| `Endpoint` | Base URL of an S3-compatible service or emulator, such as `http://127.0.0.1:9000`. Empty means Amazon S3. |
| `UsePathStyle` | Put the bucket in the URL path instead of the host name, as most emulators need. |
| `SpoolDir` | Directory for spool files. Empty means the operating system's temporary directory. |

### What was tested

- The conformance suite, `storagetest.Run`, runs against [RustFS](https://github.com/rustfs/rustfs) 1.0.5 (`rustfs/rustfs:1.0.5`, pinned by digest in the `Makefile`), with its data in memory. `make integration-s3` starts the emulator and runs it; CI runs the same target as the `integration-s3` job. Every subtest passes except `ValidKeys`, which `make integration-s3` skips: RustFS rejects any key with a `/`-separated segment longer than 255 bytes with `400 InvalidArgument`, because it stores each segment as a directory entry, while S3 accepts any key up to 1,024 bytes, so the suite's 1,024-byte single-segment key fails on the emulator. The backend's own `TestValidKeys` makes the same checks on the emulator with that one key split into segments of at most 255 bytes, and passes: after Create and after Replace it reads each key with Head, a bounded GetRange, Get and a List with the key as its prefix, and it lists the namespace after each Delete.
- The backend's own tests run against the same emulator with a fault-injecting HTTP transport between the SDK and the service: responses lost after a Create, a Replace and a Delete were applied; requests failed before they were sent; connections cut in the middle of a write's body and of a read's response; responses slower than the deadline; requests still in flight when the call returned; 500 and 503 answers before and after the service applied a write; listings served in short pages; and the retries the SDK makes on its own. They also check the stored format, foreign objects, the size limit, the interleavings of conditional writes with other clients' writes, that credentials stay out of error text, that no goroutine outlives a call, and the memory a large write uses. Further tests answer the SDK from a fake transport, with no service: reads denied or failing with a server error, which must not be reported as a missing key; writes denied or answered with a missing bucket, which must not be reported as a failed condition; a range read whose object grows after a `416`, or whose `HeadObject` then fails; ranged answers whose body must be closed and not returned; a write answered without an ETag; a context cancelled inside a body that never ends; and a listing prefix of exactly 1,024 bytes.
- RustFS enforced `If-None-Match: *` and `If-Match` under concurrency: in the backend's own test, 32 independent clients raced 30 rounds of Create and 30 of Replace on one key, and each round had exactly one winner whose bytes were stored.

### What was not tested

- Amazon S3 itself, with real credentials, real latency, real throttling or server-side encryption. Every result above comes from one emulator.
- Keys with a segment longer than 255 bytes, on any service: RustFS cannot store them.
- Other emulators. Moto 5.2.5 accepts long keys, but after 16 concurrent Replaces of 17 MiB bodies the stored object was not the one the only successful Replace wrote; and LocalStack 4.14.0 stalled reads for over 10 seconds in the suite's concurrent-read test; neither validates request signatures.
- S3's `409 ConditionalRequestConflict`, which no emulator produces; it was only injected by the test transport.
- Clients in different processes or on different hosts. The suite's clients are independent SDK clients with their own connections in one process; the conditions themselves are evaluated by the service.

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

**Version tokens.** The token is the blob's ETag, quotes included, and `Replace` sends it unchanged in `If-Match`. Azure documents that a blob's ETag changes on every write. The contract also needs an ETag never to repeat for a key, also after the blob is deleted and created again. Azure does not document that. The backend uses the ETag as the token on the assumption, not a documented guarantee, that the service never issues the same ETag twice for one blob name. That has not been verified against the service. If it proves false, the backend must change to a per-write identity stored with the blob and compared under a blob lease. A version that is not a quoted entity tag, `*` included, fails `Replace` without a request.

**Conditions.** A write's condition is checked by the service inside its one Put Blob request, so among concurrent writers exactly one succeeds. 409 `BlobAlreadyExists` and 412 are precondition failures, and so is 404 `BlobNotFound` for `Replace`.

**Upload path.** Every write is one Put Blob request; the backend never stages blocks. Azurite counts a blob that has only uncommitted blocks as existing for `If-None-Match: *`, and the real service's behaviour there is not documented, so an interrupted staged upload could block a later `Create`. Put Blob needs the body's length, so a write reads its whole body first. Up to 4 MiB stays in memory; a longer body is written to a spool file in a directory the caller chooses. The file is unlinked as soon as it is created and closed when the write returns, so it never outlives the write. A body longer than 5,000 MiB, the most one Put Blob accepts, fails with `ErrInvalidArgument` before anything is sent.

**Unknown outcomes and retries.** The SDK's retries are turned off for `Create`, `Replace` and `Delete`, so a retry can never meet a write's own effect and turn its success into a precondition failure. A mutation returns `ErrOutcomeUnknown` when its request may have reached the service and no definite answer came back: a lost response, a reset connection, the context ending while the request is in flight, a 5xx, a 408, a 2xx or 3xx other than the one the request expects (201 for Put Blob, 202 for Delete Blob), which only a non-conforming proxy sends, or any failure after a response arrived, such as a response body that breaks off or a header that does not parse. Any other 4xx is a definite error. A mutation also returns a definite error when the service cannot have applied the request: the body was not read to its end, or no response came back and either net/http tried to connect to the container's endpoint, or the proxy for it, and obtained no connection, or there was no such attempt and the error is one the SDK marks as raised before the request (for example, the credential got no token). Any other error is `ErrOutcomeUnknown`. A credential that calls its token endpoint with the call's context shares the call's connection trace, so connections to other hosts do not count. The backend cannot always tell a request that was never sent from one that was sent and lost, and then answers `ErrOutcomeUnknown` although nothing was sent, as the contract allows: for `Delete` and an empty `Create` when the context ends while the credential waits for a token, when the credential reaches its token endpoint through the container's proxy, and when the SDK fails before sending with an error it does not mark that way. Once a call returns, its request body refuses further reads, so a request still being sent cannot complete later. Reads keep the client's retries.

**Listing.** The service may return fewer entries than requested, so `List` keeps requesting until it has the number asked for or the listing ends. List Blobs has no start-after parameter on a flat namespace, so `List` reads the keys under the prefix up to `startAfter` and skips them, up to 5,000 per request.

**Errors.** An error from the service keeps only its HTTP status and Azure error code. Other errors keep their text with every URL query string removed, since a SAS URL carries its signature there.

### Configuration

`azureblob.New(client, spoolDir)` takes a `*container.Client` from the Azure SDK for Go (`github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container`) and the spool directory (empty means the system temporary directory). The package does not choose a credential: a caller builds the client with a shared key, a SAS URL, or `container.NewClient` and any `azcore.TokenCredential`, which is how Entra ID, including workload identity, can be wired in later. The container must already exist, nothing else may write to it, and the client's transport must be the SDK's default net/http transport or an `*http.Client`. The connection trace names a request's first hop, so only a proxy that the environment names (`http.ProxyFromEnvironment`, as in the SDK's default) is recognised in it; through another proxy the backend classifies outcomes as if the transport reported no trace.

The transport must also close an idle connection before the service, or a load balancer or proxy in between, does: set `http.Transport.IdleConnTimeout` below the shortest idle timeout on the path. The SDK's default transport keeps idle connections for 90 seconds, and Azurite closes them after 5. A mutation is never retried, so a write sent on a pooled connection that the other end is closing fails, often with `ErrOutcomeUnknown`. The tests set 2 seconds.

### Limits

- The spool file relies on Linux or macOS, where an open file can be unlinked.
- A `List` with `startAfter` costs requests in proportion to the keys before `startAfter` under the prefix.
- Blobs with snapshots cannot be deleted; the backend never creates snapshots.
- The SDK is pinned at azblob v1.8.0. Version 1.8.1 and later add Apache Arrow (`arrow-go`) to the dependency graph, so Dependabot is told to ignore them.

### What was tested

Tested against Azurite 3.37.0 (`mcr.microsoft.com/azure-storage/azurite`, pinned by digest in the Makefile), with Azurite's well-known development account and a container SAS:

- the full conformance suite, `storagetest.Run`;
- this backend's own tests, with a fault-injecting HTTP transport between the SDK and Azurite: responses lost after the service applied a `Create`, a `Replace` and a `Delete`; a refused connection and a request that failed before its body was read; a connection cut in the middle of the body; a request still being sent when its call returned; a response slower than the context's deadline, also through a transport that reports no connection trace; through such a transport, an applied request whose response body then fails, and one whose success has an unparseable `Date` header; 500, 503 and 408 before and after the effect; 400, 403 and a 409 with another code than `BlobAlreadyExists`; a 200 after the effect; that a successful write sends one request and returns the token of its response; a deleted container; writes on connections idle for about as long as Azurite keeps them; read retries; listings in short pages, including one longer than `storage.MaxListLimit`, and one whose second page keeps failing; a 416 for a blob that has grown; the size limit; the spool directory; and redaction of the SAS signature from the errors of every call;
- against local HTTP servers, token credentials that fail: one before any request, one whose token endpoint answers 500, one whose token endpoint refuses the connection, and one that gets a token for a container endpoint that refuses the connection; and a token credential with a transport that reports no connection trace, against a server whose answers' bodies break off.

`make integration` runs them in CI. It keeps Azurite's data on the container's disk: Azurite frees the data of deleted blobs only every 10 minutes, and the suite writes more than its in-memory store holds on a CI runner.

Not tested: the real Azure Blob Storage service, working real credentials (Entra ID, workload identity, managed identity, account keys of a real account), real latency and throttling, a proxy between the backend and the service, and the assumption that an ETag never repeats for a key. Azurite derives its ETags from the time in milliseconds multiplied by a random factor, so on the emulator two writes in the same millisecond can, rarely, share one.
