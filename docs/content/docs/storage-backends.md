---
title: Storage backends
description: The storage interface's backends, how each maps the interface onto its service, and what has been tested.
weight: 25
status: current
---

Every durable byte in skylite is to go through one interface, the `storage.Store` of package `internal/storage`. Its package documentation is the contract: create-if-absent and compare-and-swap as the only writes, version tokens that never repeat for a key, strongly consistent reads and listings, and a rule for writes whose outcome is unknown. Package `internal/storage/storagetest` is the conformance suite every backend must pass.

Each cloud backend is tested against an emulator (RustFS, fake-gcs-server, Azurite). The Azure backend has also been run once by hand against the real service. The [server](/docs/server/) can be configured to use `localfs`, `s3`, `gcs` or `azure`.

| Package | Service | Status |
|---|---|---|
| `internal/storage/localfs` | a local directory (Linux and macOS) | reference backend |
| `internal/storage/s3` | Amazon S3 and S3-compatible stores | tested against the RustFS emulator only; one conformance subtest is skipped there (see below) |
| `internal/storage/gcs` | Google Cloud Storage | tested against the fake-gcs-server emulator only |
| `internal/storage/azureblob` | Azure Blob Storage | tested against the Azurite emulator; the GORM acceptance suite run once by hand against the real service |

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

## Google Cloud Storage

Package `internal/storage/gcs` stores each key as an object in one existing bucket, through the JSON API and Google's Go client, `cloud.google.com/go/storage` v1.70.0.

### How the contract maps onto the service

| Contract | Google Cloud Storage |
|---|---|
| Key | The object name, unchanged. |
| Version token | The object's generation, in decimal. Google documents that a generation is unique to one version of an object and that a new object with the same name gets a different generation. |
| Create | Upload with `ifGenerationMatch=0`. The service answers 412 with reason `conditionNotMet` if the object exists. |
| Replace | Upload with `ifGenerationMatch=<generation>`. A token that is not a generation the store could have returned fails without a request. |
| Delete | Unconditional `DELETE`; 404 counts as success. |
| Concurrent writers | Exactly one of concurrent Creates of a key, or of Replaces from one version, succeeds across every process and host that shares the bucket, if the service evaluates each condition when it commits the object. For a single-request upload that is the request itself. For a resumable upload, used from 16 MiB on, it is an assumption: fake-gcs-server evaluates the condition at the last request of the session, and Google does not document when the service does. If the service evaluated it only when the session opens, two writers could both succeed. The [server](/docs/server/) uses this backend, and this condition has not been checked against the real service. |
| Range reads | A `Range` request. A zero-length range is answered from the object's metadata, and a range that starts at or past the end (416) is answered with an empty body once the metadata confirms the object still ends there. A download that the client resumes after a broken connection is pinned to the generation it started at. |
| Listing | `objects.list` with `prefix` and `startOffset`. The client keeps requesting pages until the listing has `limit` entries or ends, so short pages from the service are filled. |

### Uploads

A write streams its body in chunks of 16 MiB, the client's default. A body that ends within the first chunk is sent as one request after the body has ended. A longer body goes through a resumable upload session: every chunk but the last is sent while the body is read, and the object exists only when the last request of the session completes. Memory per write is bounded by one chunk plus a 256 KiB copy buffer. A body that reports its length with a `Len() int` method, such as a `bytes.Reader`, `strings.Reader` or `bytes.Buffer`, and is shorter than a chunk gets a chunk of its length plus one byte, rounded up by the client to a multiple of 256 KiB, so a small write does not allocate 16 MiB. There is no size limit beyond the service's 5 TiB per object.

### Unknown outcomes and retries

The request that can make a mutation take effect, the commit request, is the single-request upload, the last request of a resumable session, or the `DELETE`. The store marks in its own HTTP transport when a call's commit request starts.

- An error before that point means the mutation had no effect.
- After it, an error wraps `storage.ErrOutcomeUnknown`, unless the service answered 412 with reason `conditionNotMet` (a precondition failure), another 4xx other than 408, or the connection was never made. A 412 with any other reason, such as `orgPolicyConstraintFailed`, is returned as the service's error, with no effect, not as a precondition failure.
- The client library retries reads, listings and the request that opens a resumable session, because none of them can publish anything or carries any of the body. It never retries a commit request. A retried Create or Replace whose first attempt took effect would come back as a precondition failure, and a retried Delete could remove an object created later.
- It does not retry a chunk of a resumable session either. In a fault test the emulator stored a retried chunk that it had already stored a second time, and the client then reported success for an object longer than its body. Google's documentation does not say how the service treats a chunk it already has. A chunk that fails therefore fails the write, with no effect, and the caller writes again.
- Go's `net/http` can still send a request twice on its own, below the store's transport: over HTTP/2 it re-sends a request whose stream the server reset with `PROTOCOL_ERROR`, a code that does not say whether the server processed it. The store counts the attempts `net/http` makes at a commit request: each connection it hands the request, and each connection it asks for after one. If there was more than one attempt, every error of that call, a 412, another 4xx or a refused connection included, wraps `storage.ErrOutcomeUnknown`.
- A failure to fetch an access token happens before the commit request is sent, so it is a definite error.

### Configuration

`gcs.Open(ctx, gcs.Config{Bucket: "...", Endpoint: "..."})`. The store keeps working after `ctx` ends: credentials that fetch tokens later get its values but not its cancellation.

- `Bucket`: the name of an existing bucket. The bucket must not use object versioning, retention policies or holds.
- `Endpoint`: optional. If empty, the store talks to Google with Application Default Credentials and the `devstorage.read_write` scope (GKE workload identity, a service-account key named by `GOOGLE_APPLICATION_CREDENTIALS`, or the metadata server). If set, for example to an emulator's `http://127.0.0.1:4443/storage/v1/`, requests carry no credentials. `Open` refuses to use credentials while `STORAGE_EMULATOR_HOST` is set, because the client would send them to that host.

Error texts drop URL query strings, because a resumable session URL carries an `upload_id` that grants access to the upload.

### What was tested

Against fake-gcs-server 1.56.1 with its in-memory backend, the image pinned by digest in the `Makefile` (`make integration-gcs`, and the `integration-gcs` CI job):

- the whole conformance suite;
- the backend's own fault tests, with a fault-injecting HTTP transport between the client and the emulator: a lost response after the service applied a Create, a Replace and a Delete; a commit request that was never sent; a connection cut in the middle of the body; a response held past the context's deadline; 500 and 503 before and after the effect; a commit request still pending after its call returned; which requests the client retries on its own (the session request and reads) and which it must not (chunks and commit requests); listings served in pages of one and two entries, and a listing whose second page is refused; a body whose last read returns data together with the end of the body; a process killed after the first chunk of a resumable upload, after its commit request was sent and after the response arrived; bodies of exactly one and two chunks and one byte either side; an upload interrupted inside its resumable session; and conditional writes forced to meet at the service.

Without the emulator, with HTTP/2, credentials and responses faked locally: a commit request that `net/http` re-sends after an HTTP/2 stream reset, for a single-request upload, the last request of a resumable session and a `DELETE`, and a re-send whose new connection is refused; a token endpoint that refuses the credentials; credentials used after the context given to `Open` has ended; successful responses that carry no generation; and the memory a small write allocates.

The emulator ignores `ifGenerationMatch` on a delete and `ifMetagenerationMatch`. The backend uses neither.

### What was not tested

- The real Google Cloud Storage service. Its behaviour where the emulator may differ is unverified: when a resumable upload evaluates `ifGenerationMatch`, the headers of a 416 response, how it answers a repeated chunk, its page sizes, which 412 reason it sends when an organization policy refuses a write, and whether its front end resets with `PROTOCOL_ERROR` an HTTP/2 stream it has applied.
- Real credentials. The Application Default Credentials path has never run.
- Real latency, throttling (429) and service-side 5xx errors.

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

**Unknown outcomes and retries.** The SDK's retries are turned off for `Create`, `Replace` and `Delete`, so a retry can never meet a write's own effect and turn its success into a precondition failure. Go's `net/http` can still send a request twice on its own: over HTTP/2 it re-sends a request whose stream the server reset with `PROTOCOL_ERROR`, and it retries on a new connection a request it wrote nothing of on a reused one. An answer or a failed dial may then belong to the second attempt, after a first that took effect. The backend proves a single attempt from `net/http`'s client trace of the call's context: the connections `net/http` hands the request and the connections it asks for. A failed mutation is a definite error, with no effect, only when the error and that trace prove it:

- the transport did not read the write's body to its end (so a 409 or 412 answering a re-send whose body was not read to its end is a definite error, but not a precondition failure);
- the SDK failed with an error it marks non-retriable (the credential got no token, a token would go over plain HTTP, or a 401 challenge failed), and the trace saw at most one connection handed to the request and none asked for after it;
- the trace saw exactly one connection handed to the request and none asked for after it, and the service answered 409 `BlobAlreadyExists` or 412 (a precondition failure), 404 `BlobNotFound` for `Replace` (a precondition failure), or any other 4xx except 408 (a definite error). The backend assumes that such a 4xx comes from the service, or from an intermediary that refused the request itself, never from one that forwarded the request and replaced the answer;
- the trace saw a connection asked for and none handed to the request, and the error is `net/http`'s failed dial (`dial`, or `proxyconnect` through a proxy), so no connection was made.

The client's transport must pass the request's context on to `net/http`, as the SDK's default transport, any `*http.Transport` and any `*http.Client` do. A transport that drops it is not supported: a credential that fetches its token over HTTP with the call's context then puts its own connections in the trace, and a failed mutation that took effect can be reported as having none. With a transport that passes the context on, a credential's connections can only turn an answer into `ErrOutcomeUnknown`, and a non-retriable error the SDK raises after a 401 challenge counts as no effect only when the trace shows a single attempt. Everything else is `ErrOutcomeUnknown`: a lost response, a reset connection, `EOF`, the context ending, any other transport error, a 5xx, a 408, a 2xx or 3xx other than the one the request expects (201 for Put Blob, 202 for Delete Blob), which only a non-conforming proxy sends, and any failure after a response arrived, such as a response body that breaks off or a header that does not parse. The backend therefore answers `ErrOutcomeUnknown` for some requests that were never sent or had no effect, as the contract allows when a backend cannot tell: for `Delete` and a write with an empty body when the context ends while the credential waits for a token or before the transport dials, or when a refused connection follows a credential's own connection in the trace; for any request when the credential fails with an error the SDK does not mark non-retriable, or fails with one it does mark after its own connections put more than one in the trace, when the SDK fails before sending with such an error (for example, a shared key that cannot sign), when a connection fails in another way than `net/http`'s dial error, such as a TLS handshake failure or any failure through a transport that hides `net/http`'s errors, or when `net/http` retries on a new connection a request it wrote nothing of on a reused one (an HTTP/2 connection found unusable, or one the server closed with `GOAWAY`) and the new dial is refused; and, through a transport that does not pass the request's context on (not supported, see above), for a refused connection and any 4xx. Once a call returns, its request body refuses further reads, so a request still being sent cannot complete later. Reads keep the client's retries.

**Listing.** The service may return fewer entries than requested, so `List` keeps requesting until it has the number asked for or the listing ends. List Blobs has no start-after parameter on a flat namespace, so `List` reads the keys under the prefix up to `startAfter` and skips them, up to 5,000 per request.

**Errors.** An error from the service keeps only its HTTP status and Azure error code. Other errors keep their text with every URL query string removed, since a SAS URL carries its signature there.

### Configuration

`azureblob.New(client, spoolDir)` takes a `*container.Client` from the Azure SDK for Go (`github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container`) and the spool directory (empty means the system temporary directory). The package does not choose a credential: a caller builds the client with a shared key, a SAS URL, or `container.NewClient` and any `azcore.TokenCredential`, which is how Entra ID, including workload identity, can be wired in later. The container must already exist and nothing else may write to it. Any transport works, with two limits: a refused connection is a definite error only if the transport returns net/http's dial error, and through a transport that hides it the failure is `ErrOutcomeUnknown`; and the transport must not send a request again on its own, except for net/http's own re-sends, which the backend detects only if the transport passes the request's context, with its connection trace, on to net/http.

The transport must also close an idle connection before the service, or a load balancer or proxy in between, does: set `http.Transport.IdleConnTimeout` below the shortest idle timeout on the path. The SDK's default transport keeps idle connections for 90 seconds, and Azurite closes them after 5. A mutation is never retried, so a write sent on a pooled connection that the other end is closing fails, often with `ErrOutcomeUnknown`. The server and the tests set 2 seconds.

In `skylite serve`, `storage.azure` takes `account` and `container` (required), an optional `endpoint` (empty means `https://<account>.blob.core.windows.net`), and exactly one of `shared_key_file` (path to a file holding the base64 shared key), `shared_key_env` (name of an environment variable holding it) and `entra: true`. With `entra`, requests carry an Entra ID token from `azidentity`'s default credential chain: a service principal in `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and `AZURE_CLIENT_SECRET`, workload identity, or a managed identity. That identity needs the Storage Blob Data Contributor role on the container or its account.

### Limits

- The spool file relies on Linux or macOS, where an open file can be unlinked.
- A `List` with `startAfter` costs requests in proportion to the keys before `startAfter` under the prefix.
- Blobs with snapshots cannot be deleted; the backend never creates snapshots.
- The SDK is pinned at azblob v1.8.0. Version 1.8.1 and later add Apache Arrow (`arrow-go`) to the dependency graph, so Dependabot is told to ignore them.

### What was tested

Tested against Azurite 3.37.0 (`mcr.microsoft.com/azure-storage/azurite`, pinned by digest in the Makefile), with Azurite's well-known development account and a container SAS:

- the full conformance suite, `storagetest.Run`;
- this backend's own tests, with a fault-injecting HTTP transport between the SDK and Azurite: responses lost after the service applied a `Create`, a `Replace` and a `Delete`; a refused connection (net/http's dial error) and a request that failed before its body was read; a connection cut in the middle of the body; a request still being sent when its call returned; a response slower than the context's deadline, also through a transport that reports no connection trace; through such a transport, an applied request whose response body then fails, and one whose success has an unparseable `Date` header; 500, 503 and 408 before and after the effect; 400, 403 and a 409 with another code than `BlobAlreadyExists`; a 200 after the effect; that a successful write sends one request and returns the token of its response; a deleted container; writes on connections idle for about as long as Azurite keeps them; read retries; listings in short pages, including one longer than `storage.MaxListLimit`, and one whose second page keeps failing; a 416 for a blob that has grown; the size limit; the spool directory; and redaction of the SAS signature from the errors of every call;
- against local HTTP servers, token credentials that fail: one before any request, one whose token endpoint answers 500, one whose token endpoint refuses the connection; a token followed by a container endpoint that refuses the connection, definite without a token request and unknown after one; a token credential with a transport that reports no connection trace, against a server whose answers' bodies break off; a refused container endpoint through net/http's transport (definite) and through one that drops the connection trace (unknown); a transport that reports a second attempt in the trace and then fails a dial or answers 409, 412 or, for `Delete`, 404 `BlobNotFound` (a success); a transport that reports one attempt and answers 404 `BlobNotFound` (a precondition failure for `Replace`, a success for `Delete`) or fails with a read error or a dial error (unknown), a transport that reports a second attempt and then fails with an error the SDK marks non-retriable (unknown), and one that reports a connection asked for and none handed over and fails a `proxyconnect` (definite) or a read or write (unknown); and a local HTTP/2 server that applies a request and resets its stream with `PROTOCOL_ERROR`, so `net/http` re-sends it, with the trace intact and dropped, the re-send answered 409 or refused at the dial (unknown in every case); and a credential that fails to reach a closed proxy, with a transport that drops the trace, against a server that applies the request and closes the connection without an answer.

`make integration` runs them in CI, together with the server's `PUT` test against Azurite (`TestPutOnAzure`, with both a shared-key file and an environment variable). It keeps Azurite's data on the container's disk: Azurite frees the data of deleted blobs only every 10 minutes, and the suite writes more than its in-memory store holds on a CI runner.

Run once by hand against the real Azure Blob Storage service, not in CI: the GORM acceptance suite on `skylite serve` with `entra: true` and an Entra ID service principal (client secret) that holds Storage Blob Data Contributor. All of its tests passed, and the objects were in the container afterwards.

Not tested: this backend's own conformance and fault tests against the real service, workload identity, managed identity, account keys of a real account, throttling, HTTP/2 against the service, a proxy between the backend and the service, and the assumption that an ETag never repeats for a key. Azurite derives its ETags from the time in milliseconds multiplied by a random factor, so on the emulator two writes in the same millisecond can, rarely, share one.
