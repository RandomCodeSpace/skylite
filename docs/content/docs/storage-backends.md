---
title: Storage backends
description: The storage backends that exist as library packages, how each maps the storage contract onto its service, and what has been tested.
weight: 25
status: current
---

Every durable byte in skylite is meant to go through one storage interface, `storage.Store` in `internal/storage`. Its package documentation is the contract: keys, create-if-absent and compare-and-swap writes, version tokens, range reads, listing, and the rule for writes whose outcome is unknown. `internal/storage/storagetest` is the conformance suite that every backend must pass, and each backend carries its own tests for what the suite cannot check from one process.

The backends are library packages inside the repository. The server cannot be configured to use any of them yet: the `skylite` binary only prints its version and help.

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
| Concurrent writers | Exactly one of concurrent Creates of a key, or of Replaces from one version, succeeds across every process and host that shares the bucket, if the service evaluates each condition when it commits the object. For a single-request upload that is the request itself. For a resumable upload, used from 16 MiB on, it is an assumption: fake-gcs-server evaluates the condition at the last request of the session, and Google does not document when the service does. If the service evaluated it only when the session opens, two writers could both succeed. This must be checked against the real service before the server uses this backend. |
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
- Go's `net/http` can still send a request twice on its own, below the store's transport: over HTTP/2 it re-sends a request whose stream the server reset with `PROTOCOL_ERROR`, a code that does not say whether the server processed it. The store counts the connections `net/http` asks for to send a commit request. If it asked more than once, every error of that call, a 412, another 4xx or a refused connection included, wraps `storage.ErrOutcomeUnknown`.
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
