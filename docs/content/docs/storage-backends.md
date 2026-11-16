---
title: Storage backends
description: The storage interface's backends, how each maps the interface onto its service, and what has been tested.
weight: 25
status: current
---

Every durable byte in skylite is to go through one interface, the `storage.Store` of package `internal/storage`. Its package documentation is the contract: create-if-absent and compare-and-swap as the only writes, version tokens that never repeat for a key, strongly consistent reads and listings, and a rule for writes whose outcome is unknown. Package `internal/storage/storagetest` is the conformance suite every backend must pass.

The backends below are library packages. The server cannot be configured to use any of them yet.

| Package | Service | Status |
|---|---|---|
| `internal/storage/localfs` | a local directory (Linux and macOS) | reference backend |
| `internal/storage/s3` | Amazon S3 and S3-compatible stores | tested against the RustFS emulator only; one conformance subtest is skipped there (see below) |

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

- The conformance suite, `storagetest.Run`, runs against [RustFS](https://github.com/rustfs/rustfs) 1.0.5 (`rustfs/rustfs:1.0.5`, pinned by digest in the `Makefile`), with its data in memory. `make integration-s3` starts the emulator and runs it; CI runs the same target as the `integration-s3` job. Every subtest passes except `ValidKeys`, which `make integration-s3` skips: RustFS rejects any key with a `/`-separated segment longer than 255 bytes with `400 InvalidArgument`, because it stores each segment as a directory entry, while S3 accepts any key up to 1,024 bytes, so the suite's 1,024-byte single-segment key fails on the emulator. The backend's own `TestValidKeys` makes the same checks on the emulator with that one key split into segments of at most 255 bytes, and passes.
- The backend's own tests run against the same emulator with a fault-injecting HTTP transport between the SDK and the service: responses lost after a Create, a Replace and a Delete were applied; requests failed before they were sent; connections cut in the middle of a write's body and of a read's response; responses slower than the deadline; requests still in flight when the call returned; 500 and 503 answers before and after the service applied a write; listings served in short pages; and the retries the SDK makes on its own. They also check the stored format, foreign objects, the size limit, the interleavings of conditional writes with other clients' writes, that credentials stay out of error text, that no goroutine outlives a call, and the memory a large write uses. Further tests answer the SDK from a fake transport, with no service: reads denied or failing with a server error, which must not be reported as a missing key; a range read whose object grows after a `416`, or whose `HeadObject` then fails; ranged answers whose body must be closed and not returned; a write answered without an ETag; a context cancelled inside a body that never ends; and a listing prefix of exactly 1,024 bytes.
- RustFS enforced `If-None-Match: *` and `If-Match` under concurrency: in the backend's own test, 32 independent clients raced 30 rounds of Create and 30 of Replace on one key, and each round had exactly one winner whose bytes were stored.

### What was not tested

- Amazon S3 itself, with real credentials, real latency, real throttling or server-side encryption. Every result above comes from one emulator.
- Keys with a segment longer than 255 bytes, on any service: RustFS cannot store them.
- Other emulators. Moto 5.2.5 accepts long keys, but after 16 concurrent Replaces of 17 MiB bodies the stored object was not the one the only successful Replace wrote; and LocalStack 4.14.0 stalled reads for over 10 seconds in the suite's concurrent-read test; neither validates request signatures.
- S3's `409 ConditionalRequestConflict`, which no emulator produces; it was only injected by the test transport.
- Clients in different processes or on different hosts. The suite's clients are independent SDK clients with their own connections in one process; the conditions themselves are evaluated by the service.
