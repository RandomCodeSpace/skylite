//go:build integration

package durability

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"
)

// endpointEnv names the Azurite blob endpoint, for example
// http://127.0.0.1:10000/devstoreaccount1. `make integration` sets it.
const endpointEnv = "SKYLITE_TEST_AZURITE_BLOB_ENDPOINT"

// Azurite's documented default development account. The key is public and
// only works against the emulator.
const (
	azuriteAccount = "devstoreaccount1"
	azuriteKey     = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

func azuriteEndpoint(t testing.TB) string {
	t.Helper()
	ep := os.Getenv(endpointEnv)
	if ep == "" {
		t.Skipf("%s is not set; run `make integration` or point it at an Azurite blob endpoint", endpointEnv)
	}
	return ep
}

type countPolicy struct{ n *atomic.Int64 }

func (p countPolicy) Do(req *policy.Request) (*http.Response, error) {
	p.n.Add(1)
	return req.Next()
}

// newAzure returns a client for Azurite. Every HTTP attempt, retries
// included, is counted in requests when it is not nil.
func newAzure(t testing.TB, requests *atomic.Int64) *azblob.Client {
	t.Helper()
	cred, err := azblob.NewSharedKeyCredential(azuriteAccount, azuriteKey)
	if err != nil {
		t.Fatal(err)
	}
	opts := &azblob.ClientOptions{}
	// Litestream's Azure client retries up to 10 times; keep that so a lost
	// response exercises the identical-content rule the same way.
	opts.Retry = policy.RetryOptions{MaxRetries: 10, RetryDelay: 200 * time.Millisecond, MaxRetryDelay: 2 * time.Second, TryTimeout: time.Minute}
	if requests != nil {
		opts.PerRetryPolicies = []policy.Policy{countPolicy{requests}}
	}
	az, err := azblob.NewClientWithSharedKeyCredential(azuriteEndpoint(t), cred, opts)
	if err != nil {
		t.Fatal(err)
	}
	return az
}

// newContainer creates a container unique to the test and removes it after.
func newContainer(t testing.TB, az *azblob.Client) string {
	t.Helper()
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	name := "spike-" + hex.EncodeToString(b)
	if _, err := az.CreateContainer(context.Background(), name, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = az.DeleteContainer(context.Background(), name, nil) })
	return name
}

// listing maps every blob in the container to its ETag.
func listing(t testing.TB, az *azblob.Client, container string) map[string]azcore.ETag {
	t.Helper()
	out := map[string]azcore.ETag{}
	pager := az.NewListBlobsFlatPager(container, nil)
	for pager.More() {
		page, err := pager.NextPage(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range page.Segment.BlobItems {
			out[*item.Name] = *item.Properties.ETag
		}
	}
	return out
}

// restoreFresh restores the latest history into a new empty directory with
// plain Litestream and opens it read-only.
func restoreFresh(ctx context.Context, t testing.TB, az *azblob.Client, container string) (*sql.DB, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "restored.sqlite")
	opt := litestream.NewRestoreOptions()
	opt.OutputPath = path
	if err := litestream.NewReplicaWithClient(nil, NewClient(az, container, DBPrefix)).Restore(ctx, opt); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, nil
}

func integrityCheck(db *sql.DB) error {
	var res string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&res); err != nil {
		return err
	}
	if res != "ok" {
		return fmt.Errorf("integrity_check: %s", res)
	}
	return nil
}

func respErr(err error) (int, string) {
	var re *azcore.ResponseError
	if errors.As(err, &re) {
		return re.StatusCode, re.ErrorCode
	}
	return 0, ""
}

func ifNoneMatchAny() *blob.AccessConditions {
	return &blob.AccessConditions{ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfNoneMatch: to.Ptr(azcore.ETagAny)}}
}

// failingReader returns its data and then an error, like a writer that dies
// part way through a multi-block upload.
type failingReader struct {
	r io.Reader
}

func (f *failingReader) Read(p []byte) (int, error) {
	n, err := f.r.Read(p)
	if errors.Is(err, io.EOF) {
		return n, errors.New("writer died")
	}
	return n, err
}

// abortOnceReader fails its first pass after cut bytes, so the first Put Blob
// attempt dies mid-body; the SDK rewinds it and retries.
type abortOnceReader struct {
	data    []byte
	cut     int
	off     int
	aborted bool
}

func (r *abortOnceReader) Read(p []byte) (int, error) {
	if !r.aborted && r.off >= r.cut {
		r.aborted = true
		return 0, errors.New("writer died")
	}
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	limit := len(r.data)
	if !r.aborted {
		limit = r.cut
	}
	n := copy(p, r.data[r.off:limit])
	r.off += n
	return n, nil
}

func (r *abortOnceReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		r.off = int(offset)
	case io.SeekCurrent:
		r.off += int(offset)
	case io.SeekEnd:
		r.off = len(r.data) + int(offset)
	}
	return int64(r.off), nil
}

func (r *abortOnceReader) Close() error { return nil }

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func TestEmulatorSemantics(t *testing.T) {
	ctx := context.Background()
	var requests atomic.Int64
	az := newAzure(t, &requests)
	container := newContainer(t, az)

	t.Run("CreateIfAbsentRejectsExisting", func(t *testing.T) {
		opts := &azblob.UploadBufferOptions{AccessConditions: ifNoneMatchAny()}
		if _, err := az.UploadBuffer(ctx, container, "exists", []byte("first"), opts); err != nil {
			t.Fatal(err)
		}
		_, err := az.UploadBuffer(ctx, container, "exists", []byte("second"), opts)
		status, code := respErr(err)
		t.Logf("Put Blob If-None-Match:* on an existing blob: HTTP %d %s", status, code)
		if status != http.StatusConflict || code != string(bloberror.BlobAlreadyExists) {
			t.Fatalf("got %d %q (%v), want 409 BlobAlreadyExists", status, code, err)
		}
		assertContent(t, az, container, "exists", []byte("first"))
	})

	t.Run("ConcurrentCreatesOneWins", func(t *testing.T) {
		const n = 32
		var wg sync.WaitGroup
		var wins atomic.Int64
		var winner atomic.Value
		for i := range n {
			wg.Go(func() {
				body := fmt.Appendf(nil, "writer-%d", i)
				_, err := az.UploadBuffer(ctx, container, "contended", body, &azblob.UploadBufferOptions{AccessConditions: ifNoneMatchAny()})
				if err == nil {
					wins.Add(1)
					winner.Store(body)
				} else if status, code := respErr(err); status != http.StatusConflict || code != string(bloberror.BlobAlreadyExists) {
					t.Errorf("writer %d: got %d %q (%v), want 409 BlobAlreadyExists", i, status, code, err)
				}
			})
		}
		wg.Wait()
		if wins.Load() != 1 {
			t.Fatalf("%d of %d conditional creates succeeded, want exactly 1", wins.Load(), n)
		}
		assertContent(t, az, container, "contended", winner.Load().([]byte))
	})

	t.Run("IfMatchRejectsStaleETag", func(t *testing.T) {
		resp, err := az.UploadBuffer(ctx, container, "cas", []byte("v1"), nil)
		if err != nil {
			t.Fatal(err)
		}
		stale := &azblob.UploadBufferOptions{AccessConditions: &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfMatch: resp.ETag},
		}}
		if _, err := az.UploadBuffer(ctx, container, "cas", []byte("v2"), stale); err != nil {
			t.Fatalf("first If-Match update: %v", err)
		}
		_, err = az.UploadBuffer(ctx, container, "cas", []byte("v3"), stale)
		status, code := respErr(err)
		t.Logf("Put Blob If-Match with a stale ETag: HTTP %d %s", status, code)
		if status != http.StatusPreconditionFailed || code != string(bloberror.ConditionNotMet) {
			t.Fatalf("got %d %q (%v), want 412 ConditionNotMet", status, code, err)
		}
		assertContent(t, az, container, "cas", []byte("v2"))
	})

	t.Run("MultiBlockHonoursPreconditionAtCommit", func(t *testing.T) {
		const size = 4<<20 + 123
		data := randomBytes(size)
		opts := &azblob.UploadStreamOptions{BlockSize: 1 << 20, AccessConditions: ifNoneMatchAny()}

		before := requests.Load()
		if _, err := az.UploadStream(ctx, container, "large", bytes.NewReader(data), opts); err != nil {
			t.Fatal(err)
		}
		if n := requests.Load() - before; n < 6 {
			t.Fatalf("upload of %d bytes took %d requests; want Put Block x5 + Put Block List", size, n)
		}

		// Stage the blocks one request at a time, so the test shows which
		// request is rejected: every Put Block is accepted, the Put Block
		// List is not.
		bb := az.ServiceClient().NewContainerClient(container).NewBlockBlobClient("large")
		other := randomBytes(size)
		var ids []string
		for off := 0; off < len(other); off += 1 << 20 {
			id := base64.StdEncoding.EncodeToString(fmt.Appendf(nil, "%08d", len(ids)))
			block := other[off:min(off+1<<20, len(other))]
			if _, err := bb.StageBlock(ctx, id, streaming.NopCloser(bytes.NewReader(block)), nil); err != nil {
				t.Fatalf("Put Block %d on an existing blob: %v", len(ids), err)
			}
			ids = append(ids, id)
		}
		if len(ids) != 5 {
			t.Fatalf("staged %d blocks, want 5", len(ids))
		}
		_, err := bb.CommitBlockList(ctx, ids, &blockblob.CommitBlockListOptions{AccessConditions: ifNoneMatchAny()})
		status, code := respErr(err)
		t.Logf("Put Block List If-None-Match:* on an existing blob: HTTP %d %s", status, code)
		if status != http.StatusConflict || code != string(bloberror.BlobAlreadyExists) {
			t.Fatalf("got %d %q (%v), want 409 BlobAlreadyExists", status, code, err)
		}
		assertContent(t, az, container, "large", data)
	})

	t.Run("InterruptedMultiBlockLeavesNoBlob", func(t *testing.T) {
		r := &failingReader{r: bytes.NewReader(randomBytes(3<<20 + 7))}
		_, err := az.UploadStream(ctx, container, "interrupted", r, &azblob.UploadStreamOptions{BlockSize: 1 << 20, AccessConditions: ifNoneMatchAny()})
		if err == nil {
			t.Fatal("upload from a failing reader succeeded")
		}
		_, err = az.ServiceClient().NewContainerClient(container).NewBlobClient("interrupted").GetProperties(ctx, nil)
		if status, code := respErr(err); status != http.StatusNotFound || code != string(bloberror.BlobNotFound) {
			t.Fatalf("after an interrupted upload, Get Blob Properties = %d %q (%v), want 404 BlobNotFound", status, code, err)
		}
		if _, ok := listing(t, az, container)["interrupted"]; ok {
			t.Fatal("interrupted upload is visible in the listing")
		}
		// The staged blocks are invisible, yet Azurite treats the name as
		// taken for If-None-Match. This is why the replica client uploads
		// every LTX file with a single Put Blob.
		_, err = az.UploadBuffer(ctx, container, "interrupted", []byte("ok"), &azblob.UploadBufferOptions{AccessConditions: ifNoneMatchAny()})
		status, code := respErr(err)
		t.Logf("Put Blob If-None-Match:* after an interrupted multi-block upload: HTTP %d %s", status, code)
		if status != http.StatusConflict || code != string(bloberror.BlobAlreadyExists) {
			t.Fatalf("got %d %q (%v); Azurite's behaviour for uncommitted blocks changed, revisit the single Put Blob rule", status, code, err)
		}
	})

	t.Run("AbortedPutBlobLeavesNothing", func(t *testing.T) {
		data := randomBytes(2<<20 + 11)
		r := &abortOnceReader{data: data, cut: 1 << 20}
		before := requests.Load()
		_, err := az.ServiceClient().NewContainerClient(container).NewBlockBlobClient("aborted").
			Upload(ctx, r, &blockblob.UploadOptions{AccessConditions: ifNoneMatchAny()})
		if err != nil {
			t.Fatalf("conditional Put Blob retried after an aborted body: %v", err)
		}
		if n := requests.Load() - before; n < 2 {
			t.Fatalf("Put Blob took %d request(s); the abort did not happen", n)
		}
		assertContent(t, az, container, "aborted", data)
	})

	t.Run("LeaseTakeover", func(t *testing.T) {
		a, err := AcquireLease(ctx, az, container, "a")
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Renew(ctx, az, container); err != nil {
			t.Fatalf("renew by holder: %v", err)
		}
		b, err := AcquireLease(ctx, az, container, "b")
		if err != nil {
			t.Fatal(err)
		}
		if b.Epoch != a.Epoch+1 {
			t.Fatalf("takeover epoch %d, want %d", b.Epoch, a.Epoch+1)
		}
		if err := a.Renew(ctx, az, container); !bloberror.HasCode(err, bloberror.ConditionNotMet) {
			t.Fatalf("renew by replaced holder = %v, want ConditionNotMet", err)
		}
		if err := b.Renew(ctx, az, container); err != nil {
			t.Fatalf("renew by new holder: %v", err)
		}
	})

	t.Run("IdenticalRetryIsSuccessOtherContentFences", func(t *testing.T) {
		container := newContainer(t, az)
		l, _, err := TakeOffice(ctx, az, container, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		if err := l.Exec(ctx, func(tx *sql.Tx) error {
			_, err := tx.Exec(`CREATE TABLE t (x)`)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		head := l.client.Head()
		_ = l.Close(ctx)

		mine := downloadLTX(t, az, container, 0, head)
		other := downloadLTX(t, az, container, 0, head-1)

		retry := NewClient(az, container, DBPrefix)
		retry.setHead(head - 1)
		if _, err := retry.WriteLTXFile(ctx, 0, head, head, bytes.NewReader(mine)); err != nil {
			t.Fatalf("rewrite of identical bytes: %v", err)
		}
		if retry.Fenced() != nil || retry.Head() != head {
			t.Fatalf("identical rewrite left fenced=%v head=%s, want unfenced at %s", retry.Fenced(), retry.Head(), head)
		}

		rival := NewClient(az, container, DBPrefix)
		rival.setHead(head - 1)
		if _, err := rival.WriteLTXFile(ctx, 0, head, head, bytes.NewReader(other)); !errors.Is(err, ErrFenced) {
			t.Fatalf("write of other bytes to a taken slot = %v, want ErrFenced", err)
		}
		if _, err := rival.WriteLTXFile(ctx, 0, head+1, head+1, bytes.NewReader(other)); !errors.Is(err, ErrFenced) {
			t.Fatalf("write after fencing = %v, want ErrFenced", err)
		}
		if err := rival.DeleteLTXFiles(ctx, []*ltx.FileInfo{{Level: 0, MinTXID: 1, MaxTXID: 1}}); !errors.Is(err, ErrFenced) {
			t.Fatalf("delete after fencing = %v, want ErrFenced", err)
		}
		// The identical bytes at the existing slot pass every other check, as
		// retry showed above; only the latch refuses them now.
		if _, err := rival.WriteLTXFile(ctx, 0, head, head, bytes.NewReader(mine)); !errors.Is(err, ErrFenced) {
			t.Fatalf("identical rewrite by a fenced client = %v, want ErrFenced", err)
		}
		if got := downloadLTX(t, az, container, 0, head); !bytes.Equal(got, mine) {
			t.Fatal("the taken slot changed")
		}
	})
}

func assertContent(t *testing.T, az *azblob.Client, container, key string, want []byte) {
	t.Helper()
	resp, err := az.DownloadStream(context.Background(), container, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s holds %d bytes that differ from the expected %d", key, len(got), len(want))
	}
}

func downloadLTX(t *testing.T, az *azblob.Client, container string, level int, txid ltx.TXID) []byte {
	t.Helper()
	rc, err := NewClient(az, container, DBPrefix).OpenLTXFile(context.Background(), level, txid, txid, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
