package durability

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
)

// LeaseKey is the blob holding the leadership lease.
const LeaseKey = "lease.json"

// Lease is the advisory leadership record. It tells pods who should try to
// take office; it does not make writes safe. That comes from the conditional
// writes on the log.
//
// shortcut: no expiry or renewal schedule; the tests drive takeover
// explicitly. A product needs a TTL judged against the storage clock.
type Lease struct {
	Epoch int64  `json:"epoch"`
	Owner string `json:"owner"`

	etag azcore.ETag
}

// AcquireLease takes the lease for owner with the next epoch, creating it
// with If-None-Match: * or replacing the current one with If-Match.
func AcquireLease(ctx context.Context, az *azblob.Client, container, owner string) (*Lease, error) {
	cond := &blob.ModifiedAccessConditions{IfNoneMatch: to.Ptr(azcore.ETagAny)}
	next := &Lease{Epoch: 1, Owner: owner}

	resp, err := az.DownloadStream(ctx, container, LeaseKey, nil)
	switch {
	case bloberror.HasCode(err, bloberror.BlobNotFound):
	case err != nil:
		return nil, fmt.Errorf("read lease: %w", err)
	default:
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("read lease: %w", err)
		}
		var cur Lease
		if err := json.Unmarshal(body, &cur); err != nil {
			return nil, fmt.Errorf("decode lease: %w", err)
		}
		next.Epoch = cur.Epoch + 1
		cond = &blob.ModifiedAccessConditions{IfMatch: resp.ETag}
	}

	if err := next.put(ctx, az, container, cond); err != nil {
		return nil, err
	}
	return next, nil
}

// Renew rewrites the lease only if nobody has replaced it since this holder
// last wrote it.
func (l *Lease) Renew(ctx context.Context, az *azblob.Client, container string) error {
	return l.put(ctx, az, container, &blob.ModifiedAccessConditions{IfMatch: &l.etag})
}

func (l *Lease) put(ctx context.Context, az *azblob.Client, container string, cond *blob.ModifiedAccessConditions) error {
	body, err := json.Marshal(l)
	if err != nil {
		return err
	}
	resp, err := az.UploadBuffer(ctx, container, LeaseKey, body, &azblob.UploadBufferOptions{
		AccessConditions: &blob.AccessConditions{ModifiedAccessConditions: cond},
	})
	if err != nil {
		return fmt.Errorf("write lease: %w", err)
	}
	l.etag = *resp.ETag
	return nil
}
