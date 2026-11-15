// Package durability is a spike to validate the durability design: Litestream
// embedded as a library, replicating to Azure Blob Storage through a
// ReplicaClient that fences stale leaders with conditional writes. It is not
// the product, has no stable API, and may be deleted or replaced.
//
// The tests run against Azurite and are behind the "integration" build tag.
// See docs/content/docs/decisions/0002-durability-spike.md.
package durability

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"
)

// ErrFenced is returned, wrapped, by every write and delete once a client has
// seen evidence that another leader owns the history.
var ErrFenced = errors.New("fenced: another leader owns the history")

// metadataKeyTimestamp matches abs.MetadataKeyTimestamp, so files written here
// look the same to Litestream's own Azure client.
const metadataKeyTimestamp = "litestreamtimestamp"

var _ litestream.ReplicaClient = (*Client)(nil)

// Client is a litestream.ReplicaClient for Azure Blob Storage that creates
// every LTX file with If-None-Match: * and latches into a fenced state on the
// first sign that it is not the writer of the history.
//
// Head is the highest L0 TXID this client has itself confirmed in storage. A
// leader may only append L0 head+1, may only write higher-level files that end
// at or below head, and may only write those or delete anything while the
// remote L0 head is still its own.
type Client struct {
	az        *azblob.Client
	container string
	path      string

	logger *slog.Logger

	// mu is held across uploads so admission, the write and the head update
	// are one step; Litestream serialises its own syncs anyway.
	mu     sync.Mutex
	head   ltx.TXID
	fenced error

	// unsafe turns off every fencing measure. It exists only so the tests can
	// show that their checks detect a stale leader that is not fenced.
	unsafe bool
}

// NewClient returns a client for LTX files under prefix in container.
func NewClient(az *azblob.Client, container, prefix string) *Client {
	return &Client{az: az, container: container, path: prefix, logger: slog.Default()}
}

// Type implements litestream.ReplicaClient.
func (c *Client) Type() string { return "abs" }

// Init implements litestream.ReplicaClient.
func (c *Client) Init(context.Context) error { return nil }

// SetLogger implements litestream.ReplicaClient.
func (c *Client) SetLogger(logger *slog.Logger) { c.logger = logger }

// Fenced returns a wrapped ErrFenced once the client is fenced, otherwise nil.
func (c *Client) Fenced() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fenced
}

// Head returns the highest L0 TXID this client has confirmed in storage.
func (c *Client) Head() ltx.TXID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.head
}

// setHead records the TXID a leader restored to before taking office.
func (c *Client) setHead(txid ltx.TXID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.head = txid
}

// fence latches the fenced state. The caller holds c.mu.
func (c *Client) fence(format string, args ...any) error {
	if c.fenced == nil {
		c.fenced = fmt.Errorf("%w: %s", ErrFenced, fmt.Sprintf(format, args...))
		c.logger.Warn("replica client fenced", "reason", c.fenced)
	}
	return c.fenced
}

// confirmAck checks that the position Litestream reached after a sync is the
// head this client uploaded. SyncAndWait alone does not prove that: after a
// failed sync Litestream re-reads the remote head, and if another leader has
// written past the local position it uploads nothing and returns nil.
func (c *Client) confirmAck(pos ltx.TXID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.unsafe {
		return nil
	}
	if c.fenced != nil {
		return c.fenced
	}
	if pos != c.head {
		return c.fence("local position %s is not the uploaded head %s", pos, c.head)
	}
	return nil
}

// checkRemoteHead fences the client unless the newest remote L0 file is its
// own. The caller holds c.mu.
func (c *Client) checkRemoteHead(ctx context.Context) error {
	infos, err := c.list(ctx, 0, 0)
	if err != nil {
		return err
	}
	var remote ltx.TXID
	for _, info := range infos {
		remote = max(remote, info.MaxTXID)
	}
	if remote != c.head {
		return c.fence("remote L0 head %s is not this leader's head %s", remote, c.head)
	}
	return nil
}

// WriteLTXFile implements litestream.ReplicaClient.
func (c *Client) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	// shortcut: buffers the whole file so a create conflict can be compared
	// byte for byte; a product would hash while streaming.
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	hdr, _, err := ltx.PeekHeader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("read ltx header: %w", err)
	}
	createdAt := time.UnixMilli(hdr.Timestamp).UTC()

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.unsafe {
		if c.fenced != nil {
			return nil, c.fenced
		}
		switch {
		case level == 0 && (minTXID != c.head+1 || maxTXID != minTXID):
			// Litestream has moved its position somewhere this leader did not
			// put it, for example after re-reading another leader's head.
			return nil, c.fence("L0 write %s-%s does not follow head %s", minTXID, maxTXID, c.head)
		case level > 0 && maxTXID > c.head:
			return nil, c.fence("level %d write %s-%s is past head %s", level, minTXID, maxTXID, c.head)
		case level > 0:
			if err := c.checkRemoteHead(ctx); err != nil {
				return nil, err
			}
		}
	}

	key := litestream.LTXFilePath(c.path, level, minTXID, maxTXID)
	// A single Put Blob is atomic. A staged multi-block upload that dies
	// before Put Block List leaves uncommitted blocks that Azurite counts as
	// an existing blob for If-None-Match, which would wedge the slot for
	// every later leader.
	// shortcut: UploadBuffer falls back to blocks above 256 MiB; a product
	// must cap or split LTX files below the single Put Blob limit.
	opts := &azblob.UploadBufferOptions{
		HTTPHeaders: &blob.HTTPHeaders{BlobContentType: to.Ptr("application/octet-stream")},
		AccessTier:  to.Ptr(blob.AccessTierHot),
		Metadata:    map[string]*string{metadataKeyTimestamp: to.Ptr(createdAt.Format(time.RFC3339Nano))},
	}
	if !c.unsafe {
		opts.AccessConditions = &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfNoneMatch: to.Ptr(azcore.ETagAny)},
		}
	}
	_, err = c.az.UploadBuffer(ctx, c.container, key, data, opts)
	if bloberror.HasCode(err, bloberror.BlobAlreadyExists) {
		// An identical blob means an earlier attempt of this same request
		// succeeded and only its response was lost.
		same, cmpErr := c.sameContent(ctx, key, data)
		switch {
		case cmpErr != nil:
			return nil, fmt.Errorf("compare existing %q: %w", key, cmpErr)
		case same:
			err = nil
		case level == 0:
			return nil, c.fence("L0 %s already holds another leader's file", key)
		default:
			// Higher levels are derived from the history, so a different file
			// under the same name is not proof of another leader; refuse to
			// overwrite it without latching.
			return nil, fmt.Errorf("level %d file %q already exists with other content: %w", level, key, err)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("upload %q: %w", key, err)
	}

	if level == 0 {
		c.head = maxTXID
	}
	return &ltx.FileInfo{Level: level, MinTXID: minTXID, MaxTXID: maxTXID, Size: int64(len(data)), CreatedAt: createdAt}, nil
}

func (c *Client) sameContent(ctx context.Context, key string, data []byte) (bool, error) {
	resp, err := c.az.DownloadStream(ctx, c.container, key, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()
	existing, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	return bytes.Equal(existing, data), nil
}

// DeleteLTXFiles implements litestream.ReplicaClient. Retention and
// compaction cleanup delete files, so a stale leader must not reach this.
func (c *Client) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.unsafe {
		if c.fenced != nil {
			return c.fenced
		}
		if err := c.checkRemoteHead(ctx); err != nil {
			return err
		}
	}
	for _, info := range a {
		key := litestream.LTXFilePath(c.path, info.Level, info.MinTXID, info.MaxTXID)
		if _, err := c.az.DeleteBlob(ctx, c.container, key, nil); err != nil && !bloberror.HasCode(err, bloberror.BlobNotFound) {
			return fmt.Errorf("delete %q: %w", key, err)
		}
	}
	return nil
}

// DeleteAll implements litestream.ReplicaClient. Litestream only calls it
// from tests and tooling, and the spike never needs it.
func (c *Client) DeleteAll(context.Context) error {
	return errors.New("DeleteAll is not supported by the durability spike")
}

// LTXFiles implements litestream.ReplicaClient.
func (c *Client) LTXFiles(ctx context.Context, level int, seek ltx.TXID, _ bool) (ltx.FileIterator, error) {
	infos, err := c.list(ctx, level, seek)
	if err != nil {
		return nil, err
	}
	return ltx.NewFileInfoSliceIterator(infos), nil
}

// shortcut: lists every page eagerly; a paging iterator is needed once a
// level holds more files than fit comfortably in memory.
func (c *Client) list(ctx context.Context, level int, seek ltx.TXID) ([]*ltx.FileInfo, error) {
	prefix := litestream.LTXLevelDir(c.path, level) + "/"
	pager := c.az.NewListBlobsFlatPager(c.container, &azblob.ListBlobsFlatOptions{
		Prefix:  &prefix,
		Include: azblob.ListBlobsInclude{Metadata: true},
	})
	var infos []*ltx.FileInfo
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list %q: %w", prefix, err)
		}
		for _, item := range page.Segment.BlobItems {
			minTXID, maxTXID, err := ltx.ParseFilename(path.Base(*item.Name))
			if err != nil || minTXID < seek {
				continue
			}
			info := &ltx.FileInfo{Level: level, MinTXID: minTXID, MaxTXID: maxTXID, Size: *item.Properties.ContentLength}
			info.CreatedAt = item.Properties.CreationTime.UTC()
			if ts := item.Metadata[metadataKeyTimestamp]; ts != nil {
				if t, err := time.Parse(time.RFC3339Nano, *ts); err == nil {
					info.CreatedAt = t
				}
			}
			infos = append(infos, info)
		}
	}
	return infos, nil
}

// OpenLTXFile implements litestream.ReplicaClient.
func (c *Client) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	key := litestream.LTXFilePath(c.path, level, minTXID, maxTXID)
	resp, err := c.az.DownloadStream(ctx, c.container, key, &azblob.DownloadStreamOptions{
		Range: blob.HTTPRange{Offset: offset, Count: size},
	})
	if bloberror.HasCode(err, bloberror.BlobNotFound) {
		return nil, os.ErrNotExist
	} else if err != nil {
		return nil, fmt.Errorf("download %q: %w", key, err)
	}
	return resp.Body, nil
}
