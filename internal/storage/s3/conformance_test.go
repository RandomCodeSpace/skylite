//go:build integration

package s3

import (
	"net/http"
	"sync/atomic"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"

	"github.com/RandomCodeSpace/skylite/internal/storage"
	"github.com/RandomCodeSpace/skylite/internal/storage/storagetest"
)

// TestConformance runs the portable suite against the emulator named by
// SKYLITE_TEST_S3_ENDPOINT, one new bucket per namespace and one new client
// per open, each with the SDK's default HTTP client of its own. It counts
// every HTTP request the backend sends, retries included.
func TestConformance(t *testing.T) {
	testEndpoint(t)
	var requests atomic.Int64
	storagetest.Run(t, func(t *testing.T) func() storage.Store {
		b := newBucket(t)
		return func() storage.Store {
			s, err := newStore(b.cfg, countingClient{awshttp.NewBuildableClient(), &requests})
			if err != nil {
				t.Fatal(err)
			}
			return s
		}
	})
	t.Logf("the backend sent %d HTTP requests", requests.Load())
}

type countingClient struct {
	next interface {
		Do(*http.Request) (*http.Response, error)
	}
	n *atomic.Int64
}

func (c countingClient) Do(r *http.Request) (*http.Response, error) {
	c.n.Add(1)
	return c.next.Do(r)
}
