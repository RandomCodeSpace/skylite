package gcs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	gcs "cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// endpointEnv names the emulator's JSON API base URL, for example
// http://127.0.0.1:4443/storage/v1/. `make integration-gcs` sets it.
const endpointEnv = "SKYLITE_TEST_GCS_ENDPOINT"

func endpoint(t testing.TB) string {
	t.Helper()
	ep := os.Getenv(endpointEnv)
	if ep == "" {
		t.Skipf("%s is not set; run `make integration-gcs` or point it at a fake-gcs-server JSON API endpoint", endpointEnv)
	}
	return ep
}

// admin is a plain client for creating and emptying buckets.
var admin = sync.OnceValues(func() (*gcs.Client, error) {
	return gcs.NewClient(context.Background(), option.WithEndpoint(os.Getenv(endpointEnv)), option.WithoutAuthentication())
})

// newBucket creates an empty bucket on the emulator and removes it, with
// everything in it, when t ends.
func newBucket(t testing.TB) string {
	t.Helper()
	endpoint(t)
	c, err := admin()
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	name := "skylite-" + hex.EncodeToString(b)
	if err := c.Bucket(name).Create(context.Background(), "skylite-test", nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		it := c.Bucket(name).Objects(ctx, nil)
		for {
			a, err := it.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				t.Errorf("emptying bucket %s: %v", name, err)
				return
			}
			if err := c.Bucket(name).Object(a.Name).Delete(ctx); err != nil {
				t.Errorf("emptying bucket %s: %v", name, err)
			}
		}
		if err := c.Bucket(name).Delete(ctx); err != nil {
			t.Errorf("deleting bucket %s: %v", name, err)
		}
	})
	return name
}

// newTransport returns an HTTP transport of the caller's own, so that clients
// share no connections.
func newTransport(t testing.TB) *http.Transport {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(tr.CloseIdleConnections)
	return tr
}

// openStore opens a Store whose requests go through base.
func openStore(t testing.TB, cfg Config, base http.RoundTripper) *Store {
	t.Helper()
	s, err := open(context.Background(), cfg, base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// counter counts the HTTP requests, retries included, that pass through the
// transports it wraps.
type counter struct{ n atomic.Int64 }

func (c *counter) wrap(next http.RoundTripper) http.RoundTripper {
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		c.n.Add(1)
		return next.RoundTrip(r)
	})
}
