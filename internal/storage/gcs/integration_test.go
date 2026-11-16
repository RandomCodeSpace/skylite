//go:build integration

package gcs

import (
	"testing"

	"github.com/RandomCodeSpace/skylite/internal/storage"
	"github.com/RandomCodeSpace/skylite/internal/storage/storagetest"
)

// TestConformance runs the portable suite against the emulator named by
// SKYLITE_TEST_GCS_ENDPOINT, one bucket per namespace. `make integration-gcs`
// starts the emulator and sets it.
func TestConformance(t *testing.T) {
	ep := endpoint(t)
	var requests counter
	t.Cleanup(func() { t.Logf("%d HTTP requests to the emulator", requests.n.Load()) })
	storagetest.Run(t, func(t *testing.T) func() storage.Store {
		bucket := newBucket(t)
		return func() storage.Store {
			return openStore(t, Config{Bucket: bucket, Endpoint: ep}, requests.wrap(newTransport(t)))
		}
	})
}
