package gcs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/storage"
	"github.com/RandomCodeSpace/skylite/internal/storage/storagetest"
)

// TestSilentService: against a service that accepts connections and never
// answers, a create fails with an unknown outcome within the response-header
// timeout, not when its context ends. Reads are not checked: the client
// retries them, each attempt so bounded, until the context ends.
func TestSilentService(t *testing.T) {
	addr := storagetest.Silent(t, 200*time.Millisecond)
	s, err := Open(context.Background(), Config{Bucket: "b", Endpoint: "http://" + addr})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	err = storagetest.Returns(t, 10*time.Second, func(ctx context.Context) error {
		_, err := s.Create(ctx, "k", strings.NewReader("commit"))
		return err
	})
	if !errors.Is(err, storage.ErrOutcomeUnknown) {
		t.Errorf("create: %v, want an unknown outcome", err)
	}
}
