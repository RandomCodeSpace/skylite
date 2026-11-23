package s3

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/RandomCodeSpace/skylite/internal/storage"
	"github.com/RandomCodeSpace/skylite/internal/storage/storagetest"
)

// TestSilentService: against a service that accepts connections and never
// answers, a create fails with an unknown outcome and a read fails, each
// within its attempts' response-header timeouts, not when its context ends.
func TestSilentService(t *testing.T) {
	addr := storagetest.Silent(t, 200*time.Millisecond)
	creds := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "k", SecretAccessKey: "s"}, nil
	})
	s, err := New(Config{Bucket: "b", Region: "us-east-1", Credentials: creds, Endpoint: "http://" + addr, UsePathStyle: true, SpoolDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	err = storagetest.Returns(t, 10*time.Second, func(ctx context.Context) error {
		_, err := s.Create(ctx, "k", strings.NewReader("commit"))
		return err
	})
	if !errors.Is(err, storage.ErrOutcomeUnknown) {
		t.Errorf("create: %v, want an unknown outcome", err)
	}
	err = storagetest.Returns(t, 10*time.Second, func(ctx context.Context) error {
		_, err := s.Head(ctx, "k")
		return err
	})
	if err == nil || errors.Is(err, storage.ErrNotFound) {
		t.Errorf("head: %v, want a failure", err)
	}
}
