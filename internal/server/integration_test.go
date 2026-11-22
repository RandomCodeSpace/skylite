//go:build integration

package server

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestPutOnS3 creates databases through New's S3 backend against the
// emulator named by SKYLITE_TEST_S3_ENDPOINT (make integration-s3). The
// credentials reach the SDK's default chain from the environment.
func TestPutOnS3(t *testing.T) {
	ep := os.Getenv("SKYLITE_TEST_S3_ENDPOINT")
	if ep == "" {
		t.Skip("SKYLITE_TEST_S3_ENDPOINT is not set")
	}
	// RustFS's public development credentials; they only work against the emulator.
	t.Setenv("AWS_ACCESS_KEY_ID", "rustfsadmin")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "rustfsadmin")
	bucket := "skylite-server-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	ac, err := awsconfig.LoadDefaultConfig(t.Context(), awsconfig.WithRegion("us-east-1"))
	if err != nil {
		t.Fatal(err)
	}
	admin := awss3.NewFromConfig(ac, func(o *awss3.Options) { o.BaseEndpoint, o.UsePathStyle = aws.String(ep), true })
	if _, err := admin.CreateBucket(t.Context(), &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	putTwice(t, Storage{Prefix: "it", S3: &S3{Bucket: bucket, Region: "us-east-1", Endpoint: ep, PathStyle: true}})
}

// TestPutOnGCS does the same against the fake-gcs-server named by
// SKYLITE_TEST_GCS_ENDPOINT (make integration-gcs).
func TestPutOnGCS(t *testing.T) {
	ep := os.Getenv("SKYLITE_TEST_GCS_ENDPOINT")
	if ep == "" {
		t.Skip("SKYLITE_TEST_GCS_ENDPOINT is not set")
	}
	bucket := "skylite-server-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	resp, err := http.Post(ep+"b?project=test", "application/json", strings.NewReader(`{"name":"`+bucket+`"}`)) //nolint:gosec // the emulator named by the environment
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("creating bucket: %s", resp.Status)
	}
	putTwice(t, Storage{Prefix: "it", GCS: &GCS{Bucket: bucket, Endpoint: ep}})
}

// TestPutOnAzure does the same against the Azurite emulator named by
// SKYLITE_TEST_AZURITE_BLOB_ENDPOINT (make integration), exercising both a
// shared-key file and an environment variable.
func TestPutOnAzure(t *testing.T) {
	ep := os.Getenv("SKYLITE_TEST_AZURITE_BLOB_ENDPOINT")
	if ep == "" {
		t.Skip("SKYLITE_TEST_AZURITE_BLOB_ENDPOINT is not set")
	}
	// Azurite's documented default development account; the key is public and
	// only works against the emulator.
	const (
		account = "devstoreaccount1"
		key     = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" //nolint:gosec // Azurite's public development key
	)
	cred, err := service.NewSharedKeyCredential(account, key)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.NewClientWithSharedKeyCredential(ep, cred, nil)
	if err != nil {
		t.Fatal(err)
	}
	keyFile := filepath.Join(t.TempDir(), "azure.key")
	if err := os.WriteFile(keyFile, []byte(key+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SKYLITE_TEST_AZURE_KEY", key)
	for _, tc := range []struct {
		name string
		az   Azure
	}{
		{"file", Azure{Account: account, Endpoint: ep, SharedKeyFile: keyFile}},
		{"env", Azure{Account: account, Endpoint: ep, SharedKeyEnv: "SKYLITE_TEST_AZURE_KEY"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctr := "skylite-server-" + tc.name + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
			if _, err := svc.CreateContainer(t.Context(), ctr, nil); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _, _ = svc.DeleteContainer(context.Background(), ctr, nil) })
			az := tc.az
			az.Container = ctr
			putTwice(t, Storage{Prefix: "it", Azure: &az})
		})
	}
}

// putTwice serves st through New and creates one database twice.
func putTwice(t *testing.T, st Storage) {
	t.Helper()
	sig := newSigner(t, "k1")
	cfg := testConfig(t, jwksFile(t, sig))
	cfg.Storage = st
	s, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{s: s, sig: sig}
	e.start(t)
	tok := sig.token(t, "admin")
	c := h2cClient()
	if resp, m := e.put(t, c, "orders", tok); resp.StatusCode != http.StatusOK {
		t.Fatalf("first PUT: %s %v", resp.Status, m)
	}
	if resp, m := e.put(t, c, "orders", tok); resp.StatusCode != http.StatusConflict || errorCode(m) != "database_exists" {
		t.Fatalf("second PUT: %s %v", resp.Status, m)
	}
}
