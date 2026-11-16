package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/RandomCodeSpace/skylite/internal/auth"
	"github.com/RandomCodeSpace/skylite/internal/format"
)

// Config is the server's configuration, read from a JSON file by Load. It
// holds no secret: the cloud backends take their credentials from their
// SDKs' default credential chains.
type Config struct {
	// Listen is the TCP address to listen on, for example ":8443".
	Listen string `json:"listen"`
	// TLS names the server's certificate and key. Exactly one of TLS and
	// Plaintext must be set.
	TLS *TLS `json:"tls"`
	// Plaintext serves HTTP/1.1 and HTTP/2 without TLS, for a server behind
	// a mesh or proxy that terminates TLS.
	Plaintext bool `json:"plaintext"`
	// Auth is the authentication and authorisation configuration of ADR 0006.
	Auth auth.Config `json:"auth"`
	// Storage is the object store that holds the databases.
	Storage Storage `json:"storage"`
}

// TLS names PEM files holding the server's certificate chain and its key.
type TLS struct {
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
}

// Storage selects one backend and the key prefix the databases live under.
// Exactly one of LocalFS, S3 and GCS must be set.
type Storage struct {
	// Prefix is prepended, with a "/", to every database's keys. Empty means
	// the databases' keys start at the root of the store.
	Prefix  string   `json:"prefix"`
	LocalFS *LocalFS `json:"localfs"`
	S3      *S3      `json:"s3"`
	GCS     *GCS     `json:"gcs"`
}

// LocalFS is a directory on a local file system.
type LocalFS struct {
	Path string `json:"path"`
}

// S3 is an Amazon S3 bucket or one of an S3-compatible service.
type S3 struct {
	Bucket    string `json:"bucket"`
	Region    string `json:"region"`
	Endpoint  string `json:"endpoint"`
	PathStyle bool   `json:"path_style"`
}

// GCS is a Google Cloud Storage bucket.
type GCS struct {
	Bucket   string `json:"bucket"`
	Endpoint string `json:"endpoint"`
}

// Load reads and validates the configuration file at path. Unknown fields
// are refused.
func Load(path string) (Config, error) {
	b, err := os.ReadFile(path) //nolint:gosec // the operator names the file
	if err != nil {
		return Config{}, err
	}
	var c Config
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("server: config %s: %w", path, err)
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("server: config %s: data after the configuration object", path)
	}
	if err := c.Validate(); err != nil {
		return Config{}, fmt.Errorf("server: config %s: %w", path, err)
	}
	return c, nil
}

// Validate reports the first problem with the configuration, if any.
func (c Config) Validate() error {
	switch {
	case c.Listen == "":
		return errors.New("listen is empty")
	case (c.TLS == nil) == !c.Plaintext:
		return errors.New("exactly one of tls and plaintext must be set")
	case c.TLS != nil && (c.TLS.CertFile == "" || c.TLS.KeyFile == ""):
		return errors.New("tls needs cert_file and key_file")
	}
	if err := c.Auth.Validate(); err != nil {
		return err
	}
	return c.Storage.validate()
}

func (s Storage) validate() error {
	n := 0
	for _, set := range []bool{s.LocalFS != nil, s.S3 != nil, s.GCS != nil} {
		if set {
			n++
		}
	}
	switch {
	case n != 1:
		return errors.New("storage needs exactly one of localfs, s3 and gcs")
	case s.LocalFS != nil && s.LocalFS.Path == "":
		return errors.New("storage.localfs.path is empty")
	case s.S3 != nil && (s.S3.Bucket == "" || s.S3.Region == ""):
		return errors.New("storage.s3 needs bucket and region")
	case s.GCS != nil && s.GCS.Bucket == "":
		return errors.New("storage.gcs.bucket is empty")
	}
	// The longest database name must still leave room for every key.
	if _, err := s.keys(strings.Repeat("a", 63)); err != nil {
		return fmt.Errorf("storage.prefix: %w", err)
	}
	return nil
}

// keys returns the keys of database db.
func (s Storage) keys(db string) (format.Keys, error) {
	if s.Prefix == "" {
		return format.NewKeys(db)
	}
	return format.NewKeys(s.Prefix + "/" + db)
}
