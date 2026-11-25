package server

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/RandomCodeSpace/skylite/internal/auth"
	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/wire"
)

// Config is the server's configuration, read from a JSON file by Load. It
// holds no secret: S3 and GCS take their credentials from their SDKs' default
// credential chains, and Azure reads its shared key from a file or environment
// variable named in the configuration.
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
	// Databases names the databases this server stands for: for each, it
	// takes office when it can and serves query and describe while it
	// leads. A database need not exist yet; the server takes office once it
	// does.
	Databases []string `json:"databases"`
	// FollowerReads enables stateless reads on fresh local followers. When
	// the file leaves it out, Load sets it to true if Peer is set: without
	// a peer listener a ready standby could not deliver writes.
	FollowerReads bool `json:"follower_reads"`
	// Peer, if set, lets this server forward a request it cannot serve
	// itself, one hop, to the pod that can (ADR 0005 decision 7).
	Peer *Peer `json:"peer"`
	// ProbeListen, if set, is a TCP address for a plain HTTP listener that
	// answers GET /readyz: 200 while this server can serve at least one
	// database and no leader is blocked at its tail bound, 503 otherwise.
	ProbeListen string `json:"probe_listen"`
	// MemoryBudget, if set, is the memory budget of ADR 0005 decision 9 in
	// bytes, from 384 MiB to 64 GiB. When it is 0, New takes three
	// quarters of the container's memory limit, or of GOMEMLIMIT if lower,
	// and 2 GiB without either.
	MemoryBudget int64 `json:"memory_budget"`
}

// Peer is the pod-to-pod listener and its mutual TLS. Every pod of a
// deployment presents a certificate issued by CAFile, which must issue
// peer certificates and nothing else, for ServerName, and requires one
// from the pod that calls it.
type Peer struct {
	// Listen is the TCP address of the peer listener, for example ":8444".
	Listen string `json:"listen"`
	// AddressEnv names an environment variable holding the host:port other
	// pods dial to reach this pod's peer listener, for example
	// "$(POD_IP):8444" set from the downward API.
	AddressEnv string `json:"address_env"`
	// CertFile and KeyFile are this pod's peer certificate and key, used
	// both as server and as client certificate.
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
	// CAFile holds the PEM certificates that issue peer certificates.
	CAFile string `json:"ca_file"`
	// ServerName is the DNS name every peer certificate carries.
	ServerName string `json:"server_name"`
}

// TLS names PEM files holding the server's certificate chain and its key.
type TLS struct {
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
}

// Storage selects one backend and the key prefix the databases live under.
// Exactly one of LocalFS, S3, GCS and Azure must be set.
type Storage struct {
	// Prefix is prepended, with a "/", to every database's keys. Empty means
	// the databases' keys start at the root of the store.
	Prefix  string   `json:"prefix"`
	LocalFS *LocalFS `json:"localfs"`
	S3      *S3      `json:"s3"`
	GCS     *GCS     `json:"gcs"`
	Azure   *Azure   `json:"azure"`
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

// Azure is an Azure Blob Storage container.
type Azure struct {
	// Account is the storage account name, used to sign requests with a shared
	// key and, when Endpoint is empty, to form the account URL
	// https://<account>.blob.core.windows.net.
	Account string `json:"account"`
	// Container is the name of an existing container.
	Container string `json:"container"`
	// Endpoint, if set, is the blob service account URL, for example
	// "http://127.0.0.1:10000/devstoreaccount1" for Azurite. If empty,
	// https://<account>.blob.core.windows.net is used.
	Endpoint string `json:"endpoint"`
	// SharedKeyFile is the path of a file holding the base64-encoded account
	// shared key. Exactly one of SharedKeyFile, SharedKeyEnv and Entra must be
	// set.
	SharedKeyFile string `json:"shared_key_file"`
	// SharedKeyEnv names an environment variable holding the base64-encoded
	// account shared key. Exactly one of SharedKeyFile, SharedKeyEnv and Entra
	// must be set.
	SharedKeyEnv string `json:"shared_key_env"`
	// Entra signs requests with an Entra ID token from azidentity's
	// DefaultAzureCredential: a service principal in the AZURE_* environment
	// variables, workload identity, a managed identity, then the Azure CLI,
	// Azure Developer CLI and Azure PowerShell logins; AZURE_TOKEN_CREDENTIALS
	// narrows the chain. It needs an https endpoint. Exactly one of
	// SharedKeyFile, SharedKeyEnv and Entra must be set.
	Entra bool `json:"entra"`
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
	var set struct {
		FollowerReads *bool `json:"follower_reads"`
	}
	_ = json.Unmarshal(b, &set) // b decoded above
	if set.FollowerReads == nil {
		c.FollowerReads = c.Peer != nil
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
	if p := c.Peer; p != nil && (p.Listen == "" || p.CertFile == "" || p.KeyFile == "" || p.CAFile == "" || p.ServerName == "" || !validEnvName(p.AddressEnv)) {
		return errors.New("peer needs listen, cert_file, key_file, ca_file, server_name and address_env, an environment variable name")
	}
	if err := c.Auth.Validate(); err != nil {
		return err
	}
	if c.MemoryBudget != 0 && (c.MemoryBudget < minBudget || c.MemoryBudget > maxBudget) {
		return fmt.Errorf("memory_budget is %d, want 0 or from %d to %d bytes", c.MemoryBudget, int64(minBudget), int64(maxBudget))
	}
	// Without memory_budget, New checks the databases against the budget it derives.
	if _, conns, _ := shares(cmp.Or(c.MemoryBudget, maxBudget), 0); len(c.Databases) >= conns {
		return fmt.Errorf("databases lists %d, want at most %d: each led database holds a connection of the memory budget", len(c.Databases), conns-1)
	}
	seen := map[string]bool{}
	for _, d := range c.Databases {
		if !wire.ValidDatabaseName(d) || seen[d] {
			return fmt.Errorf("databases: %q is not a database name (1 to 63 of a-z, 0-9 and -, not starting with -), or is listed twice", d)
		}
		seen[d] = true
	}
	return c.Storage.validate()
}

func (s Storage) validate() error {
	n := 0
	for _, set := range []bool{s.LocalFS != nil, s.S3 != nil, s.GCS != nil, s.Azure != nil} {
		if set {
			n++
		}
	}
	switch {
	case n != 1:
		return errors.New("storage needs exactly one of localfs, s3, gcs and azure")
	case s.LocalFS != nil && s.LocalFS.Path == "":
		return errors.New("storage.localfs.path is empty")
	case s.S3 != nil && (s.S3.Bucket == "" || s.S3.Region == ""):
		return errors.New("storage.s3 needs bucket and region")
	case s.GCS != nil && s.GCS.Bucket == "":
		return errors.New("storage.gcs.bucket is empty")
	case s.S3 != nil && !validEndpoint(s.S3.Endpoint):
		return errors.New("storage.s3.endpoint " + endpointRule)
	case s.GCS != nil && !validEndpoint(s.GCS.Endpoint):
		return errors.New("storage.gcs.endpoint " + endpointRule)
	case s.Azure != nil:
		if err := s.Azure.validate(); err != nil {
			return err
		}
	}
	// The longest database name must still leave room for every key.
	if _, err := s.keys(strings.Repeat("a", 63)); err != nil {
		return fmt.Errorf("storage.prefix: %w", err)
	}
	return nil
}

func (a Azure) validate() error {
	creds := 0
	for _, set := range []bool{a.SharedKeyFile != "", a.SharedKeyEnv != "", a.Entra} {
		if set {
			creds++
		}
	}
	switch {
	case a.Account == "" || a.Container == "":
		return errors.New("storage.azure needs account and container")
	case !validAzureAccount(a.Account):
		return errors.New("storage.azure.account must not contain /, ?, #, @, : or white space")
	case !validAzureContainer(a.Container):
		return errors.New("storage.azure.container must be a single path segment with no /, ?, # or white space")
	case !validEndpoint(a.Endpoint):
		return errors.New("storage.azure.endpoint " + endpointRule)
	case creds != 1:
		return errors.New("storage.azure needs exactly one of shared_key_file, shared_key_env and entra")
	case a.Entra && a.Endpoint != "" && !strings.HasPrefix(a.Endpoint, "https://"):
		// azcore refuses to send a bearer token over plain HTTP.
		return errors.New("storage.azure.entra needs an https endpoint")
	case a.SharedKeyEnv != "" && !validEnvName(a.SharedKeyEnv):
		return errors.New("storage.azure.shared_key_env must be an environment variable name")
	}
	return nil
}

func (a Azure) containerURL() string {
	base := strings.TrimRight(a.Endpoint, "/")
	if base == "" {
		base = "https://" + a.Account + ".blob.core.windows.net"
	}
	return base + "/" + a.Container
}

func validAzureAccount(s string) bool {
	return s != "" && !strings.ContainsAny(s, "/\\?#@: \t\r\n") && validEndpoint("https://"+s+".blob.core.windows.net")
}

func validAzureContainer(s string) bool {
	return s != "" && s != "." && s != ".." && !strings.ContainsAny(s, "/\\?#% \t\r\n") && validEndpoint("https://example.invalid/"+s)
}

// validEnvName reports whether s is a POSIX environment variable name
// ([A-Za-z_][A-Za-z0-9_]*), so a pasted key (which holds =, / or +) is refused
// without repeating it.
func validEnvName(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		c := s[i]
		letter := 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || c == '_'
		digit := '0' <= c && c <= '9'
		if i == 0 && !letter || i > 0 && !letter && !digit {
			return false
		}
	}
	return true
}

const endpointRule = "must be an http or https URL with a host and no user information, query or fragment"

// validEndpoint reports whether e is empty or follows endpointRule, so that
// it holds no credential and can be logged. The value is never put in an
// error, because one that breaks the rule may hold a secret.
func validEndpoint(e string) bool {
	if e == "" {
		return true
	}
	u, err := url.Parse(e)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != "" &&
		u.User == nil && u.RawQuery == "" && !u.ForceQuery && !strings.Contains(e, "#")
}

// keys returns the keys of database db.
func (s Storage) keys(db string) (format.Keys, error) {
	if s.Prefix == "" {
		return format.NewKeys(db)
	}
	return format.NewKeys(s.Prefix + "/" + db)
}
