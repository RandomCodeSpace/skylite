package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

const goodConfig = `{
  "listen": ":8443",
  "tls": {"cert_file": "/etc/skylite/tls.crt", "key_file": "/etc/skylite/tls.key"},
  "auth": {
    "issuers": [{"issuer": "https://kubernetes.default.svc", "audience": "skylite", "jwks_file": "/var/run/jwks.json"}],
    "rules": [{"issuer": "https://kubernetes.default.svc", "subject": "system:serviceaccount:apps:admin", "database": "*", "role": "admin"}]
  },
  "storage": {"prefix": "prod/skylite", "s3": {"bucket": "b", "region": "eu-west-1", "endpoint": "http://127.0.0.1:9000", "path_style": true}},
  "databases": ["orders", "users"],
  "probe_listen": ":8081"
}`

func writeConfig(t *testing.T, s string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "skylite.json")
	if err := os.WriteFile(p, []byte(s), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoad(t *testing.T) {
	c, err := Load(writeConfig(t, goodConfig))
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != ":8443" || c.TLS.KeyFile != "/etc/skylite/tls.key" || c.Auth.Issuers[0].JWKSFile != "/var/run/jwks.json" ||
		c.Auth.Rules[0].Role != "admin" || c.Storage.Prefix != "prod/skylite" || c.Storage.S3 == nil || !c.Storage.S3.PathStyle ||
		strings.Join(c.Databases, ",") != "orders,users" || c.ProbeListen != ":8081" {
		t.Errorf("Load = %+v", c)
	}
	for _, s := range []string{
		`"storage": {"localfs": {"path": "/data"}}`,
		`"storage": {"gcs": {"bucket": "b"}}`,
		`"storage": {"azure": {"account": "acct", "container": "c", "shared_key_file": "/etc/skylite/azure.key"}}`,
		`"storage": {"azure": {"account": "devstoreaccount1", "container": "c", "endpoint": "http://127.0.0.1:10000/devstoreaccount1", "shared_key_env": "AZURE_STORAGE_KEY"}}`,
	} {
		alt := strings.Replace(goodConfig, goodConfig[strings.Index(goodConfig, `"storage"`):strings.Index(goodConfig, `,
  "databases"`)], s, 1)
		if _, err := Load(writeConfig(t, alt)); err != nil {
			t.Errorf("%s: %v", s, err)
		}
	}
	plain := strings.Replace(goodConfig, `"tls": {"cert_file": "/etc/skylite/tls.crt", "key_file": "/etc/skylite/tls.key"}`, `"plaintext": true`, 1)
	if c, err := Load(writeConfig(t, plain)); err != nil || !c.Plaintext || c.TLS != nil {
		t.Errorf("plaintext: %+v, %v", c, err)
	}
}

func TestLoadRefuses(t *testing.T) {
	edit := func(old, repl string) string {
		if !strings.Contains(goodConfig, old) {
			t.Fatalf("%q is not in the config", old)
		}
		return strings.Replace(goodConfig, old, repl, 1)
	}
	names := make([]string, maxDatabases)
	for i := range names {
		names[i] = fmt.Sprintf("%q", fmt.Sprint("d", i))
	}
	many := "[" + strings.Join(names, ", ") + "]"
	storageField := goodConfig[strings.Index(goodConfig, `"storage"`):strings.Index(goodConfig, `,
  "databases"`)]
	withStorage := func(s string) string { return edit(storageField, s) }
	for name, s := range map[string]string{
		"not JSON":                "listen",
		"data after":              goodConfig + "{}",
		"unknown field":           edit(`"listen"`, `"secret": "x", "listen"`),
		"misspelt field":          edit(`"jwks_file"`, `"jwksfile"`),
		"no listen":               edit(`"listen": ":8443",`, ""),
		"tls and plaintext":       edit(`"listen"`, `"plaintext": true, "listen"`),
		"no tls, no plaintext":    edit(`"tls": {"cert_file": "/etc/skylite/tls.crt", "key_file": "/etc/skylite/tls.key"},`, ""),
		"tls without key":         edit(`, "key_file": "/etc/skylite/tls.key"`, ""),
		"no issuers":              edit(`[{"issuer": "https://kubernetes.default.svc", "audience": "skylite", "jwks_file": "/var/run/jwks.json"}]`, `[]`),
		"rule of no issuer":       edit(`"rules": [{"issuer": "https://kubernetes.default.svc"`, `"rules": [{"issuer": "other"`),
		"database with _":         edit(`"database": "*"`, `"database": "a_*"`),
		"two backends":            edit(`"s3"`, `"gcs": {"bucket": "b"}, "s3"`),
		"s3 and azure":            edit(`"s3"`, `"azure": {"account": "acct", "container": "c", "shared_key_env": "AZURE_STORAGE_KEY"}, "s3"`),
		"no backend":              edit(`, "s3": {"bucket": "b", "region": "eu-west-1", "endpoint": "http://127.0.0.1:9000", "path_style": true}`, ""),
		"s3 without region":       edit(`, "region": "eu-west-1"`, ""),
		"azure without account":   withStorage(`"storage": {"azure": {"container": "c", "shared_key_env": "AZURE_STORAGE_KEY"}}`),
		"azure without container": withStorage(`"storage": {"azure": {"account": "acct", "shared_key_env": "AZURE_STORAGE_KEY"}}`),
		"azure without cred":      withStorage(`"storage": {"azure": {"account": "acct", "container": "c"}}`),
		"azure with both creds":   withStorage(`"storage": {"azure": {"account": "acct", "container": "c", "shared_key_file": "/etc/skylite/azure.key", "shared_key_env": "AZURE_STORAGE_KEY"}}`),
		"azure bad account":       withStorage(`"storage": {"azure": {"account": "u:hunter2@evil", "container": "c", "shared_key_env": "AZURE_STORAGE_KEY"}}`),
		"azure bad container":     withStorage(`"storage": {"azure": {"account": "acct", "container": "a/b", "shared_key_env": "AZURE_STORAGE_KEY"}}`),
		"azure container dotdot":  withStorage(`"storage": {"azure": {"account": "acct", "container": "..", "shared_key_env": "AZURE_STORAGE_KEY"}}`),
		"azure container escape":  withStorage(`"storage": {"azure": {"account": "acct", "container": "sales%2Farchive", "shared_key_env": "AZURE_STORAGE_KEY"}}`),
		"azure container query":   withStorage(`"storage": {"azure": {"account": "acct", "container": "c?sig=hunter2", "shared_key_env": "AZURE_STORAGE_KEY"}}`),
		"azure key as env name":   withStorage(`"storage": {"azure": {"account": "acct", "container": "c", "shared_key_env": "aHVudGVyMg=="}}`),
		"bad prefix":              edit(`"prod/skylite"`, `"prod//skylite"`),
		"prefix too long":         edit(`"prod/skylite"`, `"`+strings.Repeat("p", 1000)+`"`),
		"database name with _":    edit(`"users"`, `"us_ers"`),
		"database twice":          edit(`"users"`, `"orders"`),
		"too many databases":      edit(`["orders", "users"]`, many),
	} {
		_, err := Load(writeConfig(t, s))
		if err == nil {
			t.Errorf("%s: config accepted", name)
			continue
		}
		if strings.Contains(err.Error(), "hunter2") || strings.Contains(err.Error(), "aHVudGVyMg==") {
			t.Errorf("%s: error %q leaked value", name, err)
		}
	}
	if _, err := Load(filepath.Join(t.TempDir(), "absent.json")); err == nil {
		t.Error("a missing file was accepted")
	}
}

func TestLoadRefusesAnEndpointWithASecret(t *testing.T) {
	const good = `"endpoint": "http://127.0.0.1:9000"`
	for _, ep := range []string{
		"http://u:hunter2@127.0.0.1:9000",
		"http://127.0.0.1:9000/?X-Amz-Signature=hunter2",
		"http://127.0.0.1:9000/?",
		"http://127.0.0.1:9000/#hunter2",
		"http://127.0.0.1:9000/#",
		"ftp://127.0.0.1:9000",
		"127.0.0.1:9000",
		"http:///hunter2",
		"http://127.0.0.1:9000/\x7fhunter2",
	} {
		for _, backend := range []string{"s3", "gcs", "azure"} {
			s := strings.Replace(goodConfig, good, `"endpoint": "`+strings.ReplaceAll(ep, "\x7f", `\u007f`)+`"`, 1)
			switch backend {
			case "gcs":
				s = strings.Replace(s, `"s3": {"bucket": "b", "region": "eu-west-1",`, `"gcs": {"bucket": "b",`, 1)
				s = strings.Replace(s, `, "path_style": true`, "", 1)
			case "azure":
				s = strings.Replace(s, `"s3": {"bucket": "b", "region": "eu-west-1",`, `"azure": {"account": "acct", "container": "c", "shared_key_env": "AZURE_STORAGE_KEY",`, 1)
				s = strings.Replace(s, `, "path_style": true`, "", 1)
			}
			_, err := Load(writeConfig(t, s))
			if err == nil {
				t.Errorf("%s endpoint %q accepted", backend, ep)
				continue
			}
			if want := "storage." + backend + ".endpoint"; !strings.Contains(err.Error(), want) || strings.Contains(err.Error(), "hunter2") {
				t.Errorf("%s endpoint %q: error %q, want one naming %s and not the value", backend, ep, err, want)
			}
		}
	}
	for _, ep := range []string{"https://storage.example:443/base/", "http://[::1]:9000"} {
		if _, err := Load(writeConfig(t, strings.Replace(goodConfig, good, `"endpoint": "`+ep+`"`, 1))); err != nil {
			t.Errorf("endpoint %q: %v", ep, err)
		}
	}
}

func TestAzureContainerURL(t *testing.T) {
	for want, a := range map[string]Azure{
		"https://acct.blob.core.windows.net/c":       {Account: "acct", Container: "c"},
		"http://127.0.0.1:10000/devstoreaccount1/c":  {Account: "devstoreaccount1", Container: "c", Endpoint: "http://127.0.0.1:10000/devstoreaccount1"},
		"http://127.0.0.1:10000/devstoreaccount1/c2": {Account: "devstoreaccount1", Container: "c2", Endpoint: "http://127.0.0.1:10000/devstoreaccount1/"},
	} {
		if got := a.containerURL(); got != want {
			t.Errorf("containerURL(%+v) = %q, want %q", a, got, want)
		}
	}
}

func TestNewAzureSharedKey(t *testing.T) {
	sig := newSigner(t, "k1")
	jwks := jwksFile(t, sig)
	dir := t.TempDir()
	const validKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" //nolint:gosec // test value
	const badKey = "not-base64-hunter2!!!"

	goodFile := filepath.Join(dir, "good.key")
	emptyFile := filepath.Join(dir, "empty.key")
	badFile := filepath.Join(dir, "bad.key")
	bigFile := filepath.Join(dir, "big.key")
	for p, content := range map[string]string{goodFile: validKey + "\n", emptyFile: " \n", badFile: badKey, bigFile: validKey + strings.Repeat(" ", 4097)} {
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	t.Setenv("SKYLITE_TEST_AZURE_GOOD", " "+validKey+"\n")
	t.Setenv("SKYLITE_TEST_AZURE_EMPTY", "  ")
	t.Setenv("SKYLITE_TEST_AZURE_BAD", badKey)
	t.Setenv("SKYLITE_TEST_AZURE_UNSET", "")

	for name, az := range map[string]*Azure{
		"valid file": {Account: "acct", Container: "c", SharedKeyFile: goodFile},
		"valid env":  {Account: "acct", Container: "c", SharedKeyEnv: "SKYLITE_TEST_AZURE_GOOD"},
	} {
		cfg := testConfig(t, jwks)
		cfg.Storage = Storage{Prefix: "dbs", Azure: az}
		s, err := New(cfg, nil)
		if err != nil {
			t.Errorf("%s: New: %v", name, err)
			continue
		}
		if c, ok := s.store.(io.Closer); ok {
			_ = c.Close()
		}
	}

	for name, az := range map[string]*Azure{
		"missing file": {Account: "acct", Container: "c", SharedKeyFile: filepath.Join(dir, "absent.key")},
		"empty file":   {Account: "acct", Container: "c", SharedKeyFile: emptyFile},
		"bad file key": {Account: "acct", Container: "c", SharedKeyFile: badFile},
		"big file":     {Account: "acct", Container: "c", SharedKeyFile: bigFile},
		"key as path":  {Account: "acct", Container: "c", SharedKeyFile: filepath.Join(dir, "aHVudGVyMg==")},
		"unset env":    {Account: "acct", Container: "c", SharedKeyEnv: "SKYLITE_TEST_AZURE_UNSET"},
		"empty env":    {Account: "acct", Container: "c", SharedKeyEnv: "SKYLITE_TEST_AZURE_EMPTY"},
		"bad env key":  {Account: "acct", Container: "c", SharedKeyEnv: "SKYLITE_TEST_AZURE_BAD"},
	} {
		cfg := testConfig(t, jwks)
		cfg.Storage = Storage{Prefix: "dbs", Azure: az}
		_, err := New(cfg, nil)
		if err == nil {
			t.Errorf("%s: New succeeded, want error", name)
			continue
		}
		if strings.Contains(err.Error(), "hunter2") || strings.Contains(err.Error(), "aHVudGVyMg") {
			t.Errorf("%s: error %q leaked key", name, err)
		}
		if az.SharedKeyFile != "" && strings.Contains(err.Error(), az.SharedKeyFile) {
			t.Errorf("%s: error %q names the key file", name, err)
		}
	}
}

func TestAzureRefusesRedirect(t *testing.T) {
	var elsewhere atomic.Int32
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		elsewhere.Add(1)
		w.WriteHeader(http.StatusCreated)
	}))
	defer dst.Close()
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", dst.URL+r.URL.Path)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer src.Close()
	t.Setenv("SKYLITE_TEST_AZURE_KEY", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
	st, err := openAzure(&Azure{Account: "devstoreaccount1", Container: "c", Endpoint: src.URL + "/devstoreaccount1", SharedKeyEnv: "SKYLITE_TEST_AZURE_KEY"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.(io.Closer).Close() }()
	if _, err := st.Create(context.Background(), "k", strings.NewReader("database bytes")); err == nil {
		t.Error("a create answered with a redirect succeeded")
	}
	if n := elsewhere.Load(); n != 0 {
		t.Errorf("the redirect target got %d requests, want 0", n)
	}
}
