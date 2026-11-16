package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const goodConfig = `{
  "listen": ":8443",
  "tls": {"cert_file": "/etc/skylite/tls.crt", "key_file": "/etc/skylite/tls.key"},
  "auth": {
    "issuers": [{"issuer": "https://kubernetes.default.svc", "audience": "skylite", "jwks_file": "/var/run/jwks.json"}],
    "rules": [{"issuer": "https://kubernetes.default.svc", "subject": "system:serviceaccount:apps:admin", "database": "*", "role": "admin"}]
  },
  "storage": {"prefix": "prod/skylite", "s3": {"bucket": "b", "region": "eu-west-1", "endpoint": "http://127.0.0.1:9000", "path_style": true}}
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
		c.Auth.Rules[0].Role != "admin" || c.Storage.Prefix != "prod/skylite" || c.Storage.S3 == nil || !c.Storage.S3.PathStyle {
		t.Errorf("Load = %+v", c)
	}
	for _, s := range []string{
		`"storage": {"localfs": {"path": "/data"}}`,
		`"storage": {"gcs": {"bucket": "b"}}`,
	} {
		alt := strings.Replace(goodConfig, goodConfig[strings.Index(goodConfig, `"storage"`):strings.LastIndex(goodConfig, "}")], s+"\n", 1)
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
	for name, s := range map[string]string{
		"not JSON":             "listen",
		"data after":           goodConfig + "{}",
		"unknown field":        edit(`"listen"`, `"secret": "x", "listen"`),
		"misspelt field":       edit(`"jwks_file"`, `"jwksfile"`),
		"no listen":            edit(`"listen": ":8443",`, ""),
		"tls and plaintext":    edit(`"listen"`, `"plaintext": true, "listen"`),
		"no tls, no plaintext": edit(`"tls": {"cert_file": "/etc/skylite/tls.crt", "key_file": "/etc/skylite/tls.key"},`, ""),
		"tls without key":      edit(`, "key_file": "/etc/skylite/tls.key"`, ""),
		"no issuers":           edit(`[{"issuer": "https://kubernetes.default.svc", "audience": "skylite", "jwks_file": "/var/run/jwks.json"}]`, `[]`),
		"rule of no issuer":    edit(`"rules": [{"issuer": "https://kubernetes.default.svc"`, `"rules": [{"issuer": "other"`),
		"database with _":      edit(`"database": "*"`, `"database": "a_*"`),
		"two backends":         edit(`"s3"`, `"gcs": {"bucket": "b"}, "s3"`),
		"no backend":           edit(`, "s3": {"bucket": "b", "region": "eu-west-1", "endpoint": "http://127.0.0.1:9000", "path_style": true}`, ""),
		"s3 without region":    edit(`, "region": "eu-west-1"`, ""),
		"bad prefix":           edit(`"prod/skylite"`, `"prod//skylite"`),
		"prefix too long":      edit(`"prod/skylite"`, `"`+strings.Repeat("p", 1000)+`"`),
	} {
		if _, err := Load(writeConfig(t, s)); err == nil {
			t.Errorf("%s: config accepted", name)
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
		for _, backend := range []string{"s3", "gcs"} {
			s := strings.Replace(goodConfig, good, `"endpoint": "`+strings.ReplaceAll(ep, "\x7f", `\u007f`)+`"`, 1)
			if backend == "gcs" {
				s = strings.Replace(s, `"s3": {"bucket": "b", "region": "eu-west-1",`, `"gcs": {"bucket": "b",`, 1)
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
