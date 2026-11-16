package main

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"maps"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

func TestRun(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStdout string
		wantStderr string
	}{
		{name: "no args prints usage", args: nil, wantCode: 0, wantStdout: usage},
		{name: "help", args: []string{"help"}, wantCode: 0, wantStdout: usage},
		{name: "-h", args: []string{"-h"}, wantCode: 0, wantStdout: usage},
		{name: "--help", args: []string{"--help"}, wantCode: 0, wantStdout: usage},
		{name: "version", args: []string{"version"}, wantCode: 0, wantStdout: "skylite " + version() + "\n"},
		{
			name:       "unknown command",
			args:       []string{"frobnicate"},
			wantCode:   2,
			wantStderr: "skylite: unknown command \"frobnicate\"\nRun 'skylite help' for usage.\n",
		},
		{
			name:       "serve without a config",
			args:       []string{"serve"},
			wantCode:   2,
			wantStderr: "skylite serve: usage: skylite serve -config <file>\n",
		},
		{
			name:       "serve with an extra argument",
			args:       []string{"serve", "-config", "x.json", "extra"},
			wantCode:   2,
			wantStderr: "skylite serve: usage: skylite serve -config <file>\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if got := run(tt.args, &stdout, &stderr); got != tt.wantCode {
				t.Errorf("exit code = %d, want %d", got, tt.wantCode)
			}
			if got := stdout.String(); got != tt.wantStdout {
				t.Errorf("stdout = %q, want %q", got, tt.wantStdout)
			}
			if got := stderr.String(); got != tt.wantStderr {
				t.Errorf("stderr = %q, want %q", got, tt.wantStderr)
			}
		})
	}
}

func TestVersionNotEmpty(t *testing.T) {
	if v := version(); v == "" || strings.ContainsAny(v, " \n") {
		t.Errorf("version() = %q, want a non-empty single token", v)
	}
}

func TestServeRefusesABadConfig(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if got := run([]string{"serve", "-config", filepath.Join(t.TempDir(), "absent.json")}, &stdout, &stderr); got != 1 {
		t.Errorf("exit code = %d, want 1", got)
	}
	if !strings.Contains(stderr.String(), `"level":"ERROR"`) || !strings.Contains(stderr.String(), "absent.json") {
		t.Errorf("stderr = %q, want a JSON error line naming the file", stderr.String())
	}
}

// TestSecondSignalStopsAtOnce runs serve in a child process, holds a
// request in its body read, and sends SIGTERM twice: the first starts the
// drain, which would wait for the 30-second body deadline, and the second
// must end the process at once.
func TestSecondSignalStopsAtOnce(t *testing.T) {
	if cfg := os.Getenv("SKYLITE_SERVE_CHILD"); cfg != "" {
		os.Exit(run([]string{"serve", "-config", cfg}, os.Stdout, os.Stderr))
	}
	dir := t.TempDir()
	cfg, token := serveConfig(t, dir, nil)
	cmd, addr, _ := serveChild(t, "TestSecondSignalStopsAtOnce", cfg)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if _, err := io.WriteString(c, "PUT /v1/databases/held HTTP/1.1\r\nHost: skylite\r\nAuthorization: Bearer "+token+
		"\r\nContent-Type: application/json\r\nContent-Length: 10\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond) // the request is in its body read
	for range 2 {
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		var ws syscall.WaitStatus
		if ee, ok := errors.AsType[*exec.ExitError](err); ok {
			ws, _ = ee.Sys().(syscall.WaitStatus)
		}
		if !ws.Signaled() || ws.Signal() != syscall.SIGTERM {
			t.Errorf("the process ended with %v, want killed by the second SIGTERM", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("the process was still running 5 seconds after the second SIGTERM")
	}
}

// serveChild runs serve with the configuration cfg in a child process,
// which runs the test named test, and returns it once it serves, with its
// address and its log, which grows until it exits.
func serveChild(t *testing.T, test, cfg string) (cmd *exec.Cmd, addr string, log *lockedBuffer) {
	t.Helper()
	cmd = exec.Command(os.Args[0], "-test.run=^"+test+"$") //nolint:gosec // the test binary itself
	cmd.Env = append(os.Environ(), "SKYLITE_SERVE_CHILD="+cfg)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	log = &lockedBuffer{}
	lines := bufio.NewScanner(stderr)
	for addr == "" && lines.Scan() {
		log.write(lines.Bytes())
		var l struct{ Msg, Listen string }
		if json.Unmarshal(lines.Bytes(), &l) == nil && l.Msg == "serving" {
			addr = l.Listen
		}
	}
	if addr == "" {
		t.Fatalf("the server did not start: %s", log.String())
	}
	go func() {
		for lines.Scan() {
			log.write(lines.Bytes())
		}
	}()
	return cmd, addr, log
}

// lockedBuffer collects a child's log lines.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) write(line []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.b.Write(line)
	b.b.WriteByte('\n')
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

// TestServeQueryOverHTTPS runs serve over TLS in a child process, creates a
// database with PUT, waits for the server to take office for it, and
// queries and describes it with a signed token.
func TestServeQueryOverHTTPS(t *testing.T) {
	if cfg := os.Getenv("SKYLITE_SERVE_CHILD"); cfg != "" {
		os.Exit(run([]string{"serve", "-config", cfg}, os.Stdout, os.Stderr))
	}
	dir := t.TempDir()
	cert, key, roots := selfSigned(t, dir)
	cfg, token := serveConfig(t, dir, map[string]any{
		"plaintext": false, "tls": map[string]any{"cert_file": cert, "key_file": key}, "databases": []string{"e2e"},
	})
	cmd, addr, log := serveChild(t, "TestServeQueryOverHTTPS", cfg)
	c := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, ForceAttemptHTTP2: true}}
	send := func(method, path, body string) (int, []string) {
		t.Helper()
		req, err := http.NewRequest(method, "https://"+addr+"/v1/databases/e2e"+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if resp.ProtoMajor != 2 || resp.Header.Get("Skylite-Protocol") != "1" {
			t.Fatalf("%s %s: %s, protocol marker %q", method, path, resp.Proto, resp.Header.Get("Skylite-Protocol"))
		}
		return resp.StatusCode, strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
	}
	if code, frames := send(http.MethodPut, "", "{}"); code != http.StatusOK || frames[0] != `{"done":{"txn":1,"epoch":1}}` {
		t.Fatalf("PUT: %d %v", code, frames)
	}
	const sql = `{"sql":"SELECT 1 + 1 AS two, 'x' AS s, x'00ff' AS b, 1.5 AS r, NULL AS n, ? AS p","params":["hello"]}`
	var code int
	var frames []string
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		// not_serving until the server's next candidacy takes office.
		if code, frames = send(http.MethodPost, "/query", sql); code != http.StatusServiceUnavailable || time.Now().After(deadline) {
			break
		}
	}
	want := []string{
		`{"columns":[{"name":"two","decltype":null,"database":null,"table":null,"origin":null},{"name":"s","decltype":null,"database":null,"table":null,"origin":null},{"name":"b","decltype":null,"database":null,"table":null,"origin":null},{"name":"r","decltype":null,"database":null,"table":null,"origin":null},{"name":"n","decltype":null,"database":null,"table":null,"origin":null},{"name":"p","decltype":null,"database":null,"table":null,"origin":null}]}`,
		`[2,"x",{"blob":"AP8="},1.5e0,null,"hello"]`,
		`{"done":{"txn":2,"read_only":true,"rows_affected":0,"last_insert_rowid":0}}`,
	}
	if code != http.StatusOK || strings.Join(frames, "\n") != strings.Join(want, "\n") {
		t.Fatalf("query: %d\n%s\nwant\n%s", code, strings.Join(frames, "\n"), strings.Join(want, "\n"))
	}
	if code, frames := send(http.MethodPost, "/describe", `{"sql":"SELECT :name AS n"}`); code != http.StatusOK || frames[len(frames)-1] != `{"done":{"txn":2,"param_count":1}}` {
		t.Errorf("describe: %d %v", code, frames)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("serve ended with %v, want exit status 0", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("serve did not stop within 30 s of SIGTERM")
	}
	// Server logs carry no SQL text, parameters or tokens (ADR 0005 decision 6).
	if l := log.String(); strings.Contains(l, token) || strings.Contains(l, "hello") || strings.Contains(l, "SELECT") || !strings.Contains(l, "office: leading") {
		t.Errorf("the log holds a token or SQL, or no office: %s", l)
	}
}

// selfSigned writes a certificate for 127.0.0.1 and its key into dir, and
// returns their paths and a pool that trusts the certificate.
func selfSigned(t *testing.T, dir string) (cert, key string, roots *x509.CertPool) {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "skylite test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, k.Public(), k)
	if err != nil {
		t.Fatal(err)
	}
	kb, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	cert, key = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	for path, b := range map[string]*pem.Block{cert: {Type: "CERTIFICATE", Bytes: der}, key: {Type: "PRIVATE KEY", Bytes: kb}} {
		if err := os.WriteFile(path, pem.EncodeToMemory(b), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	c, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots = x509.NewCertPool()
	roots.AddCert(c)
	return cert, key, roots
}

// serveConfig writes a plaintext configuration, with a JWKS file and a
// local store in dir, changed by extra, and returns its path and an admin
// token.
func serveConfig(t *testing.T, dir string, extra map[string]any) (path, token string) {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	set, err := json.Marshal(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: k.Public(), KeyID: "k1", Algorithm: string(jose.ES256), Use: "sig"}}})
	if err != nil {
		t.Fatal(err)
	}
	js, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.ES256, Key: k}, (&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", "k1"))
	if err != nil {
		t.Fatal(err)
	}
	const iss = "https://issuer.invalid"
	token, err = jwt.Signed(js).Claims(map[string]any{"iss": iss, "sub": "admin", "aud": "skylite", "exp": time.Now().Add(time.Hour).Unix()}).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	c := map[string]any{
		"listen": "127.0.0.1:0", "plaintext": true,
		"auth": map[string]any{
			"issuers": []any{map[string]any{"issuer": iss, "audience": "skylite", "jwks_file": filepath.Join(dir, "jwks.json")}},
			"rules":   []any{map[string]any{"issuer": iss, "subject": "admin", "database": "*", "role": "admin"}},
		},
		"storage": map[string]any{"localfs": map[string]any{"path": dir}},
	}
	maps.Copy(c, extra)
	cfg, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	path = filepath.Join(dir, "skylite.json")
	for p, b := range map[string][]byte{filepath.Join(dir, "jwks.json"): set, path: cfg} {
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return path, token
}
