package main

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
	cfg, token := serveConfig(t, dir)
	cmd := exec.Command(os.Args[0], "-test.run=^TestSecondSignalStopsAtOnce$") //nolint:gosec // the test binary itself
	cmd.Env = append(os.Environ(), "SKYLITE_SERVE_CHILD="+cfg)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	lines := bufio.NewScanner(stderr)
	var addr string
	for addr == "" && lines.Scan() {
		var l struct{ Msg, Listen string }
		if json.Unmarshal(lines.Bytes(), &l) == nil && l.Msg == "serving" {
			addr = l.Listen
		}
	}
	if addr == "" {
		t.Fatal("the server did not start")
	}
	go func() { _, _ = io.Copy(io.Discard, stderr) }()
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

// serveConfig writes a plaintext configuration, with a JWKS file and a
// local store in dir, and returns its path and an admin token.
func serveConfig(t *testing.T, dir string) (path, token string) {
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
	cfg, err := json.Marshal(map[string]any{
		"listen": "127.0.0.1:0", "plaintext": true,
		"auth": map[string]any{
			"issuers": []any{map[string]any{"issuer": iss, "audience": "skylite", "jwks_file": filepath.Join(dir, "jwks.json")}},
			"rules":   []any{map[string]any{"issuer": iss, "subject": "admin", "database": "*", "role": "admin"}},
		},
		"storage": map[string]any{"localfs": map[string]any{"path": dir}},
	})
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
