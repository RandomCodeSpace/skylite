package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
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
