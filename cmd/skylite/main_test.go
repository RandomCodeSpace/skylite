package main

import (
	"bytes"
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
			args:       []string{"serve"},
			wantCode:   2,
			wantStderr: "skylite: unknown command \"serve\"\nRun 'skylite help' for usage.\n",
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
