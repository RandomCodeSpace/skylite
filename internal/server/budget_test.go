package server

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// cgroupDir writes files, by path under the cgroup root, into a new root.
func cgroupDir(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for name, body := range files {
		p := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestContainerLimit(t *testing.T) {
	for name, c := range map[string]struct {
		files map[string]string
		want  int64
		err   bool
	}{
		"v2":             {files: map[string]string{"memory.max": "2147483648\n"}, want: 2 << 30},
		"v2 max":         {files: map[string]string{"memory.max": "max\n"}},
		"v1":             {files: map[string]string{"memory/memory.limit_in_bytes": "3221225472\n"}, want: 3 << 30},
		"v1 no limit":    {files: map[string]string{"memory/memory.limit_in_bytes": "9223372036854771712\n"}},
		"v2 ahead of v1": {files: map[string]string{"memory.max": "1073741824", "memory/memory.limit_in_bytes": "3221225472"}, want: 1 << 30},
		"missing":        {},
		"not a number":   {files: map[string]string{"memory.max": "lots\n"}, err: true},
		"zero":           {files: map[string]string{"memory/memory.limit_in_bytes": "0"}, err: true},
		"a directory":    {files: map[string]string{"memory.max/x": ""}, err: true},
	} {
		got, err := containerLimit(cgroupDir(t, c.files))
		if got != c.want || (err != nil) != c.err {
			t.Errorf("%s: %d, %v; want %d, error %t", name, got, err, c.want, c.err)
		}
	}
}

func TestMemoryBudget(t *testing.T) {
	v2 := func(limit string) string { return cgroupDir(t, map[string]string{"memory.max": limit}) }
	for name, c := range map[string]struct {
		root    string
		goLimit int64
		want    int64
		from    string
	}{
		"no limit":                  {root: v2("max"), goLimit: math.MaxInt64, want: 2 << 30, from: "default"},
		"container":                 {root: v2("4294967296"), goLimit: math.MaxInt64, want: 3 << 30, from: "container"},
		"GOMEMLIMIT lower":          {root: v2("4294967296"), goLimit: 2 << 30, want: 3 << 29, from: "GOMEMLIMIT"},
		"GOMEMLIMIT higher":         {root: v2("4294967296"), goLimit: 8 << 30, want: 3 << 30, from: "container"},
		"GOMEMLIMIT alone":          {root: v2("max"), goLimit: 4 << 30, want: 3 << 30, from: "GOMEMLIMIT"},
		"at the floor":              {root: v2("1434451968"), goLimit: math.MaxInt64, want: minBudget, from: "container"}, // 1,368 MiB
		"above the cap":             {root: v2("137438953472"), goLimit: math.MaxInt64, want: maxBudget, from: "container"},
		"the 2.5 GiB of old sizing": {root: v2("2684354560"), goLimit: math.MaxInt64, want: 1920 << 20, from: "container"},
	} {
		got, from, err := memoryBudget(c.root, c.goLimit)
		if err != nil || got != c.want || !strings.Contains(from, c.from) {
			t.Errorf("%s: %d from %q, %v; want %d from %s", name, got, from, err, c.want, c.from)
		}
	}
	// Below the floor the server does not start, and says what limit it needs.
	if _, _, err := memoryBudget(v2("1434451967"), math.MaxInt64); err == nil || !strings.Contains(err.Error(), "1368 MiB") {
		t.Errorf("below the floor: %v", err)
	}
	if _, _, err := memoryBudget(v2("lots"), math.MaxInt64); err == nil {
		t.Error("an unreadable limit was taken for none")
	}
}
