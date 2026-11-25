package server

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// The memory budget of ADR 0005 decision 9 and what it is divided into:
// the body share, less the control capacity and the header reservations;
// the channels' share, which holds a receipt for each channel, held result
// rows and retained replay data; and SQLite connections at their ceiling in
// what is left. Nothing else is charged; see docs/content/docs/server.md.
const (
	defaultBudget = 2 << 30 // without a memory limit
	channelShare  = maxChannels*receiptBytes + heldShare + replayShare
	connCeiling   = 64 << 20 // SQLite connection memory ceiling
	maxConnLimit  = 256      // SQLite connections per pod
	// controlBytes is the control capacity: a 4 KiB body and a 4 KiB
	// terminal frame for each place for cancel, close and resolve. It is
	// reserved from the body share, so the connections keep what the
	// record derives for them.
	controlBytes = (maxControl + maxResolves) * 2 * controlBodyBytes
	// headerBytes is reserved for each listed database, from the body share
	// as the control capacity is: the headers its leader keeps for resolves,
	// the engine's index (at most about 98,304, 77 bytes each) and those read
	// from storage (about the resolve bound, 100,000, 223 bytes each), as
	// measured with the server's 24-byte commit identifiers: about 28 MiB.
	headerBytes = 32 << 20
	// minBudget is the smallest budget that holds one SQLite connection
	// beside the channels' share: 384 MiB. A smaller budget than 1,026 MiB,
	// plus 64 MiB per listed database, lowers the request body limit (shares).
	minBudget = 2 * (channelShare + connCeiling)
	// maxBudget bounds the budget: past about 32 GiB the connections are
	// at their limit and only the body share grows.
	maxBudget = 64 << 30
)

// shares divides a memory budget of a server that lists dbs databases:
// the body share, the SQLite connections it holds at their ceiling, the led
// databases' own included, and the request body limit: 16 MiB, or less
// below a budget of 1,026 MiB plus 64 MiB per database, so that a
// principal's quarter of the body share holds one body charged at 8 ×.
func shares(budget int64, dbs int) (body int64, conns int, bodyLimit int64) {
	body = budget/2 - controlBytes - int64(dbs)*headerBytes
	return body, int(min(maxConnLimit, (budget-budget/2-channelShare)/connCeiling)), min(16<<20, body/4/8)
}

// budgetFor is the least budget in whole MiB that holds conns SQLite connections.
func budgetFor(conns int) int64 { return 2 * (channelShare + int64(conns)*connCeiling) }

// cgroupRoot is where a container sees its own cgroup: on cgroup v2 a
// Kubernetes container has a cgroup namespace of its own, and on v1 each
// hierarchy mounts the container's cgroup.
//
// shortcut: a process outside a cgroup namespace on v2 reads the root
// cgroup, which has no memory.max, and gets the default; read the path in
// /proc/self/cgroup if that case matters.
const cgroupRoot = "/sys/fs/cgroup"

// containerLimit reads the container's memory limit under root: cgroup v2's
// memory.max, else v1's memory/memory.limit_in_bytes. It returns 0 when
// neither file exists or the limit is "max", or v1's no-limit value.
func containerLimit(root string) (int64, error) {
	for _, name := range []string{"memory.max", "memory/memory.limit_in_bytes"} {
		b, err := os.ReadFile(filepath.Join(root, name)) //nolint:gosec // a fixed path under the cgroup root
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("server: memory limit: %w; set memory_budget to skip reading it", err)
		}
		v := strings.TrimSpace(string(b))
		if v == "max" {
			return 0, nil
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 1 {
			return 0, fmt.Errorf("server: memory limit: %s holds %.32q, not a size in bytes; set memory_budget to skip reading it", name, v)
		}
		if n >= 1<<62 { // v1 reports no limit as the largest multiple of the page size
			return 0, nil
		}
		return n, nil
	}
	return 0, nil
}

// memoryBudget is ADR 0005's default budget: three quarters of the memory
// limit, the container's or goLimit (GOMEMLIMIT, math.MaxInt64 when unset)
// if that is lower, at most maxBudget; and 2 GiB without either. from says
// where it came from. newServer refuses a budget too small for the
// databases.
func memoryBudget(root string, goLimit int64) (budget int64, from string, err error) {
	limit, err := containerLimit(root)
	if err != nil {
		return 0, "", err
	}
	from = "three quarters of the container's memory limit"
	if goLimit > 0 && goLimit < math.MaxInt64 && (limit == 0 || goLimit < limit) {
		limit, from = goLimit, "three quarters of GOMEMLIMIT"
	}
	if limit == 0 {
		return defaultBudget, "the default without a memory limit", nil
	}
	return min(limit/4*3, maxBudget), from, nil
}
