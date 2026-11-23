package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/engine"
	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/sqlpolicy"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// holdStore holds the creates of commit objects while a gate is set.
type holdStore struct {
	storage.Store
	mu      sync.Mutex
	gate    *gate
	unknown atomic.Bool // a held create whose context ended has an unknown outcome
	frozen  atomic.Bool // manifest replaces fail, so no compaction publishes
}

func (s *holdStore) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	if s.frozen.Load() && strings.HasSuffix(key, "/manifest") {
		return "", errors.New("manifest frozen by the test")
	}
	return s.Store.Replace(ctx, key, version, body)
}

// gate is one hold: entered is closed when a create arrives, and the
// creates proceed once release is closed, failing if their context ended.
type gate struct {
	entered, release chan struct{}
	once             sync.Once
}

func (s *holdStore) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	s.mu.Lock()
	g := s.gate
	s.mu.Unlock()
	if g != nil && strings.Contains(key, "/log/") {
		g.once.Do(func() { close(g.entered) })
		<-g.release
		if err := ctx.Err(); err != nil {
			if s.unknown.Load() {
				return "", fmt.Errorf("%w: %w", storage.ErrOutcomeUnknown, err)
			}
			return "", err
		}
	}
	return s.Store.Create(ctx, key, body)
}

// hold sets a gate; the returned function releases it.
func (s *holdStore) hold() (*gate, func()) {
	g := &gate{entered: make(chan struct{}), release: make(chan struct{})}
	s.mu.Lock()
	s.gate = g
	s.mu.Unlock()
	var once sync.Once
	return g, func() {
		once.Do(func() {
			s.mu.Lock()
			s.gate = nil
			s.mu.Unlock()
			close(g.release)
		})
	}
}

func (g *gate) wait(t *testing.T) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("no commit reached the store")
	}
}

// newChanEnv returns a started server leading "d" on a holdStore.
func newChanEnv(t *testing.T, edit func(*env)) (*env, *holdStore) {
	t.Helper()
	var hs *holdStore
	e := newLeaderEnv(t, func(st storage.Store) storage.Store { hs = &holdStore{Store: st}; return hs }, edit)
	return e, hs
}

// open opens a write channel on "d" as subject and returns its token.
func (e *env) open(t *testing.T, subject string) string {
	t.Helper()
	a := e.post(t, "channels", subject, `{"mode":"write"}`)
	c, _ := a.done()["channel"].(map[string]any)
	tok, _ := c["token"].(string)
	if a.status != http.StatusOK || tok == "" {
		t.Fatalf("open: %d %v", a.status, a.frames)
	}
	return tok
}

// ex is the body of an execute of sql as seq on the channel of tok, with the
// extra fields.
func ex(tok string, seq int64, sql string, extra ...any) string {
	m := map[string]any{"channel": tok, "seq": seq, "op": "execute", "sql": sql}
	for i := 0; i+1 < len(extra); i += 2 {
		m[extra[i].(string)] = extra[i+1]
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func closeBody(tok string) string { return fmt.Sprintf(`{"channel":%q}`, tok) }

// num reads an integer field of a frame object.
func num(m map[string]any, key string) int64 {
	n, _ := m[key].(json.Number)
	v, _ := n.Int64()
	return v
}

// errObj returns the error object of an answer's terminal frame.
func (a answer) err() map[string]any {
	m, _ := a.last()["error"].(map[string]any)
	return m
}

// state is a channel's sequence state, for checking that a refusal changed
// nothing.
type state struct {
	last             int64
	sum              [sha256.Size]byte
	running, closing bool
	receipt          string
}

// chState returns the state of the channel of tok on "d", and whether it is
// in the table.
func (e *env) chState(tok string) (state, bool) {
	e.s.chMu.Lock()
	defer e.s.chMu.Unlock()
	t := e.s.dbs["d"].chans
	if t == nil || t.chans[tok] == nil {
		return state{}, false
	}
	ch := t.chans[tok]
	r, _ := json.Marshal(ch.receipt)
	return state{ch.last, ch.sum, ch.running, ch.closing, string(r)}, true
}

// waitChan waits until cond holds for the state of the channel of tok.
func (e *env) waitChan(t *testing.T, tok string, cond func(state, bool) bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		if cond(e.chState(tok)) {
			return
		}
		if time.Now().After(deadline) {
			st, ok := e.chState(tok)
			t.Fatalf("channel state %+v (present %v) never met the condition", st, ok)
		}
	}
}

// commits returns the headers of every commit in the log of "d".
func (e *env) commits(t *testing.T, db format.DatabaseID) []format.CommitHeader {
	t.Helper()
	keys, err := e.s.cfg.Storage.keys("d")
	if err != nil {
		t.Fatal(err)
	}
	var hs []format.CommitHeader
	after := ""
	for {
		list, err := e.store.List(context.Background(), keys.CommitPrefix(db), after, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if len(list) == 0 {
			return hs
		}
		for _, en := range list {
			rc, _, err := e.store.Get(context.Background(), en.Key)
			if err != nil {
				t.Fatal(err)
			}
			b, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil {
				t.Fatal(err)
			}
			h, err := format.DecodeCommitHeader(b[:min(len(b), format.CommitHeaderSize)], db)
			if err != nil {
				t.Fatal(err)
			}
			hs = append(hs, h)
		}
		after = list[len(list)-1].Key
	}
}

// withID counts the commits carrying id.
func withID(hs []format.CommitHeader, id []byte) int {
	n := 0
	for _, h := range hs {
		if bytes.Equal(h.CommitID, id) {
			n++
		}
	}
	return n
}

// wantID is the commit identifier of ADR 0005, computed independently.
func wantID(tok string, seq uint64) []byte {
	h := sha256.Sum256([]byte(tok))
	return binary.BigEndian.AppendUint64(append([]byte(nil), h[:16]...), seq)
}

func (e *env) dbID(t *testing.T) format.DatabaseID {
	t.Helper()
	e.leader(t)
	return e.s.dbs["d"].loop.State().Office.DB
}

// count returns the result of a one-value query on "d".
func (e *env) count(t *testing.T, sql string) string {
	t.Helper()
	a := e.post(t, "query", "reader", q(sql))
	r := a.rows()
	if a.status != http.StatusOK || len(r) != 1 {
		t.Fatalf("%s: %d %v", sql, a.status, a.frames)
	}
	return r[0]
}

func frameText(a answer) string {
	b, _ := json.Marshal(a.frames)
	return string(b)
}

func TestSequenceTableRows(t *testing.T) {
	e, hs := newChanEnv(t, nil)
	tok := e.open(t, "writer")
	post := func(body string) answer { return e.post(t, "channel", "writer", body) }
	if a := post(ex(tok, 1, "CREATE TABLE t (a)")); a.status != 200 || a.done() == nil {
		t.Fatalf("seq 1: %d %v", a.status, a.frames)
	}
	first := post(ex(tok, 2, "INSERT INTO t VALUES (1) RETURNING a"))
	if first.status != 200 || first.done() == nil || len(first.rows()) != 1 {
		t.Fatalf("seq 2: %d %v", first.status, first.frames)
	}
	before, _ := e.chState(tok)
	refused := func(body string, status int, code string, final bool) {
		t.Helper()
		a := post(body)
		er := a.err()
		if a.status != status || a.code() != code || er["final"] != final || er["not_executed"] != true || er["channel_id"] == nil || er["txn_state"] != "none" || len(a.frames) != 1 {
			t.Errorf("%s: %d %v, want %d %s final %v", body, a.status, a.frames, status, code, final)
		}
		if after, _ := e.chState(tok); after != before {
			t.Errorf("%s changed the channel: %+v, was %+v", body, after, before)
		}
	}
	// Finished: the same body is replayed, any other refused.
	if a := post(ex(tok, 2, "INSERT INTO t VALUES (1) RETURNING a")); frameText(a) != frameText(first) || a.status != 200 {
		t.Errorf("replay %d %v, want %v", a.status, a.frames, first.frames)
	}
	refused(ex(tok, 2, "INSERT INTO t VALUES (99)"), http.StatusConflict, "sequence_conflict", true)
	refused(ex(tok, 1, "CREATE TABLE t (a)"), http.StatusConflict, "stale_sequence", false)
	refused(ex(tok, 4, "INSERT INTO t VALUES (99)"), http.StatusConflict, "sequence_gap", false)
	// Running.
	g, release := hs.hold()
	running := e.postAsync(t, "channel", "writer", ex(tok, 3, "INSERT INTO t VALUES (3)"))
	g.wait(t)
	e.waitChan(t, tok, func(s state, _ bool) bool { return s.running })
	before, _ = e.chState(tok)
	refused(ex(tok, 3, "INSERT INTO t VALUES (3)"), http.StatusConflict, "in_flight", false)
	refused(ex(tok, 3, "INSERT INTO t VALUES (99)"), http.StatusConflict, "sequence_conflict", true)
	refused(ex(tok, 4, "INSERT INTO t VALUES (99)"), http.StatusConflict, "channel_busy", false)
	refused(ex(tok, 2, "INSERT INTO t VALUES (1) RETURNING a"), http.StatusConflict, "stale_sequence", false)
	refused(ex(tok, 5, "INSERT INTO t VALUES (99)"), http.StatusConflict, "sequence_gap", false)
	release()
	if a := <-running; a.status != 200 || a.done() == nil {
		t.Fatalf("seq 3: %d %v", a.status, a.frames)
	}
	// Validation after acceptance is recorded, and replayed.
	bad := fmt.Sprintf(`{"channel":%q,"seq":4,"op":"execute","sql":"SELECT 1","extra":1}`, tok)
	a := post(bad)
	if a.status != 200 || a.code() != "invalid_request" || a.err()["final"] != true || num(a.err(), "seq") != 4 {
		t.Errorf("unknown field: %d %v, want a recorded invalid_request", a.status, a.frames)
	}
	if r := post(bad); r.code() != "invalid_request" || r.err()["request_id"] != a.err()["request_id"] {
		t.Errorf("replay of a failure %v, want the receipt with its own request id %v", r.frames, a.err()["request_id"])
	}
	if got := e.count(t, "SELECT group_concat(a) FROM t"); got != `["1,3"]` {
		t.Errorf("rows %s, want 1 and 3", got)
	}
	if a := post(fmt.Sprintf(`{"channel":%q,"seq":9223372036854775808,"op":"ping"}`, tok)); a.status != http.StatusBadRequest || a.code() != "invalid_request" {
		t.Errorf("seq above 2^63-1: %d %v", a.status, a.frames)
	}
}

func TestDuplicatesAtEveryState(t *testing.T) {
	e, hs := newChanEnv(t, nil)
	db := e.dbID(t)
	a, b := e.open(t, "writer"), e.open(t, "admin")
	if r := e.post(t, "channel", "writer", ex(a, 1, "CREATE TABLE t (a)")); r.done() == nil {
		t.Fatal(r.frames)
	}
	dupes := func(subject, body string, want ...string) {
		t.Helper()
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				r, err := e.send("channel", e.sig.token(t, subject), body)
				if err != nil {
					t.Error(err)
					return
				}
				if got := r.code(); !slices.Contains(want, got) {
					t.Errorf("duplicate: %d %v, want %v", r.status, r.frames, want)
				}
			})
		}
		wg.Wait()
	}
	// A is in durable wait, holding the write lock; B waits for the lock.
	g, release := hs.hold()
	ba := ex(a, 2, "INSERT INTO t VALUES (1)")
	bb := ex(b, 1, "INSERT INTO t VALUES (2)")
	ra := e.postAsync(t, "channel", "writer", ba)
	g.wait(t)
	rb := e.postAsync(t, "channel", "admin", bb)
	e.waitChan(t, b, func(s state, _ bool) bool { return s.running })
	dupes("writer", ba, "in_flight")
	dupes("admin", bb, "in_flight")
	release()
	if r := <-ra; r.done() == nil {
		t.Fatal(r.frames)
	}
	if r := <-rb; r.done() == nil {
		t.Fatal(r.frames)
	}
	// After the answer: replayed.
	dupes("writer", ba, "")
	dupes("admin", bb, "")
	// After close: lost.
	if r := e.post(t, "channel/close", "admin", closeBody(b)); r.done() == nil {
		t.Fatal(r.frames)
	}
	dupes("admin", bb, "channel_lost")
	// After a leader change: lost, or not served while no leader is in office.
	if err := e.leader(t).Close(); err != nil {
		t.Fatal(err)
	}
	dupes("writer", ba, "channel_lost", "not_serving")
	e.leader(t)
	dupes("writer", ba, "channel_lost")
	hdrs := e.commits(t, db)
	if n, m := withID(hdrs, wantID(a, 2)), withID(hdrs, wantID(b, 1)); n != 1 || m != 1 {
		t.Errorf("commits of the two requests: %d and %d, want exactly one each", n, m)
	}
	if got := e.count(t, "SELECT count(*) FROM t"); got != "[2]" {
		t.Errorf("rows %s, want 2", got)
	}
}

func TestCloseWaitsForUpload(t *testing.T) {
	e, hs := newChanEnv(t, nil)
	tok := e.open(t, "writer")
	// A channel with nothing accepted closes at once with last_seq 0.
	if r := e.post(t, "channel/close", "writer", closeBody(e.open(t, "writer"))); r.status != 200 || frameText(r) != `[{"done":{"last_seq":0}}]` {
		t.Errorf("idle close: %d %v", r.status, r.frames)
	}
	if r := e.post(t, "channel", "writer", ex(tok, 1, "CREATE TABLE t (a)")); r.done() == nil {
		t.Fatal(r.frames)
	}
	g, release := hs.hold()
	defer release()
	run := e.postAsync(t, "channel", "writer", ex(tok, 2, "INSERT INTO t VALUES (1)"))
	g.wait(t)
	c1 := e.postAsync(t, "channel/close", "writer", closeBody(tok))
	e.waitChan(t, tok, func(s state, _ bool) bool { return s.closing })
	c2 := e.postAsync(t, "channel/close", "writer", closeBody(tok))
	select {
	case r := <-c1:
		t.Fatalf("close answered %v before the upload finished", r.frames)
	case <-time.After(300 * time.Millisecond):
	}
	if r := e.post(t, "channel", "writer", ex(tok, 3, "INSERT INTO t VALUES (9)")); r.code() != "channel_lost" {
		t.Errorf("a sequenced request while closing: %v, want channel_lost", r.frames)
	}
	release()
	r := <-run
	txn := num(r.done(), "txn")
	if r.status != 200 || txn == 0 {
		t.Fatalf("the write: %d %v", r.status, r.frames)
	}
	for _, c := range []<-chan answer{c1, c2} {
		a := <-c
		o, _ := a.done()["outcome"].(map[string]any)
		if a.status != 200 || num(a.done(), "last_seq") != 2 || o["ok"] != true || num(o, "txn") != txn || num(o, "seq") != 2 {
			t.Errorf("close: %d %v, want last_seq 2 with the committed receipt at %d", a.status, a.frames, txn)
		}
	}
	if r := e.post(t, "channel/close", "writer", closeBody(tok)); r.code() != "channel_lost" || r.status != http.StatusGone {
		t.Errorf("close after the end: %d %v", r.status, r.frames)
	}
	if _, ok := e.chState(tok); ok {
		t.Error("the closed channel is still in the table")
	}
}

// TestCloseCancelsRunningWrite: a close interrupts a write before its
// commit point; it is recorded cancelled, and nothing commits.
func TestCloseCancelsRunningWrite(t *testing.T) {
	e, _ := newChanEnv(t, nil)
	tok := e.open(t, "writer")
	e.post(t, "channel", "writer", ex(tok, 1, "CREATE TABLE t (a)"))
	head := e.leader(t).Head()
	run := e.postAsync(t, "channel", "writer", ex(tok, 2, "INSERT INTO t "+cpuBound))
	e.waitChan(t, tok, func(s state, _ bool) bool { return s.running })
	time.Sleep(100 * time.Millisecond)
	c := e.post(t, "channel/close", "writer", closeBody(tok))
	o, _ := c.done()["outcome"].(map[string]any)
	oe, _ := o["error"].(map[string]any)
	if oe["code"] != "cancelled" || oe["not_executed"] != true || num(c.done(), "last_seq") != 2 {
		t.Errorf("close: %v, want the cancelled receipt", c.frames)
	}
	if r := <-run; r.status != 200 || r.code() != "cancelled" {
		t.Errorf("the write: %d %v, want cancelled", r.status, r.frames)
	}
	if h := e.leader(t).Head(); h != head {
		t.Errorf("head %d, was %d: the cancelled write committed", h, head)
	}
}

func TestCommitIDOnEveryWritePath(t *testing.T) {
	e, _ := newChanEnv(t, nil)
	db := e.dbID(t)
	tok := e.open(t, "writer")
	last := int64(e.leader(t).Head()) //nolint:gosec // small
	steps := []struct {
		sql   string
		wrote bool
	}{
		{"CREATE TABLE t (a)", true},
		{"CREATE TABLE log (x)", true},
		{"CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO log VALUES (new.a); END", true},
		{"INSERT INTO t VALUES (7) RETURNING a", true},
		{"UPDATE t SET a = 8 WHERE a = 100", false},
	}
	for i, st := range steps {
		seq := int64(i + 1)
		a := e.post(t, "channel", "writer", ex(tok, seq, st.sql))
		d := a.done()
		if a.status != 200 || d == nil || d["read_only"] != false || strings.Contains(st.sql, "RETURNING") && (len(a.rows()) != 1 || a.rows()[0] != "[7]") {
			t.Fatalf("%s: %d %v", st.sql, a.status, a.frames)
		}
		txn := num(d, "txn")
		switch {
		case st.wrote && txn != last+1:
			t.Errorf("%s: txn %d, want %d", st.sql, txn, last+1)
		case !st.wrote && (txn != last || num(d, "rows_affected") != 0):
			t.Errorf("%s: txn %d rows %d, want the head %d and no rows", st.sql, txn, num(d, "rows_affected"), last)
		}
		last = txn
	}
	hdrs := e.commits(t, db)
	for i, st := range steps {
		n := withID(hdrs, wantID(tok, uint64(i+1))) //nolint:gosec // small
		if st.wrote != (n == 1) || n > 1 {
			t.Errorf("%s: %d commits with its identifier", st.sql, n)
		}
	}
	// Every commit but the database's first and the epoch bumps carries an
	// identifier of a request.
	for _, h := range hdrs {
		if h.Txn > 1 && !h.EpochBump && (len(h.CommitID) != 24 || !bytes.Equal(h.CommitID[:16], wantID(tok, 0)[:16])) {
			t.Errorf("commit %d carries identifier %x", h.Txn, h.CommitID)
		}
	}
	if hd := e.leader(t).Head(); hd != uint64(last) { //nolint:gosec // positive
		t.Errorf("head %d, want %d: the no-op update committed", hd, last)
	}
	if got := e.count(t, "SELECT group_concat(x) FROM log"); got != `["7"]` {
		t.Errorf("trigger rows %s", got)
	}
}

func TestInsertOrFailLeavesNothing(t *testing.T) {
	e, _ := newChanEnv(t, nil)
	tok := e.open(t, "writer")
	e.post(t, "channel", "writer", ex(tok, 1, "CREATE TABLE t (a UNIQUE)"))
	e.post(t, "channel", "writer", ex(tok, 2, "INSERT INTO t VALUES (1)"))
	head := e.leader(t).Head()
	// SQLite's FAIL keeps the row 2 it inserted before the failure; an
	// autocommit request in skylite keeps nothing.
	a := e.post(t, "channel", "writer", ex(tok, 3, "INSERT OR FAIL INTO t VALUES (2), (1)"))
	if a.status != 200 || a.code() != "sql_error" || a.err()["not_executed"] != true || a.err()["final"] != true {
		t.Errorf("%d %v, want a recorded sql_error", a.status, a.frames)
	}
	if got := e.count(t, "SELECT group_concat(a) FROM t"); got != `["1"]` {
		t.Errorf("rows %s, want 1 alone", got)
	}
	if h := e.leader(t).Head(); h != head {
		t.Errorf("head %d, was %d", h, head)
	}
}

func TestHeldRowsNotSentBeforeCommit(t *testing.T) {
	e, hs := newChanEnv(t, nil)
	tok := e.open(t, "writer")
	e.post(t, "channel", "writer", ex(tok, 1, "CREATE TABLE t (a NOT NULL)"))
	body := ex(tok, 2, "INSERT INTO t VALUES (1), (2) RETURNING a")
	g, release := hs.hold()
	defer release()
	c, err := net.Dial("tcp", e.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if _, err := fmt.Fprintf(c, "POST /v1/databases/d/channel HTTP/1.1\r\nHost: skylite\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", e.sig.token(t, "writer"), len(body), body); err != nil {
		t.Fatal(err)
	}
	g.wait(t)
	_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if n, _ := c.Read(make([]byte, 1)); n != 0 {
		t.Fatal("the response started before the commit had an outcome")
	}
	release()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	frames, err := parseFrames(b)
	if err != nil {
		t.Fatal(err)
	}
	if a := (answer{resp.StatusCode, frames}); len(a.rows()) != 2 || a.done() == nil {
		t.Errorf("after the commit: %v", frames)
	}
	// A write that fails after producing rows sends none of them.
	a := e.post(t, "channel", "writer", ex(tok, 3, "INSERT INTO t VALUES (3), (NULL) RETURNING a"))
	if len(a.frames) != 1 || a.code() != "sql_error" {
		t.Errorf("failed write: %v, want its error alone", a.frames)
	}
}

func TestDurableNeverNegative(t *testing.T) {
	e, hs := newChanEnv(t, nil)
	a, b := e.open(t, "writer"), e.open(t, "admin")
	e.post(t, "channel", "writer", ex(a, 1, "CREATE TABLE t (a)"))
	g, release := hs.hold()
	defer release()
	drop := func(token, body string) {
		c, err := net.Dial("tcp", e.addr)
		if err != nil {
			t.Error(err)
			return
		}
		if _, err := fmt.Fprintf(c, "POST /v1/databases/d/channel HTTP/1.1\r\nHost: skylite\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", token, len(body), body); err != nil {
			t.Error(err)
		}
		time.Sleep(100 * time.Millisecond)
		_ = c.Close() // the client drops; its response cannot be written
	}
	// A drops in durable wait; B drops while it waits for the write lock,
	// before its commit point.
	ba, bb := ex(a, 2, "INSERT INTO t VALUES (1)"), ex(b, 1, "INSERT INTO t VALUES (2)")
	go drop(e.sig.token(t, "writer"), ba)
	g.wait(t)
	drop(e.sig.token(t, "admin"), bb)
	e.waitChan(t, b, func(s state, _ bool) bool { return s.running })
	release()
	for _, c := range []struct{ subject, tok, body string }{{"writer", a, ba}, {"admin", b, bb}} {
		e.waitChan(t, c.tok, func(s state, _ bool) bool { return !s.running })
		r := e.post(t, "channel", c.subject, c.body)
		if r.status != 200 || r.done() == nil || num(r.done(), "txn") == 0 {
			t.Errorf("replay after the client dropped: %d %v, want committed", r.status, r.frames)
		}
	}
	if got := e.count(t, "SELECT count(*) FROM t"); got != "[2]" {
		t.Errorf("rows %s, want both", got)
	}
}

func TestReplayDataOmitted(t *testing.T) {
	e, _ := newChanEnv(t, nil)
	tok := e.open(t, "writer")
	post := func(body string) answer { return e.post(t, "channel", "writer", body) }
	post(ex(tok, 1, "CREATE TABLE t (a)"))
	small := ex(tok, 2, "INSERT INTO t VALUES (1) RETURNING a")
	first := post(small)
	if r := post(small); frameText(r) != frameText(first) || len(r.rows()) != 1 {
		t.Errorf("small replay %v, want %v", r.frames, first.frames)
	}
	// Over the retained-data bound of 64 KiB, under the held bound of 1 MiB.
	big := ex(tok, 3, "INSERT INTO t WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c LIMIT 100) SELECT printf('%01000d', x) FROM c RETURNING a")
	if r := post(big); len(r.rows()) != 100 {
		t.Fatalf("big write: %d %v", r.status, r.last())
	}
	r := post(big)
	if len(r.frames) != 1 || r.done()["data_omitted"] != true || num(r.done(), "rows_affected") != 100 {
		t.Errorf("big replay %v, want the receipt with data_omitted", r.frames)
	}
	// A write without frames, and a failure, replay without the flag.
	post(ex(tok, 4, "INSERT INTO t VALUES (1)"))
	if r := post(ex(tok, 4, "INSERT INTO t VALUES (1)")); r.done()["data_omitted"] != nil {
		t.Errorf("replay without frames %v", r.frames)
	}
	// A failure's receipt keeps its message within the error message bound.
	failed := ex(tok, 5, "INSERT INTO nope"+strings.Repeat("x", 3000)+" VALUES (1)")
	post(failed)
	if r := post(failed); r.code() != "sql_error" || r.err()["data_omitted"] != nil {
		t.Errorf("failure replay %v", r.frames)
	}
	e.s.chMu.Lock()
	m := e.s.dbs["d"].chans.chans[tok].receipt.Error.Message
	e.s.chMu.Unlock()
	if len(m) > 1024 || !strings.Contains(m, "no such table") {
		t.Errorf("recorded message of %d bytes: %.40q", len(m), m)
	}
	// The pod's replay budget full: dropped too.
	e.s.chMu.Lock()
	e.s.chl.replay = 10
	e.s.chMu.Unlock()
	post(ex(tok, 6, "INSERT INTO t VALUES (2) RETURNING a"))
	if r := post(ex(tok, 6, "INSERT INTO t VALUES (2) RETURNING a")); len(r.frames) != 1 || r.done()["data_omitted"] != true {
		t.Errorf("replay over the pod budget %v", r.frames)
	}
	e.post(t, "channel/close", "writer", closeBody(tok))
	e.s.chMu.Lock()
	defer e.s.chMu.Unlock()
	if e.s.replay != 0 || e.s.held != 0 || e.s.nChans != 0 {
		t.Errorf("after close: replay %d, held %d, channels %d; want none", e.s.replay, e.s.held, e.s.nChans)
	}
}

func TestBehindOnDeposedLeader(t *testing.T) {
	e, _ := newChanEnv(t, nil)
	tok := e.open(t, "writer")
	e.post(t, "channel", "writer", ex(tok, 1, "CREATE TABLE t (a)"))
	if a := e.post(t, "channel", "writer", ex(tok, 2, "UPDATE t SET a = 1 WHERE 0", "min_txn", e.leader(t).Head())); a.done() == nil {
		t.Errorf("min_txn at the head: %d %v, want served", a.status, a.frames)
	}
	keys, _ := e.s.cfg.Storage.keys("d")
	// The usurper's clock is past the lease, so it takes office; this
	// server learns it only at its next renewal.
	o, _, err := engine.Takeover(context.Background(), e.store, keys, engine.Candidate{Holder: "usurper", TTL: time.Minute, Now: func() time.Time { return time.Now().Add(2 * time.Minute) }})
	if err != nil {
		t.Fatal(err)
	}
	if e.s.dbs["d"].leader() == nil {
		t.Fatal("the server already left office")
	}
	body := ex(tok, 3, "UPDATE t SET a = 1 WHERE 0", "min_txn", o.Head)
	before, _ := e.chState(tok)
	a := e.post(t, "channel", "writer", body)
	if a.status != http.StatusServiceUnavailable || a.code() != "behind" || a.err()["not_executed"] != true || a.err()["final"] != false {
		t.Errorf("%d %v, want behind", a.status, a.frames)
	}
	if after, _ := e.chState(tok); after != before {
		t.Errorf("behind changed the channel: %+v", after)
	}
	// The same number is still free; a write on the deposed leader is fenced.
	a = e.post(t, "channel", "writer", ex(tok, 3, "INSERT INTO t VALUES (1)"))
	if a.status != 200 || a.code() != "commit_failed" || a.err()["final"] != true || a.err()["not_executed"] != true {
		t.Errorf("fenced write: %d %v, want a recorded commit_failed", a.status, a.frames)
	}
}

func TestTailBoundRefusal(t *testing.T) {
	// Creation and the epoch bump take two of the eight. No compaction
	// publishes, so the bound stays.
	e, hs := newChanEnv(t, func(e *env) { e.s.engine.TailBound = 8 })
	hs.frozen.Store(true)
	tok := e.open(t, "writer")
	seq := int64(0)
	var last answer
	for {
		seq++
		a := e.post(t, "channel", "writer", ex(tok, seq, fmt.Sprintf("CREATE TABLE t%d (a)", seq)))
		if a.code() == "maintenance" {
			// Refused before acceptance: the engine refuses the fresh
			// connection's Open as maintenance, without naming the bound.
			if a.status != http.StatusServiceUnavailable || a.err()["final"] != false {
				t.Errorf("%d %v, want 503 maintenance", a.status, a.frames)
			}
			break
		}
		if a.done() == nil || seq > 8 {
			t.Fatalf("seq %d: %d %v", seq, a.status, a.frames)
		}
		last = a
	}
	if seq != 7 {
		t.Errorf("refused at seq %d, want 7: six writes fill the bound", seq)
	}
	if r := e.post(t, "channel", "writer", ex(tok, seq-1, fmt.Sprintf("CREATE TABLE t%d (a)", seq-1))); frameText(r) != frameText(last) {
		t.Errorf("the last write's replay %v, want %v", r.frames, last.frames)
	}
	if s, _ := e.chState(tok); s.last != seq-1 {
		t.Errorf("last accepted %d, want %d", s.last, seq-1)
	}
}

func TestWALBoundMaintenanceThenRetry(t *testing.T) {
	e, _ := newChanEnv(t, func(e *env) { e.s.engine.WALBound = 64 << 10 })
	tok := e.open(t, "writer")
	seq := int64(1)
	e.post(t, "channel", "writer", ex(tok, seq, "CREATE TABLE t (a)"))
	// run sends sql as the next sequence number, again while it is refused
	// before acceptance, and returns the recorded answer.
	run := func(sql string) answer {
		t.Helper()
		seq++
		for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(20 * time.Millisecond) {
			a := e.post(t, "channel", "writer", ex(tok, seq, sql))
			if a.status != http.StatusServiceUnavailable {
				return a
			}
			if a.code() != "maintenance" || time.Now().After(deadline) {
				t.Fatalf("seq %d: %d %v", seq, a.status, a.frames)
			}
		}
	}
	recorded, wrote := false, 0
	for i := 0; !recorded || wrote < 3; i++ {
		a := run("INSERT INTO t VALUES (randomblob(4000))")
		switch {
		case i > 200:
			t.Fatalf("recorded maintenance %v, %d writes after it", recorded, wrote)
		case a.code() == "maintenance":
			if a.status != 200 || a.err()["final"] != true || a.err()["not_executed"] != true {
				t.Errorf("maintenance %d %v, want it recorded", a.status, a.frames)
			}
			recorded = true
		case a.done() != nil && recorded:
			wrote++
		case a.done() == nil:
			t.Fatalf("seq %d: %d %v", seq, a.status, a.frames)
		}
	}
	// A write larger than the bound by itself never commits; it may first
	// meet the bound of the WAL as it is.
	for i := 0; ; i++ {
		a := run("INSERT INTO t VALUES (randomblob(200000))")
		if a.code() == "commit_failed" && a.err()["final"] == true && a.err()["not_executed"] == true {
			break
		}
		if a.code() != "maintenance" || i > 5 {
			t.Fatalf("too large: %d %v, want commit_failed", a.status, a.frames)
		}
	}
}

func TestOtherPrincipalChannelLost(t *testing.T) {
	e, _ := newChanEnv(t, nil)
	tok := e.open(t, "writer")
	body := ex(tok, 1, "CREATE TABLE t (a)")
	for _, path := range []string{"channel", "channel/close"} {
		b := body
		if path != "channel" {
			b = closeBody(tok)
		}
		if a := e.post(t, path, "admin", b); a.status != http.StatusGone || a.code() != "channel_lost" || a.err()["channel_id"] != nil {
			t.Errorf("%s by another principal: %d %v", path, a.status, a.frames)
		}
	}
	if a := e.post(t, "channel", "reader", body); a.status != http.StatusForbidden {
		t.Errorf("a reader: %d %v, want forbidden", a.status, a.frames)
	}
	if a := e.post(t, "channel", "writer", body); a.done() == nil {
		t.Errorf("the owner after them: %d %v", a.status, a.frames)
	}
}

func TestOpenAnswerRolesAndBounds(t *testing.T) {
	e, _ := newChanEnv(t, nil)
	a := e.post(t, "channels", "writer", `{"mode":"write"}`)
	c, _ := a.done()["channel"].(map[string]any)
	st := e.s.dbs["d"].loop.State()
	tok, _ := c["token"].(string)
	parts := strings.Split(tok, ".")
	if a.status != 200 || num(a.done(), "txn") != int64(st.Leader.Head()) || num(c, "epoch") != int64(st.Office.Epoch) || len(c["id"].(string)) != 32 || c["mode"] != "write" || c["limits"] == nil || //nolint:gosec // small
		len(parts) != 5 || parts[1] != e.s.instance || parts[3] != fmt.Sprint(st.Leader.Head()) || len(parts[4]) != 43 {
		t.Errorf("open answer %v", a.frames)
	}
	if a := e.post(t, "channels", "reader", `{"mode":"write"}`); a.status != http.StatusForbidden {
		t.Errorf("a reader's write open: %d %v", a.status, a.frames)
	}
	if a := e.post(t, "channels", "reader", `{"mode":"read_only"}`); a.status != http.StatusServiceUnavailable || a.code() != "not_serving" {
		t.Errorf("read-only open: %d %v", a.status, a.frames)
	}
	e.s.chMu.Lock()
	e.s.chl.perPrincipal, e.s.chl.perDB, e.s.chl.channels = 2, 3, 3
	e.s.chMu.Unlock()
	e.open(t, "writer")
	if a := e.post(t, "channels", "writer", `{"mode":"write"}`); a.status != http.StatusTooManyRequests || a.code() != "too_many_channels" {
		t.Errorf("per principal: %d %v", a.status, a.frames)
	}
	e.open(t, "admin")
	if a := e.post(t, "channels", "admin", `{"mode":"write"}`); a.code() != "too_many_channels" {
		t.Errorf("per database: %v", a.frames)
	}
	e.s.chMu.Lock()
	e.s.chl.perDB = 10
	e.s.chMu.Unlock()
	if a := e.post(t, "channels", "admin", `{"mode":"write"}`); a.code() != "too_many_channels" {
		t.Errorf("per server: %v", a.frames)
	}
	e.post(t, "channel/close", "writer", closeBody(tok))
	e.open(t, "admin")
}

// lockEnv opens channels for a write lock test: a holder that keeps the
// lock while its commit is held, and three channels each for "writer" and
// "admin".
type lockEnv struct {
	e      *env
	hs     *holdStore
	holder string
	seq    int64
	toks   map[string][]string
}

func newLockEnv(t *testing.T, edit func(*env)) *lockEnv {
	t.Helper()
	e, hs := newChanEnv(t, edit)
	l := &lockEnv{e: e, hs: hs, holder: e.open(t, "writer"), seq: 1, toks: map[string][]string{}}
	e.post(t, "channel", "writer", ex(l.holder, 1, "CREATE TABLE t (a)"))
	for _, s := range []string{"writer", "admin"} {
		for range 3 {
			l.toks[s] = append(l.toks[s], e.open(t, s))
		}
	}
	return l
}

// hold takes the write lock and keeps it until the returned function runs.
func (l *lockEnv) hold(t *testing.T) func() {
	t.Helper()
	g, release := l.hs.hold()
	l.seq++
	done := l.e.postAsync(t, "channel", "writer", ex(l.holder, l.seq, "INSERT INTO t VALUES (0)"))
	g.wait(t)
	return func() { release(); <-done }
}

// waiting waits until n writers wait for the lock.
func (l *lockEnv) waiting(t *testing.T, n int) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		l.e.s.chMu.Lock()
		got := len(l.e.s.dbs["d"].chans.lock.queue)
		l.e.s.chMu.Unlock()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d writers waiting, want %d", got, n)
		}
	}
}

func (l *lockEnv) busy(t *testing.T, subject, tok, msg string) {
	t.Helper()
	a := l.e.post(t, "channel", subject, ex(tok, 1, "INSERT INTO t VALUES (9)"))
	if a.status != 200 || a.code() != "busy" || a.err()["final"] != true || a.err()["not_executed"] != true || !strings.Contains(a.err()["message"].(string), msg) {
		t.Errorf("%d %v, want a recorded busy: %s", a.status, a.frames, msg)
	}
}

func TestWriteLockFIFOAndBusy(t *testing.T) {
	// A long lock wait: on a slow runner the default 5 s can expire before
	// the hold is released, and the test is about order, not the wait.
	l := newLockEnv(t, func(e *env) { e.s.chl.waiters, e.s.chl.waitersBy, e.s.limits.LockWaitMS = 2, 1, 60000 })
	e := l.e
	release := l.hold(t)
	w := e.postAsync(t, "channel", "writer", ex(l.toks["writer"][0], 1, "INSERT INTO t VALUES (1)"))
	l.waiting(t, 1)
	l.busy(t, "writer", l.toks["writer"][1], "too many writers") // the principal's bound
	a := e.postAsync(t, "channel", "admin", ex(l.toks["admin"][0], 1, "INSERT INTO t VALUES (2)"))
	l.waiting(t, 2)
	e.s.chMu.Lock()
	e.s.chl.waitersBy = 2
	e.s.chMu.Unlock()
	l.busy(t, "admin", l.toks["admin"][1], "too many writers") // the database's bound
	release()
	aw, aa := <-w, <-a
	tw, ta := num(aw.done(), "txn"), num(aa.done(), "txn")
	if tw == 0 || ta != tw+1 {
		t.Errorf("transactions %d then %d, want first in, first out (writer %v, admin %v)", tw, ta, aw.err(), aa.err())
	}
	l.waiting(t, 0)
}

func TestLockWait(t *testing.T) {
	l := newLockEnv(t, func(e *env) { e.s.limits.LockWaitMS = 200 })
	release := l.hold(t)
	defer release()
	start := time.Now()
	l.busy(t, "writer", l.toks["writer"][0], "within 200 ms")
	if d := time.Since(start); d < 200*time.Millisecond || d > 3*time.Second {
		t.Errorf("busy after %v, want the lock wait", d)
	}
	l.waiting(t, 0)
}

// who is the principal of the channel of tok, as the pool counts it.
func (e *env) who(tok string) string {
	e.s.chMu.Lock()
	defer e.s.chMu.Unlock()
	return e.s.dbs["d"].chans.chans[tok].who
}

// TestWritersBeyondConnectionQuota: writers of one principal that outnumber
// its SQLite connection quota all wait for the write lock first in, first
// out, holding no connection while they wait, and each commits or gets
// busy; none is refused overloaded for want of a connection.
func TestWritersBeyondConnectionQuota(t *testing.T) {
	l := newLockEnv(t, func(e *env) { e.s.limits.LockWaitMS = 1000 })
	e := l.e
	n := 3 * e.s.pool.perPrincipal
	toks := make([]string, n)
	for i := range toks {
		toks[i] = e.open(t, "writer")
	}
	release := l.hold(t)
	start := time.Now()
	answers := make([]<-chan answer, n)
	for i, tok := range toks {
		answers[i] = e.postAsync(t, "channel", "writer", ex(tok, 1, fmt.Sprintf("INSERT INTO t VALUES (%d)", i+1)))
	}
	// Not l.waiting, whose Fatalf would leave the holder holding the lock.
	queued := 0
	for deadline := time.Now().Add(3 * time.Second); queued < n && time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		e.s.chMu.Lock()
		queued = len(e.s.dbs["d"].chans.lock.queue)
		e.s.chMu.Unlock()
	}
	e.s.pool.mu.Lock()
	busy := e.s.pool.busy[e.who(l.holder)]
	e.s.pool.mu.Unlock()
	if queued != n || busy != 1 {
		t.Errorf("%d writers waiting with %d connections of the principal in use; want %d, and the holder's alone", queued, busy, n)
	}
	release()
	for i, c := range answers {
		a := <-c
		switch {
		case a.done() != nil:
		case a.status == 200 && a.code() == "busy" && a.err()["final"] == true:
		default:
			t.Errorf("writer %d: %d %v, want done or a recorded busy", i, a.status, a.frames)
		}
	}
	if d := time.Since(start); d > time.Second+5*time.Second {
		t.Errorf("writers took %v, want about the lock wait", d)
	}
	e.quiet(t, "after the writers")
}

// TestCancelWhileWaitingForLock: a cancel or close of a write waiting for
// the lock records it cancelled and leaves no lock or connection behind.
func TestCancelWhileWaitingForLock(t *testing.T) {
	l := newLockEnv(t, nil)
	e := l.e
	release := l.hold(t)
	w := e.postAsync(t, "channel", "admin", ex(l.toks["admin"][0], 1, "INSERT INTO t VALUES (1)"))
	b := e.postAsync(t, "channel", "admin", sq(l.toks["admin"][1], 1, "begin"))
	l.waiting(t, 2)
	if c := e.post(t, "channel/cancel", "admin", cancelBody(l.toks["admin"][0], 1)); c.done() == nil {
		t.Errorf("cancel: %d %v", c.status, c.frames)
	}
	if c := e.post(t, "channel/close", "admin", closeBody(l.toks["admin"][1])); c.done() == nil {
		t.Errorf("close: %d %v", c.status, c.frames)
	}
	for _, a := range []answer{<-w, <-b} {
		if a.status != 200 || a.code() != "cancelled" || a.err()["final"] != true || a.err()["not_executed"] != true {
			t.Errorf("%d %v, want a recorded cancelled", a.status, a.frames)
		}
	}
	l.waiting(t, 0)
	release()
	if a := e.post(t, "channel", "admin", ex(l.toks["admin"][0], 2, "INSERT INTO t VALUES (2)")); a.done() == nil {
		t.Errorf("next write: %d %v", a.status, a.frames)
	}
	e.quiet(t, "after the cancels")
}

// TestNoConnectionAtGrant: a write granted the lock when its principal's
// connections are all in use records overloaded, final and not executed,
// and passes the lock on; its resend replays that receipt.
func TestNoConnectionAtGrant(t *testing.T) {
	l := newLockEnv(t, nil)
	e := l.e
	release := l.hold(t)
	w := e.postAsync(t, "channel", "admin", ex(l.toks["admin"][0], 1, "INSERT INTO t VALUES (1)"))
	b := e.postAsync(t, "channel", "admin", sq(l.toks["admin"][1], 1, "begin"))
	l.waiting(t, 2)
	admin := e.who(l.toks["admin"][0])
	taken := 0
	for ; taken < e.s.pool.perPrincipal; taken++ {
		if _, _, ok := e.s.pool.take(nil, admin); !ok {
			t.Error("the waiting writers hold the principal's connections")
			break
		}
	}
	release()
	first := <-w
	for _, a := range []answer{first, <-b} {
		if a.status != 200 || a.code() != "overloaded" || a.err()["final"] != true || a.err()["not_executed"] != true || a.err()["txn_state"] != "none" {
			t.Errorf("%d %v, want a recorded overloaded", a.status, a.frames)
		}
	}
	if r := e.post(t, "channel", "admin", ex(l.toks["admin"][0], 1, "INSERT INTO t VALUES (1)")); frameText(r) != frameText(first) {
		t.Errorf("resend %v, want the receipt %v", r.frames, first.frames)
	}
	for range taken {
		e.s.pool.release(admin, nil, false)
	}
	if a := e.post(t, "channel", "admin", ex(l.toks["admin"][0], 2, "INSERT INTO t VALUES (2)")); a.done() == nil {
		t.Errorf("next write: %d %v", a.status, a.frames)
	}
	if got := e.count(t, "SELECT count(*) FROM t WHERE a = 1"); got != "[0]" {
		t.Errorf("rows %s, want none of the refused write", got)
	}
	e.quiet(t, "after the refusals")
}

func TestUnsupportedOpsAndPing(t *testing.T) {
	e, _ := newChanEnv(t, nil)
	tok := e.open(t, "writer")
	op := func(seq int64, op string) answer {
		return e.post(t, "channel", "writer", fmt.Sprintf(`{"channel":%q,"seq":%d,"op":%q}`, tok, seq, op))
	}
	for _, o := range []string{"batch", "describe"} {
		if a := op(1, o); a.status != http.StatusServiceUnavailable || a.code() != "not_serving" {
			t.Errorf("%s: %d %v", o, a.status, a.frames)
		}
	}
	if s, _ := e.chState(tok); s.last != 0 {
		t.Errorf("a refused op took sequence number %d", s.last)
	}
	for i, o := range []string{"commit", "rollback"} {
		if a := op(int64(i+1), o); a.status != 200 || a.code() != "invalid_state" || a.err()["final"] != true || a.err()["not_executed"] != true {
			t.Errorf("%s with no transaction: %d %v", o, a.status, a.frames)
		}
	}
	if a := op(3, "ping"); a.status != 200 || num(a.done(), "txn") != int64(e.leader(t).Head()) || a.done()["txn_state"] != "none" || num(a.done(), "seq") != 3 { //nolint:gosec // small
		t.Errorf("ping: %d %v", a.status, a.frames)
	}
	if a := e.post(t, "resolve", "writer", fmt.Sprintf(`{"channel":%q,"seq":4}`, tok)); a.code() != "not_serving" {
		t.Errorf("resolve: %v", a.frames)
	}
	if a := e.post(t, "channel/cancel", "writer", fmt.Sprintf(`{"channel":%q,"seq":1,"x":%q}`, tok, strings.Repeat("a", 4100))); a.status != http.StatusRequestEntityTooLarge {
		t.Errorf("a cancel body over 4 KiB: %d %v", a.status, a.frames)
	}
	if a := e.post(t, "channel/close", "writer", fmt.Sprintf(`{"channel":%q,"x":%q}`, tok, strings.Repeat("a", 4100))); a.status != http.StatusRequestEntityTooLarge {
		t.Errorf("a close body over 4 KiB: %d %v", a.status, a.frames)
	}
}

func TestIdleChannelEnds(t *testing.T) {
	e, hs := newChanEnv(t, func(e *env) { e.s.limits.ChannelIdleMS, e.s.chl.sweep = 1000, 20*time.Millisecond })
	a := e.post(t, "channels", "writer", `{"mode":"write"}`)
	c, _ := a.done()["channel"].(map[string]any)
	if l, _ := c["limits"].(map[string]any); num(l, "channel_idle_ms") != 1000 {
		t.Errorf("limits %v", l)
	}
	tok := c["token"].(string)
	tk := e.sig.token(t, "writer")
	// Any request in time keeps it: the first is accepted, then replays and
	// refusals.
	for i := range 6 {
		time.Sleep(400 * time.Millisecond)
		seq, status := 1, 200
		if i%2 == 1 {
			seq, status = 3, http.StatusConflict
		}
		if a, err := e.send("channel", tk, fmt.Sprintf(`{"channel":%q,"seq":%d,"op":"ping"}`, tok, seq)); err != nil || a.status != status {
			t.Fatalf("request %d: %d %v %v", i, a.status, a.frames, err)
		}
	}
	// A request running past the idle limit keeps its channel.
	e.post(t, "channel", "writer", fmt.Sprintf(`{"channel":%q,"seq":2,"op":"execute","sql":"CREATE TABLE t (a)"}`, tok))
	g, release := hs.hold()
	defer release()
	run := e.postAsync(t, "channel", "writer", ex(tok, 3, "INSERT INTO t VALUES (1)"))
	g.wait(t)
	time.Sleep(1300 * time.Millisecond)
	release()
	if r := <-run; r.done() == nil {
		t.Fatalf("the long write: %v", r.frames)
	}
	if _, ok := e.chState(tok); !ok {
		t.Fatal("the channel ended while its request ran")
	}
	e.waitChan(t, tok, func(_ state, ok bool) bool { return !ok })
	if a := e.post(t, "channel", "writer", fmt.Sprintf(`{"channel":%q,"seq":1,"op":"ping"}`, tok)); a.code() != "channel_lost" {
		t.Errorf("after the idle limit: %v", a.frames)
	}
	e.s.chMu.Lock()
	defer e.s.chMu.Unlock()
	if e.s.nChans != 0 {
		t.Errorf("%d channels counted after the idle end", e.s.nChans)
	}
}

func TestHeldResultBounds(t *testing.T) {
	e, _ := newChanEnv(t, func(e *env) { e.s.limits.HeldResultBytes = 1000 })
	tok := e.open(t, "writer")
	e.post(t, "channel", "writer", ex(tok, 1, "CREATE TABLE t (a)"))
	head := e.leader(t).Head()
	a := e.post(t, "channel", "writer", ex(tok, 2, "INSERT INTO t VALUES (printf('%02000d', 1)) RETURNING a"))
	if a.status != 200 || a.code() != "result_too_large" || a.err()["not_executed"] != true || len(a.frames) != 1 {
		t.Errorf("per request: %d %v", a.status, a.frames)
	}
	e.s.chMu.Lock()
	e.s.chl.held = 50
	e.s.chMu.Unlock()
	if a := e.post(t, "channel", "writer", ex(tok, 3, "INSERT INTO t VALUES (printf('%0100d', 1)) RETURNING a")); a.code() != "result_too_large" || !strings.Contains(a.err()["message"].(string), "server's held results") {
		t.Errorf("per server: %v", a.frames)
	}
	if h := e.leader(t).Head(); h != head {
		t.Errorf("head %d, was %d", h, head)
	}
	e.s.chMu.Lock()
	defer e.s.chMu.Unlock()
	if e.s.held != 0 {
		t.Errorf("held %d after the writes", e.s.held)
	}
}

func TestTokenChecks(t *testing.T) {
	e, _ := newChanEnv(t, nil)
	tok := e.open(t, "writer")
	parts := strings.Split(tok, ".")
	alter := func(i int, v string) string {
		p := slices.Clone(parts)
		p[i] = v
		return strings.Join(p, ".")
	}
	for _, c := range []struct {
		tok, code string
	}{
		{alter(2, strings.Repeat("0", 32)), "invalid_request"},
		{"w1.x", "invalid_request"},
		{alter(1, "OTHERINSTANCE"), "channel_lost"},
		{alter(4, map[bool]string{true: "B", false: "A"}[parts[4][0] == 'A']+parts[4][1:]), "channel_lost"},
		{tok + "x", "channel_lost"},
		{alter(3, "1"), "channel_lost"},
	} {
		if a := e.post(t, "channel", "writer", ex(c.tok, 1, "CREATE TABLE t (a)")); a.code() != c.code {
			t.Errorf("%s: %v, want %s", c.tok, a.frames, c.code)
		}
	}
	if s, _ := e.chState(tok); s.last != 0 {
		t.Error("an altered token reached the channel")
	}
}

func TestLeaderStopEndsChannels(t *testing.T) {
	e, hs := newChanEnv(t, nil)
	a, b := e.open(t, "writer"), e.open(t, "writer")
	e.post(t, "channel", "writer", ex(a, 1, "CREATE TABLE t (a)"))
	// b runs a statement that never ends; a waits in its upload.
	rb := e.postAsync(t, "channel", "writer", ex(b, 1, "INSERT INTO t "+cpuBound))
	e.waitChan(t, b, func(s state, _ bool) bool { return s.running })
	time.Sleep(100 * time.Millisecond)
	l := e.leader(t)
	closed := make(chan error, 1)
	go func() { closed <- l.Close() }()
	if r := <-rb; r.status != 200 || r.code() != "channel_lost" || r.err()["not_executed"] != false {
		t.Errorf("interrupted statement: %d %v, want channel_lost, not_executed false", r.status, r.frames)
	}
	if err := <-closed; err != nil {
		t.Error(err)
	}
	// The next leader; a's channel ended with the last.
	if r := e.post(t, "channel", "writer", ex(a, 2, "INSERT INTO t VALUES (1)")); r.code() != "channel_lost" && r.code() != "not_serving" {
		t.Errorf("old channel: %v", r.frames)
	}
	for _, unknown := range []bool{false, true} {
		hs.unknown.Store(unknown)
		stopInUpload(t, e, hs, unknown)
	}
}

// stopInUpload closes the leader while a write waits in its upload, whose
// outcome is then unknown, or definitely not committed.
func stopInUpload(t *testing.T, e *env, hs *holdStore, unknown bool) {
	t.Helper()
	e.leader(t)
	a := e.open(t, "writer")
	g, release := hs.hold()
	defer release()
	ra := e.postAsync(t, "channel", "writer", ex(a, 1, "INSERT INTO t VALUES (1)"))
	g.wait(t)
	e.s.chMu.Lock()
	tb := e.s.dbs["d"].chans
	e.s.chMu.Unlock()
	l := e.leader(t)
	closed := make(chan error, 1)
	go func() { closed <- l.Close() }()
	// Ended but unsettled while its upload runs.
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		e.s.chMu.Lock()
		ch := tb.chans[a]
		present, ended := ch != nil, ch != nil && ch.ended
		e.s.chMu.Unlock()
		if ended && present {
			break
		}
		if !present || time.Now().After(deadline) {
			t.Fatalf("channel present %v, ended %v, want ended but unsettled", present, ended)
		}
	}
	release()
	want := "channel_lost"
	if unknown {
		want = "commit_outcome_unknown"
	}
	if r := <-ra; r.status != 200 || r.code() != want || r.err()["not_executed"] != false {
		t.Errorf("upload unknown %v: %d %v, want %s", unknown, r.status, r.frames, want)
	}
	e.s.chMu.Lock()
	left := tb.chans[a] != nil
	e.s.chMu.Unlock()
	if left {
		t.Error("the channel stayed in the table after its request ended")
	}
	if err := <-closed; err != nil {
		t.Error(err)
	}
	e.s.chMu.Lock()
	defer e.s.chMu.Unlock()
	if tb.chans[a] != nil || e.s.nChans != 0 {
		t.Errorf("channels left: %d; want none once the request ended", e.s.nChans)
	}
}

// TestCommittedThenFailed: a write that fails after its commit is answered
// with that commit, and a panic after acceptance ends the channel with
// channel_lost, not_executed false, leaving nothing running, held or
// locked.
func TestCommittedThenFailed(t *testing.T) {
	var mode atomic.Int32
	e, _ := newChanEnv(t, func(e *env) {
		e.s.afterCommit = func() error {
			switch mode.Load() {
			case 1:
				return fmt.Errorf("%w: injected after COMMIT", sqlpolicy.ErrBroken)
			case 2:
				panic("injected after COMMIT")
			}
			return nil
		}
	})
	db := e.dbID(t)
	tok := e.open(t, "writer")
	e.post(t, "channel", "writer", ex(tok, 1, "CREATE TABLE t (a)"))
	head := int64(e.leader(t).Head()) //nolint:gosec // small
	mode.Store(1)
	body := ex(tok, 2, "INSERT INTO t VALUES (1) RETURNING a")
	a := e.post(t, "channel", "writer", body)
	if a.status != 200 || num(a.done(), "txn") != head+1 || len(a.rows()) != 1 {
		t.Fatalf("committed, then failed: %d %v, want done at %d with its row", a.status, a.frames, head+1)
	}
	if r := e.post(t, "channel", "writer", body); frameText(r) != frameText(a) {
		t.Errorf("replay %v, want %v", r.frames, a.frames)
	}
	if c := e.post(t, "channel/close", "writer", closeBody(tok)); c.status != 200 || num(c.done(), "last_seq") != 2 {
		t.Errorf("close: %d %v", c.status, c.frames)
	}
	mode.Store(2)
	tok2 := e.open(t, "writer")
	a = e.post(t, "channel", "writer", ex(tok2, 1, "INSERT INTO t VALUES (2)"))
	mode.Store(0)
	if a.status != 200 || a.code() != "channel_lost" || a.err()["not_executed"] != false || num(a.err(), "seq") != 1 {
		t.Errorf("panic after the commit: %d %v, want channel_lost, not_executed false", a.status, a.frames)
	}
	if _, ok := e.chState(tok2); ok {
		t.Error("the channel stayed in the table")
	}
	if c := e.post(t, "channel/close", "writer", closeBody(tok2)); c.code() != "channel_lost" {
		t.Errorf("close after the panic: %v", c.frames)
	}
	// The write lock was released: another channel writes.
	tok3 := e.open(t, "writer")
	if r := e.post(t, "channel", "writer", ex(tok3, 1, "INSERT INTO t VALUES (3)")); r.done() == nil {
		t.Errorf("a write after the panic: %v", r.frames)
	}
	hdrs := e.commits(t, db)
	if n, m := withID(hdrs, wantID(tok, 2)), withID(hdrs, wantID(tok2, 1)); n != 1 || m != 1 {
		t.Errorf("commits %d and %d, want one each", n, m)
	}
	if got := e.count(t, "SELECT group_concat(a) FROM t"); got != `["1,2,3"]` {
		t.Errorf("rows %s", got)
	}
	e.s.chMu.Lock()
	defer e.s.chMu.Unlock()
	if e.s.held != 0 || e.s.nChans != 1 || e.s.dbs["d"].chans.lock.held {
		t.Errorf("held %d, channels %d, lock held %v; want 0, 1, false", e.s.held, e.s.nChans, e.s.dbs["d"].chans.lock.held)
	}
}

// TestRacingFirstDeliveries: copies of one request that arrive together
// execute once.
func TestRacingFirstDeliveries(t *testing.T) {
	e, _ := newChanEnv(t, nil)
	db := e.dbID(t)
	tok := e.open(t, "writer")
	e.post(t, "channel", "writer", ex(tok, 1, "CREATE TABLE t (a)"))
	tk := e.sig.token(t, "writer")
	for seq := int64(2); seq <= 6; seq++ {
		body := ex(tok, seq, "INSERT INTO t VALUES (1)")
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				r, err := e.send("channel", tk, body)
				if err != nil {
					t.Error(err)
					return
				}
				if r.done() == nil && r.code() != "in_flight" && r.code() != "overloaded" {
					t.Errorf("seq %d: %d %v", seq, r.status, r.frames)
				}
			})
		}
		wg.Wait()
		if n := withID(e.commits(t, db), wantID(tok, uint64(seq))); n != 1 { //nolint:gosec // small
			t.Fatalf("seq %d: %d commits, want one", seq, n)
		}
	}
	if got := e.count(t, "SELECT count(*) FROM t"); got != "[5]" {
		t.Errorf("rows %s, want 5", got)
	}
}

func TestChannelTimeoutMS(t *testing.T) {
	e, _ := newChanEnv(t, nil)
	tok := e.open(t, "writer")
	e.post(t, "channel", "writer", ex(tok, 1, "CREATE TABLE t (a)"))
	start := time.Now()
	a := e.post(t, "channel", "writer", ex(tok, 2, "INSERT INTO t "+cpuBound, "timeout_ms", 300))
	if d := time.Since(start); a.status != 200 || a.code() != "timeout" || a.err()["final"] != true || a.err()["not_executed"] != true || d > 5*time.Second {
		t.Errorf("%d %v after %v, want a recorded timeout after 300 ms", a.status, a.frames, d)
	}
}

// TestLockGrantedAsWaitEnds: a waiter granted the write lock as its wait
// ends passes the lock on.
func TestLockGrantedAsWaitEnds(t *testing.T) {
	s := &Server{chl: defaultChLimits()}
	tb := &table{lock: writeLock{by: map[string]int{}}}
	queued := func() bool {
		s.chMu.Lock()
		if len(tb.lock.queue) == 1 {
			return true // chMu stays held
		}
		s.chMu.Unlock()
		return false
	}
	for range 50 {
		if err := s.acquire(context.Background(), tb, "a"); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		got := make(chan error, 1)
		go func() { got <- s.acquire(ctx, tb, "b") }()
		for !queued() {
			time.Sleep(time.Millisecond)
		}
		// Grant the lock and end the wait together.
		cancel()
		s.releaseLocked(tb)
		s.chMu.Unlock()
		if err := <-got; err == nil {
			s.release(tb) // the waiter took the lock
		}
		s.chMu.Lock()
		held, n := tb.lock.held, len(tb.lock.queue)
		s.chMu.Unlock()
		if held || n != 0 {
			t.Fatalf("lock held %v with %d waiting after the waiter left", held, n)
		}
	}
}
