package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// The WAL file format, https://www.sqlite.org/fileformat2.html#walformat.
const (
	walHeaderSize   = 32
	frameHeaderSize = 24
	walMagic        = 0x377f0682 // low bit set: checksums in big-endian words
	walVersion      = 3007000
)

// capture is one transaction's frames, read back from the WAL at the sync
// that ends its commit.
type capture struct {
	start  int64    // offset of its first frame
	end    int64    // end of its commit frame
	size   uint32   // database size in pages after it
	pgnos  []uint32 // ascending
	offs   []int64  // WAL offset of each page's newest body
	frames int
}

// parse reads the frames written since start, up to hwm, and returns the
// transaction they hold if they end in a commit frame, or nil if no commit
// frame has been written. Every frame must carry the WAL header's salts and
// continue its checksum chain: those are the frames SQLite's own recovery
// would accept.
func (l *Leader) parse(w *walFile, start, hwm int64) (*capture, error) {
	be := binary.BigEndian
	var hdr [walHeaderSize]byte
	if _, err := w.File.ReadAt(hdr[:], 0); err != nil {
		return nil, fmt.Errorf("engine: reading the WAL header: %w", err)
	}
	magic := be.Uint32(hdr[0:])
	if magic&^1 != walMagic || be.Uint32(hdr[4:]) != walVersion || int64(be.Uint32(hdr[8:])) != l.ps {
		return nil, fmt.Errorf("engine: WAL header %x", hdr[:12])
	}
	big := magic&1 == 1
	s1, s2 := walSum(hdr[:24], 0, 0, big)
	if s1 != be.Uint32(hdr[24:]) || s2 != be.Uint32(hdr[28:]) {
		return nil, errors.New("engine: WAL header checksum")
	}
	frame := l.ps + frameHeaderSize
	if start != walHeaderSize {
		var prev [8]byte
		if _, err := w.File.ReadAt(prev[:], start-frame+16); err != nil {
			return nil, fmt.Errorf("engine: reading the frame before %d: %w", start, err)
		}
		s1, s2 = be.Uint32(prev[:]), be.Uint32(prev[4:])
	}
	buf := make([]byte, frame)
	newest := map[uint32]int64{}
	for off := start; off+frame <= hwm; off += frame {
		if _, err := w.File.ReadAt(buf, off); err != nil {
			return nil, fmt.Errorf("engine: reading the WAL frame at %d: %w", off, err)
		}
		pgno, commit := be.Uint32(buf[0:]), be.Uint32(buf[4:])
		s1, s2 = walSum(buf[:8], s1, s2, big)
		s1, s2 = walSum(buf[frameHeaderSize:], s1, s2, big)
		switch {
		case !bytes.Equal(buf[8:16], hdr[16:24]):
			return nil, fmt.Errorf("engine: WAL frame at %d has another WAL's salts", off)
		case s1 != be.Uint32(buf[16:]) || s2 != be.Uint32(buf[20:]):
			return nil, fmt.Errorf("engine: WAL frame at %d fails its checksum", off)
		case pgno == 0 || pgno == format.LockPage(uint32(l.ps)): //nolint:gosec // a page size
			return nil, fmt.Errorf("engine: WAL frame at %d holds page %d", off, pgno)
		}
		newest[pgno] = off + frameHeaderSize
		if commit == 0 {
			continue
		}
		cp := &capture{start: start, end: off + frame, size: commit, frames: int((off-start)/frame) + 1}
		for p := range newest {
			// Pages beyond the new size were spilled earlier and then
			// truncated away; SQLite leaves them out of the commit too.
			if p <= commit {
				cp.pgnos = append(cp.pgnos, p)
			}
		}
		slices.Sort(cp.pgnos)
		for _, p := range cp.pgnos {
			cp.offs = append(cp.offs, newest[p])
		}
		return cp, nil
	}
	return nil, nil
}

// walSum continues SQLite's WAL checksum over b.
func walSum(b []byte, s1, s2 uint32, big bool) (uint32, uint32) {
	var order binary.ByteOrder = binary.LittleEndian
	if big {
		order = binary.BigEndian
	}
	for i := 0; i+8 <= len(b); i += 8 {
		s1 += order.Uint32(b[i:]) + s2
		s2 += order.Uint32(b[i+4:]) + s1
	}
	return s1, s2
}

// walPages reads the newest body of each captured page from the WAL.
type walPages struct {
	w   *walFile
	cp  *capture
	ps  int64
	i   int
	off int64
}

func (r *walPages) Read(p []byte) (int, error) {
	if r.i == len(r.cp.offs) {
		return 0, io.EOF
	}
	n := int(min(int64(len(p)), r.ps-r.off))
	if _, err := r.w.File.ReadAt(p[:n], r.cp.offs[r.i]+r.off); err != nil {
		return 0, err
	}
	if r.off += int64(n); r.off == r.ps {
		r.i, r.off = r.i+1, 0
	}
	return n, nil
}

// built is a commit object encoded into the leader's scratch file.
type built struct {
	key     string
	size    int64
	sum     [sha256.Size]byte
	h       format.CommitHeader // decoded, as a reader indexes it
	entries []format.PageEntry
}

func (l *Leader) build(w *walFile, cp *capture, h format.CommitHeader) (*built, error) {
	if err := l.scratch.Truncate(0); err != nil {
		return nil, err
	}
	hash := sha256.New()
	if err := format.EncodeCommit(io.MultiWriter(&offsetWriter{f: l.scratch}, hash), h, cp.pgnos, &walPages{w: w, cp: cp, ps: l.ps}); err != nil {
		return nil, err
	}
	b := &built{key: l.keys.Commit(l.office.DB, h.Txn), size: h.Size()}
	hash.Sum(b.sum[:0])
	front := make([]byte, format.CommitHeaderSize+h.TableSize())
	if _, err := l.scratch.ReadAt(front, 0); err != nil {
		return nil, err
	}
	var err error
	if b.h, err = format.DecodeCommitHeader(front[:format.CommitHeaderSize], l.office.DB); err != nil {
		return nil, err
	}
	if b.entries, err = format.DecodeCommitTable(b.h, front[format.CommitHeaderSize:]); err != nil {
		return nil, err
	}
	return b, nil
}

type offsetWriter struct {
	f   interface{ WriteAt([]byte, int64) (int, error) }
	off int64
}

func (w *offsetWriter) Write(p []byte) (int, error) {
	n, err := w.f.WriteAt(p, w.off)
	w.off += int64(n)
	return n, err
}

// outcome is how a create resolved.
type outcome int

const (
	committed outcome = iota
	notCommitted
	fenced
	indeterminate
)

// commit turns the captured transaction into the commit object at the head
// plus one and creates it; the create is the commit point. It returns nil
// only once the create resolved as committed (decision 6). Every failure is
// an error that makes SQLite roll the transaction back, never BUSY.
func (l *Leader) commit(w *walFile, cp *capture) error {
	c := w.c
	l.cmu.Lock()
	var refuse error
	switch {
	case l.noCreates:
		refuse = fmt.Errorf("%w: %w: maintenance is running", ErrNotCommitted, ErrRetry)
	case c.id == nil:
		refuse = fmt.Errorf("%w: the transaction has no commit identifier", ErrNotCommitted)
	case c.idUsed:
		refuse = fmt.Errorf("%w: a second commit with one commit identifier", ErrNotCommitted)
	case l.head == ^uint64(0):
		refuse = l.stopLocked(errMaxTxn)
	case l.head-l.manifestTxn >= l.cfg.TailBound:
		l.startMaintenance()
		refuse = fmt.Errorf("%w: %w: the log tail holds %d commits, its bound", ErrNotCommitted, ErrRetry, l.head-l.manifestTxn)
	}
	h := format.CommitHeader{
		DB: l.office.DB, PageSize: uint32(l.ps), Txn: l.head + 1, Epoch: l.office.Epoch, //nolint:gosec // a page size
		DBSize: cp.size, PageCount: uint32(len(cp.pgnos)), CommitID: c.id, //nolint:gosec // at most the database size
	}
	if refuse == nil && h.Size() > format.MaxCommitSize {
		refuse = fmt.Errorf("%w: %w: a commit object of %d bytes, above %d", ErrNotCommitted, ErrTooLarge, h.Size(), format.MaxCommitSize)
	}
	if refuse == nil {
		c.idUsed = true
		l.uploading = true
	}
	l.cmu.Unlock()
	if refuse != nil {
		return l.fail(w, cp.start, refuse)
	}
	b, err := l.build(w, cp, h)
	if err != nil {
		return l.fail(w, cp.start, fmt.Errorf("%w: building the commit object: %w", ErrNotCommitted, err))
	}

	// The commit path: from here until SQLite completes the commit, any
	// failure the engine did not resolve stops the database.
	l.cmu.Lock()
	c.inCommit = true
	l.cmu.Unlock()
	res, version, retired, err := l.create(b)
	switch res {
	case committed:
		l.cmu.Lock()
		l.head, l.headSize = h.Txn, h.DBSize
		pc := &commit{key: b.key, version: version, h: b.h, entries: b.entries}
		if retired {
			pc.entries = nil
		}
		l.pending = append(l.pending, pc)
		c.uploaded, c.retired = h.Txn, retired
		l.wal.start, l.wal.end = -1, cp.end
		if l.head-l.manifestTxn >= l.cfg.TailBound {
			l.startMaintenance()
		}
		l.cmu.Unlock()
		if l.afterUpload != nil {
			l.afterUpload()
		}
		return nil
	case notCommitted:
		err = l.fail(w, cp.start, fmt.Errorf("%w: every attempt to create commit %d had no effect: %w", ErrNotCommitted, h.Txn, err))
		return err
	case fenced:
		err = l.fail(w, cp.start, fmt.Errorf("%w: %w: commit %d", ErrNotCommitted, ErrFenced, h.Txn))
		_ = l.stop(err)
		return err
	}
	l.cmu.Lock()
	defer l.cmu.Unlock()
	c.inCommit, l.uploading = false, false
	c.outcome = fmt.Errorf("%w: commit %d: %w", ErrUnknown, h.Txn, err)
	return l.stopLocked(c.outcome)
}

// fail ends a commit known not to be in the durable history: it truncates
// the WAL back to the end of the last uploaded commit, so the transaction's
// frames cannot come back, and releases the transaction number. If the
// truncation fails, the database stops.
func (l *Leader) fail(w *walFile, start int64, cause error) error {
	l.cmu.Lock()
	defer l.cmu.Unlock()
	w.c.inCommit, l.uploading = false, false
	l.cond.Broadcast()
	if err := l.truncateWAL(w, start); err != nil {
		w.c.outcome = fmt.Errorf("%w: %w", ErrNotCommitted, l.stopLocked(err))
		return w.c.outcome
	}
	w.c.outcome = cause
	return cause
}

// truncateWAL truncates the WAL to start, with l.cmu held.
func (l *Leader) truncateWAL(w *walFile, start int64) error {
	err := l.injected("wal", "truncate", start, 0)
	if err == nil {
		err = w.File.Truncate(start)
	}
	if err != nil {
		return fmt.Errorf("engine: truncating the WAL after a failed commit: %w", err)
	}
	err = l.injected("wal", "sync", start, 0)
	if err == nil {
		err = w.File.Sync(0)
	}
	if err != nil {
		return fmt.Errorf("engine: syncing the WAL after a failed commit: %w", err)
	}
	l.wal.start, l.wal.hwm = -1, start
	return nil
}

// create creates the commit object and resolves the outcome as decision 6
// says. It repeats the identical create after any failure, up to
// createAttempts. An unknown outcome is sticky. A precondition failure is
// resolved by reading the name.
func (l *Leader) create(b *built) (res outcome, version string, retired bool, err error) {
	var unknown, occupied bool
	for range createAttempts {
		if l.ctx.Err() != nil {
			err = l.ctx.Err()
			break
		}
		version, err = l.st.Create(l.ctx, b.key, io.NewSectionReader(l.scratch, 0, b.size))
		switch {
		case err == nil:
			return committed, version, false, nil
		case errors.Is(err, storage.ErrPreconditionFailed):
			occupied = true
			res, version, retired, err = l.readName(b)
			if err == nil {
				return res, version, retired, nil
			}
			if res == indeterminate {
				return indeterminate, "", false, err
			}
		case errors.Is(err, storage.ErrOutcomeUnknown):
			unknown = true
		}
	}
	if unknown || occupied {
		return indeterminate, "", false, err
	}
	return notCommitted, "", false, err
}

// readName reads the object at the commit's name after a precondition
// failure. Only this leader creates ordinary commits after its own last
// commit, so a commit there that is not an epoch bump is its own, retired
// or not; an epoch bump means it is fenced. Anything else breaks an
// invariant. A failed read returns notCommitted with the error, to be
// retried.
func (l *Leader) readName(b *built) (outcome, string, bool, error) {
	rc, info, err := l.st.Get(l.ctx, b.key)
	if errors.Is(err, storage.ErrNotFound) {
		return indeterminate, "", false, fmt.Errorf("%w: commit %d absent after its create failed its precondition", ErrCorrupt, b.h.Txn)
	}
	if err != nil {
		return notCommitted, "", false, err
	}
	defer func() { _ = rc.Close() }()
	front := make([]byte, format.CommitHeaderSize)
	if _, err := io.ReadFull(rc, front); err != nil {
		return notCommitted, "", false, err
	}
	h, err := format.DecodeCommitHeader(front, l.office.DB)
	if err == nil {
		err = h.CheckKey(l.office.DB, b.h.Txn)
	}
	switch {
	case err != nil:
		return indeterminate, "", false, fmt.Errorf("engine: commit %d: %w", b.h.Txn, err)
	case h.EpochBump:
		return fenced, "", false, nil
	case h.Epoch != b.h.Epoch || !bytes.Equal(h.CommitID, b.h.CommitID):
		return indeterminate, "", false, fmt.Errorf("%w: commit %d is not this leader's: epoch %d, identifier %x", ErrCorrupt, b.h.Txn, h.Epoch, h.CommitID)
	case h.Retired:
		return committed, "", true, nil
	}
	hash := sha256.New()
	hash.Write(front)
	if _, err := io.Copy(hash, rc); err != nil {
		return notCommitted, "", false, err
	}
	if !bytes.Equal(hash.Sum(nil), b.sum[:]) || info.Size != b.size {
		return indeterminate, "", false, fmt.Errorf("%w: commit %d holds other bytes than this leader built", ErrCorrupt, b.h.Txn)
	}
	return committed, info.Version, false, nil
}
