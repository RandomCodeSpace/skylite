package engine

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// ErrExists means another creator's database already holds the prefix.
var ErrExists = errors.New("engine: a database already exists under this prefix")

// createAttempts bounds how often one create is sent before the creator
// gives up on it.
const createAttempts = 5

// Create creates a new database under keys' prefix, as decision 3 of the
// storage engine record describes: it draws a database identifier, creates
// the first commit object (transaction 1, epoch 1, holding the empty WAL-mode
// database at pageSize) and only after that create has resolved as its own
// creates the first manifest. It returns the new database's identifier.
//
// If another creator's manifest is already there, Create returns that
// database's identifier and an error wrapping ErrExists; the caller uses the
// database that won. Objects a losing or abandoned creator wrote stay under an
// identifier no manifest names. Create gives up with an error when it cannot
// resolve either create within its attempts; an error that wraps
// storage.ErrOutcomeUnknown means a create may still land.
func Create(ctx context.Context, st storage.Store, keys format.Keys, pageSize, groupSize uint32) (format.DatabaseID, error) {
	var id format.DatabaseID
	var nonce [16]byte
	for id == (format.DatabaseID{}) || nonce == [16]byte{} {
		if _, err := rand.Read(id[:]); err != nil {
			return format.DatabaseID{}, err
		}
		if _, err := rand.Read(nonce[:]); err != nil {
			return format.DatabaseID{}, err
		}
	}

	var commit bytes.Buffer
	h := format.CommitHeader{DB: id, PageSize: pageSize, Txn: 1, Epoch: 1, DBSize: 1, PageCount: 1}
	if err := format.EncodeCommit(&commit, h, []uint32{1}, bytes.NewReader(emptyDatabase(pageSize))); err != nil {
		return format.DatabaseID{}, err
	}
	manifest, err := format.EncodeManifest(format.Manifest{DB: id, PageSize: pageSize, GroupSize: groupSize, Epoch: 1, Nonce: nonce})
	if err != nil {
		return format.DatabaseID{}, err
	}

	// The identifier is random and new, so bytes equal to ours can only be
	// our own create's.
	if _, err := create(ctx, st, keys.Commit(id, 1), commit.Bytes(), func(b []byte) (bool, error) {
		if !bytes.Equal(b, commit.Bytes()) {
			return false, fmt.Errorf("%w: another object at the first commit of new database %x", ErrCorrupt, id)
		}
		return true, nil
	}); err != nil {
		return format.DatabaseID{}, err
	}

	// Every manifest naming our identifier descends from our create: a
	// replacement keeps the identifier, and nobody else has it.
	var winner format.DatabaseID
	own, err := create(ctx, st, keys.Manifest(), manifest, func(b []byte) (bool, error) {
		m, err := format.DecodeManifest(b, format.DatabaseID{})
		if err != nil {
			return false, fmt.Errorf("engine: reading the existing manifest: %w", err)
		}
		winner = m.DB
		return m.DB == id, nil
	})
	switch {
	case err != nil:
		return format.DatabaseID{}, err
	case !own:
		return winner, fmt.Errorf("%w: database %x", ErrExists, winner)
	}
	return id, nil
}

// create creates key with body and resolves the outcome as decision 6 of
// the record says. It repeats the identical create after any failure, up to
// createAttempts. Success resolves it as ours; a precondition failure is
// resolved by reading the key and asking own, whose error ends the
// operation. Failures with no effect resolve nothing, because an earlier
// attempt with an unknown outcome may still land.
func create(ctx context.Context, st storage.Store, key string, body []byte, own func([]byte) (bool, error)) (bool, error) {
	var unknown, last error
	for range createAttempts {
		_, err := st.Create(ctx, key, bytes.NewReader(body))
		if err == nil {
			return true, nil
		}
		if errors.Is(err, storage.ErrPreconditionFailed) {
			b, rerr := get(ctx, st, key)
			if rerr == nil {
				return own(b)
			}
			err = rerr
		}
		if errors.Is(err, storage.ErrOutcomeUnknown) && unknown == nil {
			unknown = err
		}
		last = err
		if ctx.Err() != nil {
			break
		}
	}
	if unknown != nil {
		last = unknown
	}
	return false, fmt.Errorf("engine: create %s unresolved: %w", key, last)
}

func get(ctx context.Context, st storage.Store, key string) ([]byte, error) {
	rc, _, err := st.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	return io.ReadAll(rc)
}

// emptyDatabase returns page 1 of an empty SQLite database whose header
// already says WAL mode, so that opening it never needs SQLite's own write of
// page 1 outside the WAL (decision 5). The layout is SQLite's database file
// format, https://www.sqlite.org/fileformat2.html: the database header
// (section 1.3) followed by the header of an empty leaf table b-tree page,
// the schema table's root (section 1.6). Every field not set here is zero.
func emptyDatabase(pageSize uint32) []byte {
	be := binary.BigEndian
	p := make([]byte, pageSize)
	copy(p, "SQLite format 3\x00")
	ps := uint16(pageSize) //nolint:gosec // 65536 truncates to 0, and the header stores it as 1
	if ps == 0 {
		ps = 1
	}
	be.PutUint16(p[16:], ps)
	p[18], p[19] = 2, 2              // file format write and read versions: WAL
	p[21], p[22], p[23] = 64, 32, 32 // payload fractions, fixed by the format
	be.PutUint32(p[24:], 1)          // file change counter
	be.PutUint32(p[28:], 1)          // database size in pages
	be.PutUint32(p[44:], 4)          // schema format number
	be.PutUint32(p[56:], 1)          // text encoding: UTF-8
	be.PutUint32(p[92:], 1)          // version-valid-for, equal to the change counter
	be.PutUint32(p[96:], 3054001)    // SQLITE_VERSION_NUMBER of SQLite 3.54.1

	p[100] = 0x0d                           // leaf table b-tree page, no cells, no freeblocks
	be.PutUint16(p[105:], uint16(pageSize)) //nolint:gosec // cell content area starts at the page end; 0 means 65536
	return p
}
