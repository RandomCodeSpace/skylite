//go:build linux || darwin

// Package localfs is a storage.Store on a local directory.
//
// # Layout
//
// The store owns everything under its root directory:
//
//	lock                  empty file; flock(2) on it serialises Replace and Delete
//	tmp/<random>          writes in progress
//	objects/<sha256(key)> committed objects
//
// An object file starts with a header (the magic "SKYLOBJ1", a 32-character
// version token, the key length as a big-endian uint16, and the key) followed
// by the object's bytes. File names are the hex SHA-256 of the key, so a key
// never becomes a path: path traversal through keys is impossible, keys that
// differ only in case or Unicode normalisation stay apart on macOS, and "a/b"
// and "a/b/c" can coexist. All file access goes through os.Root, which refuses
// to follow a symbolic link that leads outside the root.
//
// # Writes
//
// Every write first writes the header and body to a new file in tmp/ and
// fsyncs it. Nothing in tmp/ is ever visible as an object, so a write that
// fails, is cancelled or is killed leaves at most an orphaned temporary file.
//
// Create commits with link(2) from the temporary file to the object's name.
// link fails if the name exists, atomically, so exactly one of any number of
// concurrent creators wins, in one process or many.
//
// Replace takes an exclusive flock on the lock file, reads the current
// object's version token, compares it, and commits with rename(2), which
// replaces the object atomically. Delete takes the same lock, so a Delete
// cannot slip between a Replace's check and its rename. flock locks belong to
// an open file description and the lock file is opened per call, so the lock
// excludes goroutines of one process as well as other processes. The kernel
// drops the lock of a killed process.
//
// After the commit, objects/ is fsynced, so a successful write has fsynced
// both the file and its directory entry. On macOS, os.File.Sync issues
// F_FULLFSYNC.
//
// Version tokens are 128 random bits from crypto/rand, written in the header
// and therefore replaced together with the content.
//
// # Limits
//
//   - Linux and macOS only. The package does not build elsewhere: it needs
//     flock(2), hard links and rename(2) replacing an existing file
//     atomically, which Windows does not provide with these semantics.
//   - Local filesystems only, all of root on one filesystem. Network
//     filesystems (NFS, SMB, FUSE mounts of object storage) are not supported:
//     flock on them may be local to one client or emulated, and link and
//     rename may not be atomic across clients. Filesystems without hard links
//     (FAT, exFAT) are not supported.
//   - Orphaned files in tmp/ left by killed processes are not removed. They
//     are harmless and may be deleted while no process has the store open.
//   - List reads the header of every object for every page: its cost grows
//     with the number of objects in the store, not with the page size.
//   - Replace and Delete are serialised across the whole store.
//   - Nothing else may write under the root.
package localfs

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/RandomCodeSpace/skylite/internal/storage"
)

const (
	lockName  = "lock"
	tmpDir    = "tmp"
	objDir    = "objects"
	magic     = "SKYLOBJ1"
	verLen    = 32
	fixedHead = len(magic) + verLen + 2
)

var errCorrupt = errors.New("localfs: corrupt object file")

// Store is a storage.Store on a local directory. Close it when done.
type Store struct {
	root *os.Root
}

var _ storage.Store = (*Store)(nil)

// Open opens the store rooted at dir, which must exist. Several Stores, in
// one process or several, may open the same directory at once.
func Open(dir string) (*Store, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	for _, d := range []string{tmpDir, objDir} {
		if err := root.Mkdir(d, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
			_ = root.Close()
			return nil, err
		}
	}
	if err := syncDir(root, "."); err != nil {
		_ = root.Close()
		return nil, err
	}
	return &Store{root: root}, nil
}

// Close releases the root directory.
func (s *Store) Close() error { return s.root.Close() }

// Create implements storage.Store.
func (s *Store) Create(ctx context.Context, key string, body io.Reader) (string, error) {
	version, err := s.create(ctx, key, body)
	if err != nil {
		return "", &storage.Error{Op: "create", Key: key, Err: err}
	}
	return version, nil
}

func (s *Store) create(ctx context.Context, key string, body io.Reader) (string, error) {
	if err := check(ctx, key); err != nil {
		return "", err
	}
	tmp, version, err := s.writeTemp(ctx, key, body)
	if err != nil {
		return "", err
	}
	defer func() { _ = s.root.Remove(tmp) }()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := s.root.Link(tmp, objPath(key)); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return "", storage.ErrPreconditionFailed
		}
		return "", err
	}
	return version, syncDir(s.root, objDir)
}

// Replace implements storage.Store.
func (s *Store) Replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	newVersion, err := s.replace(ctx, key, version, body)
	if err != nil {
		return "", &storage.Error{Op: "replace", Key: key, Err: err}
	}
	return newVersion, nil
}

func (s *Store) replace(ctx context.Context, key, version string, body io.Reader) (string, error) {
	if err := check(ctx, key); err != nil {
		return "", err
	}
	tmp, newVersion, err := s.writeTemp(ctx, key, body)
	if err != nil {
		return "", err
	}
	defer func() { _ = s.root.Remove(tmp) }()
	unlock, err := s.lock(ctx)
	if err != nil {
		return "", err
	}
	defer unlock()
	f, info, _, err := s.open(key)
	if errors.Is(err, storage.ErrNotFound) {
		return "", storage.ErrPreconditionFailed
	}
	if err != nil {
		return "", err
	}
	_ = f.Close()
	if info.Version != version {
		return "", storage.ErrPreconditionFailed
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := s.root.Rename(tmp, objPath(key)); err != nil {
		return "", err
	}
	return newVersion, syncDir(s.root, objDir)
}

// Get implements storage.Store.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, storage.Info, error) {
	return s.GetRange(ctx, key, 0, 1<<63-1)
}

// GetRange implements storage.Store.
func (s *Store) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.Info, error) {
	fail := func(err error) (io.ReadCloser, storage.Info, error) {
		return nil, storage.Info{}, &storage.Error{Op: "get", Key: key, Err: err}
	}
	if err := check(ctx, key); err != nil {
		return fail(err)
	}
	if offset < 0 || length < 0 {
		return fail(storage.ErrInvalidArgument)
	}
	f, info, dataOff, err := s.open(key)
	if err != nil {
		return fail(err)
	}
	start := min(offset, info.Size)
	n := min(length, info.Size-start)
	body := ctxReader{ctx, io.NewSectionReader(f, dataOff+start, n)}
	return struct {
		io.Reader
		io.Closer
	}{body, f}, info, nil
}

// Head implements storage.Store.
func (s *Store) Head(ctx context.Context, key string) (storage.Info, error) {
	err := check(ctx, key)
	if err == nil {
		var f *os.File
		var info storage.Info
		if f, info, _, err = s.open(key); err == nil {
			_ = f.Close()
			return info, nil
		}
	}
	return storage.Info{}, &storage.Error{Op: "head", Key: key, Err: err}
}

// List implements storage.Store.
func (s *Store) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	entries, err := s.list(ctx, prefix, startAfter, limit)
	if err != nil {
		return nil, &storage.Error{Op: "list", Err: err}
	}
	return entries, nil
}

func (s *Store) list(ctx context.Context, prefix, startAfter string, limit int) ([]storage.Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 1 || limit > storage.MaxListLimit {
		return nil, storage.ErrInvalidArgument
	}
	d, err := s.root.Open(objDir)
	if err != nil {
		return nil, err
	}
	names, err := d.Readdirnames(-1)
	_ = d.Close()
	if err != nil {
		return nil, err
	}
	var out []storage.Entry
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		f, err := s.root.Open(objDir + "/" + name)
		if errors.Is(err, fs.ErrNotExist) {
			continue // deleted or replaced since Readdirnames
		}
		if err != nil {
			return nil, err
		}
		key, info, _, err := readHeader(f)
		_ = f.Close()
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(key, prefix) && key > startAfter {
			out = append(out, storage.Entry{Key: key, Size: info.Size})
		}
	}
	slices.SortFunc(out, func(a, b storage.Entry) int { return strings.Compare(a.Key, b.Key) })
	return out[:min(len(out), limit)], nil
}

// Delete implements storage.Store.
func (s *Store) Delete(ctx context.Context, key string) error {
	if err := s.delete(ctx, key); err != nil {
		return &storage.Error{Op: "delete", Key: key, Err: err}
	}
	return nil
}

func (s *Store) delete(ctx context.Context, key string) error {
	if err := check(ctx, key); err != nil {
		return err
	}
	unlock, err := s.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	if err := s.root.Remove(objPath(key)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	return syncDir(s.root, objDir)
}

func check(ctx context.Context, key string) error {
	if err := storage.ValidateKey(key); err != nil {
		return err
	}
	return ctx.Err()
}

func objPath(key string) string {
	sum := sha256.Sum256([]byte(key))
	return objDir + "/" + hex.EncodeToString(sum[:])
}

func randomHex() string {
	var b [verLen / 2]byte
	_, _ = rand.Read(b[:]) // never fails; see crypto/rand.Read
	return hex.EncodeToString(b[:])
}

// writeTemp writes a complete, fsynced object file for key under tmp/ and
// returns its name and version token. On error the file is removed.
func (s *Store) writeTemp(ctx context.Context, key string, body io.Reader) (string, string, error) {
	name := tmpDir + "/" + randomHex()
	version := randomHex()
	f, err := s.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", "", err
	}
	fail := func(err error) (string, string, error) {
		_ = f.Close()
		_ = s.root.Remove(name)
		return "", "", err
	}
	head := make([]byte, fixedHead, fixedHead+len(key))
	copy(head, magic)
	copy(head[len(magic):], version)
	binary.BigEndian.PutUint16(head[len(magic)+verLen:], uint16(len(key))) //nolint:gosec // ValidateKey caps len(key) at 1024
	head = append(head, key...)
	if _, err := f.Write(head); err != nil {
		return fail(err)
	}
	if _, err := io.Copy(f, ctxReader{ctx, body}); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		_ = s.root.Remove(name)
		return "", "", err
	}
	return name, version, nil
}

// open opens the object file for key and returns it with the object's info
// and the offset of its first byte.
func (s *Store) open(key string) (*os.File, storage.Info, int64, error) {
	f, err := s.root.Open(objPath(key))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			err = storage.ErrNotFound
		}
		return nil, storage.Info{}, 0, err
	}
	got, info, dataOff, err := readHeader(f)
	if err == nil && got != key {
		err = errCorrupt
	}
	if err != nil {
		_ = f.Close()
		return nil, storage.Info{}, 0, err
	}
	return f, info, dataOff, nil
}

func readHeader(f *os.File) (key string, info storage.Info, dataOff int64, err error) {
	st, err := f.Stat()
	if err != nil {
		return "", info, 0, err
	}
	head := make([]byte, fixedHead)
	if _, err := f.ReadAt(head, 0); err != nil || string(head[:len(magic)]) != magic {
		return "", info, 0, errCorrupt
	}
	k := make([]byte, binary.BigEndian.Uint16(head[len(magic)+verLen:]))
	if _, err := f.ReadAt(k, int64(fixedHead)); err != nil {
		return "", info, 0, errCorrupt
	}
	dataOff = int64(fixedHead + len(k))
	info = storage.Info{Size: st.Size() - dataOff, Version: string(head[len(magic) : len(magic)+verLen])}
	return string(k), info, dataOff, nil
}

// lock takes the store-wide exclusive lock, polling so that ctx is honoured.
func (s *Store) lock(ctx context.Context) (unlock func(), err error) {
	f, err := s.root.OpenFile(lockName, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) //nolint:gosec // a file descriptor fits in an int
		if err == nil {
			return func() { _ = f.Close() }, nil // closing the descriptor releases the lock
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EINTR) {
			_ = f.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			_ = f.Close()
			return nil, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func syncDir(root *os.Root, name string) error {
	d, err := root.Open(name)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}

// ctxReader fails reads once ctx is done.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}
