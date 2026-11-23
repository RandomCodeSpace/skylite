package engine

import "github.com/RandomCodeSpace/skylite/internal/format"

// Office is what a node holds once its takeover of a database has completed:
// the right to create commits at its epoch, starting after its own epoch bump.
type Office struct {
	DB    format.DatabaseID
	Epoch uint64   // the epoch this node swapped into the manifest
	Head  uint64   // the transaction number of this node's epoch bump
	Nonce [16]byte // the writer nonce this node put in the manifest

	// tail is what the takeover's walk read, so that Lead need not read it
	// again; nil if the walk kept none.
	tail *tail
}

// tail is the version of the manifest a takeover last read, holding its
// office, and the commits after its transaction number up to the bump.
type tail struct {
	version string // the manifest's version token
	commits []*commit
}
