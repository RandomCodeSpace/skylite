package engine

import "github.com/RandomCodeSpace/skylite/internal/format"

// Office is what a node holds once its takeover of a database has completed:
// the right to create commits at its epoch, starting after its own epoch bump.
type Office struct {
	DB    format.DatabaseID
	Epoch uint64   // the epoch this node swapped into the manifest
	Head  uint64   // the transaction number of this node's epoch bump
	Nonce [16]byte // the writer nonce this node put in the manifest
}
