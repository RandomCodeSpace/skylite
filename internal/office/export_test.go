package office

import (
	"context"
	"testing"

	"github.com/RandomCodeSpace/skylite/internal/engine"
	"github.com/RandomCodeSpace/skylite/internal/format"
	"github.com/RandomCodeSpace/skylite/internal/storage"
)

// SetLead makes the loop call f in place of engine.Lead until t ends.
func SetLead(t testing.TB, f func(ctx context.Context, st storage.Store, keys format.Keys, dir string, office engine.Office, cfg engine.Config) (*engine.Leader, error)) {
	old := lead
	lead = f
	t.Cleanup(func() { lead = old })
}
