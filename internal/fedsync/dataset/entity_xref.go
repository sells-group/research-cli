package dataset

import (
	"context"
	"time"

	"github.com/sells-group/research-cli/internal/db"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/fedsync/resolve"
	"github.com/sells-group/research-cli/internal/fetcher"
)

// EntityXref implements the CRD-CIK cross-reference builder dataset.
// Performs 3-pass matching between ADV firms and EDGAR entities:
//  1. Direct CRD-CIK matches from SEC ADV data (sec_number field)
//  2. Direct matches from EDGAR SIC codes (6211, 6282 = investment advisors)
//  3. Fuzzy name matching using pg_trgm similarity
type EntityXref struct{}

// Name implements Dataset.
func (d *EntityXref) Name() string { return "entity_xref" }

// Table implements Dataset.
func (d *EntityXref) Table() string { return "fed_data.entity_xref" }

// Phase implements Dataset.
func (d *EntityXref) Phase() Phase { return Phase1B }

// Cadence implements Dataset.
func (d *EntityXref) Cadence() Cadence { return Monthly }

// ShouldRun implements Dataset.
func (d *EntityXref) ShouldRun(_ time.Time, _ *time.Time) bool {
	// Always returns true — meant to be triggered via --force or xref command.
	return true
}

// Sync fetches and loads CRD-CIK cross-reference data.
func (d *EntityXref) Sync(ctx context.Context, pool db.Pool, _ fetcher.Fetcher, _ string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "entity_xref"))

	builder := resolve.NewXrefBuilder(pool)

	log.Info("starting entity cross-reference build")

	matched, err := builder.Build(ctx)
	if err != nil {
		return nil, err
	}

	log.Info("entity_xref build complete", zap.Int64("matched", matched))

	return &SyncResult{
		RowsSynced: matched,
	}, nil
}
