package dataset

import (
	"context"
	"time"

	"github.com/sells-group/research-cli/internal/db"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/fedsync/resolve"
	"github.com/sells-group/research-cli/internal/fetcher"
)

// EntityXref implements the entity cross-reference builder dataset.
// Performs two stages:
//  1. CRD-CIK matching: 3-pass strategy between ADV firms and EDGAR entities
//     (direct sec_number, SIC-based exact name, fuzzy pg_trgm)
//  2. Multi-dataset matching: cross-references across all entity-bearing datasets
//     (ADV, EDGAR, BrokerCheck, Form BD, OSHA, EPA, FPDS, PPP, Form D)
//     using direct CRD, direct CIK, exact name+zip, exact name+state,
//     and fuzzy name+state strategies.
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

// Sync builds both the CRD-CIK cross-reference and the multi-dataset cross-reference.
func (d *EntityXref) Sync(ctx context.Context, pool db.Pool, _ fetcher.Fetcher, _ string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "entity_xref"))

	// Stage 1: CRD-CIK cross-reference (existing 3-pass matching).
	log.Info("stage 1: building CRD-CIK cross-reference")
	crdCIKBuilder := resolve.NewXrefBuilder(pool)
	crdCIKMatched, err := crdCIKBuilder.Build(ctx)
	if err != nil {
		return nil, err
	}
	log.Info("stage 1 complete", zap.Int64("crd_cik_matched", crdCIKMatched))

	// Stage 2: Multi-dataset cross-reference.
	log.Info("stage 2: building multi-dataset cross-reference")
	multiBuilder := resolve.NewMultiXrefBuilder(pool)
	multiMatched, passCounts, err := multiBuilder.Build(ctx)
	if err != nil {
		return nil, err
	}
	log.Info("stage 2 complete", zap.Int64("multi_matched", multiMatched))

	metadata := map[string]any{
		"crd_cik_matched": crdCIKMatched,
		"multi_matched":   multiMatched,
	}
	for name, count := range passCounts {
		metadata["pass_"+name] = count
	}

	return &SyncResult{
		RowsSynced: crdCIKMatched + multiMatched,
		Metadata:   metadata,
	}, nil
}
