package resolve

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"
)

// XrefBuilder builds the CRD-CIK cross-reference table by performing
// a 3-pass matching strategy between ADV firms and EDGAR entities.
type XrefBuilder struct {
	pool *pgxpool.Pool
}

// NewXrefBuilder creates a new XrefBuilder.
func NewXrefBuilder(pool *pgxpool.Pool) *XrefBuilder {
	return &XrefBuilder{pool: pool}
}

// Build executes the 3-pass matching and rebuilds the entity_xref table.
// Returns the total number of cross-references created.
func (x *XrefBuilder) Build(ctx context.Context) (int64, error) {
	log := zap.L().With(zap.String("component", "xref_builder"))

	// Truncate existing xref table for a clean rebuild.
	if _, err := x.pool.Exec(ctx, "TRUNCATE TABLE fed_data.entity_xref"); err != nil {
		return 0, eris.Wrap(err, "xref: truncate entity_xref")
	}

	var total int64

	// Pass 1: Direct CRD-CIK matches from SEC ADV sec_number field.
	log.Info("xref pass 1: direct CRD-CIK from ADV sec_number")
	n, err := x.pass1Direct(ctx)
	if err != nil {
		return total, eris.Wrap(err, "xref: pass 1 (direct CRD-CIK)")
	}
	total += n
	log.Info("xref pass 1 complete", zap.Int64("matched", n))

	// Pass 2: Direct matches from EDGAR SIC codes for investment advisors.
	log.Info("xref pass 2: EDGAR SIC code matches")
	n, err = x.pass2SIC(ctx)
	if err != nil {
		return total, eris.Wrap(err, "xref: pass 2 (SIC code)")
	}
	total += n
	log.Info("xref pass 2 complete", zap.Int64("matched", n))

	// Pass 3: Fuzzy name matching using pg_trgm similarity.
	log.Info("xref pass 3: fuzzy name matching")
	n, err = x.pass3Fuzzy(ctx)
	if err != nil {
		return total, eris.Wrap(err, "xref: pass 3 (fuzzy name)")
	}
	total += n
	log.Info("xref pass 3 complete", zap.Int64("matched", n))

	return total, nil
}

// pass1Direct matches ADV firms to EDGAR entities using the sec_number field.
func (x *XrefBuilder) pass1Direct(ctx context.Context) (int64, error) {
	sql := Pass1DirectSQL()
	tag, err := x.pool.Exec(ctx, sql)
	if err != nil {
		return 0, eris.Wrap(err, "xref: execute pass 1")
	}
	return tag.RowsAffected(), nil
}

// pass2SIC matches firms by exact name where EDGAR entities have financial services SIC codes.
func (x *XrefBuilder) pass2SIC(ctx context.Context) (int64, error) {
	sql := Pass2SICSQL()
	tag, err := x.pool.Exec(ctx, sql)
	if err != nil {
		return 0, eris.Wrap(err, "xref: execute pass 2")
	}
	return tag.RowsAffected(), nil
}

// pass3Fuzzy performs fuzzy name matching using pg_trgm similarity.
func (x *XrefBuilder) pass3Fuzzy(ctx context.Context) (int64, error) {
	sql := FuzzyMatchSQL()
	tag, err := x.pool.Exec(ctx, sql)
	if err != nil {
		return 0, eris.Wrap(err, "xref: execute pass 3")
	}
	return tag.RowsAffected(), nil
}
