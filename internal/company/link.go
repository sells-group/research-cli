package company

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/db"
	"go.uber.org/zap"
)

// Linker links companies to fed_data entities.
type Linker struct {
	pool  db.Pool
	store CompanyStore
}

// NewLinker creates a company-to-fed_data linker.
func NewLinker(pool db.Pool, store CompanyStore) *Linker {
	return &Linker{pool: pool, store: store}
}

// LinkFedData runs entity resolution to connect companies to fed_data records.
// Three-pass cascade:
//  1. Exact identifier (CRD, CIK, EIN) — confidence 1.0
//  2. Exact name+state — confidence 0.9-0.95
//  3. Fuzzy name match (pg_trgm) — confidence 0.6-0.85
func (l *Linker) LinkFedData(ctx context.Context, companyID int64) (int, error) {
	log := zap.L().With(zap.Int64("company_id", companyID))

	// Get identifiers for the company.
	identifiers, err := l.store.GetIdentifiers(ctx, companyID)
	if err != nil {
		return 0, eris.Wrap(err, "link: get identifiers")
	}

	c, err := l.store.GetCompany(ctx, companyID)
	if err != nil || c == nil {
		return 0, eris.Wrapf(err, "link: get company %d", companyID)
	}

	matched := 0

	// Pass 1: Direct CRD match to ADV firms.
	for _, id := range identifiers {
		if id.System == SystemCRD {
			n, err := l.matchCRD(ctx, companyID, id.Identifier)
			if err != nil {
				log.Warn("link: CRD match failed", zap.Error(err))
				continue
			}
			matched += n
		}
		if id.System == SystemCIK {
			n, err := l.matchCIK(ctx, companyID, id.Identifier)
			if err != nil {
				log.Warn("link: CIK match failed", zap.Error(err))
				continue
			}
			matched += n
		}
	}

	// Pass 2: Exact name+state match against EDGAR entities.
	if c.Name != "" && c.State != "" {
		n, err := l.matchNameState(ctx, companyID, c.Name, c.State)
		if err != nil {
			log.Warn("link: name+state match failed", zap.Error(err))
		} else {
			matched += n
		}
	}

	// Pass 3: Fuzzy name match against EDGAR entities.
	if c.Name != "" {
		n, err := l.matchFuzzyName(ctx, companyID, c.Name, c.State)
		if err != nil {
			log.Warn("link: fuzzy match failed", zap.Error(err))
		} else {
			matched += n
		}
	}

	log.Info("link: fed_data matching complete", zap.Int("matches", matched))
	return matched, nil
}

func (l *Linker) matchCRD(ctx context.Context, companyID int64, crd string) (int, error) {
	// Check ADV firms.
	var firmName string
	err := l.pool.QueryRow(ctx,
		`SELECT firm_name FROM fed_data.adv_firms WHERE crd_number = $1`, crd).
		Scan(&firmName)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, eris.Wrap(err, "link: query adv_firms")
	}

	m := &Match{
		CompanyID:     companyID,
		MatchedSource: "adv_firms",
		MatchedKey:    crd,
		MatchType:     "direct_crd",
		Confidence:    ptrFloat(1.0),
	}
	if err := l.store.UpsertMatch(ctx, m); err != nil {
		return 0, err
	}
	return 1, nil
}

func (l *Linker) matchCIK(ctx context.Context, companyID int64, cik string) (int, error) {
	var entityName string
	err := l.pool.QueryRow(ctx,
		`SELECT entity_name FROM fed_data.edgar_entities WHERE cik = $1`, cik).
		Scan(&entityName)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, eris.Wrap(err, "link: query edgar_entities")
	}

	m := &Match{
		CompanyID:     companyID,
		MatchedSource: "edgar_entities",
		MatchedKey:    cik,
		MatchType:     "direct_cik",
		Confidence:    ptrFloat(1.0),
	}
	if err := l.store.UpsertMatch(ctx, m); err != nil {
		return 0, err
	}
	return 1, nil
}

func (l *Linker) matchNameState(ctx context.Context, companyID int64, name, state string) (int, error) {
	rows, err := l.pool.Query(ctx, `
		SELECT cik, entity_name FROM fed_data.edgar_entities
		WHERE LOWER(entity_name) = LOWER($1) AND state_of_incorp = $2
		LIMIT 5`, name, state)
	if err != nil {
		return 0, eris.Wrap(err, "link: name+state query")
	}
	defer rows.Close()

	matched := 0
	for rows.Next() {
		var cik, entityName string
		if err := rows.Scan(&cik, &entityName); err != nil {
			return matched, eris.Wrap(err, "link: scan name+state")
		}
		m := &Match{
			CompanyID:     companyID,
			MatchedSource: "edgar_entities",
			MatchedKey:    cik,
			MatchType:     "exact_name_state",
			Confidence:    ptrFloat(0.95),
		}
		if err := l.store.UpsertMatch(ctx, m); err != nil {
			return matched, err
		}
		matched++
	}
	return matched, rows.Err()
}

func (l *Linker) matchFuzzyName(ctx context.Context, companyID int64, name, state string) (int, error) {
	// Only fuzzy match if we haven't already matched this company to EDGAR.
	existing, err := l.store.GetMatches(ctx, companyID)
	if err != nil {
		return 0, err
	}
	for _, m := range existing {
		if m.MatchedSource == "edgar_entities" {
			return 0, nil // already matched
		}
	}

	query := `
		SELECT cik, entity_name, similarity(entity_name, $1) AS sim
		FROM fed_data.edgar_entities
		WHERE entity_name %% $1`
	args := []any{name}
	argIdx := 2

	if state != "" {
		query += fmt.Sprintf(` AND state_of_incorp = $%d`, argIdx)
		args = append(args, state)
		argIdx++
	}
	_ = argIdx

	query += ` ORDER BY sim DESC LIMIT 1`

	var cik, entityName string
	var sim float64
	err = l.pool.QueryRow(ctx, query, args...).Scan(&cik, &entityName, &sim)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, eris.Wrap(err, "link: fuzzy query")
	}

	if sim < 0.6 {
		return 0, nil
	}

	m := &Match{
		CompanyID:     companyID,
		MatchedSource: "edgar_entities",
		MatchedKey:    cik,
		MatchType:     "fuzzy_name",
		Confidence:    ptrFloat(sim),
	}
	if err := l.store.UpsertMatch(ctx, m); err != nil {
		return 0, err
	}
	return 1, nil
}

func ptrFloat(f float64) *float64 {
	return &f
}
