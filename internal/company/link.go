package company

import (
	"context"

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
// Two-pass cascade:
//  1. Exact identifier (CRD, CIK, EIN) — confidence 1.0
//  2. Exact name+state — confidence 0.9-0.95
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
		if id.System == SystemFDIC {
			n, err := l.matchFDIC(ctx, companyID, id.Identifier)
			if err != nil {
				log.Warn("link: FDIC match failed", zap.Error(err))
				continue
			}
			matched += n
		}
		if id.System == SystemEIN {
			n, err := l.matchEIN(ctx, companyID, id.Identifier)
			if err != nil {
				log.Warn("link: EIN match failed", zap.Error(err))
			} else {
				matched += n
			}
			n, err = l.matchEOBMF(ctx, companyID, id.Identifier)
			if err != nil {
				log.Warn("link: EO BMF match failed", zap.Error(err))
			} else {
				matched += n
			}
		}
		if id.System == SystemUEI {
			n, err := l.matchUEI(ctx, companyID, id.Identifier)
			if err != nil {
				log.Warn("link: UEI match failed", zap.Error(err))
				continue
			}
			matched += n
		}
		if id.System == SystemDUNS {
			n, err := l.matchUSAspendingDUNS(ctx, companyID, id.Identifier)
			if err != nil {
				log.Warn("link: DUNS match failed", zap.Error(err))
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

	log.Info("link: fed_data matching complete", zap.Int("matches", matched))
	return matched, nil
}

func (l *Linker) matchFDIC(ctx context.Context, companyID int64, cert string) (int, error) {
	var name string
	err := l.pool.QueryRow(ctx,
		`SELECT name FROM fed_data.fdic_institutions WHERE cert = $1`, cert).
		Scan(&name)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, eris.Wrap(err, "link: query fdic_institutions")
	}

	m := &Match{
		CompanyID:     companyID,
		MatchedSource: "fdic_institutions",
		MatchedKey:    cert,
		MatchType:     "direct_fdic_cert",
		Confidence:    ptrFloat(1.0),
	}
	if err := l.store.UpsertMatch(ctx, m); err != nil {
		return 0, err
	}
	return 1, nil
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

func (l *Linker) matchEIN(ctx context.Context, companyID int64, ein string) (int, error) {
	var sponsorName string
	err := l.pool.QueryRow(ctx,
		`SELECT sponsor_dfe_name FROM fed_data.form_5500 WHERE spons_dfe_ein = $1 LIMIT 1`, ein).
		Scan(&sponsorName)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, eris.Wrap(err, "link: query form_5500")
	}

	m := &Match{
		CompanyID:     companyID,
		MatchedSource: "form_5500",
		MatchedKey:    ein,
		MatchType:     "direct_ein",
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

func (l *Linker) matchEOBMF(ctx context.Context, companyID int64, ein string) (int, error) {
	var orgName string
	err := l.pool.QueryRow(ctx,
		`SELECT name FROM fed_data.eo_bmf WHERE ein = $1 LIMIT 1`, ein).
		Scan(&orgName)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, eris.Wrap(err, "link: query eo_bmf")
	}

	m := &Match{
		CompanyID:     companyID,
		MatchedSource: "eo_bmf",
		MatchedKey:    ein,
		MatchType:     "direct_ein",
		Confidence:    ptrFloat(1.0),
	}
	if err := l.store.UpsertMatch(ctx, m); err != nil {
		return 0, err
	}
	return 1, nil
}

func (l *Linker) matchUEI(ctx context.Context, companyID int64, uei string) (int, error) {
	var recipientName string
	err := l.pool.QueryRow(ctx,
		`SELECT recipient_name FROM fed_data.usaspending_awards WHERE recipient_uei = $1 LIMIT 1`, uei).
		Scan(&recipientName)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, eris.Wrap(err, "link: query usaspending_awards by UEI")
	}

	m := &Match{
		CompanyID:     companyID,
		MatchedSource: "usaspending_awards",
		MatchedKey:    uei,
		MatchType:     "direct_uei",
		Confidence:    ptrFloat(1.0),
	}
	if err := l.store.UpsertMatch(ctx, m); err != nil {
		return 0, err
	}
	return 1, nil
}

func (l *Linker) matchUSAspendingDUNS(ctx context.Context, companyID int64, duns string) (int, error) {
	var recipientName string
	err := l.pool.QueryRow(ctx,
		`SELECT recipient_name FROM fed_data.usaspending_awards WHERE recipient_duns = $1 AND recipient_uei IS NULL LIMIT 1`, duns).
		Scan(&recipientName)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, eris.Wrap(err, "link: query usaspending_awards by DUNS")
	}

	m := &Match{
		CompanyID:     companyID,
		MatchedSource: "usaspending_awards",
		MatchedKey:    duns,
		MatchType:     "direct_duns",
		Confidence:    ptrFloat(1.0),
	}
	if err := l.store.UpsertMatch(ctx, m); err != nil {
		return 0, err
	}
	return 1, nil
}

func ptrFloat(f float64) *float64 {
	return &f
}
