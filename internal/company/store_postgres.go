package company

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/db"
)

// PostgresStore implements CompanyStore using pgx.
type PostgresStore struct {
	pool db.Pool
}

// NewPostgresStore creates a new PostgresStore.
func NewPostgresStore(pool db.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

// CreateCompany inserts a new company and sets its ID.
func (s *PostgresStore) CreateCompany(ctx context.Context, c *CompanyRecord) error {
	err := s.pool.QueryRow(ctx, `
		INSERT INTO companies (
			name, legal_name, domain, website, description,
			naics_code, sic_code, business_model, year_founded, ownership_type,
			phone, email,
			employee_count, employee_estimate, revenue_estimate, revenue_range, revenue_confidence,
			street, city, state, zip_code, country,
			enrichment_score, last_enriched_at, last_run_id
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10,
			$11, $12,
			$13, $14, $15, $16, $17,
			$18, $19, $20, $21, $22,
			$23, $24, $25
		) RETURNING id, created_at, updated_at`,
		c.Name, c.LegalName, c.Domain, c.Website, c.Description,
		c.NAICSCode, c.SICCode, c.BusinessModel, nilIfZero(c.YearFounded), c.OwnershipType,
		c.Phone, c.Email,
		c.EmployeeCount, c.EmployeeEstimate, c.RevenueEstimate, c.RevenueRange, c.RevenueConfidence,
		c.Street, c.City, c.State, c.ZipCode, c.Country,
		c.EnrichmentScore, c.LastEnrichedAt, c.LastRunID,
	).Scan(&c.ID, &c.CreatedAt, &c.UpdatedAt)
	if err != nil {
		return eris.Wrap(err, "company: create")
	}
	return nil
}

// UpdateCompany updates an existing company record.
func (s *PostgresStore) UpdateCompany(ctx context.Context, c *CompanyRecord) error {
	c.UpdatedAt = time.Now()
	_, err := s.pool.Exec(ctx, `
		UPDATE companies SET
			name=$2, legal_name=$3, domain=$4, website=$5, description=$6,
			naics_code=$7, sic_code=$8, business_model=$9, year_founded=$10, ownership_type=$11,
			phone=$12, email=$13,
			employee_count=$14, employee_estimate=$15, revenue_estimate=$16, revenue_range=$17, revenue_confidence=$18,
			street=$19, city=$20, state=$21, zip_code=$22, country=$23,
			enrichment_score=$24, last_enriched_at=$25, last_run_id=$26,
			updated_at=now()
		WHERE id=$1`,
		c.ID,
		c.Name, c.LegalName, c.Domain, c.Website, c.Description,
		c.NAICSCode, c.SICCode, c.BusinessModel, nilIfZero(c.YearFounded), c.OwnershipType,
		c.Phone, c.Email,
		c.EmployeeCount, c.EmployeeEstimate, c.RevenueEstimate, c.RevenueRange, c.RevenueConfidence,
		c.Street, c.City, c.State, c.ZipCode, c.Country,
		c.EnrichmentScore, c.LastEnrichedAt, c.LastRunID,
	)
	if err != nil {
		return eris.Wrapf(err, "company: update %d", c.ID)
	}
	return nil
}

// GetCompany fetches a company by ID.
func (s *PostgresStore) GetCompany(ctx context.Context, id int64) (*CompanyRecord, error) {
	c := &CompanyRecord{}
	err := s.pool.QueryRow(ctx, `SELECT `+companyColumns+` FROM companies WHERE id=$1`, id).
		Scan(companyDests(c)...)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, eris.Wrapf(err, "company: get %d", id)
	}
	return c, nil
}

// GetCompanyByDomain fetches a company by its unique domain.
func (s *PostgresStore) GetCompanyByDomain(ctx context.Context, domain string) (*CompanyRecord, error) {
	c := &CompanyRecord{}
	err := s.pool.QueryRow(ctx, `SELECT `+companyColumns+` FROM companies WHERE domain=$1`, domain).
		Scan(companyDests(c)...)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, eris.Wrapf(err, "company: get by domain %s", domain)
	}
	return c, nil
}

// SearchCompaniesByName finds companies by trigram similarity on name.
func (s *PostgresStore) SearchCompaniesByName(ctx context.Context, name string, limit int) ([]CompanyRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.pool.Query(ctx, `
		SELECT `+companyColumns+`
		FROM companies
		WHERE name % $1
		ORDER BY similarity(name, $1) DESC
		LIMIT $2`, name, limit)
	if err != nil {
		return nil, eris.Wrap(err, "company: search by name")
	}
	defer rows.Close()
	return scanCompanies(rows)
}

// UpsertIdentifier inserts or updates a company identifier.
func (s *PostgresStore) UpsertIdentifier(ctx context.Context, id *Identifier) error {
	err := s.pool.QueryRow(ctx, `
		INSERT INTO company_identifiers (company_id, system, identifier, metadata)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (company_id, system, identifier) DO UPDATE SET
			metadata = COALESCE(EXCLUDED.metadata, company_identifiers.metadata),
			updated_at = now()
		RETURNING id, created_at, updated_at`,
		id.CompanyID, id.System, id.Identifier, id.Metadata,
	).Scan(&id.ID, &id.CreatedAt, &id.UpdatedAt)
	if err != nil {
		return eris.Wrap(err, "company: upsert identifier")
	}
	return nil
}

// GetIdentifiers returns all identifiers for a company.
func (s *PostgresStore) GetIdentifiers(ctx context.Context, companyID int64) ([]Identifier, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, company_id, system, identifier, metadata, created_at, updated_at
		FROM company_identifiers WHERE company_id=$1`, companyID)
	if err != nil {
		return nil, eris.Wrap(err, "company: get identifiers")
	}
	defer rows.Close()

	var ids []Identifier
	for rows.Next() {
		var id Identifier
		if err := rows.Scan(&id.ID, &id.CompanyID, &id.System, &id.Identifier, &id.Metadata, &id.CreatedAt, &id.UpdatedAt); err != nil {
			return nil, eris.Wrap(err, "company: scan identifier")
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// FindByIdentifier finds a company by a specific system+identifier.
func (s *PostgresStore) FindByIdentifier(ctx context.Context, system, identifier string) (*CompanyRecord, error) {
	c := &CompanyRecord{}
	err := s.pool.QueryRow(ctx, `
		SELECT `+companyColumns+`
		FROM companies c
		JOIN company_identifiers ci ON c.id = ci.company_id
		WHERE ci.system = $1 AND ci.identifier = $2
		LIMIT 1`, system, identifier).
		Scan(companyDests(c)...)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, eris.Wrapf(err, "company: find by identifier %s:%s", system, identifier)
	}
	return c, nil
}

// UpsertAddress inserts or updates a company address.
func (s *PostgresStore) UpsertAddress(ctx context.Context, addr *Address) error {
	err := s.pool.QueryRow(ctx, `
		INSERT INTO company_addresses (company_id, address_type, street, city, state, zip_code, country, latitude, longitude, source, confidence, is_primary)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (id) DO UPDATE SET
			street=EXCLUDED.street, city=EXCLUDED.city, state=EXCLUDED.state,
			zip_code=EXCLUDED.zip_code, country=EXCLUDED.country,
			latitude=EXCLUDED.latitude, longitude=EXCLUDED.longitude,
			source=EXCLUDED.source, confidence=EXCLUDED.confidence,
			is_primary=EXCLUDED.is_primary, updated_at=now()
		RETURNING id, created_at, updated_at`,
		addr.CompanyID, addr.AddressType, addr.Street, addr.City, addr.State,
		addr.ZipCode, addr.Country, addr.Latitude, addr.Longitude,
		addr.Source, addr.Confidence, addr.IsPrimary,
	).Scan(&addr.ID, &addr.CreatedAt, &addr.UpdatedAt)
	if err != nil {
		return eris.Wrap(err, "company: upsert address")
	}
	return nil
}

// GetAddresses returns all addresses for a company.
func (s *PostgresStore) GetAddresses(ctx context.Context, companyID int64) ([]Address, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, company_id, address_type, street, city, state, zip_code, country,
			latitude, longitude, source, confidence, is_primary, created_at, updated_at
		FROM company_addresses WHERE company_id=$1 ORDER BY is_primary DESC`, companyID)
	if err != nil {
		return nil, eris.Wrap(err, "company: get addresses")
	}
	defer rows.Close()

	var addrs []Address
	for rows.Next() {
		var a Address
		if err := rows.Scan(&a.ID, &a.CompanyID, &a.AddressType, &a.Street, &a.City, &a.State,
			&a.ZipCode, &a.Country, &a.Latitude, &a.Longitude, &a.Source, &a.Confidence,
			&a.IsPrimary, &a.CreatedAt, &a.UpdatedAt); err != nil {
			return nil, eris.Wrap(err, "company: scan address")
		}
		addrs = append(addrs, a)
	}
	return addrs, rows.Err()
}

// UpsertContact inserts or updates a contact.
func (s *PostgresStore) UpsertContact(ctx context.Context, c *Contact) error {
	err := s.pool.QueryRow(ctx, `
		INSERT INTO contacts (company_id, first_name, last_name, full_name, title, role_type,
			email, phone, linkedin_url, ownership_pct, is_control_person, is_primary, source, confidence)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		ON CONFLICT (id) DO UPDATE SET
			first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name,
			full_name=EXCLUDED.full_name, title=EXCLUDED.title, role_type=EXCLUDED.role_type,
			email=EXCLUDED.email, phone=EXCLUDED.phone, linkedin_url=EXCLUDED.linkedin_url,
			ownership_pct=EXCLUDED.ownership_pct, is_control_person=EXCLUDED.is_control_person,
			is_primary=EXCLUDED.is_primary, source=EXCLUDED.source, confidence=EXCLUDED.confidence,
			updated_at=now()
		RETURNING id, created_at, updated_at`,
		c.CompanyID, c.FirstName, c.LastName, c.FullName, c.Title, c.RoleType,
		c.Email, c.Phone, c.LinkedInURL, c.OwnershipPct, c.IsControlPerson,
		c.IsPrimary, c.Source, c.Confidence,
	).Scan(&c.ID, &c.CreatedAt, &c.UpdatedAt)
	if err != nil {
		return eris.Wrap(err, "company: upsert contact")
	}
	return nil
}

// GetContacts returns all contacts for a company.
func (s *PostgresStore) GetContacts(ctx context.Context, companyID int64) ([]Contact, error) {
	return s.queryContacts(ctx, `
		SELECT id, company_id, first_name, last_name, full_name, title, role_type,
			email, phone, linkedin_url, ownership_pct, is_control_person, is_primary,
			source, confidence, created_at, updated_at
		FROM contacts WHERE company_id=$1 ORDER BY is_primary DESC, full_name`, companyID)
}

// GetContactsByRole returns contacts for a company with a specific role.
func (s *PostgresStore) GetContactsByRole(ctx context.Context, companyID int64, roleType string) ([]Contact, error) {
	return s.queryContacts(ctx, `
		SELECT id, company_id, first_name, last_name, full_name, title, role_type,
			email, phone, linkedin_url, ownership_pct, is_control_person, is_primary,
			source, confidence, created_at, updated_at
		FROM contacts WHERE company_id=$1 AND role_type=$2 ORDER BY is_primary DESC, full_name`, companyID, roleType)
}

func (s *PostgresStore) queryContacts(ctx context.Context, sql string, args ...any) ([]Contact, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, eris.Wrap(err, "company: query contacts")
	}
	defer rows.Close()

	var contacts []Contact
	for rows.Next() {
		var c Contact
		if err := rows.Scan(&c.ID, &c.CompanyID, &c.FirstName, &c.LastName, &c.FullName,
			&c.Title, &c.RoleType, &c.Email, &c.Phone, &c.LinkedInURL,
			&c.OwnershipPct, &c.IsControlPerson, &c.IsPrimary,
			&c.Source, &c.Confidence, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, eris.Wrap(err, "company: scan contact")
		}
		contacts = append(contacts, c)
	}
	return contacts, rows.Err()
}

// UpsertLicense inserts or updates a license.
func (s *PostgresStore) UpsertLicense(ctx context.Context, l *License) error {
	err := s.pool.QueryRow(ctx, `
		INSERT INTO licenses (company_id, license_type, license_number, authority, state, status,
			issued_date, expiry_date, source, raw_text)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (id) DO UPDATE SET
			license_type=EXCLUDED.license_type, license_number=EXCLUDED.license_number,
			authority=EXCLUDED.authority, state=EXCLUDED.state, status=EXCLUDED.status,
			issued_date=EXCLUDED.issued_date, expiry_date=EXCLUDED.expiry_date,
			source=EXCLUDED.source, raw_text=EXCLUDED.raw_text, updated_at=now()
		RETURNING id, created_at, updated_at`,
		l.CompanyID, l.LicenseType, l.LicenseNumber, l.Authority, l.State, l.Status,
		l.IssuedDate, l.ExpiryDate, l.Source, l.RawText,
	).Scan(&l.ID, &l.CreatedAt, &l.UpdatedAt)
	if err != nil {
		return eris.Wrap(err, "company: upsert license")
	}
	return nil
}

// GetLicenses returns all licenses for a company.
func (s *PostgresStore) GetLicenses(ctx context.Context, companyID int64) ([]License, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, company_id, license_type, license_number, authority, state, status,
			issued_date, expiry_date, source, raw_text, created_at, updated_at
		FROM licenses WHERE company_id=$1`, companyID)
	if err != nil {
		return nil, eris.Wrap(err, "company: get licenses")
	}
	defer rows.Close()

	var lics []License
	for rows.Next() {
		var l License
		if err := rows.Scan(&l.ID, &l.CompanyID, &l.LicenseType, &l.LicenseNumber, &l.Authority,
			&l.State, &l.Status, &l.IssuedDate, &l.ExpiryDate, &l.Source, &l.RawText,
			&l.CreatedAt, &l.UpdatedAt); err != nil {
			return nil, eris.Wrap(err, "company: scan license")
		}
		lics = append(lics, l)
	}
	return lics, rows.Err()
}

// UpsertSource inserts or updates a company source record.
func (s *PostgresStore) UpsertSource(ctx context.Context, src *Source) error {
	err := s.pool.QueryRow(ctx, `
		INSERT INTO company_sources (company_id, source, source_id, raw_data, extracted_fields, data_as_of, fetched_at, run_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (company_id, source, source_id) DO UPDATE SET
			raw_data=EXCLUDED.raw_data, extracted_fields=EXCLUDED.extracted_fields,
			data_as_of=EXCLUDED.data_as_of, fetched_at=EXCLUDED.fetched_at,
			run_id=EXCLUDED.run_id, updated_at=now()
		RETURNING id, created_at, updated_at`,
		src.CompanyID, src.SourceName, src.SourceID, src.RawData, src.ExtractedFields,
		src.DataAsOf, src.FetchedAt, src.RunID,
	).Scan(&src.ID, &src.CreatedAt, &src.UpdatedAt)
	if err != nil {
		return eris.Wrap(err, "company: upsert source")
	}
	return nil
}

// GetSources returns all sources for a company.
func (s *PostgresStore) GetSources(ctx context.Context, companyID int64) ([]Source, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, company_id, source, source_id, raw_data, extracted_fields, data_as_of, fetched_at, run_id, created_at, updated_at
		FROM company_sources WHERE company_id=$1 ORDER BY fetched_at DESC`, companyID)
	if err != nil {
		return nil, eris.Wrap(err, "company: get sources")
	}
	defer rows.Close()

	var srcs []Source
	for rows.Next() {
		var src Source
		if err := rows.Scan(&src.ID, &src.CompanyID, &src.SourceName, &src.SourceID,
			&src.RawData, &src.ExtractedFields, &src.DataAsOf, &src.FetchedAt,
			&src.RunID, &src.CreatedAt, &src.UpdatedAt); err != nil {
			return nil, eris.Wrap(err, "company: scan source")
		}
		srcs = append(srcs, src)
	}
	return srcs, rows.Err()
}

// GetSource returns a specific source for a company.
func (s *PostgresStore) GetSource(ctx context.Context, companyID int64, sourceName, sourceID string) (*Source, error) {
	var src Source
	err := s.pool.QueryRow(ctx, `
		SELECT id, company_id, source, source_id, raw_data, extracted_fields, data_as_of, fetched_at, run_id, created_at, updated_at
		FROM company_sources WHERE company_id=$1 AND source=$2 AND source_id=$3`,
		companyID, sourceName, sourceID).
		Scan(&src.ID, &src.CompanyID, &src.SourceName, &src.SourceID,
			&src.RawData, &src.ExtractedFields, &src.DataAsOf, &src.FetchedAt,
			&src.RunID, &src.CreatedAt, &src.UpdatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, eris.Wrap(err, "company: get source")
	}
	return &src, nil
}

// UpsertFinancial inserts or updates a financial metric.
func (s *PostgresStore) UpsertFinancial(ctx context.Context, f *Financial) error {
	err := s.pool.QueryRow(ctx, `
		INSERT INTO company_financials (company_id, period_type, period_date, metric, value, source)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (company_id, period_type, period_date, metric, source) DO UPDATE SET
			value=EXCLUDED.value
		RETURNING id, created_at`,
		f.CompanyID, f.PeriodType, f.PeriodDate, f.Metric, f.Value, f.SourceName,
	).Scan(&f.ID, &f.CreatedAt)
	if err != nil {
		return eris.Wrap(err, "company: upsert financial")
	}
	return nil
}

// GetFinancials returns time-series metrics for a company, optionally filtered by metric name.
func (s *PostgresStore) GetFinancials(ctx context.Context, companyID int64, metric string) ([]Financial, error) {
	var rows pgx.Rows
	var err error
	if metric != "" {
		rows, err = s.pool.Query(ctx, `
			SELECT id, company_id, period_type, period_date, metric, value, source, created_at
			FROM company_financials WHERE company_id=$1 AND metric=$2 ORDER BY period_date DESC`, companyID, metric)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT id, company_id, period_type, period_date, metric, value, source, created_at
			FROM company_financials WHERE company_id=$1 ORDER BY period_date DESC`, companyID)
	}
	if err != nil {
		return nil, eris.Wrap(err, "company: get financials")
	}
	defer rows.Close()

	var fins []Financial
	for rows.Next() {
		var f Financial
		if err := rows.Scan(&f.ID, &f.CompanyID, &f.PeriodType, &f.PeriodDate,
			&f.Metric, &f.Value, &f.SourceName, &f.CreatedAt); err != nil {
			return nil, eris.Wrap(err, "company: scan financial")
		}
		fins = append(fins, f)
	}
	return fins, rows.Err()
}

// SetTags replaces all tags of a given type for a company.
func (s *PostgresStore) SetTags(ctx context.Context, companyID int64, tagType string, values []string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return eris.Wrap(err, "company: begin set tags")
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Delete existing tags of this type.
	if _, err := tx.Exec(ctx, `DELETE FROM company_tags WHERE company_id=$1 AND tag_type=$2`, companyID, tagType); err != nil {
		return eris.Wrap(err, "company: delete old tags")
	}

	// Insert new tags.
	for _, v := range values {
		if _, err := tx.Exec(ctx, `INSERT INTO company_tags (company_id, tag_type, tag_value) VALUES ($1, $2, $3)`,
			companyID, tagType, v); err != nil {
			return eris.Wrap(err, "company: insert tag")
		}
	}

	return tx.Commit(ctx)
}

// GetTags returns all tags for a company.
func (s *PostgresStore) GetTags(ctx context.Context, companyID int64) ([]Tag, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT company_id, tag_type, tag_value, created_at
		FROM company_tags WHERE company_id=$1 ORDER BY tag_type, tag_value`, companyID)
	if err != nil {
		return nil, eris.Wrap(err, "company: get tags")
	}
	defer rows.Close()

	var tags []Tag
	for rows.Next() {
		var t Tag
		if err := rows.Scan(&t.CompanyID, &t.TagType, &t.TagValue, &t.CreatedAt); err != nil {
			return nil, eris.Wrap(err, "company: scan tag")
		}
		tags = append(tags, t)
	}
	return tags, rows.Err()
}

// UpsertMatch inserts or updates a company match to fed_data.
func (s *PostgresStore) UpsertMatch(ctx context.Context, m *Match) error {
	err := s.pool.QueryRow(ctx, `
		INSERT INTO company_matches (company_id, matched_source, matched_key, match_type, confidence)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (company_id, matched_source, matched_key) DO UPDATE SET
			match_type=EXCLUDED.match_type, confidence=EXCLUDED.confidence
		RETURNING id, created_at`,
		m.CompanyID, m.MatchedSource, m.MatchedKey, m.MatchType, m.Confidence,
	).Scan(&m.ID, &m.CreatedAt)
	if err != nil {
		return eris.Wrap(err, "company: upsert match")
	}
	return nil
}

// GetMatches returns all fed_data matches for a company.
func (s *PostgresStore) GetMatches(ctx context.Context, companyID int64) ([]Match, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, company_id, matched_source, matched_key, match_type, confidence, created_at
		FROM company_matches WHERE company_id=$1`, companyID)
	if err != nil {
		return nil, eris.Wrap(err, "company: get matches")
	}
	defer rows.Close()

	var matches []Match
	for rows.Next() {
		var m Match
		if err := rows.Scan(&m.ID, &m.CompanyID, &m.MatchedSource, &m.MatchedKey,
			&m.MatchType, &m.Confidence, &m.CreatedAt); err != nil {
			return nil, eris.Wrap(err, "company: scan match")
		}
		matches = append(matches, m)
	}
	return matches, rows.Err()
}

// FindByMatch finds a company linked to a fed_data entity.
func (s *PostgresStore) FindByMatch(ctx context.Context, matchedSource, matchedKey string) (*CompanyRecord, error) {
	c := &CompanyRecord{}
	err := s.pool.QueryRow(ctx, `
		SELECT `+companyColumns+`
		FROM companies c
		JOIN company_matches cm ON c.id = cm.company_id
		WHERE cm.matched_source = $1 AND cm.matched_key = $2
		LIMIT 1`, matchedSource, matchedKey).
		Scan(companyDests(c)...)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, eris.Wrapf(err, "company: find by match %s:%s", matchedSource, matchedKey)
	}
	return c, nil
}

// companyColumns is the standard column list for company queries.
const companyColumns = `c.id, c.name, c.legal_name, c.domain, c.website, c.description,
	c.naics_code, c.sic_code, c.business_model, c.year_founded, c.ownership_type,
	c.phone, c.email,
	c.employee_count, c.employee_estimate, c.revenue_estimate, c.revenue_range, c.revenue_confidence,
	c.street, c.city, c.state, c.zip_code, c.country,
	c.enrichment_score, c.last_enriched_at, c.last_run_id,
	c.created_at, c.updated_at`

// companyDests returns scan destinations for a CompanyRecord.
func companyDests(c *CompanyRecord) []any {
	return []any{
		&c.ID, &c.Name, &c.LegalName, &c.Domain, &c.Website, &c.Description,
		&c.NAICSCode, &c.SICCode, &c.BusinessModel, &c.YearFounded, &c.OwnershipType,
		&c.Phone, &c.Email,
		&c.EmployeeCount, &c.EmployeeEstimate, &c.RevenueEstimate, &c.RevenueRange, &c.RevenueConfidence,
		&c.Street, &c.City, &c.State, &c.ZipCode, &c.Country,
		&c.EnrichmentScore, &c.LastEnrichedAt, &c.LastRunID,
		&c.CreatedAt, &c.UpdatedAt,
	}
}

func scanCompanies(rows pgx.Rows) ([]CompanyRecord, error) {
	var companies []CompanyRecord
	for rows.Next() {
		var c CompanyRecord
		if err := rows.Scan(companyDests(&c)...); err != nil {
			return nil, eris.Wrap(err, "company: scan")
		}
		companies = append(companies, c)
	}
	return companies, rows.Err()
}

func nilIfZero(v int) any {
	if v == 0 {
		return nil
	}
	return v
}
