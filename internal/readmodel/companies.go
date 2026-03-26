package readmodel

import (
	"context"
	"encoding/json"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/company"
	"github.com/sells-group/research-cli/internal/db"
)

type postgresCompanies struct {
	pool         db.Pool
	companyStore company.CompanyStore
}

// ListCompanies implements CompaniesReader.
func (p *postgresCompanies) ListCompanies(ctx context.Context, filter CompaniesFilter) ([]company.CompanyRecord, int, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}

	var (
		rowsQuery  string
		countQuery string
		args       []any
		countArgs  []any
	)

	if filter.Search != "" {
		rowsQuery = `
			SELECT row_to_json(c)
			FROM companies c
			WHERE c.name ILIKE $1 OR c.domain ILIKE $1
			ORDER BY c.name
			LIMIT $2 OFFSET $3`
		countQuery = `SELECT COUNT(*) FROM companies c WHERE c.name ILIKE $1 OR c.domain ILIKE $1`
		args = []any{"%" + filter.Search + "%", limit, filter.Offset}
		countArgs = []any{"%" + filter.Search + "%"}
	} else {
		rowsQuery = `
			SELECT row_to_json(c)
			FROM companies c
			ORDER BY c.name
			LIMIT $1 OFFSET $2`
		countQuery = `SELECT COUNT(*) FROM companies c`
		args = []any{limit, filter.Offset}
	}

	rows, err := p.pool.Query(ctx, rowsQuery, args...)
	if err != nil {
		return nil, 0, eris.Wrap(err, "readmodel: list companies")
	}
	defer rows.Close()

	var companies []company.CompanyRecord
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, 0, eris.Wrap(err, "readmodel: scan company row")
		}

		var c company.CompanyRecord
		if err := json.Unmarshal(payload, &c); err != nil {
			return nil, 0, eris.Wrap(err, "readmodel: unmarshal company row")
		}
		companies = append(companies, c)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, eris.Wrap(err, "readmodel: iterate companies")
	}

	if companies == nil {
		companies = []company.CompanyRecord{}
	}

	var total int
	if err := p.pool.QueryRow(ctx, countQuery, countArgs...).Scan(&total); err != nil {
		return nil, 0, eris.Wrap(err, "readmodel: count companies")
	}

	return companies, total, nil
}

// GetCompany implements CompaniesReader.
func (p *postgresCompanies) GetCompany(ctx context.Context, id int64) (*company.CompanyRecord, error) {
	return p.companyStore.GetCompany(ctx, id)
}

// SearchCompanies implements CompaniesReader.
func (p *postgresCompanies) SearchCompanies(ctx context.Context, name string, limit int) ([]company.CompanyRecord, error) {
	return p.companyStore.SearchCompaniesByName(ctx, name, limit)
}

// ListCompanyIdentifiers implements CompaniesReader.
func (p *postgresCompanies) ListCompanyIdentifiers(ctx context.Context, companyID int64) ([]company.Identifier, error) {
	return p.companyStore.GetIdentifiers(ctx, companyID)
}

// ListCompanyAddresses implements CompaniesReader.
func (p *postgresCompanies) ListCompanyAddresses(ctx context.Context, companyID int64) ([]company.Address, error) {
	return p.companyStore.GetAddresses(ctx, companyID)
}

// ListCompanyMatches implements CompaniesReader.
func (p *postgresCompanies) ListCompanyMatches(ctx context.Context, companyID int64) ([]company.Match, error) {
	return p.companyStore.GetMatches(ctx, companyID)
}

// ListCompanyMSAs implements CompaniesReader.
func (p *postgresCompanies) ListCompanyMSAs(ctx context.Context, companyID int64) ([]company.AddressMSA, error) {
	return p.companyStore.GetCompanyMSAs(ctx, companyID)
}

// ListCompanyGeoPoints implements CompaniesReader.
func (p *postgresCompanies) ListCompanyGeoPoints(ctx context.Context, limit int) ([]CompanyGeoPoint, error) {
	if limit <= 0 {
		limit = 1000
	}

	rows, err := p.pool.Query(ctx, `
		SELECT
			ca.id,
			ca.company_id,
			c.name,
			c.domain,
			ca.street,
			ca.city,
			ca.state,
			ca.zip_code,
			ca.latitude,
			ca.longitude,
			c.enrichment_score
		FROM company_addresses ca
		JOIN companies c ON c.id = ca.company_id
		WHERE ca.latitude IS NOT NULL AND ca.longitude IS NOT NULL
		ORDER BY ca.is_primary DESC, ca.id
		LIMIT $1`,
		limit,
	)
	if err != nil {
		return nil, eris.Wrap(err, "readmodel: list company geo points")
	}
	defer rows.Close()

	var points []CompanyGeoPoint
	for rows.Next() {
		var point CompanyGeoPoint
		if err := rows.Scan(
			&point.AddressID,
			&point.CompanyID,
			&point.Name,
			&point.Domain,
			&point.Street,
			&point.City,
			&point.State,
			&point.ZipCode,
			&point.Latitude,
			&point.Longitude,
			&point.EnrichmentScore,
		); err != nil {
			return nil, eris.Wrap(err, "readmodel: scan company geo point")
		}
		points = append(points, point)
	}

	return points, eris.Wrap(rows.Err(), "readmodel: iterate company geo points")
}
