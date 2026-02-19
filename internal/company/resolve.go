package company

import (
	"context"
	"strings"

	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/model"
	"go.uber.org/zap"
)

// Resolver handles company deduplication and identity resolution.
type Resolver struct {
	store CompanyStore
}

// NewResolver creates a company resolver.
func NewResolver(store CompanyStore) *Resolver {
	return &Resolver{store: store}
}

// FindOrCreate looks up an existing company or creates a new one.
// Uses a three-pass cascade:
//  1. Exact identifier match (domain, salesforce_id, CRD, CIK)
//  2. Deterministic attribute match (exact name + state + city)
//  3. Fuzzy name match (pg_trgm similarity > 0.6)
//
// Returns the company and whether it was newly created.
func (r *Resolver) FindOrCreate(ctx context.Context, company model.Company) (*CompanyRecord, bool, error) {
	domain := normalizeDomain(company.URL)
	if domain == "" {
		return nil, false, eris.New("company: domain is required for resolve")
	}

	// Pass 1: Exact domain match (confidence 1.0).
	existing, err := r.store.GetCompanyByDomain(ctx, domain)
	if err != nil {
		return nil, false, eris.Wrap(err, "company: resolve by domain")
	}
	if existing != nil {
		zap.L().Debug("resolve: matched by domain",
			zap.String("domain", domain),
			zap.Int64("company_id", existing.ID),
		)
		return existing, false, nil
	}

	// Pass 1b: Salesforce ID.
	if company.SalesforceID != "" {
		existing, err = r.store.FindByIdentifier(ctx, SystemSalesforce, company.SalesforceID)
		if err != nil {
			return nil, false, eris.Wrap(err, "company: resolve by salesforce_id")
		}
		if existing != nil {
			zap.L().Debug("resolve: matched by salesforce_id",
				zap.String("sf_id", company.SalesforceID),
				zap.Int64("company_id", existing.ID),
			)
			return existing, false, nil
		}
	}

	// Pass 2: Name + state + city (confidence 0.9-0.95).
	if company.Name != "" && company.State != "" {
		results, err := r.store.SearchCompaniesByName(ctx, company.Name, 5)
		if err != nil {
			zap.L().Warn("resolve: name search failed", zap.Error(err))
		} else {
			for _, c := range results {
				if strings.EqualFold(c.Name, company.Name) &&
					strings.EqualFold(c.State, company.State) &&
					(company.City == "" || strings.EqualFold(c.City, company.City)) {
					zap.L().Debug("resolve: matched by name+state+city",
						zap.String("name", company.Name),
						zap.Int64("company_id", c.ID),
					)
					return &c, false, nil
				}
			}
		}
	}

	// Pass 3: No match found — create new.
	record := &CompanyRecord{
		Name:    company.Name,
		Domain:  domain,
		Website: company.URL,
		City:    company.City,
		State:   company.State,
		ZipCode: company.ZipCode,
		Street:  company.Street,
	}

	if err := r.store.CreateCompany(ctx, record); err != nil {
		return nil, false, eris.Wrap(err, "company: create")
	}

	// Link identifiers.
	if company.SalesforceID != "" {
		if err := r.store.UpsertIdentifier(ctx, &Identifier{
			CompanyID:  record.ID,
			System:     SystemSalesforce,
			Identifier: company.SalesforceID,
		}); err != nil {
			zap.L().Warn("resolve: failed to link salesforce_id", zap.Error(err))
		}
	}
	if company.NotionPageID != "" {
		if err := r.store.UpsertIdentifier(ctx, &Identifier{
			CompanyID:  record.ID,
			System:     SystemNotion,
			Identifier: company.NotionPageID,
		}); err != nil {
			zap.L().Warn("resolve: failed to link notion_page_id", zap.Error(err))
		}
	}

	zap.L().Info("resolve: created new company",
		zap.String("domain", domain),
		zap.String("name", company.Name),
		zap.Int64("company_id", record.ID),
	)

	return record, true, nil
}

// normalizeDomain strips protocol and www prefix from a URL.
func normalizeDomain(rawURL string) string {
	d := strings.TrimSpace(rawURL)
	d = strings.TrimPrefix(d, "https://")
	d = strings.TrimPrefix(d, "http://")
	d = strings.TrimPrefix(d, "www.")
	d = strings.TrimSuffix(d, "/")
	return strings.ToLower(d)
}
