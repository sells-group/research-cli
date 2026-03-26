// Package readmodel provides typed Postgres-backed query services for read-side APIs.
package readmodel

import (
	"context"

	"github.com/sells-group/research-cli/internal/company"
	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync"
)

// CompaniesReader serves company and map-oriented read queries.
type CompaniesReader interface {
	ListCompanies(ctx context.Context, filter CompaniesFilter) ([]company.CompanyRecord, int, error)
	GetCompany(ctx context.Context, id int64) (*company.CompanyRecord, error)
	SearchCompanies(ctx context.Context, name string, limit int) ([]company.CompanyRecord, error)
	ListCompanyIdentifiers(ctx context.Context, companyID int64) ([]company.Identifier, error)
	ListCompanyAddresses(ctx context.Context, companyID int64) ([]company.Address, error)
	ListCompanyMatches(ctx context.Context, companyID int64) ([]company.Match, error)
	ListCompanyMSAs(ctx context.Context, companyID int64) ([]company.AddressMSA, error)
	ListCompanyGeoPoints(ctx context.Context, limit int) ([]CompanyGeoPoint, error)
}

// DataReader serves data explorer metadata and query operations.
type DataReader interface {
	TableExists(ctx context.Context, table string) (bool, error)
	ColumnExists(ctx context.Context, table, column string) (bool, error)
	ListDataTables(ctx context.Context) ([]TableMeta, error)
	QueryDataTable(ctx context.Context, params DataQueryParams) (*DataQueryResult, error)
	GetDataRow(ctx context.Context, table, id string) (map[string]any, error)
	GetDataFilterValues(ctx context.Context, table, column string, limit int) ([]any, error)
	AggregateData(ctx context.Context, params DataAggregateParams) (*DataAggregateResult, error)
}

// AnalyticsReader serves reporting and analytics queries.
type AnalyticsReader interface {
	SyncTrends(ctx context.Context, days int) ([]SyncTrend, error)
	IdentifierCoverage(ctx context.Context) ([]IdentifierCoverage, error)
	XrefCoverage(ctx context.Context) ([]XrefCoverage, error)
	EnrichmentStats(ctx context.Context, days int) (*EnrichmentStats, error)
	CostBreakdown(ctx context.Context, days int) ([]CostBreakdownRow, error)
}

// FedsyncReader serves fedsync status and sync log queries.
type FedsyncReader interface {
	ListDatasetStatuses(ctx context.Context) ([]DatasetStatus, error)
	ListSyncEntries(ctx context.Context) ([]fedsync.SyncEntry, error)
}

// Service groups the read-side query services used by the API.
type Service struct {
	Companies CompaniesReader
	Data      DataReader
	Analytics AnalyticsReader
	Fedsync   FedsyncReader
}

// NewPostgresService creates a Postgres-backed readmodel service bundle.
func NewPostgresService(pool db.Pool, cfg *config.Config) *Service {
	if pool == nil {
		return nil
	}

	companyStore := company.NewPostgresStore(pool)

	return &Service{
		Companies: &postgresCompanies{
			pool:         pool,
			companyStore: companyStore,
		},
		Data: &postgresData{
			pool: pool,
		},
		Analytics: &postgresAnalytics{
			pool: pool,
		},
		Fedsync: &postgresFedsync{
			pool:     pool,
			registry: newRegistry(cfg),
			syncLog:  fedsync.NewSyncLog(pool),
		},
	}
}
