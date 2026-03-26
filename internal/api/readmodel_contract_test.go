package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/company"
	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/readmodel"
)

type fakeCompaniesReader struct {
	listCompanies      []company.CompanyRecord
	listCompaniesTotal int
	searchCompanies    []company.CompanyRecord
	company            *company.CompanyRecord
	identifiers        []company.Identifier
	addresses          []company.Address
	matches            []company.Match
	msas               []company.AddressMSA
	geoPoints          []readmodel.CompanyGeoPoint
}

func (f *fakeCompaniesReader) ListCompanies(context.Context, readmodel.CompaniesFilter) ([]company.CompanyRecord, int, error) {
	return f.listCompanies, f.listCompaniesTotal, nil
}

func (f *fakeCompaniesReader) GetCompany(context.Context, int64) (*company.CompanyRecord, error) {
	return f.company, nil
}

func (f *fakeCompaniesReader) SearchCompanies(context.Context, string, int) ([]company.CompanyRecord, error) {
	return f.searchCompanies, nil
}

func (f *fakeCompaniesReader) ListCompanyIdentifiers(context.Context, int64) ([]company.Identifier, error) {
	return f.identifiers, nil
}

func (f *fakeCompaniesReader) ListCompanyAddresses(context.Context, int64) ([]company.Address, error) {
	return f.addresses, nil
}

func (f *fakeCompaniesReader) ListCompanyMatches(context.Context, int64) ([]company.Match, error) {
	return f.matches, nil
}

func (f *fakeCompaniesReader) ListCompanyMSAs(context.Context, int64) ([]company.AddressMSA, error) {
	return f.msas, nil
}

func (f *fakeCompaniesReader) ListCompanyGeoPoints(context.Context, int) ([]readmodel.CompanyGeoPoint, error) {
	return f.geoPoints, nil
}

type fakeDataReader struct {
	tables       map[string]bool
	columns      map[string]map[string]bool
	tableList    []readmodel.TableMeta
	queryResult  *readmodel.DataQueryResult
	row          map[string]any
	filterValues []any
	aggregate    *readmodel.DataAggregateResult
}

func (f *fakeDataReader) TableExists(_ context.Context, table string) (bool, error) {
	return f.tables[table], nil
}

func (f *fakeDataReader) ColumnExists(_ context.Context, table, column string) (bool, error) {
	return f.columns[table][column], nil
}

func (f *fakeDataReader) ListDataTables(context.Context) ([]readmodel.TableMeta, error) {
	return f.tableList, nil
}

func (f *fakeDataReader) QueryDataTable(context.Context, readmodel.DataQueryParams) (*readmodel.DataQueryResult, error) {
	return f.queryResult, nil
}

func (f *fakeDataReader) GetDataRow(context.Context, string, string) (map[string]any, error) {
	return f.row, nil
}

func (f *fakeDataReader) GetDataFilterValues(context.Context, string, string, int) ([]any, error) {
	return f.filterValues, nil
}

func (f *fakeDataReader) AggregateData(context.Context, readmodel.DataAggregateParams) (*readmodel.DataAggregateResult, error) {
	return f.aggregate, nil
}

type fakeAnalyticsReader struct {
	trends    []readmodel.SyncTrend
	coverage  []readmodel.IdentifierCoverage
	xref      []readmodel.XrefCoverage
	stats     *readmodel.EnrichmentStats
	breakdown []readmodel.CostBreakdownRow
}

func (f *fakeAnalyticsReader) SyncTrends(context.Context, int) ([]readmodel.SyncTrend, error) {
	return f.trends, nil
}

func (f *fakeAnalyticsReader) IdentifierCoverage(context.Context) ([]readmodel.IdentifierCoverage, error) {
	return f.coverage, nil
}

func (f *fakeAnalyticsReader) XrefCoverage(context.Context) ([]readmodel.XrefCoverage, error) {
	return f.xref, nil
}

func (f *fakeAnalyticsReader) EnrichmentStats(context.Context, int) (*readmodel.EnrichmentStats, error) {
	return f.stats, nil
}

func (f *fakeAnalyticsReader) CostBreakdown(context.Context, int) ([]readmodel.CostBreakdownRow, error) {
	return f.breakdown, nil
}

type fakeFedsyncReader struct {
	statuses []readmodel.DatasetStatus
	entries  []fedsync.SyncEntry
}

func (f *fakeFedsyncReader) ListDatasetStatuses(context.Context) ([]readmodel.DatasetStatus, error) {
	return f.statuses, nil
}

func (f *fakeFedsyncReader) ListSyncEntries(context.Context) ([]fedsync.SyncEntry, error) {
	return f.entries, nil
}

func newReadModelRouter(readSvc *readmodel.Service) http.Handler {
	cfg := &config.Config{Server: config.ServerConfig{Port: 8080}}
	return Router(NewHandlers(cfg, nil, nil, nil, readSvc))
}

func TestReadModelRoutes_NotConfigured(t *testing.T) {
	router := newReadModelRouter(nil)

	cases := []struct {
		path string
	}{
		{path: "/api/v1/companies"},
		{path: "/api/v1/fedsync/statuses"},
		{path: "/api/v1/data/tables"},
		{path: "/api/v1/analytics/sync-trends"},
	}

	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, tc.path, nil)
			router.ServeHTTP(w, r)

			assert.Equal(t, http.StatusServiceUnavailable, w.Code)
		})
	}
}

func TestCompaniesRoutes_Success(t *testing.T) {
	router := newReadModelRouter(&readmodel.Service{
		Companies: &fakeCompaniesReader{
			listCompanies:      []company.CompanyRecord{{ID: 1, Name: "Acme Advisors", Domain: "acme.com"}},
			listCompaniesTotal: 1,
			searchCompanies:    []company.CompanyRecord{{ID: 1, Name: "Acme Advisors", Domain: "acme.com"}},
			identifiers:        []company.Identifier{{ID: 10, CompanyID: 1, System: "cik", Identifier: "0001"}},
			geoPoints: []readmodel.CompanyGeoPoint{{
				AddressID: 1, CompanyID: 1, Name: "Acme Advisors", Domain: "acme.com",
				Latitude: 40.0, Longitude: -74.0,
			}},
		},
	})

	t.Run("list", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/api/v1/companies?limit=1", nil)
		router.ServeHTTP(w, r)

		require.Equal(t, http.StatusOK, w.Code)
		var body struct {
			Companies []company.CompanyRecord `json:"companies"`
			Total     int                     `json:"total"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		require.Len(t, body.Companies, 1)
		assert.Equal(t, 1, body.Total)
		assert.Equal(t, "Acme Advisors", body.Companies[0].Name)
	})

	t.Run("search", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/api/v1/companies/search?name=Acme", nil)
		router.ServeHTTP(w, r)

		require.Equal(t, http.StatusOK, w.Code)
		var body struct {
			Total int `json:"total"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, 1, body.Total)
	})

	t.Run("geojson", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/api/v1/companies/geojson", nil)
		router.ServeHTTP(w, r)

		require.Equal(t, http.StatusOK, w.Code)
		var body struct {
			Type     string `json:"type"`
			Features []struct {
				Type       string         `json:"type"`
				Properties map[string]any `json:"properties"`
			} `json:"features"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, "FeatureCollection", body.Type)
		require.Len(t, body.Features, 1)
		assert.Equal(t, "Acme Advisors", body.Features[0].Properties["name"])
	})
}

func TestDataRoutes_SuccessAndValidation(t *testing.T) {
	router := newReadModelRouter(&readmodel.Service{
		Data: &fakeDataReader{
			tables: map[string]bool{"fpds_contracts": true},
			columns: map[string]map[string]bool{
				"fpds_contracts": {
					"recipient_name": true,
				},
			},
			tableList: []readmodel.TableMeta{{
				ID: "fpds_contracts", Name: "fpds_contracts", Category: "Contracts", EstimatedRowCount: 1234,
				Columns: []readmodel.TableColumn{{Key: "recipient_name", Label: "recipient_name", Type: "text", Sortable: true}},
			}},
			queryResult: &readmodel.DataQueryResult{
				Rows:      []map[string]any{{"recipient_name": "Acme Advisors"}},
				TotalRows: 2,
				Page:      2,
				PageSize:  25,
				Sort:      &readmodel.DataQuerySort{Column: "recipient_name", Direction: "asc"},
				Filter:    &readmodel.DataQueryFilter{Column: "recipient_name", Value: "acme"},
			},
			aggregate: &readmodel.DataAggregateResult{
				Table: "fpds_contracts", GroupBy: "recipient_name", Aggregation: "count",
				Rows: []readmodel.DataAggregateRow{{Key: "Acme Advisors", Value: int64(2)}},
			},
		},
	})

	t.Run("tables", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/api/v1/data/tables", nil)
		router.ServeHTTP(w, r)

		require.Equal(t, http.StatusOK, w.Code)
		var body struct {
			Tables []readmodel.TableMeta `json:"tables"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		require.Len(t, body.Tables, 1)
		assert.Equal(t, int64(1234), body.Tables[0].EstimatedRowCount)
	})

	t.Run("query", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/api/v1/data/fpds_contracts?limit=25&offset=25&sort=recipient_name&dir=asc&search_col=recipient_name&search_val=acme", nil)
		router.ServeHTTP(w, r)

		require.Equal(t, http.StatusOK, w.Code)
		var body readmodel.DataQueryResult
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, int64(2), body.TotalRows)
		assert.Equal(t, 25, body.PageSize)
		require.NotNil(t, body.Sort)
		assert.Equal(t, "recipient_name", body.Sort.Column)
		require.NotNil(t, body.Filter)
		assert.Equal(t, "acme", body.Filter.Value)
	})

	t.Run("aggregate", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/api/v1/data/fpds_contracts/aggregate?group_by=recipient_name&func=count", nil)
		router.ServeHTTP(w, r)

		require.Equal(t, http.StatusOK, w.Code)
		var body readmodel.DataAggregateResult
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, "count", body.Aggregation)
		require.Len(t, body.Rows, 1)
		assert.Equal(t, "Acme Advisors", body.Rows[0].Key)
	})

	t.Run("invalid-table-name", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/api/v1/data/not-valid", nil)
		router.ServeHTTP(w, r)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
}

func TestFedsyncRoutes_Success(t *testing.T) {
	now := time.Date(2026, 3, 25, 12, 0, 0, 0, time.UTC)
	router := newReadModelRouter(&readmodel.Service{
		Fedsync: &fakeFedsyncReader{
			statuses: []readmodel.DatasetStatus{{
				Name: "cbp", Table: "fed_data.cbp_data", Phase: "1", Cadence: "annual",
				LastSync: &now, LastStatus: "complete", RowsSynced: 100, RowCount: 2500,
			}},
			entries: []fedsync.SyncEntry{{
				ID: 1, Dataset: "cbp", Status: "complete", StartedAt: now, RowsSynced: 100,
			}},
		},
	})

	t.Run("statuses", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/api/v1/fedsync/statuses", nil)
		router.ServeHTTP(w, r)

		require.Equal(t, http.StatusOK, w.Code)
		var body struct {
			Datasets []readmodel.DatasetStatus `json:"datasets"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		require.Len(t, body.Datasets, 1)
		assert.Equal(t, "cbp", body.Datasets[0].Name)
	})

	t.Run("sync-log", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/api/v1/fedsync/sync-log", nil)
		router.ServeHTTP(w, r)

		require.Equal(t, http.StatusOK, w.Code)
		var body struct {
			Entries []fedsync.SyncEntry `json:"entries"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		require.Len(t, body.Entries, 1)
		assert.Equal(t, "complete", body.Entries[0].Status)
	})
}

func TestAnalyticsRoutes_Success(t *testing.T) {
	router := newReadModelRouter(&readmodel.Service{
		Analytics: &fakeAnalyticsReader{
			trends:   []readmodel.SyncTrend{{Date: "2026-03-25", Dataset: "cbp", RowsSynced: 100}},
			coverage: []readmodel.IdentifierCoverage{{System: "cik", Count: 10, Total: 20, Percentage: 50}},
			xref:     []readmodel.XrefCoverage{{SourceA: "adv", SourceB: "edgar", Count: 8, AvgConfidence: 0.9}},
			stats: &readmodel.EnrichmentStats{
				TotalRuns:         10,
				AvgScore:          0.85,
				ScoreDistribution: []readmodel.ScoreDistributionBucket{{Bucket: 80, Count: 5}},
				PhaseDurations:    []readmodel.PhaseDuration{{Phase: "crawl", AvgMS: 1500}},
			},
			breakdown: []readmodel.CostBreakdownRow{{Date: "2026-03-25", Tier: "all", Cost: 1.25, Tokens: 2500}},
		},
	})

	t.Run("sync-trends", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/api/v1/analytics/sync-trends?days=14", nil)
		router.ServeHTTP(w, r)

		require.Equal(t, http.StatusOK, w.Code)
		var body struct {
			Days   int                   `json:"days"`
			Trends []readmodel.SyncTrend `json:"trends"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, 14, body.Days)
		require.Len(t, body.Trends, 1)
	})

	t.Run("enrichment-stats", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/api/v1/analytics/enrichment-stats", nil)
		router.ServeHTTP(w, r)

		require.Equal(t, http.StatusOK, w.Code)
		var body readmodel.EnrichmentStats
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, 10, body.TotalRuns)
		assert.InDelta(t, 0.85, body.AvgScore, 0.001)
	})

	t.Run("identifier-coverage", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/api/v1/analytics/identifier-coverage", nil)
		router.ServeHTTP(w, r)

		require.Equal(t, http.StatusOK, w.Code)
		var body struct {
			Coverage []readmodel.IdentifierCoverage `json:"coverage"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		require.Len(t, body.Coverage, 1)
		assert.Equal(t, "cik", body.Coverage[0].System)
	})
}
