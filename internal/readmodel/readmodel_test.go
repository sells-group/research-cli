package readmodel

import (
	"context"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
)

func newMockPool(t *testing.T) pgxmock.PgxPoolIface {
	t.Helper()
	mock, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherRegexp))
	require.NoError(t, err)
	t.Cleanup(func() { mock.Close() })
	return mock
}

func TestPostgresCompanies_ListCompanies(t *testing.T) {
	mock := newMockPool(t)
	reader := &postgresCompanies{pool: mock}

	mock.ExpectQuery(`SELECT row_to_json\(c\)`).
		WithArgs("%acme%", 10, 0).
		WillReturnRows(pgxmock.NewRows([]string{"row_to_json"}).AddRow([]byte(`{"id":1,"name":"Acme","domain":"acme.com","website":"https://acme.com"}`)))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM companies`).
		WithArgs("%acme%").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(1))

	companies, total, err := reader.ListCompanies(context.Background(), CompaniesFilter{
		Search: "acme",
		Limit:  10,
	})
	require.NoError(t, err)
	require.Len(t, companies, 1)
	assert.Equal(t, 1, total)
	assert.Equal(t, "Acme", companies[0].Name)
	assert.Equal(t, "acme.com", companies[0].Domain)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresData_ListDataTables(t *testing.T) {
	mock := newMockPool(t)
	reader := &postgresData{pool: mock}

	mock.ExpectQuery(`SELECT table_name, column_name, data_type`).
		WillReturnRows(pgxmock.NewRows([]string{"table_name", "column_name", "data_type"}).
			AddRow("fpds_contracts", "id", "bigint").
			AddRow("fpds_contracts", "recipient_name", "text").
			AddRow("sync_log", "id", "bigint"))
	mock.ExpectQuery(`SELECT relname, COALESCE\(n_live_tup, 0\)`).
		WillReturnRows(pgxmock.NewRows([]string{"relname", "count"}).
			AddRow("fpds_contracts", int64(1234)).
			AddRow("sync_log", int64(99)))

	tables, err := reader.ListDataTables(context.Background())
	require.NoError(t, err)
	require.Len(t, tables, 1)
	assert.Equal(t, "fpds_contracts", tables[0].ID)
	assert.Equal(t, int64(1234), tables[0].EstimatedRowCount)
	assert.Len(t, tables[0].Columns, 2)
	assert.Equal(t, "number", tables[0].Columns[0].Type)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresData_QueryDataTable(t *testing.T) {
	mock := newMockPool(t)
	reader := &postgresData{pool: mock}

	mock.ExpectQuery(`SELECT EXISTS\(`).
		WithArgs("fpds_contracts").
		WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(`SELECT EXISTS\(`).
		WithArgs("fpds_contracts", "recipient_name").
		WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(`SELECT EXISTS\(`).
		WithArgs("fpds_contracts", "recipient_name").
		WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM fed_data.fpds_contracts WHERE recipient_name::text ILIKE \$1`).
		WithArgs("%acme%").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(2)))
	mock.ExpectQuery(`SELECT \* FROM fed_data.fpds_contracts WHERE recipient_name::text ILIKE \$1 ORDER BY recipient_name asc LIMIT \$2 OFFSET \$3`).
		WithArgs("%acme%", 25, 25).
		WillReturnRows(pgxmock.NewRows([]string{"id", "recipient_name"}).
			AddRow(int64(1), "Acme Advisors").
			AddRow(int64(2), "Acme Holdings"))

	result, err := reader.QueryDataTable(context.Background(), DataQueryParams{
		Table:         "fpds_contracts",
		Limit:         25,
		Offset:        25,
		SortColumn:    "recipient_name",
		SortDirection: "asc",
		SearchColumn:  "recipient_name",
		SearchValue:   "acme",
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int64(2), result.TotalRows)
	assert.Equal(t, 2, result.Page)
	assert.Equal(t, 25, result.PageSize)
	require.NotNil(t, result.Sort)
	assert.Equal(t, "recipient_name", result.Sort.Column)
	require.NotNil(t, result.Filter)
	assert.Equal(t, "acme", result.Filter.Value)
	assert.Len(t, result.Rows, 2)
	assert.Equal(t, "Acme Advisors", result.Rows[0]["recipient_name"])
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresAnalytics_CostBreakdown(t *testing.T) {
	mock := newMockPool(t)
	reader := &postgresAnalytics{pool: mock}

	mock.ExpectQuery(`SELECT\s+date_trunc\('day', created_at\)::date AS cost_date`).
		WithArgs(14).
		WillReturnRows(pgxmock.NewRows([]string{"cost_date", "tier", "cost", "tokens"}).
			AddRow("2026-03-25", "all", 1.25, int64(2500)).
			AddRow("2026-03-24", "all", 0.75, int64(1800)))

	rows, err := reader.CostBreakdown(context.Background(), 14)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, "all", rows[0].Tier)
	assert.Equal(t, "2026-03-25", rows[0].Date)
	assert.InDelta(t, 1.25, rows[0].Cost, 0.001)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresFedsync_ListDatasetStatuses(t *testing.T) {
	mock := newMockPool(t)
	now := time.Date(2026, 3, 25, 12, 0, 0, 0, time.UTC)

	mock.ExpectQuery(`SELECT dataset, status, rows_synced, started_at, metadata\s+FROM fed_data\.mv_dataset_status_latest`).
		WillReturnRows(pgxmock.NewRows([]string{"dataset", "status", "rows_synced", "started_at", "metadata"}).
			AddRow("cbp", "complete", int64(100), now, map[string]any{"year": 2025}))
	mock.ExpectQuery(`SELECT relname, COALESCE\(n_live_tup, 0\)`).
		WillReturnRows(pgxmock.NewRows([]string{"relname", "count"}).
			AddRow("cbp_data", int64(2500)))
	mock.ExpectQuery(`SELECT DISTINCT ON \(dataset\) dataset, started_at`).
		WillReturnRows(pgxmock.NewRows([]string{"dataset", "started_at"}).
			AddRow("cbp", now))

	reader := &postgresFedsync{
		pool:     mock,
		registry: dataset.NewRegistry(nil),
		syncLog:  fedsync.NewSyncLog(mock),
	}

	statuses, err := reader.ListDatasetStatuses(context.Background())
	require.NoError(t, err)
	require.Len(t, statuses, 42)

	var cbpStatus *DatasetStatus
	for i := range statuses {
		if statuses[i].Name == "cbp" {
			cbpStatus = &statuses[i]
			break
		}
	}
	require.NotNil(t, cbpStatus)
	assert.Equal(t, "complete", cbpStatus.LastStatus)
	assert.Equal(t, int64(100), cbpStatus.RowsSynced)
	assert.Equal(t, int64(2500), cbpStatus.RowCount)
	require.NotNil(t, cbpStatus.LastSync)
	require.NotNil(t, cbpStatus.NextDue)
	assert.Equal(t, now.AddDate(1, 0, 0), *cbpStatus.NextDue)
	assert.NoError(t, mock.ExpectationsWereMet())
}
