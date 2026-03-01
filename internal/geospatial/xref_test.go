package geospatial

import (
	"context"
	"fmt"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRefreshView_Valid(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("REFRESH MATERIALIZED VIEW CONCURRENTLY geo\\.mv_county_economics").
		WillReturnResult(pgxmock.NewResult("REFRESH", 0))

	err = RefreshView(context.Background(), mock, "geo.mv_county_economics")
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRefreshView_InvalidName(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	err = RefreshView(context.Background(), mock, "geo.mv_evil_injection; DROP TABLE users")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown view")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRefreshView_DBError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("REFRESH MATERIALIZED VIEW CONCURRENTLY geo\\.mv_cbsa_summary").
		WillReturnError(fmt.Errorf("connection refused"))

	err = RefreshView(context.Background(), mock, "geo.mv_cbsa_summary")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refresh view")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRefreshAllViews_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	for _, v := range knownViews {
		mock.ExpectExec(fmt.Sprintf("REFRESH MATERIALIZED VIEW CONCURRENTLY %s", v)).
			WillReturnResult(pgxmock.NewResult("REFRESH", 0))
	}

	err = RefreshAllViews(context.Background(), mock)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRefreshAllViews_PartialFailure(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// First view succeeds, second fails.
	mock.ExpectExec("REFRESH MATERIALIZED VIEW CONCURRENTLY geo\\.mv_county_economics").
		WillReturnResult(pgxmock.NewResult("REFRESH", 0))
	mock.ExpectExec("REFRESH MATERIALIZED VIEW CONCURRENTLY geo\\.mv_cbsa_summary").
		WillReturnError(fmt.Errorf("lock timeout"))

	err = RefreshAllViews(context.Background(), mock)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mv_cbsa_summary")
}

func TestListViews_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows([]string{"name", "row_count"}).
		AddRow("geo.mv_cbsa_summary", int64(150)).
		AddRow("geo.mv_county_economics", int64(3200))

	mock.ExpectQuery("SELECT .+ FROM pg_matviews").WillReturnRows(rows)

	views, err := ListViews(context.Background(), mock)
	require.NoError(t, err)
	require.Len(t, views, 2)
	assert.Equal(t, "geo.mv_cbsa_summary", views[0].Name)
	assert.Equal(t, int64(150), views[0].RowCount)
	assert.Equal(t, "geo.mv_county_economics", views[1].Name)
	assert.Equal(t, int64(3200), views[1].RowCount)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestListViews_Empty(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows([]string{"name", "row_count"})
	mock.ExpectQuery("SELECT .+ FROM pg_matviews").WillReturnRows(rows)

	views, err := ListViews(context.Background(), mock)
	require.NoError(t, err)
	assert.Empty(t, views)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestListViews_DBError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT .+ FROM pg_matviews").
		WillReturnError(fmt.Errorf("permission denied"))

	views, err := ListViews(context.Background(), mock)
	require.Error(t, err)
	assert.Nil(t, views)
	assert.Contains(t, err.Error(), "list materialized views")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestIsKnownView(t *testing.T) {
	assert.True(t, isKnownView("geo.mv_county_economics"))
	assert.True(t, isKnownView("geo.mv_adv_firms_by_state"))
	assert.False(t, isKnownView("geo.mv_unknown"))
	assert.False(t, isKnownView(""))
	assert.False(t, isKnownView("public.some_table"))
}

func TestListViews_ScanError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Return a row with wrong column type (string instead of int64 for row_count) to trigger scan error.
	rows := pgxmock.NewRows([]string{"name", "row_count"}).
		AddRow("geo.mv_cbsa_summary", "not_a_number")

	mock.ExpectQuery("SELECT .+ FROM pg_matviews").WillReturnRows(rows)

	_, err = ListViews(context.Background(), mock)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scan view info")
	require.NoError(t, mock.ExpectationsWereMet())
}
