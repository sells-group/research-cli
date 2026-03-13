package scraper

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tealeg/xlsx/v2"

	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

// buildFMRXLSX creates an in-memory XLSX file with the given header and data rows.
func buildFMRXLSX(t *testing.T, header []string, rows [][]string) []byte {
	t.Helper()
	f := xlsx.NewFile()
	sheet, err := f.AddSheet("Sheet1")
	require.NoError(t, err)

	hdr := sheet.AddRow()
	for _, h := range header {
		hdr.AddCell().SetString(h)
	}
	for _, r := range rows {
		row := sheet.AddRow()
		for _, v := range r {
			row.AddCell().SetString(v)
		}
	}

	var buf bytes.Buffer
	require.NoError(t, f.Write(&buf))
	return buf.Bytes()
}

func expectFMRUpsert(mock pgxmock.PgxPoolIface, rows int64) {
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_geo_fair_market_rents"}, fmrCols).WillReturnResult(rows)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", rows))
	mock.ExpectCommit()
}

var fmrHeader = []string{"stusps", "state", "hud_area_code", "countyname", "county_town_name", "metro", "hud_area_name", "fips", "pop2023", "fmr_0", "fmr_1", "fmr_2", "fmr_3", "fmr_4"}

func TestHUDFMR_Metadata(t *testing.T) {
	s := &HUDFMR{}
	assert.Equal(t, "hud_fmr", s.Name())
	assert.Equal(t, "geo.fair_market_rents", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestHUDFMR_ShouldRun(t *testing.T) {
	s := &HUDFMR{}
	now := fixedNow()

	assert.True(t, s.ShouldRun(now, nil))

	recent := now
	assert.False(t, s.ShouldRun(now, &recent))
}

func TestHUDFMR_Sync(t *testing.T) {
	xlsxData := buildFMRXLSX(t, fmrHeader, [][]string{
		{"AL", "01", "METRO33860M33860", "Autauga County", "", "1", "Montgomery, AL MSA", "0100199999", "58761", "860", "870", "1016", "1304", "1537"},
		{"TX", "48", "METRO12420M12420", "Travis County", "", "1", "Austin, TX MSA", "4845399999", "1290188", "1200", "1400", "1700", "2100", "2400"},
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(xlsxData)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectFMRUpsert(mock, 2)

	s := &HUDFMR{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestHUDFMR_EmptyFIPS(t *testing.T) {
	xlsxData := buildFMRXLSX(t, fmrHeader, [][]string{
		{"TX", "48", "METRO", "Travis County", "", "1", "Austin", "", "100", "1200", "1400", "1700", "2100", "2400"},
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(xlsxData)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &HUDFMR{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestHUDFMR_DownloadError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &HUDFMR{baseURL: "http://127.0.0.1:1/fmr.xlsx"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestHUDFMR_UpsertError(t *testing.T) {
	xlsxData := buildFMRXLSX(t, fmrHeader, [][]string{
		{"AL", "01", "METRO", "Autauga County", "", "1", "Montgomery", "0100199999", "58761", "860", "870", "1016", "1304", "1537"},
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(xlsxData)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin().WillReturnError(assert.AnError)

	s := &HUDFMR{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestHUDFMR_HeaderOnly(t *testing.T) {
	xlsxData := buildFMRXLSX(t, fmrHeader, nil)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(xlsxData)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &HUDFMR{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no data rows")
}

func TestHUDFMR_InvalidXLSX(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not an xlsx file"))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &HUDFMR{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open xlsx")
}

func TestCsvFMRInt(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"$1,200", 1200},
		{"$2,400", 2400},
		{"1700", 1700},
		{"$0", 0},
		{"", 0},
		{"$3,400", 3400},
		{"  $1,800  ", 1800},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, csvFMRInt(tt.input), "input: %q", tt.input)
	}
}
