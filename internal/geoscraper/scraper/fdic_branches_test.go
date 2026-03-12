package scraper

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

// fdicAPIResponse builds a FDIC BankFind API JSON response.
func fdicAPIResponse(total int, records []map[string]any) []byte {
	type rec struct {
		Data  map[string]any `json:"data"`
		Score int            `json:"score"`
	}
	resp := struct {
		Meta struct {
			Total int `json:"total"`
		} `json:"meta"`
		Data []rec `json:"data"`
	}{}
	resp.Meta.Total = total
	for _, r := range records {
		resp.Data = append(resp.Data, rec{Data: r})
	}
	b, _ := json.Marshal(resp)
	return b
}

func TestFDICBranches_Metadata(t *testing.T) {
	s := &FDICBranches{}
	assert.Equal(t, "fdic_branches", s.Name())
	assert.Equal(t, "geo.infrastructure", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestFDICBranches_ShouldRun(t *testing.T) {
	s := &FDICBranches{}
	now := fixedNow()

	assert.True(t, s.ShouldRun(now, nil))

	// fixedNow() is March 1 — before October release, so any non-nil lastSync returns false.
	recent := now
	assert.False(t, s.ShouldRun(now, &recent))
}

func TestFDICBranches_Sync(t *testing.T) {
	records := []map[string]any{
		{
			"UNINUMBR":       float64(100001),
			"NAMEFULL":       "First National Bank Main",
			"BKCLASS":        "NM",
			"DEPSUMBR":       float64(500000000),
			"SIMS_LATITUDE":  float64(30.267),
			"SIMS_LONGITUDE": float64(-97.743),
			"CITY":           "Austin",
			"STALPBR":        "TX",
			"ADDRESBR":       "123 Main St",
			"ZIPBR":          "78701",
		},
		{
			"UNINUMBR":       float64(100002),
			"NAMEFULL":       "Community Trust Downtown",
			"BKCLASS":        "SM",
			"DEPSUMBR":       float64(200000000),
			"SIMS_LATITUDE":  float64(32.776),
			"SIMS_LONGITUDE": float64(-96.797),
			"CITY":           "Dallas",
			"STALPBR":        "TX",
			"ADDRESBR":       "456 Elm St",
			"ZIPBR":          "75201",
		},
	}
	body := fdicAPIResponse(2, records)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectBulkUpsert(mock, 2)

	s := &FDICBranches{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFDICBranches_NullCoords(t *testing.T) {
	records := []map[string]any{
		{
			"UNINUMBR":       float64(100001),
			"NAMEFULL":       "Zero Bank",
			"BKCLASS":        "NM",
			"DEPSUMBR":       float64(100000000),
			"SIMS_LATITUDE":  float64(0),
			"SIMS_LONGITUDE": float64(0),
		},
		{
			"UNINUMBR":       float64(100002),
			"NAMEFULL":       "Also Zero",
			"BKCLASS":        "SM",
			"DEPSUMBR":       float64(200000000),
			"SIMS_LATITUDE":  float64(0),
			"SIMS_LONGITUDE": float64(0),
		},
	}
	body := fdicAPIResponse(2, records)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FDICBranches{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFDICBranches_MissingID(t *testing.T) {
	records := []map[string]any{
		{
			"NAMEFULL":       "No ID Bank",
			"BKCLASS":        "NM",
			"DEPSUMBR":       float64(100000000),
			"SIMS_LATITUDE":  float64(30.267),
			"SIMS_LONGITUDE": float64(-97.743),
		},
	}
	body := fdicAPIResponse(1, records)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FDICBranches{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFDICBranches_APIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FDICBranches{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "probe")
}

func TestFDICBranches_EmptyResponse(t *testing.T) {
	body := fdicAPIResponse(0, nil)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FDICBranches{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFDICBranches_ProbeYear_Non200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	s := &FDICBranches{}
	total, err := s.probeYear(context.Background(), srv.URL, 2024)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 503")
	assert.Equal(t, 0, total)
}

func TestFDICBranches_ProbeYear_DecodeError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not json"))
	}))
	defer srv.Close()

	s := &FDICBranches{}
	total, err := s.probeYear(context.Background(), srv.URL, 2024)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode probe response")
	assert.Equal(t, 0, total)
}

func TestFDICBranches_ProbeYear_RequestBuildError(t *testing.T) {
	s := &FDICBranches{}
	// Invalid URL triggers request build error.
	total, err := s.probeYear(context.Background(), "://bad url", 2024)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "build probe request")
	assert.Equal(t, 0, total)
}

func TestFDICBranches_ProbeYear_ClientError(t *testing.T) {
	s := &FDICBranches{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	total, err := s.probeYear(ctx, "http://127.0.0.1:1", 2024)
	require.Error(t, err)
	assert.Equal(t, 0, total)
}

func TestFDICBranches_FetchPage_Non200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	s := &FDICBranches{}
	records, err := s.fetchPage(context.Background(), srv.URL, 2024, 100, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 502")
	assert.Nil(t, records)
}

func TestFDICBranches_FetchPage_DecodeError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("{invalid json"))
	}))
	defer srv.Close()

	s := &FDICBranches{}
	records, err := s.fetchPage(context.Background(), srv.URL, 2024, 100, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode page response")
	assert.Nil(t, records)
}

func TestFDICBranches_FetchPage_RequestBuildError(t *testing.T) {
	s := &FDICBranches{}
	records, err := s.fetchPage(context.Background(), "://bad url", 2024, 100, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "build page request")
	assert.Nil(t, records)
}

func TestFDICBranches_FetchPage_ClientError(t *testing.T) {
	s := &FDICBranches{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	records, err := s.fetchPage(ctx, "http://127.0.0.1:1", 2024, 100, 0)
	require.Error(t, err)
	assert.Nil(t, records)
}

func TestFDICBranches_FetchPage_Success(t *testing.T) {
	body := fdicAPIResponse(1, []map[string]any{
		{"UNINUMBR": float64(100001), "NAMEFULL": "Test Bank"},
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	s := &FDICBranches{}
	records, err := s.fetchPage(context.Background(), srv.URL, 2024, 100, 0)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, "Test Bank", records[0].Data["NAMEFULL"])
}

func TestFdicString_JSONNumber(t *testing.T) {
	data := map[string]any{"ID": json.Number("12345")}
	assert.Equal(t, "12345", fdicString(data, "ID"))
}

func TestFdicString_Default(t *testing.T) {
	data := map[string]any{"VAL": true}
	assert.Equal(t, "true", fdicString(data, "VAL"))
}

func TestFdicString_FloatDecimal(t *testing.T) {
	// Non-integer float64 should format with decimal digits.
	data := map[string]any{"VAL": 3.14}
	assert.Equal(t, "3.14", fdicString(data, "VAL"))
}

func TestFdicFloat64_JSONNumber(t *testing.T) {
	data := map[string]any{"LAT": json.Number("30.267")}
	assert.InDelta(t, 30.267, fdicFloat64(data, "LAT"), 0.001)
}

func TestFdicFloat64_Default(t *testing.T) {
	data := map[string]any{"LAT": "not a number"}
	assert.Equal(t, float64(0), fdicFloat64(data, "LAT"))
}

func TestFdicFloat64_NilValue(t *testing.T) {
	data := map[string]any{"LAT": nil}
	assert.Equal(t, float64(0), fdicFloat64(data, "LAT"))
}

func TestFdicFloat64_MissingKey(t *testing.T) {
	data := map[string]any{}
	assert.Equal(t, float64(0), fdicFloat64(data, "LAT"))
}

func TestFDICBranches_UpsertError(t *testing.T) {
	records := []map[string]any{
		{
			"UNINUMBR":       float64(100001),
			"NAMEFULL":       "Bank A",
			"BKCLASS":        "NM",
			"DEPSUMBR":       float64(100000000),
			"SIMS_LATITUDE":  float64(30.267),
			"SIMS_LONGITUDE": float64(-97.743),
		},
	}
	body := fdicAPIResponse(1, records)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin().WillReturnError(assert.AnError)

	s := &FDICBranches{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestFDICBranches_FetchPageError(t *testing.T) {
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		if callCount == 1 {
			// Probe succeeds.
			_, _ = w.Write(fdicAPIResponse(10, nil))
		} else {
			// Page fetch fails.
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FDICBranches{baseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetch page")
}

func TestFDICBranches_Pagination(t *testing.T) {
	rec1 := map[string]any{
		"UNINUMBR":       float64(100001),
		"NAMEFULL":       "Bank A",
		"BKCLASS":        "NM",
		"DEPSUMBR":       float64(100000000),
		"SIMS_LATITUDE":  float64(30.267),
		"SIMS_LONGITUDE": float64(-97.743),
	}
	rec2 := map[string]any{
		"UNINUMBR":       float64(100002),
		"NAMEFULL":       "Bank B",
		"BKCLASS":        "SM",
		"DEPSUMBR":       float64(200000000),
		"SIMS_LATITUDE":  float64(32.776),
		"SIMS_LONGITUDE": float64(-96.797),
	}

	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		q := r.URL.Query()
		offset := q.Get("offset")
		limit := q.Get("limit")

		// Probe call (limit=1).
		if limit == "1" {
			_, _ = w.Write(fdicAPIResponse(2, []map[string]any{rec1}))
			return
		}

		// Paginated data calls with limit=1 to test pagination.
		switch offset {
		case "0":
			_, _ = w.Write(fdicAPIResponse(2, []map[string]any{rec1}))
		case "1":
			_, _ = w.Write(fdicAPIResponse(2, []map[string]any{rec2}))
		default:
			_, _ = w.Write(fdicAPIResponse(2, nil))
		}
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectBulkUpsert(mock, 2)

	s := &FDICBranches{baseURL: srv.URL, pageLimit: 1}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	// Probe (1) + page 0 (1) + page 1 (1) = 3 calls.
	assert.Equal(t, 3, callCount)
	require.NoError(t, mock.ExpectationsWereMet())
}
