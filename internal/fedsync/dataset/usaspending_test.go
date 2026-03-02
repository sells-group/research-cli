package dataset

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	fetchermocks "github.com/sells-group/research-cli/internal/fetcher/mocks"
)

func TestUSAspending_Metadata(t *testing.T) {
	ds := &USAspending{}
	assert.Equal(t, "usaspending", ds.Name())
	assert.Equal(t, "fed_data.usaspending_awards", ds.Table())
	assert.Equal(t, Phase1, ds.Phase())
	assert.Equal(t, Monthly, ds.Cadence())
}

func TestUSAspending_ShouldRun(t *testing.T) {
	ds := &USAspending{}

	// Never synced -> should run.
	now := time.Date(2024, time.April, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, nil))

	// Synced this month -> should not run.
	thisMonth := time.Date(2024, time.April, 2, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(now, &thisMonth))

	// Synced last month -> should run.
	lastMonth := time.Date(2024, time.March, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, &lastMonth))
}

func TestUSAspending_ClassifyAwardType(t *testing.T) {
	tests := []struct {
		code     string
		expected string
	}{
		{"A", "contract"}, {"B", "contract"}, {"C", "contract"}, {"D", "contract"}, {"IDV", "contract"},
		{"02", "grant"}, {"03", "grant"}, {"04", "grant"}, {"05", "grant"},
		{"06", "direct_payment"}, {"10", "direct_payment"},
		{"07", "loan"}, {"08", "loan"},
		{"09", "other"}, {"11", "other"},
		{"", "other"}, {"ZZ", "other"},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("code_%s", tt.code), func(t *testing.T) {
			assert.Equal(t, tt.expected, classifyAwardType(tt.code))
		})
	}
}

const usaspendingCSVHeader = "contract_award_unique_key,assistance_award_unique_key,award_type_code,award_type,piid,fain,uri,awarding_agency_code,awarding_agency_name,awarding_sub_agency_code,awarding_sub_agency_name,funding_agency_code,funding_agency_name,recipient_uei,recipient_duns,recipient_name,parent_uei,parent_recipient_name,recipient_address_line_1,recipient_city_name,recipient_state_code,recipient_zip_4_code,recipient_country_code,total_obligated_amount,total_outlayed_amount,naics_code,naics_description,product_or_service_code,cfda_number,cfda_title,award_base_action_date,award_latest_action_date,period_of_performance_start_date,period_of_performance_current_end_date,last_modified_date,primary_place_of_performance_city_name,primary_place_of_performance_state_code,primary_place_of_performance_zip_4,primary_place_of_performance_country_code,award_description,usaspending_permalink\n"

const usaspendingContractRow = "CONT_AWD_0001_4700,,A,BPA Call,0001,,,047,GENERAL SERVICES ADMINISTRATION,4700,GSA - FEDERAL ACQUISITION SERVICE,047,GENERAL SERVICES ADMINISTRATION,JQKDEL9XJH45,832025241,ACME CONSULTING LLC,MNLQY117AB37,ACME HOLDINGS INC,100 MAIN ST,WASHINGTON,DC,20001,USA,250000.00,180000.00,541611,Administrative Management Consulting Services,R499,,,2023-01-15,2024-06-30,2023-01-15,2025-01-14,2024-06-30,WASHINGTON,DC,20001,USA,Management consulting services,https://www.usaspending.gov/award/CONT_AWD_0001\n"

const usaspendingGrantRow = ",ASST_NON_2024_GRANT_001,02,Grant,,,ASST_NON_2024_GRANT_001,075,DEPARTMENT OF HEALTH AND HUMAN SERVICES,7529,NATIONAL INSTITUTES OF HEALTH,075,DEPARTMENT OF HEALTH AND HUMAN SERVICES,RSCH7890KL12,555123456,RESEARCH UNIVERSITY INC,,,456 UNIVERSITY BLVD,BETHESDA,MD,20892,USA,1500000.00,1200000.00,611310,Colleges and Universities,,93.855,Allergy and Infectious Diseases Research,2024-07-01,2024-09-15,2024-07-01,2027-06-30,2024-09-15,BETHESDA,MD,20892,USA,Biomedical research grant,https://www.usaspending.gov/award/ASST_NON_2024\n"

const usaspendingLoanRow = ",ASST_NON_2024_LOAN_001,07,Direct Loan,,,,086,DEPARTMENT OF HOUSING AND URBAN DEVELOPMENT,8600,HOUSING PROGRAMS,086,DEPARTMENT OF HOUSING AND URBAN DEVELOPMENT,HOUS4567MN89,,COMMUNITY HOUSING CORP,,,789 HOUSING LN,DENVER,CO,80201,USA,5000000.00,4500000.00,531110,Lessors of Residential Buildings,,14.871,Section 8 Housing Choice Vouchers,2024-01-10,2024-06-01,2024-01-10,2054-01-09,2024-06-01,DENVER,CO,80201,USA,Affordable housing loan,https://www.usaspending.gov/award/ASST_NON_LOAN\n"

func TestUSAspending_ParseCSV_Contracts(t *testing.T) {
	csvContent := usaspendingCSVHeader + usaspendingContractRow

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsert(pool, "fed_data.usaspending_awards", usaspendingColumns, 1)

	ds := &USAspending{}
	rows, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent))
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestUSAspending_ParseCSV_Assistance(t *testing.T) {
	csvContent := usaspendingCSVHeader + usaspendingGrantRow

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsert(pool, "fed_data.usaspending_awards", usaspendingColumns, 1)

	ds := &USAspending{}
	rows, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent))
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestUSAspending_ParseCSV_MixedTypes(t *testing.T) {
	csvContent := usaspendingCSVHeader +
		usaspendingContractRow +
		usaspendingGrantRow +
		usaspendingLoanRow

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsert(pool, "fed_data.usaspending_awards", usaspendingColumns, 3)

	ds := &USAspending{}
	rows, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent))
	require.NoError(t, err)
	assert.Equal(t, int64(3), rows)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestUSAspending_ParseCSV_SkipsEmptyID(t *testing.T) {
	// Row with neither contract_award_unique_key nor assistance_award_unique_key.
	emptyIDRow := ",,A,BPA Call,0001,,,047,GSA,4700,GSA - FAS,047,GSA,UEI123,DUNS456,ACME LLC,,,100 MAIN ST,WASHINGTON,DC,20001,USA,250000.00,180000.00,541611,Consulting,R499,,,2023-01-15,2024-06-30,2023-01-15,2025-01-14,2024-06-30,WASHINGTON,DC,20001,USA,Consulting,https://example.com\n"
	csvContent := usaspendingCSVHeader + emptyIDRow + usaspendingContractRow

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsert(pool, "fed_data.usaspending_awards", usaspendingColumns, 1)

	ds := &USAspending{}
	rows, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent))
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows) // only the valid row
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestUSAspending_ParseCSV_EmptyCSV(t *testing.T) {
	csvContent := usaspendingCSVHeader

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ds := &USAspending{}
	rows, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent))
	require.NoError(t, err)
	assert.Equal(t, int64(0), rows)
}

func TestUSAspending_ParseCSV_InvalidHeader(t *testing.T) {
	csvContent := "bogus,columns,nothing\n1,2,3\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ds := &USAspending{}
	_, err = ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no award key column found")
}

func TestUSAspending_ParseCSV_EmptyReader(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ds := &USAspending{}
	_, err = ds.parseCSV(context.Background(), pool, strings.NewReader(""))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "usaspending: read CSV header")
}

func TestUSAspending_ParseCSV_UpsertError(t *testing.T) {
	csvContent := usaspendingCSVHeader + usaspendingContractRow

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	// Make the BulkUpsert fail at the Begin step.
	pool.ExpectBegin().WillReturnError(assert.AnError)

	ds := &USAspending{}
	_, err = ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "usaspending: bulk upsert")
}

func TestUSAspending_ParseCSV_FinalBatchUpsertError(t *testing.T) {
	csvContent := usaspendingCSVHeader +
		usaspendingContractRow +
		usaspendingGrantRow

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	// Final batch upsert fails.
	pool.ExpectBegin().WillReturnError(assert.AnError)

	ds := &USAspending{}
	_, err = ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "usaspending: bulk upsert final batch")
}

func TestUSAspending_RequestBulkDownload(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		var req bulkDownloadRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Equal(t, "csv", req.FileFormat)
		assert.Equal(t, "last_modified_date", req.Filters.DateType)
		assert.Equal(t, "2024-01-01", req.Filters.DateRange.StartDate)

		resp := bulkDownloadResponse{
			Status:   "started",
			FileName: "test_file.zip",
			FileURL:  "",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	ds := &USAspending{
		httpClient:      srv.Client(),
		bulkDownloadURL: srv.URL,
	}

	resp, err := ds.requestBulkDownload(context.Background(), "2024-01-01", "2024-02-01")
	require.NoError(t, err)
	assert.Equal(t, "started", resp.Status)
	assert.Equal(t, "test_file.zip", resp.FileName)
}

func TestUSAspending_RequestBulkDownload_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	ds := &USAspending{
		httpClient:      srv.Client(),
		bulkDownloadURL: srv.URL,
	}

	_, err := ds.requestBulkDownload(context.Background(), "2024-01-01", "2024-02-01")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 500")
}

func TestUSAspending_PollStatus_Finished(t *testing.T) {
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "test_file.zip", r.URL.Query().Get("file_name"))
		callCount++

		status := bulkDownloadStatus{
			Status:  "finished",
			FileURL: "https://files.usaspending.gov/test_file.zip",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
	}))
	defer srv.Close()

	ds := &USAspending{
		httpClient:      srv.Client(),
		pollInterval:    1 * time.Millisecond,
		maxPollAttempts: 5,
		statusURL:       srv.URL,
	}

	result, err := ds.pollStatus(context.Background(), "test_file.zip")
	require.NoError(t, err)
	assert.Equal(t, "finished", result.Status)
	assert.Equal(t, "https://files.usaspending.gov/test_file.zip", result.FileURL)
	assert.Equal(t, 1, callCount)
}

func TestUSAspending_PollStatus_Failed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		status := bulkDownloadStatus{
			Status:  "failed",
			Message: "internal error",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
	}))
	defer srv.Close()

	ds := &USAspending{
		httpClient:      srv.Client(),
		pollInterval:    1 * time.Millisecond,
		maxPollAttempts: 5,
		statusURL:       srv.URL,
	}

	_, err := ds.pollStatus(context.Background(), "test_file.zip")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bulk download failed")
	assert.Contains(t, err.Error(), "internal error")
}

func TestUSAspending_PollStatus_Timeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		status := bulkDownloadStatus{Status: "running"}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
	}))
	defer srv.Close()

	ds := &USAspending{
		httpClient:      srv.Client(),
		pollInterval:    1 * time.Millisecond,
		maxPollAttempts: 3,
		statusURL:       srv.URL,
	}

	_, err := ds.pollStatus(context.Background(), "test_file.zip")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "polling timed out")
}

func TestUSAspending_PollStatus_ContextCanceled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		status := bulkDownloadStatus{Status: "running"}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	ds := &USAspending{
		httpClient:      srv.Client(),
		pollInterval:    1 * time.Millisecond,
		maxPollAttempts: 100,
		statusURL:       srv.URL,
	}

	_, err := ds.pollStatus(ctx, "test_file.zip")
	require.Error(t, err)
}

func TestUSAspending_MapRow_Contract(t *testing.T) {
	header := strings.Split(strings.TrimSuffix(usaspendingCSVHeader, "\n"), ",")
	colIdx := mapColumnsNormalized(header)

	record := strings.Split(strings.TrimSuffix(usaspendingContractRow, "\n"), ",")

	ds := &USAspending{}
	row := ds.mapRow(record, colIdx)
	require.NotNil(t, row)
	assert.Len(t, row, len(usaspendingColumns))

	// award_id
	assert.Equal(t, "CONT_AWD_0001_4700", row[0])
	// award_type (classified)
	assert.Equal(t, "contract", row[1])
	// recipient_name
	assert.Equal(t, "ACME CONSULTING LLC", row[14])
	// total_obligated_amount
	assert.Equal(t, 250000.0, row[22])
}

func TestUSAspending_MapRow_Grant(t *testing.T) {
	header := strings.Split(strings.TrimSuffix(usaspendingCSVHeader, "\n"), ",")
	colIdx := mapColumnsNormalized(header)

	record := strings.Split(strings.TrimSuffix(usaspendingGrantRow, "\n"), ",")

	ds := &USAspending{}
	row := ds.mapRow(record, colIdx)
	require.NotNil(t, row)

	// award_id comes from assistance_award_unique_key
	assert.Equal(t, "ASST_NON_2024_GRANT_001", row[0])
	// award_type
	assert.Equal(t, "grant", row[1])
	// recipient_name
	assert.Equal(t, "RESEARCH UNIVERSITY INC", row[14])
}

func TestUSAspending_MapRow_EmptyID(t *testing.T) {
	header := strings.Split(strings.TrimSuffix(usaspendingCSVHeader, "\n"), ",")
	colIdx := mapColumnsNormalized(header)

	// Both key columns empty.
	fields := make([]string, len(header))
	record := fields

	ds := &USAspending{}
	row := ds.mapRow(record, colIdx)
	assert.Nil(t, row)
}

func TestUSAspending_MapRow_EdgeCases(t *testing.T) {
	header := strings.Split(strings.TrimSuffix(usaspendingCSVHeader, "\n"), ",")
	colIdx := mapColumnsNormalized(header)

	// Build a row with edge-case values: invalid dates, "0" amount, long state code.
	fields := make([]string, len(header))
	fields[colIdx["contract_award_unique_key"]] = "EDGE_CASE_001"
	fields[colIdx["award_type_code"]] = "A"
	fields[colIdx["total_obligated_amount"]] = "0"
	fields[colIdx["total_outlayed_amount"]] = "not_a_number"
	fields[colIdx["award_base_action_date"]] = "bad-date"
	fields[colIdx["recipient_state_code"]] = "CALIFORNIA" // >2 chars, should truncate to "CA"
	fields[colIdx["primary_place_of_performance_state_code"]] = "NYX"

	ds := &USAspending{}
	row := ds.mapRow(fields, colIdx)
	require.NotNil(t, row)

	// award_id
	assert.Equal(t, "EDGE_CASE_001", row[0])
	// total_obligated_amount: "0" should be recognized as valid zero
	assert.Equal(t, 0.0, row[22])
	// total_outlayed_amount: "not_a_number" should be nil
	assert.Nil(t, row[23])
	// award_base_action_date: "bad-date" should be nil
	assert.Nil(t, row[29])
	// recipient_state_code: truncated to 2 chars
	assert.Equal(t, "CA", row[19])
	// pop_state: truncated to 2 chars
	assert.Equal(t, "NY", row[35])
}

func TestUSAspending_GetterDefaults(t *testing.T) {
	ds := &USAspending{}

	assert.Equal(t, http.DefaultClient, ds.client())
	assert.Equal(t, usaspendingPollInterval, ds.getPollInterval())
	assert.Equal(t, usaspendingMaxPollAttempts, ds.getMaxPollAttempts())
	assert.Equal(t, usaspendingBulkDownloadURL, ds.getBulkDownloadURL())
	assert.Equal(t, usaspendingStatusURL, ds.getStatusURL())
}

func TestUSAspending_RequestBulkDownload_DecodeError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("not json"))
	}))
	defer srv.Close()

	ds := &USAspending{
		httpClient:      srv.Client(),
		bulkDownloadURL: srv.URL,
	}

	_, err := ds.requestBulkDownload(context.Background(), "2024-01-01", "2024-02-01")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode response")
}

func TestUSAspending_DownloadAndProcess_Success(t *testing.T) {
	dir := t.TempDir()
	csvContent := usaspendingCSVHeader + usaspendingContractRow

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsert(pool, "fed_data.usaspending_awards", usaspendingColumns, 1)

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, "awards.csv", csvContent))

	ds := &USAspending{}
	rows, err := ds.downloadAndProcess(context.Background(), pool, f, dir, "https://example.com/test.zip", "test_usa")
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestUSAspending_DownloadAndProcess_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), assert.AnError)

	ds := &USAspending{}
	_, err = ds.downloadAndProcess(context.Background(), pool, f, t.TempDir(), "https://example.com/test.zip", "test_usa")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "download ZIP")
}

func TestUSAspending_DownloadAndProcess_SkipsNonCSV(t *testing.T) {
	dir := t.TempDir()

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	// ZIP with a non-CSV file only.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, "readme.txt", "This is not CSV data"))

	ds := &USAspending{}
	rows, err := ds.downloadAndProcess(context.Background(), pool, f, dir, "https://example.com/test.zip", "test_usa")
	require.NoError(t, err)
	assert.Equal(t, int64(0), rows)
}

// usaspendingTestServer creates mock HTTP server for bulk download + status endpoints.
func usaspendingTestServer(t *testing.T, fileURL string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/bulk_download/", func(w http.ResponseWriter, _ *http.Request) {
		resp := bulkDownloadResponse{
			Status:   "started",
			FileName: "test_file.zip",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/status/", func(w http.ResponseWriter, _ *http.Request) {
		status := bulkDownloadStatus{
			Status:  "finished",
			FileURL: fileURL,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
	})

	return httptest.NewServer(mux)
}

func TestUSAspending_Sync_Success(t *testing.T) {
	dir := t.TempDir()
	csvContent := usaspendingCSVHeader + usaspendingContractRow

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsert(pool, "fed_data.usaspending_awards", usaspendingColumns, 1)

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, "awards.csv", csvContent))

	srv := usaspendingTestServer(t, "https://example.com/test.zip")
	defer srv.Close()

	ds := &USAspending{
		httpClient:      srv.Client(),
		pollInterval:    1 * time.Millisecond,
		maxPollAttempts: 5,
		bulkDownloadURL: srv.URL + "/bulk_download/",
		statusURL:       srv.URL + "/status/",
	}

	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
	assert.Contains(t, result.Metadata["date_range"], "to")
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestUSAspending_Sync_RequestError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	ds := &USAspending{
		httpClient:      srv.Client(),
		bulkDownloadURL: srv.URL,
	}

	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 500")
}

func TestUSAspending_Sync_PollError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/bulk_download/", func(w http.ResponseWriter, _ *http.Request) {
		resp := bulkDownloadResponse{Status: "started", FileName: "test.zip"}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/status/", func(w http.ResponseWriter, _ *http.Request) {
		status := bulkDownloadStatus{Status: "failed", Message: "server error"}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	ds := &USAspending{
		httpClient:      srv.Client(),
		pollInterval:    1 * time.Millisecond,
		maxPollAttempts: 2,
		bulkDownloadURL: srv.URL + "/bulk_download/",
		statusURL:       srv.URL + "/status/",
	}

	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bulk download failed")
}

func TestUSAspending_SyncFull_Success(t *testing.T) {
	dir := t.TempDir()
	csvContent := usaspendingCSVHeader + usaspendingContractRow

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	// FY2017 through current year.
	currentYear := time.Now().Year()
	fyCount := currentYear - 2017 + 1

	for range fyCount {
		expectBulkUpsert(pool, "fed_data.usaspending_awards", usaspendingColumns, 1)
	}

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, "awards.csv", csvContent)).
		Times(fyCount)

	srv := usaspendingTestServer(t, "https://example.com/test.zip")
	defer srv.Close()

	ds := &USAspending{
		httpClient:      srv.Client(),
		pollInterval:    1 * time.Millisecond,
		maxPollAttempts: 5,
		bulkDownloadURL: srv.URL + "/bulk_download/",
		statusURL:       srv.URL + "/status/",
	}

	result, err := ds.SyncFull(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(fyCount), result.RowsSynced)
	assert.Equal(t, "full", result.Metadata["method"])
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestUSAspending_SyncFull_RequestError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	ds := &USAspending{
		httpClient:      srv.Client(),
		bulkDownloadURL: srv.URL,
	}

	_, err = ds.SyncFull(context.Background(), pool, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "FY2017 request")
}

func TestUSAspending_SyncFull_PollError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/bulk_download/", func(w http.ResponseWriter, _ *http.Request) {
		resp := bulkDownloadResponse{Status: "started", FileName: "test.zip"}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/status/", func(w http.ResponseWriter, _ *http.Request) {
		status := bulkDownloadStatus{Status: "failed", Message: "server error"}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	ds := &USAspending{
		httpClient:      srv.Client(),
		pollInterval:    1 * time.Millisecond,
		maxPollAttempts: 2,
		bulkDownloadURL: srv.URL + "/bulk_download/",
		statusURL:       srv.URL + "/status/",
	}

	_, err = ds.SyncFull(context.Background(), pool, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "FY2017 poll")
}

func TestUSAspending_SyncFull_ProcessError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), assert.AnError)

	srv := usaspendingTestServer(t, "https://example.com/test.zip")
	defer srv.Close()

	ds := &USAspending{
		httpClient:      srv.Client(),
		pollInterval:    1 * time.Millisecond,
		maxPollAttempts: 5,
		bulkDownloadURL: srv.URL + "/bulk_download/",
		statusURL:       srv.URL + "/status/",
	}

	_, err = ds.SyncFull(context.Background(), pool, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "FY2017 process")
}

func TestUSAspending_Sync_DownloadProcessError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), assert.AnError)

	srv := usaspendingTestServer(t, "https://example.com/test.zip")
	defer srv.Close()

	ds := &USAspending{
		httpClient:      srv.Client(),
		pollInterval:    1 * time.Millisecond,
		maxPollAttempts: 5,
		bulkDownloadURL: srv.URL + "/bulk_download/",
		statusURL:       srv.URL + "/status/",
	}

	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "download ZIP")
}

func TestUSAspending_DownloadAndProcess_CSVParseError(t *testing.T) {
	dir := t.TempDir()
	// ZIP with a CSV that has invalid header (no award key column).
	badCSV := "bogus,columns,nothing\n1,2,3\n"

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, "awards.csv", badCSV))

	ds := &USAspending{}
	_, err = ds.downloadAndProcess(context.Background(), pool, f, dir, "https://example.com/test.zip", "test_usa")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse awards.csv")
}

func TestUSAspending_ParseCSV_MalformedRowSkipped(t *testing.T) {
	// Row with wrong number of fields is silently skipped.
	malformedRow := "CONT_001,,A,Extra,field,too,many\n"
	csvContent := usaspendingCSVHeader + malformedRow + usaspendingContractRow

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	expectBulkUpsert(pool, "fed_data.usaspending_awards", usaspendingColumns, 1)

	ds := &USAspending{}
	rows, err := ds.parseCSV(context.Background(), pool, strings.NewReader(csvContent))
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows) // only the valid contract row
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestUSAspending_PollStatus_DecodeError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("not json"))
	}))
	defer srv.Close()

	ds := &USAspending{
		httpClient:      srv.Client(),
		pollInterval:    1 * time.Millisecond,
		maxPollAttempts: 2,
		statusURL:       srv.URL,
	}

	_, err := ds.pollStatus(context.Background(), "test_file.zip")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode status")
}

func TestUSAspending_RequestBulkDownload_ConnectionError(t *testing.T) {
	// Use a server that is immediately closed to cause a connection error.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Close()

	ds := &USAspending{
		httpClient:      srv.Client(),
		bulkDownloadURL: srv.URL,
	}

	_, err := ds.requestBulkDownload(context.Background(), "2024-01-01", "2024-02-01")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "POST bulk download")
}

func TestUSAspending_PollStatus_ConnectionError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Close()

	ds := &USAspending{
		httpClient:      srv.Client(),
		pollInterval:    1 * time.Millisecond,
		maxPollAttempts: 2,
		statusURL:       srv.URL,
	}

	_, err := ds.pollStatus(context.Background(), "test_file.zip")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "GET status")
}

func TestUSAspending_MapRow_EmptyNumericAndDate(t *testing.T) {
	header := strings.Split(strings.TrimSuffix(usaspendingCSVHeader, "\n"), ",")
	colIdx := mapColumnsNormalized(header)

	// Build a minimal row with only award_id set — all other fields empty.
	fields := make([]string, len(header))
	fields[colIdx["contract_award_unique_key"]] = "MINIMAL_001"

	ds := &USAspending{}
	row := ds.mapRow(fields, colIdx)
	require.NotNil(t, row)

	assert.Equal(t, "MINIMAL_001", row[0])
	// Numeric columns should be nil for empty values.
	assert.Nil(t, row[22]) // total_obligated_amount
	assert.Nil(t, row[23]) // total_outlayed_amount
	// Date columns should be nil for empty values.
	assert.Nil(t, row[29]) // award_base_action_date
	assert.Nil(t, row[30]) // award_latest_action_date
	assert.Nil(t, row[31]) // period_of_perf_start
	assert.Nil(t, row[32]) // period_of_perf_end
	assert.Nil(t, row[33]) // last_modified_date
	// Text columns should be nil.
	assert.Nil(t, row[14]) // recipient_name
	// State codes should be nil.
	assert.Nil(t, row[19]) // recipient_state
}
