package dataset

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	fetchermocks "github.com/sells-group/research-cli/internal/fetcher/mocks"
)

// createTestZIP creates a ZIP file at zipPath containing a single file with the given content.
func createTestZIP(t *testing.T, zipPath, innerName, content string) {
	t.Helper()
	f, err := os.Create(zipPath)
	require.NoError(t, err)
	defer f.Close() //nolint:errcheck

	w := zip.NewWriter(f)
	fw, err := w.Create(innerName)
	require.NoError(t, err)
	_, err = fw.Write([]byte(content))
	require.NoError(t, err)
	require.NoError(t, w.Close())
}

// mockDownloadToFileZIP returns a RunAndReturn func that writes a ZIP to the requested path.
func mockDownloadToFileZIP(t *testing.T, innerName, content string) func(context.Context, string, string) (int64, error) {
	t.Helper()
	return func(_ context.Context, _ string, path string) (int64, error) {
		createTestZIP(t, path, innerName, content)
		return int64(len(content)), nil
	}
}

// expectBulkUpsert sets up pgxmock expectations for a db.BulkUpsert call.
// BulkUpsert does: Begin -> CREATE TEMP TABLE -> COPY -> DELETE (dedup) -> INSERT ON CONFLICT -> Commit.
func expectBulkUpsert(m pgxmock.PgxPoolIface, table string, cols []string, n int64) {
	tempTable := fmt.Sprintf("_tmp_upsert_%s", strings.ReplaceAll(table, ".", "_"))
	m.ExpectBegin()
	m.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	m.ExpectCopyFrom(pgx.Identifier{tempTable}, cols).WillReturnResult(n)
	m.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	m.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", n))
	m.ExpectCommit()
}

// jsonBody returns an io.ReadCloser with JSON-encoded data.
func jsonBody(t *testing.T, v any) io.ReadCloser {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	return io.NopCloser(strings.NewReader(string(data)))
}

// foiaMetadataBody builds the real FOIA metadata API JSON format from a foiaReportsMetadata.
// The real API nests files under year keys: {"advFilingData": {"2025": {"files": [...]}, ...}}.
func foiaMetadataBody(t *testing.T, meta foiaReportsMetadata) io.ReadCloser {
	t.Helper()
	nestSection := func(entries []foiaFileEntry) map[string]any {
		section := map[string]any{}
		byYear := map[string][]foiaFileEntry{}
		for _, e := range entries {
			yr := e.Year
			if yr == "" {
				yr = "2026"
			}
			byYear[yr] = append(byYear[yr], e)
		}
		for yr, files := range byYear {
			section[yr] = map[string]any{"files": files}
		}
		return section
	}
	raw := map[string]any{
		"advFilingData":  nestSection(meta.ADVFilingData),
		"advBrochures":   nestSection(meta.ADVBrochures),
		"advFirmCRSDocs": nestSection(meta.ADVFirmCRSDocs),
		"advFirmCRS":     nestSection(meta.ADVFirmCRS),
	}
	data, err := json.Marshal(raw)
	require.NoError(t, err)
	return io.NopCloser(strings.NewReader(string(data)))
}

// --- NES ---

var nesCols = []string{"year", "naics", "geo_id", "firmpdemp", "rcppdemp", "payann_pct"}

func TestNES_Sync(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	censusResp := [][]string{
		{"NAICS2017", "GEO_ID", "FIRMPDEMP", "RCPPDEMP", "PAYANN_PCT", "us"},
		{"5200", "0100000US", "1000", "500000", "12.5", "1"},
		{"5300", "0100000US", "800", "300000", "10.2", "1"},
	}

	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "nonemp")
	})).Return(jsonBody(t, censusResp), nil)

	expectBulkUpsert(pool, "fed_data.nes_data", nesCols, 2)

	ds := &NES{cfg: &config.Config{Fedsync: config.FedsyncConfig{CensusKey: "test-key"}}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestNES_Sync_EmptyResponse(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	censusResp := [][]string{
		{"NAICS2017", "GEO_ID", "FIRMPDEMP", "RCPPDEMP", "PAYANN_PCT", "us"},
	}

	f.EXPECT().Download(mock.Anything, mock.Anything).Return(jsonBody(t, censusResp), nil)

	ds := &NES{cfg: &config.Config{Fedsync: config.FedsyncConfig{CensusKey: "test-key"}}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestNES_Sync_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().Download(mock.Anything, mock.Anything).Return(nil, errors.New("network error"))

	ds := &NES{cfg: &config.Config{Fedsync: config.FedsyncConfig{CensusKey: "test-key"}}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

// --- ASM ---

var asmCols = []string{"year", "naics", "geo_id", "valadd", "totval_ship", "prodwrkrs"}

func TestASM_Sync(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	censusResp := [][]string{
		{"NAICS2017", "GEO_ID", "VALADD", "TOTVAL_SHIP", "PRODWRKRS", "us"},
		{"5200", "0100000US", "100000", "200000", "5000", "1"},
	}

	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "asm/product")
	})).Return(jsonBody(t, censusResp), nil)

	expectBulkUpsert(pool, "fed_data.asm_data", asmCols, 1)

	ds := &ASM{cfg: &config.Config{Fedsync: config.FedsyncConfig{CensusKey: "test-key"}}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

// --- ABS ---

var absCols = []string{"year", "naics", "geo_id", "firmpdemp", "rcppdemp", "payann"}

func TestABS_Sync(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	censusResp := [][]string{
		{"NAICS2017", "GEO_ID", "FIRMPDEMP", "RCPPDEMP", "PAYANN", "us"},
		{"5400", "0100000US", "500", "100000", "50000", "1"},
		{"5300", "0100000US", "300", "80000", "40000", "1"},
	}

	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "abscs")
	})).Return(jsonBody(t, censusResp), nil)

	expectBulkUpsert(pool, "fed_data.abs_data", absCols, 2)

	ds := &ABS{cfg: &config.Config{Fedsync: config.FedsyncConfig{CensusKey: "test-key"}}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

// --- FPDS ---

var fpdsCols = []string{
	"contract_id", "piid", "agency_id", "agency_name",
	"vendor_name", "vendor_duns", "vendor_uei",
	"vendor_city", "vendor_state", "vendor_zip",
	"naics", "psc", "date_signed", "dollars_obligated", "description",
}

func TestFPDS_Sync(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	samResp := samResponse{
		TotalRecords: 1,
		OpportunitiesData: []samOpportunity{
			{
				NoticeID:   "CONTRACT-001",
				PIID:       "SOL-001",
				Agency:     "Treasury",
				AgencyCode: "2000",
				Title:      "Financial advisory",
				NAICS:      "523110",
				PSC:        "R408",
				PostedDate: "2024-06-01",
				Award: &samAward{
					Amount: 100000,
					Date:   "2024-06-10",
					Awardee: &samAwardee{
						Name: "Acme LLC",
						UEI:  "ABC123",
						DUNS: "9876543",
						Location: &samLocation{
							City:  "NYC",
							State: "NY",
							Zip:   "10001",
						},
					},
				},
			},
		},
	}

	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "sam.gov")
	})).Return(jsonBody(t, samResp), nil)

	expectBulkUpsert(pool, "fed_data.fpds_contracts", fpdsCols, 1)

	ds := &FPDS{cfg: &config.Config{Fedsync: config.FedsyncConfig{SAMKey: "test-key"}}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestFPDS_Sync_NoAPIKey(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	ds := &FPDS{cfg: &config.Config{}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SAM API key")
}

// --- ECI ---

var eciCols = []string{"series_id", "year", "period", "value"}

func TestECI_Sync(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	blsResp := blsSeriesResponse{
		Status: "REQUEST_SUCCEEDED",
	}
	blsResp.Results.Series = []struct {
		SeriesID string `json:"seriesID"`
		Data     []struct {
			Year   string `json:"year"`
			Period string `json:"period"`
			Value  string `json:"value"`
		} `json:"data"`
	}{
		{
			SeriesID: "CIU1010000000000A",
			Data: []struct {
				Year   string `json:"year"`
				Period string `json:"period"`
				Value  string `json:"value"`
			}{
				{Year: "2024", Period: "Q01", Value: "154.2"},
				{Year: "2024", Period: "Q02", Value: "155.1"},
			},
		},
	}

	// ECI fetches per series (5 total). Return data for first, errors for rest.
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "CIU1010000000000A")
	})).Return(jsonBody(t, blsResp), nil)

	for i := 0; i < 4; i++ {
		f.EXPECT().Download(mock.Anything, mock.Anything).Return(nil, errors.New("skip")).Maybe()
	}

	expectBulkUpsert(pool, "fed_data.eci_data", eciCols, 2)

	ds := &ECI{cfg: &config.Config{Fedsync: config.FedsyncConfig{BLSKey: "test-key"}}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
}

// --- FRED ---

var fredCols = []string{"series_id", "obs_date", "value"}

func TestFRED_Sync(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	fredResp := fredResponse{
		Observations: []struct {
			Date  string `json:"date"`
			Value string `json:"value"`
		}{
			{Date: "2024-06-01", Value: "5.33"},
			{Date: "2024-05-01", Value: "5.33"},
			{Date: "2024-04-01", Value: "."}, // "." should be skipped
		},
	}

	// FRED iterates over 15 series. Return data for first, errors for rest.
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "GDP")
	})).Return(jsonBody(t, fredResp), nil)

	for i := 0; i < 14; i++ {
		f.EXPECT().Download(mock.Anything, mock.Anything).Return(nil, errors.New("skip")).Maybe()
	}

	expectBulkUpsert(pool, "fed_data.fred_series", fredCols, 2)

	ds := &FRED{cfg: &config.Config{Fedsync: config.FedsyncConfig{FREDKey: "test-key"}}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
}

// --- EconCensus ---

var econCensusCols = []string{"year", "geo_id", "naics", "estab", "rcptot", "payann", "emp"}

func TestEconCensus_Sync(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	censusResp := [][]string{
		{"GEO_ID", "NAICS2017", "ESTAB", "RCPTOT", "PAYANN", "EMP", "state"},
		{"0400000US01", "5200", "100", "50000", "20000", "500", "01"},
	}

	// EconCensus fetches for each census year (2017, 2022).
	// Use RunAndReturn to generate a fresh ReadCloser on each call.
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "ecnbasic")
	})).RunAndReturn(func(_ context.Context, _ string) (io.ReadCloser, error) {
		return jsonBody(t, censusResp), nil
	}).Times(2)

	// Two upsert calls (one per year)
	expectBulkUpsert(pool, "fed_data.economic_census", econCensusCols, 1)
	expectBulkUpsert(pool, "fed_data.economic_census", econCensusCols, 1)

	ds := &EconCensus{cfg: &config.Config{Fedsync: config.FedsyncConfig{CensusKey: "test-key"}}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
}

func TestEconCensus_Sync_NoAPIKey(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	ds := &EconCensus{cfg: &config.Config{}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Census API key")
}

// --- FPDS parseResponse edge cases ---

func TestFPDS_ParseResponse_NoAward(t *testing.T) {
	ds := &FPDS{}

	data := []byte(`{
		"totalRecords": 1,
		"opportunitiesData": [{
			"noticeId": "CONTRACT-002",
			"title": "No award contract",
			"naicsCode": "523",
			"classificationCode": "R408"
		}]
	}`)

	rows, hasMore, err := ds.parseResponse(data)
	require.NoError(t, err)
	assert.False(t, hasMore)
	assert.Len(t, rows, 1)
	assert.Equal(t, "", rows[0][4])   // vendor_name
	assert.Equal(t, 0.0, rows[0][13]) // dollars_obligated
}

func TestFPDS_ParseResponse_EmptyNoticeID(t *testing.T) {
	ds := &FPDS{}

	data := []byte(`{
		"totalRecords": 1,
		"opportunitiesData": [{
			"noticeId": "",
			"title": "Missing ID"
		}]
	}`)

	rows, _, err := ds.parseResponse(data)
	require.NoError(t, err)
	assert.Empty(t, rows)
}

func TestFPDS_ParseResponse_InvalidJSON(t *testing.T) {
	ds := &FPDS{}
	_, _, err := ds.parseResponse([]byte("not json"))
	assert.Error(t, err)
}

// --- CPSLAUS ---

var lausCols = []string{"series_id", "year", "period", "value"}

func TestCPSLAUS_Sync(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	blsResp := blsSeriesResponse{Status: "REQUEST_SUCCEEDED"}
	blsResp.Results.Series = []struct {
		SeriesID string `json:"seriesID"`
		Data     []struct {
			Year   string `json:"year"`
			Period string `json:"period"`
			Value  string `json:"value"`
		} `json:"data"`
	}{
		{
			SeriesID: "LASST060000000000003",
			Data: []struct {
				Year   string `json:"year"`
				Period string `json:"period"`
				Value  string `json:"value"`
			}{
				{Year: "2024", Period: "M06", Value: "4.2"},
			},
		},
	}

	// CPSLAUS iterates over 10 series. Return data for first, errors for rest.
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "LASST060000000000003")
	})).Return(jsonBody(t, blsResp), nil)

	for i := 0; i < 9; i++ {
		f.EXPECT().Download(mock.Anything, mock.Anything).Return(nil, errors.New("skip")).Maybe()
	}

	expectBulkUpsert(pool, "fed_data.laus_data", lausCols, 1)

	ds := &CPSLAUS{cfg: &config.Config{Fedsync: config.FedsyncConfig{BLSKey: "test-key"}}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
}

// --- M3 ---

var m3Cols = []string{"category", "data_type", "year", "month", "value"}

func TestM3_Sync(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// M3 now uses a single consolidated API call returning all data types.
	// Census API returns "time" column (not "time_slot_id") when time=from+YYYY is used.
	censusResp := [][]string{
		{"cell_value", "time", "category_code", "data_type_code", "seasonally_adj", "us"},
		{"150000", "2024-06", "MTM", "NO", "yes", "*"},
		{"120000", "2024-05", "MTM", "VS", "yes", "*"},
	}

	f.EXPECT().Download(mock.Anything, mock.Anything).Return(jsonBody(t, censusResp), nil).Once()

	expectBulkUpsert(pool, "fed_data.m3_data", m3Cols, 2)

	ds := &M3{cfg: &config.Config{Fedsync: config.FedsyncConfig{CensusKey: "test-key"}}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
}

// --- IA Compilation: parseAndLoad ---

func TestIACompilation_ParseAndLoad(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	xmlData := `<?xml version="1.0"?>
<IAPDFirmSECReport GenOn="2024-06-01">
  <Firms>
    <Firm>
      <Info FirmCrdNb="12345" SECNb="801-12345" BusNm="Acme Advisors"/>
      <MainAddr City="New York" State="NY" Cntry="US"/>
      <Filing Dt="2024-06-01"/>
      <FormInfo><Part1A>
        <Item1><WebAddrs><WebAddr>https://acme.com</WebAddr></WebAddrs></Item1>
        <Item5A TtlEmp="0"/>
        <Item5F Q5F2C="5000000" Q5F2F="100"/>
      </Part1A></FormInfo>
    </Firm>
    <Firm>
      <Info FirmCrdNb="67890" SECNb="801-67890" BusNm="Beta Capital"/>
      <MainAddr City="Chicago" State="IL" Cntry="US"/>
      <Filing Dt="2024-06-15"/>
      <FormInfo><Part1A>
        <Item1><WebAddrs><WebAddr>https://beta.com</WebAddr></WebAddrs></Item1>
        <Item5A TtlEmp="0"/>
        <Item5F Q5F2C="2000000" Q5F2F="50"/>
      </Part1A></FormInfo>
    </Firm>
  </Firms>
</IAPDFirmSECReport>`

	r := strings.NewReader(xmlData)

	iaFirmCols := []string{"crd_number", "firm_name", "sec_number", "city", "state", "country", "website"}
	iaFilingCols := []string{"crd_number", "filing_date", "aum", "num_accounts", "legal_name", "num_employees", "total_employees", "sec_registered"}
	expectBulkUpsert(pool, "fed_data.adv_firms", iaFirmCols, 2)
	expectBulkUpsert(pool, "fed_data.adv_filings", iaFilingCols, 2)

	ds := &IACompilation{}
	log := zap.NewNop()
	result, err := ds.parseAndLoad(context.Background(), pool, r, log)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestIACompilation_ParseAndLoad_SkipZeroCRD(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	xmlData := `<?xml version="1.0"?>
<IAPDFirmSECReport GenOn="2024-06-01">
  <Firms>
    <Firm>
      <Info FirmCrdNb="0" BusNm="Bad Firm"/>
    </Firm>
  </Firms>
</IAPDFirmSECReport>`

	r := strings.NewReader(xmlData)

	ds := &IACompilation{}
	log := zap.NewNop()
	result, err := ds.parseAndLoad(context.Background(), pool, r, log)
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestIACompilation_ParseAndLoad_TruncatesLongState(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	xmlData := `<?xml version="1.0"?>
<IAPDFirmSECReport GenOn="2024-01-15">
  <Firms>
    <Firm>
      <Info FirmCrdNb="11111" SECNb="801-11111" BusNm="Long State Firm"/>
      <MainAddr City="Boston" State="MASSACHUSETTS" Cntry="US"/>
      <Filing Dt="2024-01-15"/>
      <FormInfo><Part1A>
        <Item1><WebAddrs></WebAddrs></Item1>
        <Item5A TtlEmp="0"/>
        <Item5F Q5F2C="1000000" Q5F2F="10"/>
      </Part1A></FormInfo>
    </Firm>
  </Firms>
</IAPDFirmSECReport>`

	r := strings.NewReader(xmlData)

	iaFirmCols := []string{"crd_number", "firm_name", "sec_number", "city", "state", "country", "website"}
	iaFilingCols := []string{"crd_number", "filing_date", "aum", "num_accounts", "legal_name", "num_employees", "total_employees", "sec_registered"}
	expectBulkUpsert(pool, "fed_data.adv_firms", iaFirmCols, 1)
	expectBulkUpsert(pool, "fed_data.adv_filings", iaFilingCols, 1)

	ds := &IACompilation{}
	log := zap.NewNop()
	result, err := ds.parseAndLoad(context.Background(), pool, r, log)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

// --- Form D: parseFormDXML ---

func TestFormD_ParseFormDXML(t *testing.T) {
	xmlData := `<?xml version="1.0"?>
<edgarSubmission>
  <headerData>
    <accessionNumber>0001234567-24-000001</accessionNumber>
  </headerData>
  <formData>
    <issuerList>
      <issuer>
        <issuerCIK>1234567</issuerCIK>
        <issuerName>Acme Fund LP</issuerName>
        <issuerEntityType>Limited Partnership</issuerEntityType>
        <issuerYearOfInc>2020</issuerYearOfInc>
        <issuerStateOrCountryOfInc>DE</issuerStateOrCountryOfInc>
      </issuer>
    </issuerList>
    <offeringData>
      <industryGroup>
        <industryGroupType>Pooled Investment Fund</industryGroupType>
      </industryGroup>
      <issuerSize>
        <revenueRange>Decline to Disclose</revenueRange>
      </issuerSize>
      <offeringSalesAmounts>
        <totalOfferingAmount>50000000</totalOfferingAmount>
        <totalAmountSold>25000000</totalAmountSold>
      </offeringSalesAmounts>
    </offeringData>
  </formData>
</edgarSubmission>`

	r := strings.NewReader(xmlData)
	ds := &FormD{}

	row, err := ds.parseFormDXML(r, "0001234567-24-000001", "1234567", "2024-06-15")
	require.NoError(t, err)
	require.Len(t, row, 11)

	assert.Equal(t, "0001234567-24-000001", row[0]) // accession
	assert.Equal(t, "1234567", row[1])               // cik
	assert.Equal(t, "Acme Fund LP", row[2])           // entity_name
	assert.Equal(t, "Limited Partnership", row[3])     // entity_type
	assert.Equal(t, "2020", row[4])                    // year_of_inc
	assert.Equal(t, "DE", row[5])                      // state_of_inc
	assert.Equal(t, "Pooled Investment Fund", row[6])  // industry_group
	assert.Equal(t, "Decline to Disclose", row[7])     // revenue_range
	assert.Equal(t, int64(50000000), row[8])           // total_offering
	assert.Equal(t, int64(25000000), row[9])           // total_sold
	assert.NotNil(t, row[10])                          // filing_date
}

func TestFormD_ParseFormDXML_TruncatesLongState(t *testing.T) {
	xmlData := `<?xml version="1.0"?>
<edgarSubmission>
  <headerData>
    <accessionNumber>0001234567-24-000002</accessionNumber>
  </headerData>
  <formData>
    <issuerList>
      <issuer>
        <issuerCIK>9999999</issuerCIK>
        <issuerName>Test Corp</issuerName>
        <issuerEntityType>Corporation</issuerEntityType>
        <issuerYearOfInc>2015</issuerYearOfInc>
        <issuerStateOrCountryOfInc>CALIFORNIA</issuerStateOrCountryOfInc>
      </issuer>
    </issuerList>
    <offeringData>
      <industryGroup><industryGroupType>Technology</industryGroupType></industryGroup>
      <issuerSize><revenueRange>$1-$5M</revenueRange></issuerSize>
      <offeringSalesAmounts>
        <totalOfferingAmount>1000000</totalOfferingAmount>
        <totalAmountSold>500000</totalAmountSold>
      </offeringSalesAmounts>
    </offeringData>
  </formData>
</edgarSubmission>`

	r := strings.NewReader(xmlData)
	ds := &FormD{}

	row, err := ds.parseFormDXML(r, "0001234567-24-000002", "9999999", "2024-07-01")
	require.NoError(t, err)
	// State should be truncated to 2 characters.
	assert.Equal(t, "CA", row[5])
}

func TestFormD_ParseFormDXML_InvalidXML(t *testing.T) {
	r := strings.NewReader("not xml")
	ds := &FormD{}

	_, err := ds.parseFormDXML(r, "acc", "cik", "2024-01-01")
	assert.Error(t, err)
}

// --- EDGAR Submissions: decodeSubmission ---

func TestEDGARSubmissions_DecodeSubmission(t *testing.T) {
	jsonData := `{
		"cik": "1234567",
		"entityType": "operating",
		"sic": "6282",
		"sicDescription": "Investment Advice",
		"name": "Acme Financial",
		"stateOfIncorporation": "DE",
		"ein": "123456789",
		"tickers": ["ACME"],
		"exchanges": ["NYSE"],
		"filings": {
			"recent": {
				"accessionNumber": ["0001234567-24-000001", "0001234567-24-000002"],
				"filingDate": ["2024-06-01", "2024-05-15"],
				"form": ["10-K", "10-Q"],
				"primaryDocument": ["doc1.htm", "doc2.htm"],
				"primaryDocDescription": ["Annual Report", "Quarterly Report"],
				"items": ["", ""],
				"size": [1000, 500],
				"isXBRL": [1, 1],
				"isInlineXBRL": [1, 0]
			}
		}
	}`

	ds := &EDGARSubmissions{}
	r := strings.NewReader(jsonData)

	sub, err := ds.decodeSubmission(r)
	require.NoError(t, err)
	assert.Equal(t, "1234567", sub.CIK)
	assert.Equal(t, "Acme Financial", sub.Name)
	assert.Equal(t, "6282", sub.SIC)
	assert.Equal(t, "DE", sub.StateOfInc)
	assert.Equal(t, []string{"ACME"}, sub.Tickers)
	assert.Equal(t, []string{"NYSE"}, sub.Exchanges)
	assert.Len(t, sub.RecentFilings.Recent.AccessionNumber, 2)
	assert.Equal(t, "10-K", sub.RecentFilings.Recent.Form[0])
}

func TestEDGARSubmissions_DecodeSubmission_InvalidJSON(t *testing.T) {
	ds := &EDGARSubmissions{}
	r := strings.NewReader("not json")

	_, err := ds.decodeSubmission(r)
	assert.Error(t, err)
}

func TestEDGARSubmissions_DecodeSubmission_EmptyFilings(t *testing.T) {
	jsonData := `{
		"cik": "9999999",
		"name": "No Filings Corp",
		"filings": {
			"recent": {
				"accessionNumber": [],
				"filingDate": [],
				"form": [],
				"primaryDocument": [],
				"primaryDocDescription": [],
				"items": [],
				"size": [],
				"isXBRL": [],
				"isInlineXBRL": []
			}
		}
	}`

	ds := &EDGARSubmissions{}
	r := strings.NewReader(jsonData)

	sub, err := ds.decodeSubmission(r)
	require.NoError(t, err)
	assert.Equal(t, "No Filings Corp", sub.Name)
	assert.Empty(t, sub.RecentFilings.Recent.AccessionNumber)
}

// --- XBRL Facts: Sync ---

func TestXBRLFacts_Sync(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Mock the CIK query: returns 2 CIKs.
	cikRows := pgxmock.NewRows([]string{"cik"}).
		AddRow("1234567").
		AddRow("9876543")
	pool.ExpectQuery("SELECT DISTINCT cik FROM fed_data.entity_xref").WillReturnRows(cikRows)

	// Company facts JSON for CIK 1234567 with one fact.
	factsJSON1 := map[string]any{
		"cik":        1234567,
		"entityName": "Acme Corp",
		"facts": map[string]any{
			"us-gaap": map[string]any{
				"Assets": map[string]any{
					"label": "Assets",
					"units": map[string]any{
						"USD": []map[string]any{
							{"end": "2024-12-31", "val": 1000000, "accn": "0001-24-000001", "fy": 2024, "form": "10-K", "filed": "2025-02-15"},
						},
					},
				},
			},
		},
	}

	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "CIK0001234567")
	})).Return(jsonBody(t, factsJSON1), nil)

	// Second CIK returns error (skipped).
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "CIK0009876543")
	})).Return(nil, errors.New("not found"))

	xbrlCols := []string{"cik", "fact_name", "period_end", "value", "unit", "form", "fy", "accession"}
	expectBulkUpsert(pool, "fed_data.xbrl_facts", xbrlCols, 1)

	ds := &XBRLFacts{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestXBRLFacts_Sync_NoCIKs(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// No CIKs returned.
	cikRows := pgxmock.NewRows([]string{"cik"})
	pool.ExpectQuery("SELECT DISTINCT cik FROM fed_data.entity_xref").WillReturnRows(cikRows)

	ds := &XBRLFacts{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestXBRLFacts_Sync_QueryError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	pool.ExpectQuery("SELECT DISTINCT cik FROM fed_data.entity_xref").
		WillReturnError(errors.New("db error"))

	ds := &XBRLFacts{cfg: &config.Config{}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "query CIKs")
}

// --- Entity Xref: Sync ---

func TestEntityXref_Sync(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// XrefBuilder.Build() calls:
	// 1. Truncate entity_xref
	pool.ExpectExec("TRUNCATE TABLE fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("TRUNCATE", 0))

	// 2. Pass 1: direct CRD-CIK match (Exec)
	pool.ExpectExec("INSERT INTO fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("INSERT", 50))

	// 3. Pass 2: SIC code match (Exec)
	pool.ExpectExec("INSERT INTO fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("INSERT", 30))

	// 4. Pass 3: fuzzy name match (Exec)
	pool.ExpectExec("INSERT INTO fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	ds := &EntityXref{}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(81), result.RowsSynced)
}

func TestEntityXref_Sync_TruncateError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	pool.ExpectExec("TRUNCATE TABLE fed_data.entity_xref").
		WillReturnError(errors.New("permission denied"))

	ds := &EntityXref{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "truncate")
}

// --- Holdings 13F: parseHoldingsXML ---

func TestHoldings13F_ParseHoldingsXML(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	xmlData := `<?xml version="1.0"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  <infoTable>
    <nameOfIssuer>Apple Inc</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>037833100</cusip>
    <value>150000</value>
    <shrsOrPrnAmt>
      <sshPrnamt>1000</sshPrnamt>
      <sshPrnamtType>SH</sshPrnamtType>
    </shrsOrPrnAmt>
    <putCall></putCall>
  </infoTable>
  <infoTable>
    <nameOfIssuer>Microsoft Corp</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>594918104</cusip>
    <value>200000</value>
    <shrsOrPrnAmt>
      <sshPrnamt>500</sshPrnamt>
      <sshPrnamtType>SH</sshPrnamtType>
    </shrsOrPrnAmt>
    <putCall></putCall>
  </infoTable>
  <infoTable>
    <nameOfIssuer>Bad CUSIP</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>SHORT</cusip>
    <value>100</value>
    <shrsOrPrnAmt>
      <sshPrnamt>10</sshPrnamt>
      <sshPrnamtType>SH</sshPrnamtType>
    </shrsOrPrnAmt>
  </infoTable>
</informationTable>`

	r := strings.NewReader(xmlData)

	holdingsCols := []string{"cik", "period", "cusip", "issuer_name", "class_title", "value", "shares", "sh_prn_type", "put_call"}
	expectBulkUpsert(pool, "fed_data.f13_holdings", holdingsCols, 2)

	ds := &Holdings13F{}
	period := time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)
	log := zap.NewNop()

	rows, err := ds.parseHoldingsXML(context.Background(), pool, r, "1234567", &period, log)
	require.NoError(t, err)
	// 2 rows (bad CUSIP filtered out)
	assert.Len(t, rows, 2)

	// Check first holding: Apple
	assert.Equal(t, "1234567", rows[0][0])
	assert.Equal(t, "037833100", rows[0][2])
	assert.Equal(t, "Apple Inc", rows[0][3])
	assert.Equal(t, int64(150000000), rows[0][5]) // value * 1000
	assert.Equal(t, int64(1000), rows[0][6])

	// Check second holding: Microsoft
	assert.Equal(t, "594918104", rows[1][2])
	assert.Equal(t, "Microsoft Corp", rows[1][3])

	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestHoldings13F_ParseHoldingsXML_Empty(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	xmlData := `<?xml version="1.0"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
</informationTable>`

	r := strings.NewReader(xmlData)
	ds := &Holdings13F{}
	period := time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)
	log := zap.NewNop()

	rows, err := ds.parseHoldingsXML(context.Background(), pool, r, "1234567", &period, log)
	require.NoError(t, err)
	assert.Empty(t, rows)
}

// --- CBP: parseCSV ---

func TestCBP_ParseCSV(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	csvData := `fipstate,fipscty,naics,emp,emp_nf,qp1,qp1_nf,ap,ap_nf,est
01,001,523110,500,,25000,,100000,,50
01,001,111110,200,,10000,,40000,,20
01,001,524210,300,,15000,,60000,,30
`
	r := strings.NewReader(csvData)

	cbpCols := []string{"year", "fips_state", "fips_county", "naics", "emp", "emp_nf", "qp1", "qp1_nf", "ap", "ap_nf", "est"}
	expectBulkUpsert(pool, "fed_data.cbp_data", cbpCols, 2)

	ds := &CBP{}
	n, err := ds.parseCSV(context.Background(), pool, r, 2022)
	require.NoError(t, err)
	// 2 rows: 523110 and 524210 (relevant NAICS), 111110 is agriculture (filtered)
	assert.Equal(t, int64(2), n)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestCBP_ParseCSV_EmptyCSV(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	csvData := `fipstate,fipscty,naics,emp,emp_nf,qp1,qp1_nf,ap,ap_nf,est
`
	r := strings.NewReader(csvData)

	ds := &CBP{}
	n, err := ds.parseCSV(context.Background(), pool, r, 2022)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

// --- SUSB: parseCSV ---

func TestSUSB_ParseCSV(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	csvData := `statefips,naics,entrsizedscr,firm,estb,empl,payr
01,523110,01: Total,100,120,500,25000
01,111110,01: Total,50,60,200,10000
01,524210,02: 1-4 employees,30,35,100,5000
`
	r := strings.NewReader(csvData)

	susbCols := []string{"year", "fips_state", "naics", "entrsizedscr", "firm", "estb", "empl", "payr"}
	expectBulkUpsert(pool, "fed_data.susb_data", susbCols, 2)

	ds := &SUSB{}
	n, err := ds.parseCSV(context.Background(), pool, r, 2021)
	require.NoError(t, err)
	// 2 rows (523110, 524210) - 111110 filtered
	assert.Equal(t, int64(2), n)
	assert.NoError(t, pool.ExpectationsWereMet())
}

// --- OEWS: parseCSV ---

func TestOEWS_ParseCSV(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	csvData := `area,area_type,naics,occ_code,tot_emp,h_mean,a_mean,h_median,a_median
0000000,1,523110,13-2051,1000,35.50,73840,33.00,68640
0000000,1,111110,45-2092,500,15.00,31200,14.00,29120
0000000,1,524210,13-2052,800,40.00,83200,38.50,80080
`
	r := strings.NewReader(csvData)

	oewsCols := []string{"area_code", "area_type", "naics", "occ_code", "year", "tot_emp", "h_mean", "a_mean", "h_median", "a_median"}
	expectBulkUpsert(pool, "fed_data.oews_data", oewsCols, 2)

	ds := &OEWS{}
	n, err := ds.parseCSV(context.Background(), pool, r, 2023)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
	assert.NoError(t, pool.ExpectationsWereMet())
}

// --- QCEW: parseCSV ---

func TestQCEW_ParseCSV(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	csvData := `area_fips,own_code,industry_code,qtr,month1_emplvl,month2_emplvl,month3_emplvl,total_qtrly_wages,avg_wkly_wage,qtrly_estabs
01001,5,523110,1,100,105,110,2500000,1800,50
01001,5,111110,1,200,210,220,1500000,1200,30
01001,5,524210,2,300,310,320,3500000,2200,80
01001,5,523110,0,400,410,420,10000000,2000,60
`
	r := strings.NewReader(csvData)

	qcewCols := []string{"area_fips", "own_code", "industry_code", "year", "qtr", "month1_emplvl", "month2_emplvl", "month3_emplvl", "total_qtrly_wages", "avg_wkly_wage", "qtrly_estabs"}
	expectBulkUpsert(pool, "fed_data.qcew_data", qcewCols, 2)

	ds := &QCEW{}
	n, err := ds.parseCSV(context.Background(), pool, r, 2024)
	require.NoError(t, err)
	// 2 rows: 523110 Q1, 524210 Q2. 111110 filtered, qtr=0 skipped.
	assert.Equal(t, int64(2), n)
	assert.NoError(t, pool.ExpectationsWereMet())
}

// --- NewRegistry ---

func TestNewRegistry_AllDatasets(t *testing.T) {
	cfg := &config.Config{}
	reg := NewRegistry(cfg)
	names := reg.AllNames()

	// Should have 26 datasets.
	assert.GreaterOrEqual(t, len(names), 20, "expected at least 20 registered datasets")

	// Spot-check a few well-known datasets exist.
	for _, expected := range []string{"cbp", "fpds", "edgar_submissions", "xbrl_facts", "fred", "entity_xref"} {
		ds, getErr := reg.Get(expected)
		assert.NoError(t, getErr, "expected dataset %q to be registered", expected)
		if getErr == nil {
			assert.Equal(t, expected, ds.Name())
		}
	}
}


// --- getColIdx ---

func TestGetColIdx(t *testing.T) {
	colIdx := map[string]int{"A": 0, "B": 1}
	record := []string{"val_a", "val_b"}

	assert.Equal(t, "val_a", getColIdx(record, colIdx, "A"))
	assert.Equal(t, "val_b", getColIdx(record, colIdx, "B"))
	assert.Equal(t, "", getColIdx(record, colIdx, "C"))

	colIdx["D"] = 99
	assert.Equal(t, "", getColIdx(record, colIdx, "D"))
}

// =====================================================================
// Additional coverage tests — schedule edge cases
// =====================================================================

func TestWeeklySchedule_Sunday(t *testing.T) {
	// Sunday March 10, 2024 — weekday==0 triggers the special branch
	now := time.Date(2024, time.March, 10, 12, 0, 0, 0, time.UTC)

	// Synced before this week's Monday (March 4)
	lastWeek := time.Date(2024, time.March, 3, 0, 0, 0, 0, time.UTC)
	assert.True(t, WeeklySchedule(now, &lastWeek))

	// Synced this week (after Monday March 4)
	thisWeek := time.Date(2024, time.March, 5, 0, 0, 0, 0, time.UTC)
	assert.False(t, WeeklySchedule(now, &thisWeek))
}

func TestQuarterlyAfterDelay_BothQuartersBefore(t *testing.T) {
	// Test the inner now.Before(available) path: both current and previous
	// quarters are too recent (delay not yet elapsed).
	// In Q1 (Jan-Mar), most recent completed quarter is Q4 of prev year (Dec 31).
	// Q4 ends Dec 31 2023 + 120 days = Apr 29 2024. Now is Jan 15 2024 -> too early.
	// Previous Q3 ends Sep 30 2023 + 120 days = Jan 28 2024. Now is Jan 15 -> also too early.
	now := time.Date(2024, time.January, 15, 0, 0, 0, 0, time.UTC)
	lastSync := time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC)
	assert.False(t, QuarterlyAfterDelay(now, &lastSync, 120))
}

func TestQuarterlyWithLag_InnerBranch(t *testing.T) {
	// Test the inner now.Before(available) path in QuarterlyWithLag.
	// Q1 ends March 31; 8 months lag = November 30.
	// Now is April 15 -> current quarter not available, previous Q4
	// ended Dec 31 + 8 months = Aug 31 -> also not available yet.
	now := time.Date(2024, time.April, 15, 0, 0, 0, 0, time.UTC)
	lastSync := time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC)
	// Q4 2023 ended Dec 31, +8 months = Aug 31 2024. Apr < Aug, so not available.
	// Q3 2023 ended Sep 30, +8 months = May 30 2024. Apr < May, so not available.
	assert.False(t, QuarterlyWithLag(now, &lastSync, 8))
}

// =====================================================================
// Additional coverage tests — EconCensus ShouldRun edge cases
// =====================================================================

func TestEconCensus_ShouldRun_FutureCensusYear(t *testing.T) {
	ds := &EconCensus{}

	// Test the future census year loop (line 55-61).
	// 2027 census -> release 2029. If now is 2030, synced in 2028 -> should run.
	now := time.Date(2030, time.April, 1, 0, 0, 0, 0, time.UTC)
	sync2028 := time.Date(2028, time.May, 1, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, &sync2028))

	// Already synced in 2029 after release -> should not run again
	sync2029 := time.Date(2029, time.June, 1, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(now, &sync2029))
}

func TestEconCensus_ShouldRun_NoNewCensusData(t *testing.T) {
	ds := &EconCensus{}

	// In 2026, no new census data (2022 was last, next is 2027 release 2029).
	// Synced in 2025 after the 2022 data release -> should not run.
	now := time.Date(2026, time.April, 1, 0, 0, 0, 0, time.UTC)
	sync2025 := time.Date(2025, time.May, 1, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(now, &sync2025))
}

// =====================================================================
// Additional coverage tests — Census API error paths (ABS, ASM, NES)
// =====================================================================

func TestABS_Sync_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().Download(mock.Anything, mock.Anything).Return(nil, errors.New("timeout"))

	ds := &ABS{cfg: &config.Config{Fedsync: config.FedsyncConfig{CensusKey: "key"}}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestABS_Sync_InvalidJSON(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(io.NopCloser(strings.NewReader("not json")), nil)

	ds := &ABS{cfg: &config.Config{Fedsync: config.FedsyncConfig{CensusKey: "key"}}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parse json")
}

func TestABS_Sync_EmptyResponse(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	censusResp := [][]string{
		{"NAICS2017", "GEO_ID", "FIRMPDEMP", "RCPPDEMP", "PAYANN", "us"},
	}
	f.EXPECT().Download(mock.Anything, mock.Anything).Return(jsonBody(t, censusResp), nil)

	ds := &ABS{cfg: &config.Config{Fedsync: config.FedsyncConfig{CensusKey: "key"}}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestASM_Sync_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().Download(mock.Anything, mock.Anything).Return(nil, errors.New("timeout"))

	ds := &ASM{cfg: &config.Config{Fedsync: config.FedsyncConfig{CensusKey: "key"}}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestASM_Sync_InvalidJSON(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(io.NopCloser(strings.NewReader("not json")), nil)

	ds := &ASM{cfg: &config.Config{Fedsync: config.FedsyncConfig{CensusKey: "key"}}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parse json")
}

func TestASM_Sync_EmptyResponse(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	censusResp := [][]string{
		{"NAICS2017", "GEO_ID", "VALADD", "TOTVAL_SHIP", "PRODWRKRS", "us"},
	}
	f.EXPECT().Download(mock.Anything, mock.Anything).Return(jsonBody(t, censusResp), nil)

	ds := &ASM{cfg: &config.Config{Fedsync: config.FedsyncConfig{CensusKey: "key"}}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestNES_Sync_InvalidJSON(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(io.NopCloser(strings.NewReader("not json")), nil)

	ds := &NES{cfg: &config.Config{Fedsync: config.FedsyncConfig{CensusKey: "key"}}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parse json")
}

// =====================================================================
// Additional coverage — EconCensus Sync error paths
// =====================================================================

func TestEconCensus_Sync_CtxCancelled(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancel

	ds := &EconCensus{cfg: &config.Config{Fedsync: config.FedsyncConfig{CensusKey: "key"}}}
	_, err = ds.Sync(ctx, pool, f, t.TempDir())
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestEconCensus_Sync_FetchYearErrorContinues(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Both years return download errors -> Sync logs warning and continues, returns 0 rows.
	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(nil, errors.New("fetch error")).Times(len(econCensusYears))

	ds := &EconCensus{cfg: &config.Config{Fedsync: config.FedsyncConfig{CensusKey: "key"}}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

// =====================================================================
// Additional coverage — M3 edge cases
// =====================================================================

func TestM3_Sync_DownloadFail(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Single API call fails -> error returned.
	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(nil, errors.New("network error")).Once()

	ds := &M3{cfg: &config.Config{Fedsync: config.FedsyncConfig{CensusKey: "key"}}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "m3: download")
}

func TestM3_ParseTimeSlot(t *testing.T) {
	y, m := parseTimeSlot("2024-06")
	assert.Equal(t, 2024, y)
	assert.Equal(t, 6, m)

	// Too short
	y, m = parseTimeSlot("short")
	assert.Equal(t, 0, y)
	assert.Equal(t, 0, m)

	// No dash at position 4
	y, m = parseTimeSlot("2024X06")
	assert.Equal(t, 0, y)
	assert.Equal(t, 0, m)
}

// =====================================================================
// Additional coverage — FRED edge cases
// =====================================================================

func TestFRED_Sync_NoAPIKey(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// All series fail -> 0 rows
	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(nil, errors.New("skip")).Times(len(fredTargetSeries))

	expectBulkUpsert(pool, "fed_data.fred_series", fredCols, 0)

	ds := &FRED{cfg: &config.Config{Fedsync: config.FedsyncConfig{FREDKey: "key"}}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

// =====================================================================
// Additional coverage — CPSLAUS edge cases
// =====================================================================

func TestCPSLAUS_Sync_AllSeriesFail(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(nil, errors.New("skip")).Times(len(lausSeries))

	expectBulkUpsert(pool, "fed_data.laus_data", lausCols, 0)

	ds := &CPSLAUS{cfg: &config.Config{Fedsync: config.FedsyncConfig{BLSKey: "key"}}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

// =====================================================================
// Additional coverage — ECI edge cases
// =====================================================================

func TestECI_Sync_AllSeriesFail(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(nil, errors.New("skip")).Times(len(eciSeries))

	expectBulkUpsert(pool, "fed_data.eci_data", eciCols, 0)

	ds := &ECI{cfg: &config.Config{Fedsync: config.FedsyncConfig{BLSKey: "key"}}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

// =====================================================================
// Additional coverage — FPDS edge cases
// =====================================================================

func TestFPDS_Sync_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().Download(mock.Anything, mock.Anything).Return(nil, errors.New("timeout"))

	ds := &FPDS{cfg: &config.Config{Fedsync: config.FedsyncConfig{SAMKey: "key"}}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fetch page")
}

// =====================================================================
// Additional coverage — XBRLFacts ctx.Done() path
// =====================================================================

func TestXBRLFacts_Sync_CtxCancelled(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	cikRows := pgxmock.NewRows([]string{"cik"}).AddRow("1234567")
	pool.ExpectQuery("SELECT DISTINCT cik FROM fed_data.entity_xref").WillReturnRows(cikRows)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancel

	ds := &XBRLFacts{cfg: &config.Config{}}
	_, err = ds.Sync(ctx, pool, f, t.TempDir())
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// =====================================================================
// Additional coverage — FormD ctx.Done() path
// =====================================================================

func TestFormD_Sync_CtxCancelled(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	searchResult := map[string]any{
		"hits": map[string]any{
			"total": 1,
			"hits": []map[string]any{
				{
					"_source": map[string]any{
						"entity_cik":   "1234567",
						"entity_name":  "Test Corp",
						"form_type":    "D",
						"file_date":    "2024-06-15",
						"accession_no": "0001234567-24-000001",
					},
				},
			},
		},
	}

	f.EXPECT().Download(mock.Anything, mock.Anything).Return(jsonBody(t, searchResult), nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancel

	ds := &FormD{cfg: &config.Config{}}
	_, err = ds.Sync(ctx, pool, f, t.TempDir())
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// =====================================================================
// Additional coverage — FRED ctx.Done() path
// =====================================================================

func TestFRED_Sync_CtxCancelled(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancel

	ds := &FRED{cfg: &config.Config{Fedsync: config.FedsyncConfig{FREDKey: "key"}}}
	_, err = ds.Sync(ctx, pool, f, t.TempDir())
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// =====================================================================
// Additional coverage — EconCensus upsertRows error
// =====================================================================

func TestEconCensus_UpsertRows_Error(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	rows := [][]any{
		{int16(2022), "0400000US06", "523110", 100, int64(5000000), int64(2000000), 500},
	}

	pool.ExpectBegin().WillReturnError(errors.New("db error"))

	ds := &EconCensus{}
	_, err = ds.upsertRows(context.Background(), pool, rows)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "bulk upsert")
}

// =====================================================================
// Mid-batch flush tests — BrokerCheck (>5000 rows)
// =====================================================================

func TestBrokerCheck_Sync_MidBatchFlush(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Generate 5002 pipe-delimited rows to trigger mid-batch flush at 5000.
	var sb strings.Builder
	sb.WriteString("CRD|Firm Name|SEC Number|City|State|Offices|Reps\n")
	for i := 1; i <= 5002; i++ {
		fmt.Fprintf(&sb, "%d|Firm %d|801-%d|City|NY|1|10\n", i, i, i)
	}

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, "firm.txt", sb.String()))

	bcCols := []string{"crd_number", "firm_name", "sec_number", "main_addr_city", "main_addr_state", "num_branch_offices", "num_registered_reps"}
	// First batch of 5000
	expectBulkUpsert(pool, "fed_data.brokercheck", bcCols, 5000)
	// Final batch of 2
	expectBulkUpsert(pool, "fed_data.brokercheck", bcCols, 2)

	ds := &BrokerCheck{}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(5002), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

// =====================================================================
// Mid-batch flush tests — FormBD (>5000 rows)
// =====================================================================

func TestFormBD_Sync_MidBatchFlush(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	var sb strings.Builder
	sb.WriteString("CRD|SEC|Name|City|State|FYE|Reps\n")
	for i := 1; i <= 5002; i++ {
		fmt.Fprintf(&sb, "%d|8-%d|Firm %d|City|ST|12|%d\n", i, i, i, i)
	}

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, "bd_firm.txt", sb.String()))

	bdCols := []string{"crd_number", "sec_number", "firm_name", "city", "state", "fiscal_year_end", "num_reps"}
	expectBulkUpsert(pool, "fed_data.form_bd", bdCols, 5000)
	expectBulkUpsert(pool, "fed_data.form_bd", bdCols, 2)

	ds := &FormBD{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(5002), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

// =====================================================================
// Mid-batch flush tests — OSHA ITA (>5000 rows)
// =====================================================================

func TestOSHITA_Sync_MidBatchFlush(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	var sb strings.Builder
	sb.WriteString("activity_nr,estab_name,site_city,site_state,site_zip,naics_code,sic_code,open_date,close_case_date,case_type,safety_hlth,total_penalty\n")
	for i := 1; i <= 5002; i++ {
		fmt.Fprintf(&sb, "%d,Firm %d,City,ST,12345,523110,6211,01/01/2024,,R,S,%d.00\n", 100000000+i, i, i*10)
	}

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, "severeinjury.csv", sb.String()))

	oshaCols := []string{"activity_nr", "estab_name", "site_city", "site_state", "site_zip", "naics_code", "sic_code", "open_date", "close_case_date", "case_type", "safety_hlth", "total_penalty"}
	expectBulkUpsert(pool, "fed_data.osha_inspections", oshaCols, 5000)
	expectBulkUpsert(pool, "fed_data.osha_inspections", oshaCols, 2)

	ds := &OSHITA{}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(5002), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

// =====================================================================
// Mid-batch flush tests — EPA ECHO (>5000 rows)
// =====================================================================

func TestEPAECHO_Sync_MidBatchFlush(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	var sb strings.Builder
	sb.WriteString("REGISTRY_ID,PRIMARY_NAME,CITY_NAME,STATE_CODE,POSTAL_CODE,col5,col6,LATITUDE83,LONGITUDE83\n")
	for i := 1; i <= 5002; i++ {
		fmt.Fprintf(&sb, "%d,Facility %d,City,ST,12345,x,y,39.78,-89.65\n", 110000000+i, i)
	}

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, "NATIONAL_FACILITY_FILE.CSV", sb.String()))

	epaCols := []string{"registry_id", "fac_name", "fac_city", "fac_state", "fac_zip", "fac_lat", "fac_long"}
	expectBulkUpsert(pool, "fed_data.epa_facilities", epaCols, 5000)
	expectBulkUpsert(pool, "fed_data.epa_facilities", epaCols, 2)

	ds := &EPAECHO{}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(5002), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

// =====================================================================
// Additional coverage — FormD parse error fallback path
// =====================================================================

func TestFormD_Sync_XMLParseError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	tempDir := t.TempDir()

	searchResult := map[string]any{
		"hits": map[string]any{
			"total": 1,
			"hits": []map[string]any{
				{
					"_source": map[string]any{
						"entity_cik":   "7777777",
						"entity_name":  "Bad XML Corp",
						"form_type":    "D",
						"file_date":    "2024-08-01",
						"accession_no": "0007777777-24-000001",
					},
				},
			},
		},
	}

	f.EXPECT().Download(mock.Anything, mock.Anything).Return(jsonBody(t, searchResult), nil)

	// XML download succeeds but content is invalid XML
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, path string) (int64, error) {
			return int64(7), os.WriteFile(path, []byte("not xml"), 0o644)
		})

	formDCols := []string{"accession_number", "cik", "entity_name", "entity_type", "year_of_inc", "state_of_inc", "industry_group", "revenue_range", "total_offering", "total_sold", "filing_date"}
	expectBulkUpsert(pool, "fed_data.form_d", formDCols, 1)

	ds := &FormD{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, tempDir)
	require.NoError(t, err)
	// Falls back to search metadata row
	assert.Equal(t, int64(1), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

// =====================================================================
// Additional coverage — EDGAR Submissions ctx.Done path
// =====================================================================

func TestEDGARSubmissions_Sync_CtxCancelled(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	tempDir := t.TempDir()

	subJSON := `{"cik":"111","name":"Corp A","entityType":"op","sic":"6200","sicDescription":"Sec","stateOfIncorporation":"NY","ein":"111","tickers":[],"exchanges":[],"filings":{"recent":{"accessionNumber":["ACC-1"],"filingDate":["2024-01-01"],"form":["10-K"],"primaryDocument":["d.htm"],"primaryDocDescription":["AR"],"items":[""],"size":[100],"isXBRL":[0],"isInlineXBRL":[0]}}}`

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, path string) (int64, error) {
			createTestZIP(t, path, "CIK0000000111.json", subJSON)
			return 1000, nil
		})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancel

	ds := &EDGARSubmissions{cfg: &config.Config{}}
	_, err = ds.Sync(ctx, pool, f, tempDir)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// =====================================================================
// Additional coverage — parseDate edge cases
// =====================================================================

func TestParseDate(t *testing.T) {
	// Standard format
	d := parseDate("2024-06-15")
	require.NotNil(t, d)
	assert.Equal(t, 2024, d.Year())
	assert.Equal(t, time.June, d.Month())

	// US format
	d = parseDate("06/15/2024")
	require.NotNil(t, d)
	assert.Equal(t, 2024, d.Year())

	// Short US format
	d = parseDate("6/5/2024")
	require.NotNil(t, d)

	// ISO datetime
	d = parseDate("2024-06-15T10:30:00")
	require.NotNil(t, d)

	// Dash format
	d = parseDate("06-15-2024")
	require.NotNil(t, d)

	// Empty string
	d = parseDate("")
	assert.Nil(t, d)

	// Whitespace only
	d = parseDate("   ")
	assert.Nil(t, d)

	// Unrecognized format
	d = parseDate("June 15, 2024")
	assert.Nil(t, d)
}

// =====================================================================
// Additional coverage — Holdings13F parseHoldingsXML with upsert error
// =====================================================================

// =====================================================================
// Mid-batch flush tests — IA Compilation parseAndLoad (>2000 rows)
// =====================================================================

func TestIACompilation_ParseAndLoad_MidBatchFlush(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	var sb strings.Builder
	sb.WriteString(`<?xml version="1.0"?>` + "\n<IAPDFirmSECReport GenOn=\"2024-06-01\">\n<Firms>\n")
	for i := 1; i <= 2002; i++ {
		fmt.Fprintf(&sb, `  <Firm>
    <Info FirmCrdNb="%d" SECNb="801-%d" BusNm="Firm %d"/>
    <MainAddr City="City" State="NY" Cntry="US"/>
    <Filing Dt="2024-06-01"/>
    <FormInfo><Part1A><Item1><WebAddrs></WebAddrs></Item1><Item5A TtlEmp="0"/><Item5F Q5F2C="1000000" Q5F2F="10"/></Part1A></FormInfo>
  </Firm>
`, i, i, i)
	}
	sb.WriteString("</Firms>\n</IAPDFirmSECReport>")

	r := strings.NewReader(sb.String())

	iaFirmCols := []string{"crd_number", "firm_name", "sec_number", "city", "state", "country", "website"}
	iaFilingCols := []string{"crd_number", "filing_date", "aum", "num_accounts", "legal_name", "num_employees", "total_employees", "sec_registered"}
	expectBulkUpsert(pool, "fed_data.adv_firms", iaFirmCols, 2000)
	expectBulkUpsert(pool, "fed_data.adv_filings", iaFilingCols, 2000)
	expectBulkUpsert(pool, "fed_data.adv_firms", iaFirmCols, 2)
	expectBulkUpsert(pool, "fed_data.adv_filings", iaFilingCols, 2)

	ds := &IACompilation{}
	log := zap.NewNop()
	result, err := ds.parseAndLoad(context.Background(), pool, r, log)
	require.NoError(t, err)
	assert.Equal(t, int64(2002), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}


// =====================================================================
// Mid-batch flush tests — CBP parseCSV (>10000 rows)
// =====================================================================

func TestCBP_ParseCSV_MidBatchFlush(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	// cbpBatchSize = 5000, so generate 5002 rows to trigger one mid-batch flush + one final flush.
	var sb strings.Builder
	sb.WriteString("fipstate,fipscty,naics,emp,emp_nf,qp1,qp1_nf,ap,ap_nf,est\n")
	for i := 1; i <= 5002; i++ {
		fmt.Fprintf(&sb, "01,%03d,523110,%d,,%d,,%d,,5\n", i%999, i*10, i*100, i*1000)
	}

	r := strings.NewReader(sb.String())

	cbpCols := []string{"year", "fips_state", "fips_county", "naics", "emp", "emp_nf", "qp1", "qp1_nf", "ap", "ap_nf", "est"}
	expectBulkUpsert(pool, "fed_data.cbp_data", cbpCols, 5000)
	expectBulkUpsert(pool, "fed_data.cbp_data", cbpCols, 2)

	ds := &CBP{}
	n, err := ds.parseCSV(context.Background(), pool, r, 2022)
	require.NoError(t, err)
	assert.Equal(t, int64(5002), n)
	assert.NoError(t, pool.ExpectationsWereMet())
}

// =====================================================================
// Mid-batch flush tests — QCEW parseCSV (>10000 rows)
// =====================================================================

func TestQCEW_ParseCSV_MidBatchFlush(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	var sb strings.Builder
	sb.WriteString("area_fips,own_code,industry_code,qtr,month1_emplvl,month2_emplvl,month3_emplvl,total_qtrly_wages,avg_wkly_wage,qtrly_estabs\n")
	for i := 1; i <= 20002; i++ {
		fmt.Fprintf(&sb, "%05d,5,523110,%d,100,105,110,2500000,1800,50\n", i, (i%4)+1)
	}

	r := strings.NewReader(sb.String())

	qcewCols := []string{"area_fips", "own_code", "industry_code", "year", "qtr", "month1_emplvl", "month2_emplvl", "month3_emplvl", "total_qtrly_wages", "avg_wkly_wage", "qtrly_estabs"}
	expectBulkUpsert(pool, "fed_data.qcew_data", qcewCols, 20000)
	expectBulkUpsert(pool, "fed_data.qcew_data", qcewCols, 2)

	ds := &QCEW{}
	n, err := ds.parseCSV(context.Background(), pool, r, 2024)
	require.NoError(t, err)
	assert.Equal(t, int64(20002), n)
	assert.NoError(t, pool.ExpectationsWereMet())
}

// =====================================================================
// Mid-batch flush tests — XBRL Facts Sync (>500 rows)
// =====================================================================

func TestXBRLFacts_Sync_MidBatchFlush(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Use 2 CIKs so rows accumulate across CIKs and cross the batchSize=500 boundary.
	// CIK 1: 300 facts, CIK 2: 300 facts = 600 total. Flush at >=500, remainder 100.
	cikRows := pgxmock.NewRows([]string{"cik"}).AddRow("1234567").AddRow("9876543")
	pool.ExpectQuery("SELECT DISTINCT cik FROM fed_data.entity_xref").WillReturnRows(cikRows)

	makeFacts := func(cik int, n int) map[string]any {
		fv := make([]map[string]any, n)
		for i := range fv {
			fv[i] = map[string]any{
				"end": fmt.Sprintf("2024-%02d-%02d", (i%12)+1, (i%28)+1), "val": 1000 + i,
				"accn": fmt.Sprintf("0001-24-%06d", i), "fy": 2024, "form": "10-K", "filed": "2025-02-15",
			}
		}
		return map[string]any{
			"cik": cik, "entityName": "Corp",
			"facts": map[string]any{
				"us-gaap": map[string]any{
					"Assets": map[string]any{
						"label": "Assets",
						"units": map[string]any{"USD": fv},
					},
				},
			},
		}
	}

	f.EXPECT().Download(mock.Anything, mock.Anything).Return(jsonBody(t, makeFacts(1234567, 300)), nil).Once()
	f.EXPECT().Download(mock.Anything, mock.Anything).Return(jsonBody(t, makeFacts(9876543, 300)), nil).Once()

	xbrlCols := []string{"cik", "fact_name", "period_end", "value", "unit", "form", "fy", "accession"}
	// After CIK 1: 300 rows (no flush). After CIK 2: 600 rows >= 500 → flush 600, remainder 0.
	// Actually the check is after appending all facts for each CIK, so:
	// After CIK 1: len(rows)=300, 300 < 500 → no flush.
	// After CIK 2: len(rows)=600, 600 >= 500 → flush all 600, rows[:0].
	// Final: len(rows)=0 → no final flush. So one upsert of 600.
	expectBulkUpsert(pool, "fed_data.xbrl_facts", xbrlCols, 600)

	ds := &XBRLFacts{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(600), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestHoldings13F_ParseHoldingsXML_UpsertError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	xmlData := `<?xml version="1.0"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  <infoTable>
    <nameOfIssuer>Apple Inc</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>037833100</cusip>
    <value>150000</value>
    <shrsOrPrnAmt>
      <sshPrnamt>1000</sshPrnamt>
      <sshPrnamtType>SH</sshPrnamtType>
    </shrsOrPrnAmt>
  </infoTable>
</informationTable>`

	r := strings.NewReader(xmlData)

	// BulkUpsert fails
	pool.ExpectBegin().WillReturnError(errors.New("db error"))

	ds := &Holdings13F{}
	period := time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)
	log := zap.NewNop()

	_, err = ds.parseHoldingsXML(context.Background(), pool, r, "1234567", &period, log)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

// =====================================================================
// SUSB parseCSV mid-batch flush (susbBatchSize=5000)
// =====================================================================

func TestSUSB_ParseCSV_MidBatchFlush(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	var sb strings.Builder
	sb.WriteString("statefips,naics,entrsizedscr,firm,estb,empl,payr\n")
	for i := 1; i <= 5002; i++ {
		fmt.Fprintf(&sb, "%02d,523110,Size%d,%d,%d,%d,%d\n", i%56+1, i, i, i*2, i*10, i*100)
	}

	r := strings.NewReader(sb.String())

	susbCols := []string{"year", "fips_state", "naics", "entrsizedscr", "firm", "estb", "empl", "payr"}
	expectBulkUpsert(pool, "fed_data.susb_data", susbCols, 5000)
	expectBulkUpsert(pool, "fed_data.susb_data", susbCols, 2)

	ds := &SUSB{}
	n, err := ds.parseCSV(context.Background(), pool, r, 2022)
	require.NoError(t, err)
	assert.Equal(t, int64(5002), n)
	assert.NoError(t, pool.ExpectationsWereMet())
}

// =====================================================================
// OEWS parseCSV mid-batch flush (oewsBatchSize=5000)
// =====================================================================

func TestOEWS_ParseCSV_MidBatchFlush(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	var sb strings.Builder
	sb.WriteString("area,area_type,naics,occ_code,tot_emp,h_mean,a_mean,h_median,a_median\n")
	for i := 1; i <= 5002; i++ {
		fmt.Fprintf(&sb, "%05d,1,523110,%02d-%04d,%d,55.5,%d,50.0,%d\n", i, i/10000+11, i%10000, i*10, i*100, i*90)
	}

	r := strings.NewReader(sb.String())

	oewsCols := []string{"area_code", "area_type", "naics", "occ_code", "year", "tot_emp", "h_mean", "a_mean", "h_median", "a_median"}
	expectBulkUpsert(pool, "fed_data.oews_data", oewsCols, 5000)
	expectBulkUpsert(pool, "fed_data.oews_data", oewsCols, 2)

	ds := &OEWS{}
	n, err := ds.parseCSV(context.Background(), pool, r, 2023)
	require.NoError(t, err)
	assert.Equal(t, int64(5002), n)
	assert.NoError(t, pool.ExpectationsWereMet())
}


// =====================================================================
// EDGAR Submissions — mid-batch flush (entity+filing batches)
// =====================================================================

func TestEDGARSubmissions_Sync_MidBatchFlush(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Create a ZIP with >5000 submission JSON files.
	// Each file is a small JSON representing a company with 1 filing.
	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "submissions.zip")

	zf, err := os.Create(zipPath)
	require.NoError(t, err)
	w := zip.NewWriter(zf)

	for i := 1; i <= 5002; i++ {
		sub := map[string]any{
			"cik": fmt.Sprintf("%d", 1000000+i), "name": fmt.Sprintf("Company %d", i),
			"entityType": "Corp", "sic": "6211", "sicDescription": "Financial",
			"stateOfIncorporation": "DE", "ein": fmt.Sprintf("12-%07d", i),
			"tickers": []string{"TKR"}, "exchanges": []string{"NYSE"},
			"filings": map[string]any{
				"recent": map[string]any{
					"accessionNumber": []string{fmt.Sprintf("0001-%010d", i)},
					"filingDate":      []string{"2024-06-01"},
					"form":            []string{"10-K"},
					"primaryDocument": []string{"doc.htm"},
					"primaryDocDescription": []string{"Annual Report"},
					"items":           []string{""},
					"size":            []int{1000},
					"isXBRL":          []int{1},
					"isInlineXBRL":    []int{1},
				},
			},
		}
		data, _ := json.Marshal(sub)
		fw, err := w.Create(fmt.Sprintf("CIK%010d.json", 1000000+i))
		require.NoError(t, err)
		_, err = fw.Write(data)
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	_ = zf.Close()

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ string, dest string) (int64, error) {
			data, _ := os.ReadFile(zipPath)
			return int64(len(data)), os.WriteFile(dest, data, 0o644)
		},
	)

	entityCols := []string{"cik", "entity_name", "entity_type", "sic", "sic_description", "state_of_inc", "state_of_business", "ein", "tickers", "exchanges"}
	filingCols := []string{"accession_number", "cik", "form_type", "filing_date", "primary_doc", "primary_doc_desc", "items", "size", "is_xbrl", "is_inline_xbrl"}

	pool.MatchExpectationsInOrder(false)

	// With batch size 10000, 5002 entities fit in one batch, 5002 filings in one batch.
	expectBulkUpsert(pool, "fed_data.edgar_entities", entityCols, 5002)
	expectBulkUpsert(pool, "fed_data.edgar_filings", filingCols, 5002)

	ds := &EDGARSubmissions{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, tmpDir)
	require.NoError(t, err)
	assert.Equal(t, int64(5002), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

// =====================================================================
// FPDS — multi-page + upsert batch tests
// =====================================================================

func TestFPDS_Sync_MultiPage(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Page 1: exactly 100 results (fpdsPageSize) → triggers next page
	opps1 := make([]map[string]any, 100)
	for i := range opps1 {
		opps1[i] = map[string]any{
			"noticeId": fmt.Sprintf("N%05d", i), "solicitationNumber": fmt.Sprintf("S%d", i),
			"fullParentPathName": "Agency", "fullParentPathCode": "AG01",
			"title": "Contract", "naicsCode": "523110", "classificationCode": "PS1",
			"postedDate": "2024-06-01",
		}
	}
	page1 := map[string]any{"opportunitiesData": opps1, "totalRecords": 105}

	// Page 2: 5 results → no more pages
	opps2 := make([]map[string]any, 5)
	for i := range opps2 {
		opps2[i] = map[string]any{
			"noticeId": fmt.Sprintf("N%05d", 100+i), "solicitationNumber": fmt.Sprintf("S%d", 100+i),
			"fullParentPathName": "Agency", "fullParentPathCode": "AG01",
			"title": "Contract", "naicsCode": "523110", "classificationCode": "PS1",
			"postedDate": "2024-06-01",
		}
	}
	page2 := map[string]any{"opportunitiesData": opps2, "totalRecords": 105}

	f.EXPECT().Download(mock.Anything, mock.Anything).Return(jsonBody(t, page1), nil).Once()
	f.EXPECT().Download(mock.Anything, mock.Anything).Return(jsonBody(t, page2), nil).Once()

	contractCols := []string{"contract_id", "piid", "agency_id", "agency_name", "vendor_name", "vendor_duns", "vendor_uei", "vendor_city", "vendor_state", "vendor_zip", "naics", "psc", "date_signed", "dollars_obligated", "description"}
	expectBulkUpsert(pool, "fed_data.fpds_contracts", contractCols, 100)
	expectBulkUpsert(pool, "fed_data.fpds_contracts", contractCols, 5)

	ds := &FPDS{cfg: &config.Config{Fedsync: config.FedsyncConfig{SAMKey: "test-key"}}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(105), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestFPDS_ParseResponse_WithAward(t *testing.T) {
	data := `{
		"opportunitiesData": [{
			"noticeId": "N1", "solicitationNumber": "SOL1",
			"fullParentPathName": "Dept.Agency", "fullParentPathCode": "DEPT.SUB",
			"title": "Title", "description": "Full description",
			"naicsCode": "523110", "classificationCode": "PS123",
			"postedDate": "2024-06-01",
			"award": {
				"amount": 1000000.50,
				"date": "2024-07-15",
				"awardee": {
					"name": "Vendor LLC", "ueiSAM": "UEI123", "duns": "DUNS123",
					"location": {"city": "NYC", "state": "NY", "zip": "10001"}
				}
			}
		}],
		"totalRecords": 1
	}`

	ds := &FPDS{}
	rows, hasMore, err := ds.parseResponse([]byte(data))
	require.NoError(t, err)
	assert.False(t, hasMore)
	require.Len(t, rows, 1)

	row := rows[0]
	assert.Equal(t, "N1", row[0])       // contract_id
	assert.Equal(t, "Vendor LLC", row[4]) // vendor_name
	assert.Equal(t, "DUNS123", row[5])    // vendor_duns
	assert.Equal(t, "UEI123", row[6])     // vendor_uei
	assert.Equal(t, "NYC", row[7])        // vendor_city
	assert.Equal(t, "NY", row[8])         // vendor_state
	assert.Equal(t, 1000000.50, row[13])  // dollars_obligated
}

// =====================================================================
// M3 — Sync with valid data and upsert error
// =====================================================================

func TestM3_Sync_UpsertError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Return valid M3 data with recognized data_type_code
	validResp := [][]string{
		{"cell_value", "time", "category_code", "data_type_code", "seasonally_adj", "us"},
		{"1000", "2024-01", "MTM", "NO", "yes", "*"},
	}
	f.EXPECT().Download(mock.Anything, mock.Anything).Return(jsonBody(t, validResp), nil).Once()

	// BulkUpsert fails
	pool.ExpectBegin().WillReturnError(errors.New("db error"))

	ds := &M3{cfg: &config.Config{Fedsync: config.FedsyncConfig{CensusKey: "test-key"}}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "m3: upsert")
}

func TestM3_Sync_ReadAllError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Return a reader that fails on ReadAll
	f.EXPECT().Download(mock.Anything, mock.Anything).Return(
		io.NopCloser(&failReader{}), nil,
	).Once()

	ds := &M3{cfg: &config.Config{Fedsync: config.FedsyncConfig{CensusKey: "test-key"}}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "m3: read response")
}

type failReader struct{}

func (r *failReader) Read(p []byte) (int, error) { return 0, errors.New("read error") }

func TestM3_Sync_InvalidJSON(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Return invalid JSON
	f.EXPECT().Download(mock.Anything, mock.Anything).Return(
		io.NopCloser(strings.NewReader("not json")), nil,
	).Once()

	ds := &M3{cfg: &config.Config{Fedsync: config.FedsyncConfig{CensusKey: "test-key"}}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "m3: parse json")
}

// =====================================================================
// Form D — XML parse success + download fallback
// =====================================================================

func TestFormD_Sync_DownloadFallback(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Search returns 1 filing
	searchResp := map[string]any{
		"hits": map[string]any{
			"total": 1,
			"hits": []map[string]any{
				{
					"_source": map[string]any{
						"entity_cik": "0001234567", "entity_name": "Test Corp",
						"form_type": "D", "file_date": "2024-06-15",
						"accession_no": "0001234567-24-000001",
					},
				},
			},
		},
	}
	f.EXPECT().Download(mock.Anything, mock.Anything).Return(jsonBody(t, searchResp), nil).Once()

	// DownloadToFile fails → triggers fallback path
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).Return(int64(0), errors.New("timeout")).Once()

	formDCols := []string{"accession_number", "cik", "entity_name", "entity_type", "year_of_inc", "state_of_inc", "industry_group", "revenue_range", "total_offering", "total_sold", "filing_date"}
	expectBulkUpsert(pool, "fed_data.form_d", formDCols, 1)

	ds := &FormD{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

// =====================================================================
// Holdings 13F — mid-batch flush (holdingsBatchSize=5000)
// =====================================================================

func TestHoldings13F_ParseHoldingsXML_MidBatchFlush(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	var sb strings.Builder
	sb.WriteString(`<?xml version="1.0"?>` + "\n")
	sb.WriteString(`<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">` + "\n")
	for i := 0; i < 5002; i++ {
		fmt.Fprintf(&sb, `  <infoTable>
    <nameOfIssuer>Company %d</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>%09d</cusip>
    <value>%d</value>
    <shrsOrPrnAmt><sshPrnamt>%d</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
  </infoTable>
`, i, 100000000+i, 1000+i, 100+i)
	}
	sb.WriteString("</informationTable>")

	r := strings.NewReader(sb.String())

	holdCols := []string{"cik", "period", "cusip", "issuer_name", "class_title", "value", "shares", "sh_prn_type", "put_call"}
	expectBulkUpsert(pool, "fed_data.f13_holdings", holdCols, 5000)
	expectBulkUpsert(pool, "fed_data.f13_holdings", holdCols, 2)

	ds := &Holdings13F{}
	period := time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)
	log := zap.NewNop()
	rows, err := ds.parseHoldingsXML(context.Background(), pool, r, "1234567", &period, log)
	require.NoError(t, err)
	assert.Len(t, rows, 5002)
	assert.NoError(t, pool.ExpectationsWereMet())
}

// =====================================================================
// BrokerCheck / FormBD / OSHA / EPA — error on mid-batch upsert
// =====================================================================

func TestBrokerCheck_Sync_UpsertError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// 2 data rows (pipe delimited, 7 fields required: crd|name|sec|city|state|branches|reps)
	csvContent := "crd_number|firm_name|sec_number|city|state|num_branch|num_reps\n1001|Firm A|SEC001|NYC|NY|5|100\n1002|Firm B|SEC002|LA|CA|3|50\n"
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		mockDownloadToFileZIP(t, "data.txt", csvContent),
	)

	// BulkUpsert fails
	pool.ExpectBegin().WillReturnError(errors.New("db error"))

	ds := &BrokerCheck{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "brokercheck")
}

func TestEPAECHO_Sync_UpsertError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	csvContent := "REGISTRY_ID,PRIMARY_NAME,CITY_NAME,STATE_CODE,POSTAL_CODE,col5,col6,LATITUDE83,LONGITUDE83\nEPA001,Firm,NYC,NY,10001,x,y,40.7128,-74.0060\n"
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		mockDownloadToFileZIP(t, "data.csv", csvContent),
	)

	pool.ExpectBegin().WillReturnError(errors.New("db error"))

	ds := &EPAECHO{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "epa_echo")
}

// =====================================================================
// OEWS processZip — fallback to first CSV (non-"nat" file)
// =====================================================================

func TestOEWS_ProcessZip_FallbackCSV(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	// Create ZIP with a file that doesn't match "nat" in name
	tmpDir := t.TempDir()
	zipPath := filepath.Join(tmpDir, "oews.zip")

	csvContent := "area,area_type,naics,occ_code,tot_emp,h_mean,a_mean,h_median,a_median\n00001,1,523110,11-1011,100,50.0,100000,45.0,90000\n"

	zf, err := os.Create(zipPath)
	require.NoError(t, err)
	w := zip.NewWriter(zf)
	fw, err := w.Create("alldata.csv") // NOT "nat" in name
	require.NoError(t, err)
	_, _ = fw.Write([]byte(csvContent))
	_ = w.Close()
	_ = zf.Close()

	oewsCols := []string{"area_code", "area_type", "naics", "occ_code", "year", "tot_emp", "h_mean", "a_mean", "h_median", "a_median"}
	expectBulkUpsert(pool, "fed_data.oews_data", oewsCols, 1)

	ds := &OEWS{}
	n, err := ds.processZip(context.Background(), pool, zipPath, 2023)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	assert.NoError(t, pool.ExpectationsWereMet())
}

// =====================================================================
// FPDS — upsertContracts mid-batch (fpdsBatchSize=1000)
// =====================================================================

func TestFPDS_UpsertContracts_MidBatch(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	// 5002 rows → 5000 + 2
	var rows [][]any
	for i := 0; i < 5002; i++ {
		rows = append(rows, []any{
			fmt.Sprintf("C%d", i), "PIID", "AG01", "Agency", "Vendor", "DUNS", "UEI",
			"City", "ST", "00000", "523110", "PS1", nil, 1000.0, "desc",
		})
	}

	contractCols := []string{"contract_id", "piid", "agency_id", "agency_name", "vendor_name", "vendor_duns", "vendor_uei", "vendor_city", "vendor_state", "vendor_zip", "naics", "psc", "date_signed", "dollars_obligated", "description"}
	expectBulkUpsert(pool, "fed_data.fpds_contracts", contractCols, 5000)
	expectBulkUpsert(pool, "fed_data.fpds_contracts", contractCols, 2)

	ds := &FPDS{}
	n, err := ds.upsertContracts(context.Background(), pool, rows)
	require.NoError(t, err)
	assert.Equal(t, int64(5002), n)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestEDGARSubmissions_DecodeSubmission_Invalid(t *testing.T) {
	ds := &EDGARSubmissions{}
	_, err := ds.decodeSubmission(strings.NewReader("invalid"))
	assert.Error(t, err)
}

