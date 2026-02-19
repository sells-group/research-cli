package dataset

import (
	"archive/zip"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	fetchermocks "github.com/sells-group/research-cli/internal/fetcher/mocks"
)

// nopLog returns a no-op zap logger for tests.
func nopLog() *zap.Logger { return zap.NewNop() }

// createMultiZIP creates a ZIP with multiple named files.
func createMultiZIP(t *testing.T, zipPath string, files map[string][]byte) {
	t.Helper()
	f, err := os.Create(zipPath)
	require.NoError(t, err)
	defer f.Close()
	w := zip.NewWriter(f)
	for name, data := range files {
		entry, err := w.Create(name)
		require.NoError(t, err)
		_, err = entry.Write(data)
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
}

// --------------------------------------------------------------------------
// ADV Part 1 - Sync coverage (weekly FOIA)
// --------------------------------------------------------------------------

func TestADVPart1_Sync_MetadataFetchFails(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	// Metadata fetch fails.
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "reports_metadata.json")
	})).Return(nil, errors.New("connection refused"))

	ds := &ADVPart1{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "FOIA metadata")
}

func TestADVPart1_Sync_DownloadFails(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	// Metadata fetch succeeds.
	meta := foiaReportsMetadata{
		ADVFilingData: []foiaFileEntry{
			{FileName: "ADV_Filing_Data_20260101_20260131.zip", Year: "2026", UploadedOn: "2026-02-02 14:00:00"},
		},
	}
	f.EXPECT().Download(mock.Anything, mock.Anything).Return(foiaMetadataBody(t, meta), nil)
	// ZIP download fails.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), errors.New("404 not found"))

	ds := &ADVPart1{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestADVPart1_Sync_WithFOIAZip(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Historical filing-level format (ERA_ADV_Base) — columns: FilingID, 1A, 1D, 1E1, 1F1-City, 1F1-State, 1F1-Country, DateSubmitted.
	csvContent := "FilingID,1A,1D,1E1,1F1-City,1F1-State,1F1-Country,5F2C,5F2F,5H,DateSubmitted\n" +
		"2041401,Acme Advisors,801-12345,12345,New York,NY,United States,5000000,100,11-25,03/15/2025\n"

	// Metadata fetch succeeds.
	meta := foiaReportsMetadata{
		ADVFilingData: []foiaFileEntry{
			{FileName: "ADV_Filing_Data_20250301_20250331.zip", Year: "2025", UploadedOn: "2025-04-02 10:00:00"},
		},
	}
	f.EXPECT().Download(mock.Anything, mock.Anything).Return(foiaMetadataBody(t, meta), nil)

	// ZIP download succeeds — file must be named ERA_ADV_Base_*.csv.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, "ERA_ADV_Base_20250301_20250331.csv", csvContent)).Once()

	firmCols := []string{"crd_number", "firm_name", "sec_number", "city", "state", "country", "website"}

	expectBulkUpsert(pool, "fed_data.adv_firms", firmCols, 1)
	expectBulkUpsert(pool, "fed_data.adv_filings", advFilingCols, 1)

	ds := &ADVPart1{}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
	assert.Equal(t, int64(1), result.Metadata["firms"])
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestADVPart1_Sync_EmptyMetadata(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Metadata returns empty ADVFilingData array.
	meta := foiaReportsMetadata{ADVFilingData: []foiaFileEntry{}}
	f.EXPECT().Download(mock.Anything, mock.Anything).Return(foiaMetadataBody(t, meta), nil)

	ds := &ADVPart1{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no advFilingData entries")
}

func TestADVPart1_SyncFull_HistoricalFailsContinuesToFOIA(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	// Historical Part 1 download fails → SyncFull logs and continues to FOIA months.
	f.EXPECT().DownloadToFile(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "part1.zip")
	}), mock.Anything).Return(int64(0), errors.New("download failed"))

	// FOIA metadata fetch returns empty → 0 months to process.
	meta := foiaReportsMetadata{ADVFilingData: []foiaFileEntry{}}
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "reports_metadata.json")
	})).Return(foiaMetadataBody(t, meta), nil)

	ds := &ADVPart1{}
	result, err := ds.SyncFull(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

// --------------------------------------------------------------------------
// EDGAR Submissions - additional coverage
// --------------------------------------------------------------------------

func TestEDGARSubmissions_Sync_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), errors.New("download failed"))

	ds := &EDGARSubmissions{cfg: &config.Config{}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download ZIP")
}

func TestEDGARSubmissions_Sync_SkipsNonJSON(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	tempDir := t.TempDir()

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, path string) (int64, error) {
			createTestZIP(t, path, "README.txt", "readme content")
			return 100, nil
		})

	ds := &EDGARSubmissions{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, tempDir)
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestEDGARSubmissions_Sync_SkipsEmptyName(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	tempDir := t.TempDir()

	subJSON := `{"cik":"1234567","name":"","filings":{"recent":{"accessionNumber":[],"filingDate":[],"form":[],"primaryDocument":[],"primaryDocDescription":[],"items":[],"size":[],"isXBRL":[],"isInlineXBRL":[]}}}`

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, path string) (int64, error) {
			createTestZIP(t, path, "CIK0001234567.json", subJSON)
			return int64(len(subJSON)), nil
		})

	ds := &EDGARSubmissions{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, tempDir)
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestEDGARSubmissions_Sync_SkipsFilingsPrefix(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	tempDir := t.TempDir()

	subJSON := `{"cik":"999","name":"Skip Me","filings":{"recent":{"accessionNumber":[],"filingDate":[],"form":[],"primaryDocument":[],"primaryDocDescription":[],"items":[],"size":[],"isXBRL":[],"isInlineXBRL":[]}}}`

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, path string) (int64, error) {
			createTestZIP(t, path, "filings-recent-CIK999.json", subJSON)
			return int64(len(subJSON)), nil
		})

	ds := &EDGARSubmissions{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, tempDir)
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestEDGARSubmissions_Sync_MultipleEntities(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)
	tempDir := t.TempDir()

	sub1 := `{"cik":"111","name":"Corp A","entityType":"op","sic":"6200","sicDescription":"Sec","stateOfIncorporation":"NY","ein":"111","tickers":[],"exchanges":[],"filings":{"recent":{"accessionNumber":["ACC-1"],"filingDate":["2024-01-01"],"form":["10-K"],"primaryDocument":["d.htm"],"primaryDocDescription":["AR"],"items":[""],"size":[100],"isXBRL":[0],"isInlineXBRL":[0]}}}`
	sub2 := `{"cik":"222","name":"Corp B","entityType":"op","sic":"6300","sicDescription":"Ins","stateOfIncorporation":"CA","ein":"222","tickers":["B"],"exchanges":["NASDAQ"],"filings":{"recent":{"accessionNumber":["ACC-2"],"filingDate":["2024-02-01"],"form":["10-Q"],"primaryDocument":["q.htm"],"primaryDocDescription":["QR"],"items":["1"],"size":[200],"isXBRL":[1],"isInlineXBRL":[1]}}}`

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, path string) (int64, error) {
			createMultiZIP(t, path, map[string][]byte{
				"CIK0000000111.json": []byte(sub1),
				"CIK0000000222.json": []byte(sub2),
			})
			return 2000, nil
		})

	entityCols := []string{"cik", "entity_name", "entity_type", "sic", "sic_description", "state_of_inc", "state_of_business", "ein", "tickers", "exchanges"}
	filingCols := []string{"accession_number", "cik", "form_type", "filing_date", "primary_doc", "primary_doc_desc", "items", "size", "is_xbrl", "is_inline_xbrl"}

	expectBulkUpsert(pool, "fed_data.edgar_entities", entityCols, 2)
	expectBulkUpsert(pool, "fed_data.edgar_filings", filingCols, 2)

	ds := &EDGARSubmissions{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, tempDir)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	assert.Equal(t, int64(2), result.Metadata["entities"])
	assert.Equal(t, int64(2), result.Metadata["filings"])
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestEDGARSubmissions_ParseSubmissionFile_Success(t *testing.T) {
	tempDir := t.TempDir()
	jsonData := `{"cik":"9999999","name":"Test Corp","filings":{"recent":{"accessionNumber":[],"filingDate":[],"form":[],"primaryDocument":[],"primaryDocDescription":[],"items":[],"size":[],"isXBRL":[],"isInlineXBRL":[]}}}`
	jsonPath := filepath.Join(tempDir, "CIK9999999.json")
	require.NoError(t, os.WriteFile(jsonPath, []byte(jsonData), 0o644))

	ds := &EDGARSubmissions{}
	sub, err := ds.parseSubmissionFile(jsonPath)
	require.NoError(t, err)
	assert.Equal(t, "Test Corp", sub.Name)
	assert.Equal(t, "9999999", sub.CIK)
}

func TestEDGARSubmissions_ParseSubmissionFile_NotFound(t *testing.T) {
	ds := &EDGARSubmissions{}
	_, err := ds.parseSubmissionFile("/nonexistent/path.json")
	assert.Error(t, err)
}

// --------------------------------------------------------------------------
// Holdings 13F - full Sync flow
// --------------------------------------------------------------------------

func TestHoldings13F_Sync_Full(t *testing.T) {
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
						"entity_cik":       "1234567",
						"entity_name":      "Acme Capital",
						"form_type":        "13F-HR",
						"file_date":        "2024-06-15",
						"accession_no":     "0001234567-24-000001",
						"period_of_report": "2024-03-31",
					},
				},
			},
		},
	}

	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "search-index") && strings.Contains(url, "13F-HR")
	})).Return(jsonBody(t, searchResult), nil)

	filerCols := []string{"cik", "company_name", "form_type", "filing_date", "period_of_report", "total_value"}
	expectBulkUpsert(pool, "fed_data.f13_filers", filerCols, 1)

	holdingsXML := `<?xml version="1.0"?>
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
</informationTable>`

	f.EXPECT().DownloadToFile(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "Archives/edgar")
	}), mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, path string) (int64, error) {
			return int64(len(holdingsXML)), os.WriteFile(path, []byte(holdingsXML), 0o644)
		})

	holdingsCols := []string{"cik", "period", "cusip", "issuer_name", "class_title", "value", "shares", "sh_prn_type", "put_call"}
	expectBulkUpsert(pool, "fed_data.f13_holdings", holdingsCols, 1)

	pool.ExpectExec("UPDATE fed_data.f13_filers SET total_value").
		WithArgs(int64(150000000), "1234567").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	ds := &Holdings13F{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, tempDir)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestHoldings13F_Sync_SearchError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().Download(mock.Anything, mock.Anything).Return(nil, errors.New("EFTS error"))

	ds := &Holdings13F{cfg: &config.Config{}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "search EFTS")
}

func TestHoldings13F_Sync_EmptySearchResults(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	searchResult := map[string]any{
		"hits": map[string]any{
			"total": 0,
			"hits":  []map[string]any{},
		},
	}
	f.EXPECT().Download(mock.Anything, mock.Anything).Return(jsonBody(t, searchResult), nil)

	ds := &Holdings13F{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestHoldings13F_DownloadAndParseHoldings_Success(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	tempDir := t.TempDir()

	holdingsXML := `<?xml version="1.0"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  <infoTable>
    <nameOfIssuer>Google LLC</nameOfIssuer>
    <titleOfClass>CL A</titleOfClass>
    <cusip>02079K107</cusip>
    <value>50000</value>
    <shrsOrPrnAmt>
      <sshPrnamt>200</sshPrnamt>
      <sshPrnamtType>SH</sshPrnamtType>
    </shrsOrPrnAmt>
  </infoTable>
</informationTable>`

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, path string) (int64, error) {
			return int64(len(holdingsXML)), os.WriteFile(path, []byte(holdingsXML), 0o644)
		})

	holdingsCols := []string{"cik", "period", "cusip", "issuer_name", "class_title", "value", "shares", "sh_prn_type", "put_call"}
	expectBulkUpsert(pool, "fed_data.f13_holdings", holdingsCols, 1)

	ds := &Holdings13F{}
	rows, err := ds.downloadAndParseHoldings(context.Background(), f, pool, "https://example.com/13f.xml", "9876543", nil, tempDir, nopLog())
	require.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, "02079K107", rows[0][2])
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestHoldings13F_DownloadAndParseHoldings_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), errors.New("404 not found"))

	ds := &Holdings13F{}
	_, err = ds.downloadAndParseHoldings(context.Background(), f, pool, "https://example.com/13f.xml", "123", nil, t.TempDir(), nopLog())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download 13F holdings")
}

// --------------------------------------------------------------------------
// Form D - full Sync flow
// --------------------------------------------------------------------------

func TestFormD_Sync_Full(t *testing.T) {
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
						"entity_cik":   "1234567",
						"entity_name":  "Acme Fund LP",
						"form_type":    "D",
						"file_date":    "2024-06-15",
						"accession_no": "0001234567-24-000001",
					},
				},
			},
		},
	}

	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "search-index") && strings.Contains(url, "forms=D")
	})).Return(jsonBody(t, searchResult), nil)

	formDXML := `<?xml version="1.0"?>
<edgarSubmission>
  <headerData><accessionNumber>0001234567-24-000001</accessionNumber></headerData>
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
      <industryGroup><industryGroupType>Pooled Investment Fund</industryGroupType></industryGroup>
      <issuerSize><revenueRange>Decline to Disclose</revenueRange></issuerSize>
      <offeringSalesAmounts>
        <totalOfferingAmount>50000000</totalOfferingAmount>
        <totalAmountSold>25000000</totalAmountSold>
      </offeringSalesAmounts>
    </offeringData>
  </formData>
</edgarSubmission>`

	f.EXPECT().DownloadToFile(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "Archives/edgar")
	}), mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, path string) (int64, error) {
			return int64(len(formDXML)), os.WriteFile(path, []byte(formDXML), 0o644)
		})

	formDCols := []string{"accession_number", "cik", "entity_name", "entity_type", "year_of_inc", "state_of_inc", "industry_group", "revenue_range", "total_offering", "total_sold", "filing_date"}
	expectBulkUpsert(pool, "fed_data.form_d", formDCols, 1)

	ds := &FormD{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, tempDir)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestFormD_Sync_SearchError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().Download(mock.Anything, mock.Anything).Return(nil, errors.New("EFTS down"))

	ds := &FormD{cfg: &config.Config{}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "search EFTS")
}

func TestFormD_Sync_XMLDownloadFallback(t *testing.T) {
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
						"entity_cik":   "9999999",
						"entity_name":  "Fallback Corp",
						"form_type":    "D",
						"file_date":    "2024-07-01",
						"accession_no": "0009999999-24-000001",
					},
				},
			},
		},
	}

	f.EXPECT().Download(mock.Anything, mock.Anything).Return(jsonBody(t, searchResult), nil)

	// XML download fails -> falls back to search metadata.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), errors.New("404 not found"))

	formDCols := []string{"accession_number", "cik", "entity_name", "entity_type", "year_of_inc", "state_of_inc", "industry_group", "revenue_range", "total_offering", "total_sold", "filing_date"}
	expectBulkUpsert(pool, "fed_data.form_d", formDCols, 1)

	ds := &FormD{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, tempDir)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestFormD_Sync_EmptySearchResults(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	searchResult := map[string]any{
		"hits": map[string]any{
			"total": 0,
			"hits":  []map[string]any{},
		},
	}
	f.EXPECT().Download(mock.Anything, mock.Anything).Return(jsonBody(t, searchResult), nil)

	ds := &FormD{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

// --------------------------------------------------------------------------
// IA Compilation - additional Sync coverage
// --------------------------------------------------------------------------

func TestIACompilation_Sync_ManifestUnavailable(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Manifest fetch fails → returns 0 rows with no error.
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "CompilationReports.manifest.json")
	})).Return(nil, errors.New("connection refused"))

	ds := &IACompilation{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	assert.Equal(t, "manifest_unavailable", result.Metadata["status"])
}

// --------------------------------------------------------------------------
// IA Compilation - full Sync flow
// --------------------------------------------------------------------------

func TestIACompilation_ParseAndLoad_XMLParsing(t *testing.T) {
	// Test the parseAndLoad method directly (Sync is now short-circuited).
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	xmlContent := `<?xml version="1.0"?>
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
  </Firms>
</IAPDFirmSECReport>`

	iaFirmCols := []string{"crd_number", "firm_name", "sec_number", "city", "state", "country", "website"}
	iaFilingCols := []string{"crd_number", "filing_date", "aum", "num_accounts", "legal_name", "num_employees", "total_employees", "sec_registered"}
	expectBulkUpsert(pool, "fed_data.adv_firms", iaFirmCols, 1)
	expectBulkUpsert(pool, "fed_data.adv_filings", iaFilingCols, 1)

	ds := &IACompilation{cfg: &config.Config{}}
	result, err := ds.parseAndLoad(context.Background(), pool, strings.NewReader(xmlContent), nopLog())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

// --------------------------------------------------------------------------
// BrokerCheck - full Sync flow + additional coverage
// --------------------------------------------------------------------------

func TestBrokerCheck_Sync_Success(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	csvContent := "crd_number|firm_name|sec_number|main_addr_city|main_addr_state|num_branch_offices|num_registered_reps\n12345|Acme Advisors|801-12345|New York|NY|5|25\n67890|Beta Capital|801-67890|Chicago|IL|3|15\n"

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, "firm.txt", csvContent))

	bcCols := []string{"crd_number", "firm_name", "sec_number", "main_addr_city", "main_addr_state", "num_branch_offices", "num_registered_reps"}
	expectBulkUpsert(pool, "fed_data.brokercheck", bcCols, 2)

	ds := &BrokerCheck{}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestBrokerCheck_Sync_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), errors.New("download failed"))

	ds := &BrokerCheck{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestBrokerCheck_Sync_SkipShortRows(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	csvContent := "CRD|Firm Name|SEC Number|City|State|Offices|Reps\n12345|Short\n"

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, "firm.txt", csvContent))

	ds := &BrokerCheck{}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

// --------------------------------------------------------------------------
// FormBD - full Sync flow + additional coverage
// --------------------------------------------------------------------------

func TestFormBD_Sync_Success(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	csvContent := "crd_number|sec_number|firm_name|city|state|fiscal_year_end|num_reps\n11111|8-12345|Alpha Securities|Boston|MA|12|50\n22222|8-67890|Beta Brokerage|Dallas|TX|06|30\n"

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, "bd_firm.txt", csvContent))

	bdCols := []string{"crd_number", "sec_number", "firm_name", "city", "state", "fiscal_year_end", "num_reps"}
	expectBulkUpsert(pool, "fed_data.form_bd", bdCols, 2)

	ds := &FormBD{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestFormBD_Sync_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), errors.New("download error"))

	ds := &FormBD{cfg: &config.Config{}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestFormBD_Sync_SkipShortRows(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	csvContent := "CRD|SEC|Name|City|State|FYE|Reps\n11111|8-11111|Short\n"

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, "bd_firm.txt", csvContent))

	ds := &FormBD{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

// --------------------------------------------------------------------------
// OSHA ITA - full Sync flow + additional coverage
// --------------------------------------------------------------------------

func TestOSHITA_Sync_Success(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	csvContent := "activity_nr,estab_name,site_city,site_state,site_zip,naics_code,sic_code,open_date,close_case_date,case_type,safety_hlth,total_penalty\n123456789,Acme Corp,Springfield,IL,62701,523110,6211,01/15/2024,03/20/2024,R,S,5000.00\n987654321,Beta Inc,Austin,TX,78701,524210,6282,02/01/2024,,I,H,0.00\n"

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, "severeinjury.csv", csvContent))

	oshaCols := []string{"activity_nr", "estab_name", "site_city", "site_state", "site_zip", "naics_code", "sic_code", "open_date", "close_case_date", "case_type", "safety_hlth", "total_penalty"}
	expectBulkUpsert(pool, "fed_data.osha_inspections", oshaCols, 2)

	ds := &OSHITA{}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestOSHITA_Sync_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), errors.New("download failed"))

	ds := &OSHITA{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestOSHITA_Sync_SkipShortRows(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	csvContent := "a,b,c,d,e,f,g,h,i,j,k,l\n12345,Short Row,City\n"

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, "severeinjury.csv", csvContent))

	ds := &OSHITA{}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

// --------------------------------------------------------------------------
// EPA ECHO - full Sync flow + additional coverage
// --------------------------------------------------------------------------

func TestEPAECHO_Sync_Success(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	csvContent := "REGISTRY_ID,PRIMARY_NAME,CITY_NAME,STATE_CODE,POSTAL_CODE,col5,col6,LATITUDE83,LONGITUDE83\n110000001,Acme Plant,Springfield,IL,62701,x,y,39.7817,-89.6501\n110000002,Beta Factory,Austin,TX,78701,a,b,30.2672,-97.7431\n"

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, "NATIONAL_FACILITY_FILE.CSV", csvContent))

	epaCols := []string{"registry_id", "fac_name", "fac_city", "fac_state", "fac_zip", "fac_lat", "fac_long"}
	expectBulkUpsert(pool, "fed_data.epa_facilities", epaCols, 2)

	ds := &EPAECHO{}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestEPAECHO_Sync_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), errors.New("download error"))

	ds := &EPAECHO{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestEPAECHO_Sync_SkipShortRows(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	csvContent := "REGISTRY_ID,PRIMARY_NAME,CITY_NAME,STATE_CODE,POSTAL_CODE,col5,col6,LATITUDE83,LONGITUDE83\n"

	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(mockDownloadToFileZIP(t, "NATIONAL_FACILITY_FILE.CSV", csvContent))

	ds := &EPAECHO{}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

// --------------------------------------------------------------------------
// ADV Part 2 - Sync (bulk brochure ZIP)
// --------------------------------------------------------------------------

func TestADVPart2_Sync_BadOCRProvider(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	ds := &ADVPart2{cfg: &config.Config{Fedsync: config.FedsyncConfig{OCR: config.OCRConfig{Provider: "invalid_provider"}}}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "OCR extractor")
}

func TestADVPart2_Sync_MetadataFetchFails(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	// Metadata fetch fails.
	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(nil, errors.New("connection refused"))

	ds := &ADVPart2{cfg: &config.Config{Fedsync: config.FedsyncConfig{OCR: config.OCRConfig{Provider: "local"}}}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "FOIA metadata")
}

func TestADVPart2_Sync_DownloadFails(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	// Metadata succeeds.
	meta := foiaReportsMetadata{
		ADVBrochures: []foiaFileEntry{
			{FileName: "ADV_Brochures_2026_January.zip", Year: "2026", UploadedOn: "2026-02-01 10:00:00"},
		},
	}
	f.EXPECT().Download(mock.Anything, mock.Anything).Return(foiaMetadataBody(t, meta), nil)
	// ZIP download fails.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), errors.New("404 not found"))

	ds := &ADVPart2{cfg: &config.Config{Fedsync: config.FedsyncConfig{OCR: config.OCRConfig{Provider: "local"}}}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download brochure ZIP")
}

// --------------------------------------------------------------------------
// ADV Part 3 - Sync (bulk CRS ZIP via FOIA metadata)
// --------------------------------------------------------------------------

func TestADVPart3_Sync_BadOCRProvider(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	ds := &ADVPart3{cfg: &config.Config{Fedsync: config.FedsyncConfig{OCR: config.OCRConfig{Provider: "invalid_provider"}}}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "OCR extractor")
}

func TestADVPart3_Sync_MetadataFetchFails(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	// Metadata fetch fails.
	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(nil, errors.New("connection refused"))

	ds := &ADVPart3{cfg: &config.Config{Fedsync: config.FedsyncConfig{OCR: config.OCRConfig{Provider: "local"}}}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "FOIA metadata")
}

func TestADVPart3_Sync_DownloadFails(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	// Metadata succeeds.
	meta := foiaReportsMetadata{
		ADVFirmCRSDocs: []foiaFileEntry{
			{FileName: "FIRM_CRS_DOCS_MONTHLY_20260131_558.zip", Year: "2026", UploadedOn: "2026-02-01 10:00:00"},
		},
	}
	f.EXPECT().Download(mock.Anything, mock.Anything).Return(foiaMetadataBody(t, meta), nil)
	// ZIP download fails.
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), errors.New("404 not found"))

	ds := &ADVPart3{cfg: &config.Config{Fedsync: config.FedsyncConfig{OCR: config.OCRConfig{Provider: "local"}}}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download CRS ZIP")
}

// --------------------------------------------------------------------------
// XBRL Facts - additional coverage
// --------------------------------------------------------------------------

func TestXBRLFacts_Sync_AllCIKsFail(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	cikRows := pgxmock.NewRows([]string{"cik"}).
		AddRow("1111111").
		AddRow("2222222")
	pool.ExpectQuery("SELECT DISTINCT cik FROM fed_data.entity_xref").WillReturnRows(cikRows)

	f.EXPECT().Download(mock.Anything, mock.Anything).Return(nil, errors.New("not found")).Times(2)

	ds := &XBRLFacts{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

// --------------------------------------------------------------------------
// Entity Xref - additional error path coverage
// --------------------------------------------------------------------------

func TestEntityXref_Sync_Pass1Error(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	pool.ExpectExec("TRUNCATE TABLE fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("TRUNCATE", 0))
	pool.ExpectExec("INSERT INTO fed_data.entity_xref").
		WillReturnError(errors.New("pass1 error"))

	ds := &EntityXref{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pass 1")
}

func TestEntityXref_Sync_Pass2Error(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	pool.ExpectExec("TRUNCATE TABLE fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("TRUNCATE", 0))
	pool.ExpectExec("INSERT INTO fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("INSERT", 10))
	pool.ExpectExec("INSERT INTO fed_data.entity_xref").
		WillReturnError(errors.New("pass2 error"))

	ds := &EntityXref{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pass 2")
}

func TestEntityXref_Sync_Pass3Error(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	pool.ExpectExec("TRUNCATE TABLE fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("TRUNCATE", 0))
	pool.ExpectExec("INSERT INTO fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("INSERT", 10))
	pool.ExpectExec("INSERT INTO fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("INSERT", 5))
	pool.ExpectExec("INSERT INTO fed_data.entity_xref").
		WillReturnError(errors.New("pass3 error"))

	ds := &EntityXref{}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pass 3")
}
