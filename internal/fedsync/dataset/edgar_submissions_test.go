package dataset

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
	fetchermocks "github.com/sells-group/research-cli/internal/fetcher/mocks"
)

func TestEDGARSubmissions_Name(t *testing.T) {
	d := &EDGARSubmissions{}
	assert.Equal(t, "edgar_submissions", d.Name())
}

func TestEDGARSubmissions_Table(t *testing.T) {
	d := &EDGARSubmissions{}
	assert.Equal(t, "fed_data.edgar_entities", d.Table())
}

func TestEDGARSubmissions_Phase(t *testing.T) {
	d := &EDGARSubmissions{}
	assert.Equal(t, Phase1B, d.Phase())
}

func TestEDGARSubmissions_Cadence(t *testing.T) {
	d := &EDGARSubmissions{}
	assert.Equal(t, Weekly, d.Cadence())
}

func TestEDGARSubmissions_ShouldRun_NilLastSync(t *testing.T) {
	d := &EDGARSubmissions{}
	assert.True(t, d.ShouldRun(time.Now(), nil))
}

func TestEDGARSubmissions_ShouldRun_SameWeek(t *testing.T) {
	d := &EDGARSubmissions{}
	// Wednesday March 12
	now := time.Date(2025, 3, 12, 0, 0, 0, 0, time.UTC)
	// Monday March 10 (same week)
	lastSync := time.Date(2025, 3, 10, 12, 0, 0, 0, time.UTC)
	assert.False(t, d.ShouldRun(now, &lastSync))
}

func TestEDGARSubmissions_ShouldRun_PreviousWeek(t *testing.T) {
	d := &EDGARSubmissions{}
	// Wednesday March 12
	now := time.Date(2025, 3, 12, 0, 0, 0, 0, time.UTC)
	// Friday March 7 (previous week)
	lastSync := time.Date(2025, 3, 7, 12, 0, 0, 0, time.UTC)
	assert.True(t, d.ShouldRun(now, &lastSync))
}

func TestEDGARSubmissions_ShouldRun_CrossYear(t *testing.T) {
	d := &EDGARSubmissions{}
	// Thursday Jan 2
	now := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	// Monday Dec 23 (previous year, different week)
	lastSync := time.Date(2024, 12, 23, 0, 0, 0, 0, time.UTC)
	assert.True(t, d.ShouldRun(now, &lastSync))
}

func TestEDGARSubmissions_ImplementsDataset(t *testing.T) {
	var _ Dataset = &EDGARSubmissions{}
}

func TestSafeIndex(t *testing.T) {
	s := []string{"a", "b", "c"}
	assert.Equal(t, "a", safeIndex(s, 0))
	assert.Equal(t, "c", safeIndex(s, 2))
	assert.Equal(t, "", safeIndex(s, 3))
	assert.Equal(t, "", safeIndex(nil, 0))
}

func TestSafeIntIndex(t *testing.T) {
	s := []int{10, 20, 30}
	assert.Equal(t, 10, safeIntIndex(s, 0))
	assert.Equal(t, 30, safeIntIndex(s, 2))
	assert.Equal(t, 0, safeIntIndex(s, 5))
	assert.Equal(t, 0, safeIntIndex(nil, 0))
}

// makeSubmissionJSON builds a valid EDGAR submission JSON string.
func makeSubmissionJSON(t *testing.T, cik, name, entityType, sic string, filings int) string {
	t.Helper()
	sub := map[string]any{
		"cik":                  cik,
		"entityType":           entityType,
		"sic":                  sic,
		"sicDescription":       "Test SIC",
		"name":                 name,
		"stateOfIncorporation": "DE",
		"ein":                  "123456789",
		"tickers":              []string{"TEST"},
		"exchanges":            []string{"NYSE"},
		"filings": map[string]any{
			"recent": makeFilings(filings),
		},
	}
	data, err := json.Marshal(sub)
	require.NoError(t, err)
	return string(data)
}

func makeFilings(n int) map[string]any {
	accessions := make([]string, n)
	dates := make([]string, n)
	forms := make([]string, n)
	docs := make([]string, n)
	descs := make([]string, n)
	items := make([]string, n)
	sizes := make([]int, n)
	xbrl := make([]int, n)
	inline := make([]int, n)

	for i := range n {
		accessions[i] = "0001234-24-" + string(rune('A'+i)) + "00001"
		dates[i] = "2024-01-15"
		forms[i] = "10-K"
		docs[i] = "doc.htm"
		descs[i] = "Annual Report"
		items[i] = ""
		sizes[i] = 12345
		xbrl[i] = 1
		inline[i] = 1
	}

	return map[string]any{
		"accessionNumber":      accessions,
		"filingDate":           dates,
		"form":                 forms,
		"primaryDocument":      docs,
		"primaryDocDescription": descs,
		"items":                items,
		"size":                 sizes,
		"isXBRL":               xbrl,
		"isInlineXBRL":         inline,
	}
}

var edgarEntityCols = []string{"cik", "entity_name", "entity_type", "sic", "sic_description", "state_of_inc", "state_of_business", "ein", "tickers", "exchanges"}
var edgarFilingCols = []string{"accession_number", "cik", "form_type", "filing_date", "primary_doc", "primary_doc_desc", "items", "size", "is_xbrl", "is_inline_xbrl"}

func TestEDGARSubmissions_Sync_ParallelDecode(t *testing.T) {
	dir := t.TempDir()

	// Create a ZIP with 3 valid submission JSON files.
	files := map[string]string{
		"CIK0000001234.json": makeSubmissionJSON(t, "1234", "Alpha Corp", "operating", "6200", 2),
		"CIK0000005678.json": makeSubmissionJSON(t, "5678", "Beta Inc", "operating", "6200", 1),
		"CIK0000009999.json": makeSubmissionJSON(t, "9999", "Gamma LLC", "operating", "5400", 1),
	}

	zipPath := createTestZipMulti(t, dir, "submissions.zip", files)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)

	// Mock DownloadToFile to copy the pre-built ZIP to the requested path.
	mockDownloadToFile(f, zipPath)

	// 3 entities, 4 filings total (2+1+1).
	expectBulkUpsertZip(pool, "fed_data.edgar_entities", edgarEntityCols, 3)
	expectBulkUpsertZip(pool, "fed_data.edgar_filings", edgarFilingCols, 4)

	ds := &EDGARSubmissions{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.RowsSynced)
	assert.Equal(t, int64(4), result.Metadata["filings"])
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestEDGARSubmissions_Sync_DecodeError(t *testing.T) {
	dir := t.TempDir()

	// 2 valid JSONs and 1 malformed JSON.
	files := map[string]string{
		"CIK0000001234.json": makeSubmissionJSON(t, "1234", "Alpha Corp", "operating", "6200", 1),
		"CIK0000005678.json": "{ invalid json !!!",
		"CIK0000009999.json": makeSubmissionJSON(t, "9999", "Gamma LLC", "operating", "5400", 1),
	}

	zipPath := createTestZipMulti(t, dir, "submissions.zip", files)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)
	mockDownloadToFile(f, zipPath)

	// 2 valid entities, 2 filings (1+1). Malformed file is skipped.
	expectBulkUpsertZip(pool, "fed_data.edgar_entities", edgarEntityCols, 2)
	expectBulkUpsertZip(pool, "fed_data.edgar_filings", edgarFilingCols, 2)

	ds := &EDGARSubmissions{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestEDGARSubmissions_Sync_IgnoresFilingsPrefix(t *testing.T) {
	dir := t.TempDir()

	// "filings-*.json" files should be skipped per the filter logic.
	files := map[string]string{
		"CIK0000001234.json":     makeSubmissionJSON(t, "1234", "Alpha Corp", "operating", "6200", 1),
		"filings-recent-001.json": `{"accessionNumber": ["0001-24-000001"]}`,
	}

	zipPath := createTestZipMulti(t, dir, "submissions.zip", files)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)
	mockDownloadToFile(f, zipPath)

	// Only 1 entity, 1 filing. The filings- file is ignored.
	expectBulkUpsertZip(pool, "fed_data.edgar_entities", edgarEntityCols, 1)
	expectBulkUpsertZip(pool, "fed_data.edgar_filings", edgarFilingCols, 1)

	ds := &EDGARSubmissions{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestEDGARSubmissions_Sync_DownloadZIPError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)
	f.EXPECT().DownloadToFile(mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), assert.AnError)

	ds := &EDGARSubmissions{cfg: &config.Config{}}
	_, err = ds.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestEDGARSubmissions_Sync_SkipsEmptyNameEntity(t *testing.T) {
	dir := t.TempDir()

	// Submission with empty name -> should be skipped.
	emptyNameSub := map[string]any{
		"cik":        "9999",
		"entityType": "operating",
		"sic":        "6200",
		"name":       "",
		"filings":    map[string]any{"recent": makeFilings(0)},
	}
	data, err := json.Marshal(emptyNameSub)
	require.NoError(t, err)

	files := map[string]string{
		"CIK0000009999.json": string(data),
		"CIK0000001234.json": makeSubmissionJSON(t, "1234", "Valid Corp", "operating", "6200", 1),
	}

	zipPath := createTestZipMulti(t, dir, "submissions.zip", files)

	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	pool.MatchExpectationsInOrder(false)

	f := fetchermocks.NewMockFetcher(t)
	mockDownloadToFile(f, zipPath)

	// Only 1 valid entity (empty name is skipped).
	expectBulkUpsertZip(pool, "fed_data.edgar_entities", edgarEntityCols, 1)
	expectBulkUpsertZip(pool, "fed_data.edgar_filings", edgarFilingCols, 1)

	ds := &EDGARSubmissions{cfg: &config.Config{}}
	result, err := ds.Sync(context.Background(), pool, f, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}
