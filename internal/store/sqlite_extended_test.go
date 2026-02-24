package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/model"
)

// TestNewSQLite_InvalidDSN verifies that NewSQLite returns an error for
// an invalid DSN (e.g., a path inside a nonexistent directory).
func TestNewSQLite_InvalidDSN(t *testing.T) {
	// Use a path that cannot be created (nested under a nonexistent parent).
	_, err := NewSQLite("/nonexistent/dir/subdir/test.db")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sqlite")
}

// TestNewSQLite_ValidPath confirms NewSQLite succeeds with a valid path and
// sets up WAL mode properly.
func TestNewSQLite_ValidPath(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "valid.db")
	s, err := NewSQLite(dbPath)
	require.NoError(t, err)
	require.NotNil(t, s)
	t.Cleanup(func() { s.Close() }) //nolint:errcheck

	// Verify WAL mode was set by querying the journal_mode pragma.
	var mode string
	err = s.db.QueryRow("PRAGMA journal_mode").Scan(&mode)
	require.NoError(t, err)
	assert.Equal(t, "wal", mode)
}

// TestNewSQLite_CloseAndReopen verifies the database can be closed and reopened.
func TestNewSQLite_CloseAndReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "reopen.db")

	s1, err := NewSQLite(dbPath)
	require.NoError(t, err)
	require.NoError(t, s1.Migrate(context.Background()))
	require.NoError(t, s1.Close())

	s2, err := NewSQLite(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { s2.Close() }) //nolint:errcheck

	// Tables should already exist from the first migration.
	ctx := context.Background()
	_, err = s2.CreateRun(ctx, model.Company{URL: "https://test.com", Name: "Test"})
	require.NoError(t, err)
}

// TestScanRun_NotFound exercises the sql.ErrNoRows path inside scanRun.
func TestScanRun_NotFound(t *testing.T) {
	s := newTestSQLiteRaw(t)
	ctx := context.Background()

	_, err := s.GetRun(ctx, "totally-missing-id")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// TestScanRun_WithResult verifies scanRun correctly unmarshals runs that have
// a non-null result JSON column (covers the resultJSON.Valid branch).
func TestScanRun_WithResult(t *testing.T) {
	s := newTestSQLiteRaw(t)
	ctx := context.Background()

	run, err := s.CreateRun(ctx, model.Company{URL: "https://test.com", Name: "Test"})
	require.NoError(t, err)

	result := &model.RunResult{
		Score:       0.92,
		FieldsFound: 18,
		FieldsTotal: 20,
		TotalTokens: 40000,
		TotalCost:   0.99,
		Report:      "Great results",
	}
	err = s.UpdateRunResult(ctx, run.ID, result)
	require.NoError(t, err)

	got, err := s.GetRun(ctx, run.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Result)
	assert.InDelta(t, 0.92, got.Result.Score, 0.001)
	assert.Equal(t, 18, got.Result.FieldsFound)
	assert.Equal(t, 20, got.Result.FieldsTotal)
	assert.Equal(t, 40000, got.Result.TotalTokens)
	assert.InDelta(t, 0.99, got.Result.TotalCost, 0.001)
	assert.Equal(t, "Great results", got.Result.Report)
}

// TestScanRun_CorruptCompanyJSON covers the error path where company JSON is
// invalid (can't be unmarshalled).
func TestScanRun_CorruptCompanyJSON(t *testing.T) {
	s := newTestSQLiteRaw(t)
	ctx := context.Background()

	// Insert a row with corrupt company JSON directly via SQL.
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO runs (id, company, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		"corrupt-company-id", "not-valid-json{{{", "queued", time.Now().UTC(), time.Now().UTC(),
	)
	require.NoError(t, err)

	_, err = s.GetRun(ctx, "corrupt-company-id")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal company")
}

// TestScanRun_CorruptResultJSON covers the error path where result JSON is
// present but invalid.
func TestScanRun_CorruptResultJSON(t *testing.T) {
	s := newTestSQLiteRaw(t)
	ctx := context.Background()

	companyJSON, _ := json.Marshal(model.Company{URL: "https://test.com", Name: "Test"})
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO runs (id, company, status, result, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
		"corrupt-result-id", string(companyJSON), "complete", "not-valid-json{{{", time.Now().UTC(), time.Now().UTC(),
	)
	require.NoError(t, err)

	_, err = s.GetRun(ctx, "corrupt-result-id")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal result")
}

// TestCheckRowsAffected_ZeroRows verifies the "not found" error when no rows
// are affected.
func TestCheckRowsAffected_ZeroRows(t *testing.T) {
	res := &fakeResult{rowsAffected: 0, err: nil}
	err := checkRowsAffected(res, "widget", "abc-123")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "widget not found: abc-123")
}

// TestCheckRowsAffected_Error verifies error propagation from RowsAffected().
func TestCheckRowsAffected_Error(t *testing.T) {
	res := &fakeResult{rowsAffected: 0, err: assert.AnError}
	err := checkRowsAffected(res, "widget", "abc-123")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rows affected")
}

// TestCheckRowsAffected_Success verifies nil error when rows > 0.
func TestCheckRowsAffected_Success(t *testing.T) {
	res := &fakeResult{rowsAffected: 1, err: nil}
	err := checkRowsAffected(res, "widget", "abc-123")
	require.NoError(t, err)
}

// TestUpdateRunStatus_NonexistentRun verifies the "not found" error when
// updating status of a run that does not exist.
func TestUpdateRunStatus_NonexistentRun(t *testing.T) {
	s := newTestSQLiteRaw(t)
	ctx := context.Background()

	err := s.UpdateRunStatus(ctx, "does-not-exist", model.RunStatusCrawling)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// TestUpdateRunResult_NonexistentRun verifies the "not found" error for
// UpdateRunResult on a missing run.
func TestUpdateRunResult_NonexistentRun(t *testing.T) {
	s := newTestSQLiteRaw(t)
	ctx := context.Background()

	err := s.UpdateRunResult(ctx, "does-not-exist", &model.RunResult{Score: 0.5})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// TestCompletePhase_NonexistentPhase verifies the "not found" error when
// completing a phase that does not exist.
func TestCompletePhase_NonexistentPhase(t *testing.T) {
	s := newTestSQLiteRaw(t)
	ctx := context.Background()

	result := &model.PhaseResult{
		Name:   "crawl",
		Status: model.PhaseStatusComplete,
	}
	err := s.CompletePhase(ctx, "does-not-exist", result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// TestCreatePhase_InvalidRunID verifies that creating a phase with a
// non-existent run ID fails with a foreign key error (SQLite enforces FK).
func TestCreatePhase_InvalidRunID(t *testing.T) {
	s := newTestSQLiteRaw(t)
	ctx := context.Background()

	// Enable foreign key enforcement.
	_, err := s.db.ExecContext(ctx, "PRAGMA foreign_keys = ON")
	require.NoError(t, err)

	_, err = s.CreatePhase(ctx, "nonexistent-run-id", "crawl")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sqlite: insert phase")
}

// TestGetCachedCrawl_NoMatch verifies that GetCachedCrawl returns nil when
// there is no matching entry (cache miss).
func TestGetCachedCrawl_NoMatch(t *testing.T) {
	s := newTestSQLiteRaw(t)
	ctx := context.Background()

	got, err := s.GetCachedCrawl(ctx, "https://nonexistent.com")
	require.NoError(t, err)
	assert.Nil(t, got)
}

// TestGetCachedCrawl_ExpiredEntry verifies that GetCachedCrawl returns nil
// for an expired entry.
func TestGetCachedCrawl_ExpiredEntry(t *testing.T) {
	s := newTestSQLiteRaw(t)
	ctx := context.Background()

	pages := []model.CrawledPage{
		{URL: "https://expired.com/", Title: "Expired", Markdown: "# Expired", StatusCode: 200},
	}
	err := s.SetCachedCrawl(ctx, "https://expired.com", pages, -2*time.Hour)
	require.NoError(t, err)

	got, err := s.GetCachedCrawl(ctx, "https://expired.com")
	require.NoError(t, err)
	assert.Nil(t, got)
}

// TestGetCachedCrawl_CorruptPagesJSON covers the error path where pages JSON
// stored in the database is corrupt.
func TestGetCachedCrawl_CorruptPagesJSON(t *testing.T) {
	s := newTestSQLiteRaw(t)
	ctx := context.Background()

	expiresAt := time.Now().UTC().Add(24 * time.Hour)
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO crawl_cache (id, company_url, pages, crawled_at, expires_at) VALUES (?, ?, ?, ?, ?)`,
		"corrupt-pages-id", "https://corrupt.com", "not-valid-json{{{",
		time.Now().UTC(), expiresAt,
	)
	require.NoError(t, err)

	_, err = s.GetCachedCrawl(ctx, "https://corrupt.com")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal cached pages")
}

// TestDeleteExpiredCrawls_MultipleExpired verifies that multiple expired
// entries are cleaned up in one call.
func TestDeleteExpiredCrawls_MultipleExpired(t *testing.T) {
	s := newTestSQLiteRaw(t)
	ctx := context.Background()

	pages := []model.CrawledPage{
		{URL: "https://example.com/", Title: "Ex", Markdown: "# Ex", StatusCode: 200},
	}

	// Insert 3 expired entries for different URLs.
	for _, url := range []string{"https://a.com", "https://b.com", "https://c.com"} {
		err := s.SetCachedCrawl(ctx, url, pages, -1*time.Hour)
		require.NoError(t, err)
	}

	// Also insert 1 non-expired entry.
	err := s.SetCachedCrawl(ctx, "https://live.com", pages, 24*time.Hour)
	require.NoError(t, err)

	n, err := s.DeleteExpiredCrawls(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, n)

	// The live entry should still be accessible.
	got, err := s.GetCachedCrawl(ctx, "https://live.com")
	require.NoError(t, err)
	require.NotNil(t, got)
}

// TestCreateRun_MultipleThenList verifies CreateRun works for multiple runs
// and ListRuns returns them in descending order.
func TestCreateRun_MultipleThenList(t *testing.T) {
	s := newTestSQLiteRaw(t)
	ctx := context.Background()

	r1, err := s.CreateRun(ctx, model.Company{URL: "https://first.com", Name: "First"})
	require.NoError(t, err)
	r2, err := s.CreateRun(ctx, model.Company{URL: "https://second.com", Name: "Second"})
	require.NoError(t, err)

	runs, err := s.ListRuns(ctx, RunFilter{})
	require.NoError(t, err)
	assert.Len(t, runs, 2)

	// Most recent first (descending by created_at).
	assert.Equal(t, r2.ID, runs[0].ID)
	assert.Equal(t, r1.ID, runs[1].ID)
}

// TestUpdateRunStatus_MultipleTransitions verifies a run can transition
// through multiple status values.
func TestUpdateRunStatus_MultipleTransitions(t *testing.T) {
	s := newTestSQLiteRaw(t)
	ctx := context.Background()

	run, err := s.CreateRun(ctx, model.Company{URL: "https://multi.com", Name: "Multi"})
	require.NoError(t, err)

	transitions := []model.RunStatus{
		model.RunStatusCrawling,
		model.RunStatusClassifying,
		model.RunStatusExtracting,
		model.RunStatusAggregating,
		model.RunStatusWritingSF,
		model.RunStatusComplete,
	}

	for _, status := range transitions {
		err := s.UpdateRunStatus(ctx, run.ID, status)
		require.NoError(t, err)

		got, err := s.GetRun(ctx, run.ID)
		require.NoError(t, err)
		assert.Equal(t, status, got.Status)
	}
}

// TestCompletePhase_WithFailedStatus verifies that CompletePhase correctly
// stores a "failed" phase result.
func TestCompletePhase_WithFailedStatus(t *testing.T) {
	s := newTestSQLiteRaw(t)
	ctx := context.Background()

	run, err := s.CreateRun(ctx, model.Company{URL: "https://test.com", Name: "Test"})
	require.NoError(t, err)

	phase, err := s.CreatePhase(ctx, run.ID, "extract")
	require.NoError(t, err)

	result := &model.PhaseResult{
		Name:     "extract",
		Status:   model.PhaseStatusFailed,
		Duration: 500,
		Error:    "timeout connecting to API",
	}

	err = s.CompletePhase(ctx, phase.ID, result)
	require.NoError(t, err)

	// Verify by reading the phase row directly.
	var status, resultJSON string
	err = s.db.QueryRowContext(ctx,
		`SELECT status, result FROM run_phases WHERE id = ?`, phase.ID,
	).Scan(&status, &resultJSON)
	require.NoError(t, err)
	assert.Equal(t, string(model.PhaseStatusFailed), status)

	var stored model.PhaseResult
	require.NoError(t, json.Unmarshal([]byte(resultJSON), &stored))
	assert.Equal(t, "timeout connecting to API", stored.Error)
}

// TestCompletePhase_WithMetadata verifies that phase metadata is stored
// correctly.
func TestCompletePhase_WithMetadata(t *testing.T) {
	s := newTestSQLiteRaw(t)
	ctx := context.Background()

	run, err := s.CreateRun(ctx, model.Company{URL: "https://test.com", Name: "Test"})
	require.NoError(t, err)

	phase, err := s.CreatePhase(ctx, run.ID, "crawl")
	require.NoError(t, err)

	result := &model.PhaseResult{
		Name:     "crawl",
		Status:   model.PhaseStatusComplete,
		Duration: 2500,
		Metadata: map[string]any{
			"pages_found": float64(15),
			"source":      "local",
			"blocked":     false,
		},
	}

	err = s.CompletePhase(ctx, phase.ID, result)
	require.NoError(t, err)

	// Verify metadata was stored.
	var resultJSON string
	err = s.db.QueryRowContext(ctx,
		`SELECT result FROM run_phases WHERE id = ?`, phase.ID,
	).Scan(&resultJSON)
	require.NoError(t, err)

	var stored model.PhaseResult
	require.NoError(t, json.Unmarshal([]byte(resultJSON), &stored))
	assert.Equal(t, float64(15), stored.Metadata["pages_found"])
	assert.Equal(t, "local", stored.Metadata["source"])
}

// TestMigrate_Idempotent verifies that calling Migrate multiple times is safe.
func TestMigrate_Idempotent(t *testing.T) {
	s := newTestSQLiteRaw(t)
	ctx := context.Background()

	// Second migrate should succeed (CREATE TABLE IF NOT EXISTS).
	err := s.Migrate(ctx)
	require.NoError(t, err)

	// Third time for good measure.
	err = s.Migrate(ctx)
	require.NoError(t, err)
}

// TestCreateRun_CompanyWithAllFields verifies that all Company fields are
// round-tripped through JSON serialization.
func TestCreateRun_CompanyWithAllFields(t *testing.T) {
	s := newTestSQLiteRaw(t)
	ctx := context.Background()

	company := model.Company{
		URL:          "https://full.com",
		Name:         "Full Company Inc",
		SalesforceID: "001xx000003abc",
		NotionPageID: "notion-page-abc",
		Location:     "New York, NY",
	}

	run, err := s.CreateRun(ctx, company)
	require.NoError(t, err)

	got, err := s.GetRun(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, "https://full.com", got.Company.URL)
	assert.Equal(t, "Full Company Inc", got.Company.Name)
	assert.Equal(t, "001xx000003abc", got.Company.SalesforceID)
	assert.Equal(t, "notion-page-abc", got.Company.NotionPageID)
	assert.Equal(t, "New York, NY", got.Company.Location)
}

// TestUpdateRunResult_FullResult exercises round-tripping a RunResult with
// all fields populated, including phases and answers.
func TestUpdateRunResult_FullResult(t *testing.T) {
	s := newTestSQLiteRaw(t)
	ctx := context.Background()

	run, err := s.CreateRun(ctx, model.Company{URL: "https://test.com", Name: "Test"})
	require.NoError(t, err)

	result := &model.RunResult{
		Score:       0.78,
		FieldsFound: 15,
		FieldsTotal: 20,
		TotalTokens: 100000,
		TotalCost:   2.50,
		Phases: []model.PhaseResult{
			{Name: "crawl", Status: model.PhaseStatusComplete, Duration: 1000},
			{Name: "classify", Status: model.PhaseStatusComplete, Duration: 500},
		},
		Report:         "Enrichment complete",
		SalesforceSync: true,
	}

	err = s.UpdateRunResult(ctx, run.ID, result)
	require.NoError(t, err)

	got, err := s.GetRun(ctx, run.ID)
	require.NoError(t, err)
	require.NotNil(t, got.Result)
	assert.Equal(t, model.RunStatusComplete, got.Status)
	assert.InDelta(t, 0.78, got.Result.Score, 0.001)
	assert.Equal(t, 15, got.Result.FieldsFound)
	assert.Equal(t, 100000, got.Result.TotalTokens)
	assert.InDelta(t, 2.50, got.Result.TotalCost, 0.001)
	assert.Len(t, got.Result.Phases, 2)
	assert.Equal(t, "crawl", got.Result.Phases[0].Name)
	assert.True(t, got.Result.SalesforceSync)
}

// TestListRuns_CombinedFilters verifies ListRuns with both status and
// company URL filters applied simultaneously.
func TestListRuns_CombinedFilters(t *testing.T) {
	s := newTestSQLiteRaw(t)
	ctx := context.Background()

	r1, err := s.CreateRun(ctx, model.Company{URL: "https://alpha.com", Name: "Alpha"})
	require.NoError(t, err)
	r2, err := s.CreateRun(ctx, model.Company{URL: "https://beta.com", Name: "Beta"})
	require.NoError(t, err)
	_, err = s.CreateRun(ctx, model.Company{URL: "https://alpha.com", Name: "Alpha2"})
	require.NoError(t, err)

	// Move r1 to crawling.
	err = s.UpdateRunStatus(ctx, r1.ID, model.RunStatusCrawling)
	require.NoError(t, err)
	// Move r2 to crawling.
	err = s.UpdateRunStatus(ctx, r2.ID, model.RunStatusCrawling)
	require.NoError(t, err)

	// Filter by both status=crawling AND companyURL=alpha.
	runs, err := s.ListRuns(ctx, RunFilter{
		Status:     model.RunStatusCrawling,
		CompanyURL: "https://alpha.com",
	})
	require.NoError(t, err)
	assert.Len(t, runs, 1)
	assert.Equal(t, r1.ID, runs[0].ID)
}

// TestSetCachedCrawl_EmptyPages verifies that an empty page slice can be
// cached and retrieved.
func TestSetCachedCrawl_EmptyPages(t *testing.T) {
	s := newTestSQLiteRaw(t)
	ctx := context.Background()

	err := s.SetCachedCrawl(ctx, "https://empty.com", []model.CrawledPage{}, 24*time.Hour)
	require.NoError(t, err)

	got, err := s.GetCachedCrawl(ctx, "https://empty.com")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Empty(t, got.Pages)
}

// TestCrawlCache_LargePageSet verifies that a large set of cached pages
// round-trips correctly.
func TestCrawlCache_LargePageSet(t *testing.T) {
	s := newTestSQLiteRaw(t)
	ctx := context.Background()

	pages := make([]model.CrawledPage, 50)
	for i := range pages {
		pages[i] = model.CrawledPage{
			URL:        "https://example.com/page/" + string(rune('a'+i%26)),
			Title:      "Page",
			Markdown:   "# Content",
			StatusCode: 200,
		}
	}

	err := s.SetCachedCrawl(ctx, "https://example.com", pages, 24*time.Hour)
	require.NoError(t, err)

	got, err := s.GetCachedCrawl(ctx, "https://example.com")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Len(t, got.Pages, 50)
}

// TestClose_OperationsAfterClose verifies that operations fail after Close.
func TestClose_OperationsAfterClose(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "close.db")
	s, err := NewSQLite(dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Migrate(context.Background()))

	// Create a run and phase before closing so we have valid IDs.
	ctx := context.Background()
	run, err := s.CreateRun(ctx, model.Company{URL: "https://test.com", Name: "Test"})
	require.NoError(t, err)
	phase, err := s.CreatePhase(ctx, run.ID, "crawl")
	require.NoError(t, err)

	// Also insert a cached crawl so DeleteExpiredCrawls has something.
	err = s.SetCachedCrawl(ctx, "https://cached.com", []model.CrawledPage{
		{URL: "https://cached.com/", Title: "Cached", Markdown: "# C", StatusCode: 200},
	}, -1*time.Hour)
	require.NoError(t, err)

	require.NoError(t, s.Close())

	// All operations should now fail with a closed-DB error.
	_, err = s.CreateRun(ctx, model.Company{URL: "https://test.com", Name: "Test"})
	require.Error(t, err)

	err = s.UpdateRunStatus(ctx, run.ID, model.RunStatusCrawling)
	require.Error(t, err)

	err = s.UpdateRunResult(ctx, run.ID, &model.RunResult{Score: 0.5})
	require.Error(t, err)

	_, err = s.GetRun(ctx, run.ID)
	require.Error(t, err)

	_, err = s.ListRuns(ctx, RunFilter{})
	require.Error(t, err)

	err = s.CompletePhase(ctx, phase.ID, &model.PhaseResult{
		Name:   "crawl",
		Status: model.PhaseStatusComplete,
	})
	require.Error(t, err)

	_, err = s.GetCachedCrawl(ctx, "https://cached.com")
	require.Error(t, err)

	err = s.SetCachedCrawl(ctx, "https://new.com", []model.CrawledPage{}, 24*time.Hour)
	require.Error(t, err)

	_, err = s.DeleteExpiredCrawls(ctx)
	require.Error(t, err)

	err = s.Migrate(ctx)
	require.Error(t, err)
}

// TestClose_CreatePhaseAfterClose verifies CreatePhase fails on a closed DB.
func TestClose_CreatePhaseAfterClose(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "close-phase.db")
	s, err := NewSQLite(dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Migrate(context.Background()))

	ctx := context.Background()
	run, err := s.CreateRun(ctx, model.Company{URL: "https://test.com", Name: "Test"})
	require.NoError(t, err)

	require.NoError(t, s.Close())

	_, err = s.CreatePhase(ctx, run.ID, "crawl")
	require.Error(t, err)
}

// -- helpers --

// newTestSQLiteRaw returns a *SQLiteStore (not the Store interface) so we can
// access the underlying db for direct SQL injection in edge-case tests.
func newTestSQLiteRaw(t *testing.T) *SQLiteStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := NewSQLite(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() }) //nolint:errcheck
	require.NoError(t, s.Migrate(context.Background()))
	return s
}

// fakeResult implements sql.Result for testing checkRowsAffected.
type fakeResult struct {
	rowsAffected int64
	err          error
}

func (f *fakeResult) LastInsertId() (int64, error) { return 0, nil }
func (f *fakeResult) RowsAffected() (int64, error) { return f.rowsAffected, f.err }

// Verify fakeResult implements sql.Result at compile time.
var _ sql.Result = (*fakeResult)(nil)
