package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/model"
)

func newTestSQLiteStore(t *testing.T) *SQLiteStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	st, err := NewSQLite(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() }) //nolint:errcheck
	require.NoError(t, st.Migrate(context.Background()))
	return st
}

// --- Scrape Cache ---

func TestSQLite_ScrapeCache_SetAndGet(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	err := st.SetCachedScrape(ctx, "hash123", []byte("page content"), 1*time.Hour)
	require.NoError(t, err)

	data, err := st.GetCachedScrape(ctx, "hash123")
	require.NoError(t, err)
	assert.Equal(t, "page content", string(data))
}

func TestSQLite_ScrapeCache_Missing(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	data, err := st.GetCachedScrape(ctx, "nonexistent")
	require.NoError(t, err)
	assert.Nil(t, data)
}

func TestSQLite_ScrapeCache_Expired(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	// Set with already-expired TTL (-1 hour in the past).
	err := st.SetCachedScrape(ctx, "expired-hash", []byte("old data"), -1*time.Hour)
	require.NoError(t, err)

	data, err := st.GetCachedScrape(ctx, "expired-hash")
	require.NoError(t, err)
	assert.Nil(t, data) // Should not be returned (expired)
}

func TestSQLite_ScrapeCache_Overwrite(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	err := st.SetCachedScrape(ctx, "hash-ow", []byte("original"), 1*time.Hour)
	require.NoError(t, err)

	err = st.SetCachedScrape(ctx, "hash-ow", []byte("updated"), 1*time.Hour)
	require.NoError(t, err)

	data, err := st.GetCachedScrape(ctx, "hash-ow")
	require.NoError(t, err)
	assert.Equal(t, "updated", string(data))
}

// --- Checkpoint ---

func TestSQLite_Checkpoint_SaveLoadDelete(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	testData := []byte(`{"answers":[{"field_key":"revenue","value":100}]}`)

	err := st.SaveCheckpoint(ctx, "company-1", "t1_complete", testData)
	require.NoError(t, err)

	cp, err := st.LoadCheckpoint(ctx, "company-1")
	require.NoError(t, err)
	require.NotNil(t, cp)
	assert.Equal(t, "company-1", cp.CompanyID)
	assert.Equal(t, "t1_complete", cp.Phase)
	assert.Equal(t, testData, cp.Data)

	err = st.DeleteCheckpoint(ctx, "company-1")
	require.NoError(t, err)

	cp, err = st.LoadCheckpoint(ctx, "company-1")
	require.NoError(t, err)
	assert.Nil(t, cp) // Should be gone
}

func TestSQLite_Checkpoint_Overwrite(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	err := st.SaveCheckpoint(ctx, "company-1", "t1_complete", []byte("old data"))
	require.NoError(t, err)

	err = st.SaveCheckpoint(ctx, "company-1", "t2_complete", []byte("new data"))
	require.NoError(t, err)

	cp, err := st.LoadCheckpoint(ctx, "company-1")
	require.NoError(t, err)
	require.NotNil(t, cp)
	assert.Equal(t, "t2_complete", cp.Phase)
	assert.Equal(t, []byte("new data"), cp.Data)
}

func TestSQLite_Checkpoint_LoadMissing(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	cp, err := st.LoadCheckpoint(ctx, "nonexistent")
	require.NoError(t, err)
	assert.Nil(t, cp)
}

// --- Crawl Cache ---

func TestSQLite_CrawlCache_SetAndGet(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	pages := []model.CrawledPage{
		{URL: "https://acme.com", Title: "Home", Markdown: "Welcome"},
		{URL: "https://acme.com/about", Title: "About", Markdown: "About us"},
	}

	err := st.SetCachedCrawl(ctx, "https://acme.com", pages, 1*time.Hour)
	require.NoError(t, err)

	cached, err := st.GetCachedCrawl(ctx, "https://acme.com")
	require.NoError(t, err)
	require.NotNil(t, cached)
	assert.Equal(t, "https://acme.com", cached.CompanyURL)
	assert.Len(t, cached.Pages, 2)
	assert.Equal(t, "https://acme.com/about", cached.Pages[1].URL)
}

func TestSQLite_CrawlCache_Missing(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	cached, err := st.GetCachedCrawl(ctx, "https://unknown.com")
	require.NoError(t, err)
	assert.Nil(t, cached)
}

func TestSQLite_CrawlCache_Expired(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	pages := []model.CrawledPage{{URL: "https://old.com", Title: "Old", Markdown: "old"}}
	err := st.SetCachedCrawl(ctx, "https://old.com", pages, -1*time.Hour)
	require.NoError(t, err)

	cached, err := st.GetCachedCrawl(ctx, "https://old.com")
	require.NoError(t, err)
	assert.Nil(t, cached)
}

func TestSQLite_CrawlCache_DeleteExpired(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	// Insert one expired and one fresh entry.
	pages := []model.CrawledPage{{URL: "https://a.com", Title: "A", Markdown: "a"}}
	err := st.SetCachedCrawl(ctx, "https://expired.com", pages, -1*time.Hour)
	require.NoError(t, err)
	err = st.SetCachedCrawl(ctx, "https://fresh.com", pages, 1*time.Hour)
	require.NoError(t, err)

	deleted, err := st.DeleteExpiredCrawls(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, deleted)

	// Fresh entry should still be there.
	cached, err := st.GetCachedCrawl(ctx, "https://fresh.com")
	require.NoError(t, err)
	assert.NotNil(t, cached)
}

// --- LinkedIn Cache ---

func TestSQLite_LinkedInCache_SetAndGet(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	err := st.SetCachedLinkedIn(ctx, "acme.com", []byte(`{"name":"Acme"}`), 1*time.Hour)
	require.NoError(t, err)

	data, err := st.GetCachedLinkedIn(ctx, "acme.com")
	require.NoError(t, err)
	assert.Equal(t, `{"name":"Acme"}`, string(data))
}

func TestSQLite_LinkedInCache_Missing(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	data, err := st.GetCachedLinkedIn(ctx, "unknown.com")
	require.NoError(t, err)
	assert.Nil(t, data)
}

func TestSQLite_LinkedInCache_Expired(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	err := st.SetCachedLinkedIn(ctx, "old.com", []byte("old"), -1*time.Hour)
	require.NoError(t, err)

	data, err := st.GetCachedLinkedIn(ctx, "old.com")
	require.NoError(t, err)
	assert.Nil(t, data)
}

// --- Runs ---

func TestSQLite_CreateRun_And_GetRun(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	company := model.Company{URL: "https://acme.com", Name: "Acme"}
	run, err := st.CreateRun(ctx, company)
	require.NoError(t, err)
	assert.NotEmpty(t, run.ID)
	assert.Equal(t, model.RunStatusQueued, run.Status)
	assert.Equal(t, "Acme", run.Company.Name)

	fetched, err := st.GetRun(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, run.ID, fetched.ID)
	assert.Equal(t, "https://acme.com", fetched.Company.URL)
}

func TestSQLite_UpdateRunStatus(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	company := model.Company{URL: "https://acme.com", Name: "Acme"}
	run, err := st.CreateRun(ctx, company)
	require.NoError(t, err)

	err = st.UpdateRunStatus(ctx, run.ID, model.RunStatusCrawling)
	require.NoError(t, err)

	fetched, err := st.GetRun(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, model.RunStatusCrawling, fetched.Status)
}

func TestSQLite_UpdateRunResult(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	company := model.Company{URL: "https://acme.com", Name: "Acme"}
	run, err := st.CreateRun(ctx, company)
	require.NoError(t, err)

	result := &model.RunResult{
		Score:       0.85,
		FieldsFound: 5,
		FieldsTotal: 10,
		TotalTokens: 1000,
		Answers: []model.ExtractionAnswer{
			{FieldKey: "industry", Value: "Tech", Confidence: 0.95, Tier: 1},
		},
	}
	err = st.UpdateRunResult(ctx, run.ID, result)
	require.NoError(t, err)

	fetched, err := st.GetRun(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, model.RunStatusComplete, fetched.Status)
	require.NotNil(t, fetched.Result)
	assert.Equal(t, 0.85, fetched.Result.Score)
	assert.Len(t, fetched.Result.Answers, 1)
}

func TestSQLite_ListRuns(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	// Create two runs.
	c1 := model.Company{URL: "https://a.com", Name: "A"}
	c2 := model.Company{URL: "https://b.com", Name: "B"}
	_, err := st.CreateRun(ctx, c1)
	require.NoError(t, err)
	_, err = st.CreateRun(ctx, c2)
	require.NoError(t, err)

	runs, err := st.ListRuns(ctx, RunFilter{Limit: 10})
	require.NoError(t, err)
	assert.Len(t, runs, 2)
}

func TestSQLite_ListRuns_FilterByStatus(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	company := model.Company{URL: "https://acme.com", Name: "Acme"}
	run, err := st.CreateRun(ctx, company)
	require.NoError(t, err)

	err = st.UpdateRunStatus(ctx, run.ID, model.RunStatusComplete)
	require.NoError(t, err)

	// Create another run that stays queued.
	_, err = st.CreateRun(ctx, model.Company{URL: "https://other.com", Name: "Other"})
	require.NoError(t, err)

	runs, err := st.ListRuns(ctx, RunFilter{Status: model.RunStatusComplete, Limit: 10})
	require.NoError(t, err)
	assert.Len(t, runs, 1)
	assert.Equal(t, run.ID, runs[0].ID)
}

// --- Phases ---

func TestSQLite_CreatePhase_And_CompletePhase(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	company := model.Company{URL: "https://acme.com", Name: "Acme"}
	run, err := st.CreateRun(ctx, company)
	require.NoError(t, err)

	phase, err := st.CreatePhase(ctx, run.ID, "1a_crawl")
	require.NoError(t, err)
	assert.NotEmpty(t, phase.ID)
	assert.Equal(t, "1a_crawl", phase.Name)
	assert.Equal(t, model.PhaseStatusRunning, phase.Status)

	err = st.CompletePhase(ctx, phase.ID, &model.PhaseResult{
		Name:   "1a_crawl",
		Status: model.PhaseStatusComplete,
		Metadata: map[string]any{
			"pages_count": 5,
		},
	})
	require.NoError(t, err)
}

// --- GetHighConfidenceAnswers ---

func TestSQLite_GetHighConfidenceAnswers_NoResults(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	answers, err := st.GetHighConfidenceAnswers(ctx, "https://unknown.com", 0.8, 0)
	require.NoError(t, err)
	assert.Nil(t, answers)
}

func TestSQLite_GetHighConfidenceAnswers_FiltersByConfidence(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	// Create a completed run with answers at various confidence levels.
	company := model.Company{URL: "https://acme.com", Name: "Acme"}
	run, err := st.CreateRun(ctx, company)
	require.NoError(t, err)

	result := &model.RunResult{
		Answers: []model.ExtractionAnswer{
			{FieldKey: "industry", Value: "Tech", Confidence: 0.95, Tier: 1},
			{FieldKey: "revenue", Value: "$10M", Confidence: 0.5, Tier: 1},
			{FieldKey: "employees", Value: 200, Confidence: 0.9, Tier: 2},
		},
	}
	err = st.UpdateRunResult(ctx, run.ID, result)
	require.NoError(t, err)

	// Query with threshold 0.8.
	answers, err := st.GetHighConfidenceAnswers(ctx, "https://acme.com", 0.8, 0)
	require.NoError(t, err)
	require.Len(t, answers, 2) // industry (0.95) and employees (0.9)

	fieldKeys := make(map[string]bool)
	for _, a := range answers {
		fieldKeys[a.FieldKey] = true
	}
	assert.True(t, fieldKeys["industry"])
	assert.True(t, fieldKeys["employees"])
	assert.False(t, fieldKeys["revenue"]) // 0.5 < 0.8
}

func TestSQLite_GetHighConfidenceAnswers_UsesLatestRun(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	company := model.Company{URL: "https://acme.com", Name: "Acme"}

	// First run with old answers.
	run1, err := st.CreateRun(ctx, company)
	require.NoError(t, err)
	err = st.UpdateRunResult(ctx, run1.ID, &model.RunResult{
		Answers: []model.ExtractionAnswer{
			{FieldKey: "industry", Value: "Old Industry", Confidence: 0.95, Tier: 1},
		},
	})
	require.NoError(t, err)

	// Second run with newer answers.
	run2, err := st.CreateRun(ctx, company)
	require.NoError(t, err)
	err = st.UpdateRunResult(ctx, run2.ID, &model.RunResult{
		Answers: []model.ExtractionAnswer{
			{FieldKey: "industry", Value: "New Industry", Confidence: 0.95, Tier: 1},
		},
	})
	require.NoError(t, err)

	answers, err := st.GetHighConfidenceAnswers(ctx, "https://acme.com", 0.8, 0)
	require.NoError(t, err)
	require.Len(t, answers, 1)
	// Should get the latest run's answer (ORDER BY created_at DESC LIMIT 1).
	assert.Equal(t, "New Industry", answers[0].Value)
}

func TestSQLite_GetHighConfidenceAnswers_Staleness(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	company := model.Company{URL: "https://old.com", Name: "Old Corp"}
	run, err := st.CreateRun(ctx, company)
	require.NoError(t, err)

	err = st.UpdateRunResult(ctx, run.ID, &model.RunResult{
		Answers: []model.ExtractionAnswer{
			{FieldKey: "industry", Value: "Stale", Confidence: 0.95, Tier: 1},
		},
	})
	require.NoError(t, err)

	// With no maxAge, should return answers.
	answers, err := st.GetHighConfidenceAnswers(ctx, "https://old.com", 0.8, 0)
	require.NoError(t, err)
	require.Len(t, answers, 1)

	// With a very short maxAge (1 nanosecond), the run is already "stale".
	answers, err = st.GetHighConfidenceAnswers(ctx, "https://old.com", 0.8, time.Nanosecond)
	require.NoError(t, err)
	assert.Nil(t, answers, "stale answers should be excluded by maxAge")
}

// --- ListStaleCompanies ---

func TestSQLite_ListStaleCompanies_Empty(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	results, err := st.ListStaleCompanies(ctx, StaleCompanyFilter{
		LastEnrichedBefore: time.Now().Add(time.Hour),
		Limit:              10,
	})
	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestSQLite_ListStaleCompanies_FindsStale(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	company := model.Company{URL: "https://stale.com", Name: "Stale Inc"}
	run, err := st.CreateRun(ctx, company)
	require.NoError(t, err)

	err = st.UpdateRunResult(ctx, run.ID, &model.RunResult{
		Score: 0.75,
		Answers: []model.ExtractionAnswer{
			{FieldKey: "industry", Value: "Tech", Confidence: 0.9, Tier: 1},
		},
	})
	require.NoError(t, err)

	// Cutoff in the future → everything is "stale".
	results, err := st.ListStaleCompanies(ctx, StaleCompanyFilter{
		LastEnrichedBefore: time.Now().Add(time.Hour),
		Limit:              10,
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "https://stale.com", results[0].Company.URL)
}

func TestSQLite_ListStaleCompanies_ExcludesRecent(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	company := model.Company{URL: "https://fresh.com", Name: "Fresh Inc"}
	run, err := st.CreateRun(ctx, company)
	require.NoError(t, err)

	err = st.UpdateRunResult(ctx, run.ID, &model.RunResult{
		Score: 0.85,
		Answers: []model.ExtractionAnswer{
			{FieldKey: "industry", Value: "Finance", Confidence: 0.95, Tier: 1},
		},
	})
	require.NoError(t, err)

	// Cutoff is 1 second in the past — the run we just created is newer, so it's NOT stale.
	// The run's created_at is roughly "now", and the cutoff is 1 second before "now",
	// so created_at < cutoff is false.
	time.Sleep(10 * time.Millisecond) // ensure created_at is before cutoff
	results, err := st.ListStaleCompanies(ctx, StaleCompanyFilter{
		LastEnrichedBefore: time.Now().Add(-24 * time.Hour),
		Limit:              10,
	})
	require.NoError(t, err)
	assert.Empty(t, results)
}

// --- Migrate ---

func TestSQLite_Migrate_Idempotent(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	// Migrate was already called in newTestSQLite; calling again should not error.
	err := st.Migrate(ctx)
	require.NoError(t, err)
}
