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

func newTestSQLite(t *testing.T) Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := NewSQLite(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	require.NoError(t, s.Migrate(context.Background()))
	return s
}

func storeTestSuite(t *testing.T, newStore func(t *testing.T) Store) {
	t.Run("CreateAndGetRun", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		company := model.Company{
			URL:          "https://acme.com",
			Name:         "Acme Corp",
			SalesforceID: "001xx000003abc",
			NotionPageID: "notion-123",
		}

		run, err := s.CreateRun(ctx, company)
		require.NoError(t, err)
		assert.NotEmpty(t, run.ID)
		assert.Equal(t, model.RunStatusQueued, run.Status)
		assert.Equal(t, company.URL, run.Company.URL)
		assert.Equal(t, company.Name, run.Company.Name)

		got, err := s.GetRun(ctx, run.ID)
		require.NoError(t, err)
		assert.Equal(t, run.ID, got.ID)
		assert.Equal(t, model.RunStatusQueued, got.Status)
		assert.Equal(t, "Acme Corp", got.Company.Name)
	})

	t.Run("UpdateRunStatus", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		run, err := s.CreateRun(ctx, model.Company{URL: "https://test.com", Name: "Test"})
		require.NoError(t, err)

		err = s.UpdateRunStatus(ctx, run.ID, model.RunStatusCrawling)
		require.NoError(t, err)

		got, err := s.GetRun(ctx, run.ID)
		require.NoError(t, err)
		assert.Equal(t, model.RunStatusCrawling, got.Status)
	})

	t.Run("UpdateRunStatusNotFound", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		err := s.UpdateRunStatus(ctx, "nonexistent-id", model.RunStatusCrawling)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("UpdateRunResult", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		run, err := s.CreateRun(ctx, model.Company{URL: "https://test.com", Name: "Test"})
		require.NoError(t, err)

		result := &model.RunResult{
			Score:       0.85,
			FieldsFound: 20,
			FieldsTotal: 25,
			TotalTokens: 50000,
			TotalCost:   1.23,
			Report:      "All good",
		}

		err = s.UpdateRunResult(ctx, run.ID, result)
		require.NoError(t, err)

		got, err := s.GetRun(ctx, run.ID)
		require.NoError(t, err)
		assert.Equal(t, model.RunStatusComplete, got.Status)
		require.NotNil(t, got.Result)
		assert.InDelta(t, 0.85, got.Result.Score, 0.001)
		assert.Equal(t, 20, got.Result.FieldsFound)
		assert.Equal(t, "All good", got.Result.Report)
	})

	t.Run("ListRuns", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		_, err := s.CreateRun(ctx, model.Company{URL: "https://a.com", Name: "A"})
		require.NoError(t, err)
		run2, err := s.CreateRun(ctx, model.Company{URL: "https://b.com", Name: "B"})
		require.NoError(t, err)
		err = s.UpdateRunStatus(ctx, run2.ID, model.RunStatusCrawling)
		require.NoError(t, err)

		// List all
		all, err := s.ListRuns(ctx, RunFilter{})
		require.NoError(t, err)
		assert.Len(t, all, 2)

		// Filter by status
		queued, err := s.ListRuns(ctx, RunFilter{Status: model.RunStatusQueued})
		require.NoError(t, err)
		assert.Len(t, queued, 1)
		assert.Equal(t, "A", queued[0].Company.Name)

		crawling, err := s.ListRuns(ctx, RunFilter{Status: model.RunStatusCrawling})
		require.NoError(t, err)
		assert.Len(t, crawling, 1)
		assert.Equal(t, "B", crawling[0].Company.Name)

		// Limit
		limited, err := s.ListRuns(ctx, RunFilter{Limit: 1})
		require.NoError(t, err)
		assert.Len(t, limited, 1)
	})

	t.Run("CreateAndCompletePhase", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		run, err := s.CreateRun(ctx, model.Company{URL: "https://test.com", Name: "Test"})
		require.NoError(t, err)

		phase, err := s.CreatePhase(ctx, run.ID, "crawl")
		require.NoError(t, err)
		assert.NotEmpty(t, phase.ID)
		assert.Equal(t, run.ID, phase.RunID)
		assert.Equal(t, "crawl", phase.Name)
		assert.Equal(t, model.PhaseStatusRunning, phase.Status)

		result := &model.PhaseResult{
			Name:     "crawl",
			Status:   model.PhaseStatusComplete,
			Duration: 1500,
			Metadata: map[string]any{"pages_found": float64(12)},
		}

		err = s.CompletePhase(ctx, phase.ID, result)
		require.NoError(t, err)
	})

	t.Run("CompletePhaseNotFound", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		result := &model.PhaseResult{
			Name:   "crawl",
			Status: model.PhaseStatusComplete,
		}

		err := s.CompletePhase(ctx, "nonexistent-id", result)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("CrawlCacheSetAndGet", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		pages := []model.CrawledPage{
			{URL: "https://acme.com/", Title: "Acme Home", Markdown: "# Acme", StatusCode: 200},
			{URL: "https://acme.com/about", Title: "About", Markdown: "# About Us", StatusCode: 200},
		}

		err := s.SetCachedCrawl(ctx, "https://acme.com", pages, 24*time.Hour)
		require.NoError(t, err)

		got, err := s.GetCachedCrawl(ctx, "https://acme.com")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, "https://acme.com", got.CompanyURL)
		assert.Len(t, got.Pages, 2)
		assert.Equal(t, "Acme Home", got.Pages[0].Title)
		assert.True(t, got.ExpiresAt.After(time.Now()))

		// No cache for different URL
		miss, err := s.GetCachedCrawl(ctx, "https://other.com")
		require.NoError(t, err)
		assert.Nil(t, miss)
	})

	t.Run("CrawlCacheExpiry", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		pages := []model.CrawledPage{
			{URL: "https://old.com/", Title: "Old", Markdown: "# Old", StatusCode: 200},
		}

		// Insert with already-expired TTL
		err := s.SetCachedCrawl(ctx, "https://old.com", pages, -1*time.Hour)
		require.NoError(t, err)

		// Should not return expired entries
		got, err := s.GetCachedCrawl(ctx, "https://old.com")
		require.NoError(t, err)
		assert.Nil(t, got)

		// DeleteExpiredCrawls should clean it up
		n, err := s.DeleteExpiredCrawls(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, n)

		// Second delete should find nothing
		n, err = s.DeleteExpiredCrawls(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, n)
	})

	t.Run("ListRuns_ByCompanyURL", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		_, err := s.CreateRun(ctx, model.Company{URL: "https://a.com", Name: "A"})
		require.NoError(t, err)
		_, err = s.CreateRun(ctx, model.Company{URL: "https://b.com", Name: "B"})
		require.NoError(t, err)

		filtered, err := s.ListRuns(ctx, RunFilter{CompanyURL: "https://a.com"})
		require.NoError(t, err)
		assert.Len(t, filtered, 1)
		assert.Equal(t, "A", filtered[0].Company.Name)
	})

	t.Run("ListRuns_WithOffset", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		_, err := s.CreateRun(ctx, model.Company{URL: "https://a.com", Name: "A"})
		require.NoError(t, err)
		_, err = s.CreateRun(ctx, model.Company{URL: "https://b.com", Name: "B"})
		require.NoError(t, err)
		_, err = s.CreateRun(ctx, model.Company{URL: "https://c.com", Name: "C"})
		require.NoError(t, err)

		// Offset 1, limit 1 should skip the first result
		paged, err := s.ListRuns(ctx, RunFilter{Limit: 1, Offset: 1})
		require.NoError(t, err)
		assert.Len(t, paged, 1)
	})

	t.Run("ListRuns_Empty", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		runs, err := s.ListRuns(ctx, RunFilter{})
		require.NoError(t, err)
		assert.Empty(t, runs)
	})

	t.Run("GetRun_NotFound", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		_, err := s.GetRun(ctx, "nonexistent")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("UpdateRunResult_NotFound", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		err := s.UpdateRunResult(ctx, "nonexistent", &model.RunResult{Score: 0.5})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("DeleteExpiredCrawls_NoExpired", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		// No crawl cache at all
		n, err := s.DeleteExpiredCrawls(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, n)
	})

	t.Run("CrawlCacheOverwrite", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		pages1 := []model.CrawledPage{
			{URL: "https://acme.com/", Title: "Old", Markdown: "# Old", StatusCode: 200},
		}
		pages2 := []model.CrawledPage{
			{URL: "https://acme.com/", Title: "New", Markdown: "# New", StatusCode: 200},
			{URL: "https://acme.com/about", Title: "About", Markdown: "# About", StatusCode: 200},
		}

		err := s.SetCachedCrawl(ctx, "https://acme.com", pages1, 24*time.Hour)
		require.NoError(t, err)
		err = s.SetCachedCrawl(ctx, "https://acme.com", pages2, 24*time.Hour)
		require.NoError(t, err)

		// Should get the latest crawl
		got, err := s.GetCachedCrawl(ctx, "https://acme.com")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Len(t, got.Pages, 2)
		assert.Equal(t, "New", got.Pages[0].Title)
	})
}

func TestSQLiteStore(t *testing.T) {
	storeTestSuite(t, newTestSQLite)
}
