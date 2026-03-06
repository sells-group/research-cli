package geoscraper

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

func TestScrapeWorkflow_NoScrapers(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnActivity((*Activities).SelectScrapers, mock.Anything, mock.Anything, mock.Anything).
		Return(&SelectScrapersResult{ScraperNames: nil}, nil)

	env.ExecuteWorkflow(ScrapeWorkflow, ScrapeParams{Force: true})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result ScrapeResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 0, result.Synced)
	require.Equal(t, 0, result.Failed)
}

func TestScrapeWorkflow_TwoScrapers(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnActivity((*Activities).SelectScrapers, mock.Anything, mock.Anything, mock.Anything).
		Return(&SelectScrapersResult{ScraperNames: []string{"hifld", "fema_flood"}}, nil)

	// Mock child workflows (ScrapeSingleWorkflow).
	env.OnWorkflow(ScrapeSingleWorkflow, mock.Anything, mock.Anything).
		Return(&ScrapeSingleResult{RowsSynced: 500}, nil)

	env.ExecuteWorkflow(ScrapeWorkflow, ScrapeParams{Force: true})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result ScrapeResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 2, result.Synced)
	require.Equal(t, 0, result.Failed)
}

func TestScrapeWorkflow_PartialFailure(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnActivity((*Activities).SelectScrapers, mock.Anything, mock.Anything, mock.Anything).
		Return(&SelectScrapersResult{ScraperNames: []string{"hifld", "fema_flood"}}, nil)

	env.OnWorkflow(ScrapeSingleWorkflow, mock.Anything, ScrapeSingleParams{Scraper: "hifld"}).
		Return(&ScrapeSingleResult{RowsSynced: 500}, nil)
	env.OnWorkflow(ScrapeSingleWorkflow, mock.Anything, ScrapeSingleParams{Scraper: "fema_flood"}).
		Return(nil, testsuite.ErrMockStartChildWorkflowFailed)

	env.ExecuteWorkflow(ScrapeWorkflow, ScrapeParams{Force: true})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result ScrapeResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 1, result.Synced)
	require.Equal(t, 1, result.Failed)
}

func TestScrapeSingleWorkflow_Success(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnActivity((*Activities).StartSyncLog, mock.Anything, mock.Anything, mock.Anything).
		Return(&StartSyncLogResult{SyncID: 42}, nil)
	env.OnActivity((*Activities).SyncScraper, mock.Anything, mock.Anything, mock.Anything).
		Return(&SyncScraperResult{RowsSynced: 1500}, nil)
	env.OnActivity((*Activities).CompleteSyncLog, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	env.ExecuteWorkflow(ScrapeSingleWorkflow, ScrapeSingleParams{Scraper: "hifld"})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result ScrapeSingleResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, int64(1500), result.RowsSynced)
}

func TestScrapeSingleWorkflow_SyncFails(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnActivity((*Activities).StartSyncLog, mock.Anything, mock.Anything, mock.Anything).
		Return(&StartSyncLogResult{SyncID: 42}, nil)
	env.OnActivity((*Activities).SyncScraper, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, testsuite.ErrMockStartChildWorkflowFailed)
	env.OnActivity((*Activities).FailSyncLog, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	env.ExecuteWorkflow(ScrapeSingleWorkflow, ScrapeSingleParams{Scraper: "hifld"})
	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func TestScrapeWorkflow_ProgressQuery(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnActivity((*Activities).SelectScrapers, mock.Anything, mock.Anything, mock.Anything).
		Return(&SelectScrapersResult{ScraperNames: []string{"hifld"}}, nil)

	env.OnWorkflow(ScrapeSingleWorkflow, mock.Anything, mock.Anything).
		Return(&ScrapeSingleResult{RowsSynced: 500}, nil)

	env.ExecuteWorkflow(ScrapeWorkflow, ScrapeParams{Force: true})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	// Query after completion to verify the handler was registered and
	// progress was tracked correctly.
	result, err := env.QueryWorkflow("scrape_progress")
	require.NoError(t, err)
	var progress ScrapeProgress
	require.NoError(t, result.Get(&progress))
	require.Equal(t, 1, progress.Total)
	require.Equal(t, 1, progress.Completed)
}
