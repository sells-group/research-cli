package enrichment

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/sells-group/research-cli/internal/model"
)

func TestEnrichCompanyWorkflow_Success(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnActivity((*Activities).RunEnrichment, mock.Anything, mock.Anything, mock.Anything).
		Return(&RunEnrichmentResult{Score: 0.85, Answers: 42}, nil)

	params := EnrichCompanyParams{
		Company: model.Company{URL: "acme.com", SalesforceID: "001xx"},
	}

	env.ExecuteWorkflow(EnrichCompanyWorkflow, params)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result EnrichCompanyResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.InDelta(t, 0.85, result.Score, 0.01)
	require.Equal(t, 42, result.Answers)
}

func TestEnrichCompanyWorkflow_Failure(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnActivity((*Activities).RunEnrichment, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, testsuite.ErrMockStartChildWorkflowFailed)

	params := EnrichCompanyParams{
		Company: model.Company{URL: "fail.com"},
	}

	env.ExecuteWorkflow(EnrichCompanyWorkflow, params)
	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func TestEnrichCompanyWorkflow_ProgressQuery(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnActivity((*Activities).RunEnrichment, mock.Anything, mock.Anything, mock.Anything).
		Return(&RunEnrichmentResult{Score: 0.9, Answers: 10}, nil)

	env.ExecuteWorkflow(EnrichCompanyWorkflow, EnrichCompanyParams{
		Company: model.Company{URL: "acme.com"},
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	// Query after completion to verify handler was registered.
	result, err := env.QueryWorkflow("enrichment_progress")
	require.NoError(t, err)
	var progress Progress
	require.NoError(t, result.Get(&progress))
	require.Equal(t, "acme.com", progress.Company)
	require.Equal(t, "complete", progress.CurrentPhase)
}

func TestBatchEnrichWorkflow_Success(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	companies := []model.Company{
		{URL: "a.com"},
		{URL: "b.com"},
		{URL: "c.com"},
	}

	env.OnWorkflow(EnrichCompanyWorkflow, mock.Anything, mock.Anything).
		Return(&EnrichCompanyResult{Score: 0.8, Answers: 20}, nil)

	// Use concurrency >= len(companies) to avoid semaphore blocking
	// in the test environment's cooperative scheduler.
	env.ExecuteWorkflow(BatchEnrichWorkflow, BatchEnrichParams{
		Companies:   companies,
		Concurrency: 10,
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result BatchEnrichResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 3, result.Succeeded)
	require.Equal(t, 0, result.Failed)
}

func TestBatchEnrichWorkflow_PartialFailure(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	companies := []model.Company{
		{URL: "a.com"},
		{URL: "fail.com"},
	}

	callCount := 0
	env.OnWorkflow(EnrichCompanyWorkflow, mock.Anything, mock.Anything).
		Return(func(_ workflow.Context, _ EnrichCompanyParams) (*EnrichCompanyResult, error) {
			callCount++
			if callCount == 2 {
				return nil, fmt.Errorf("enrichment failed")
			}
			return &EnrichCompanyResult{Score: 0.8, Answers: 20}, nil
		})

	env.ExecuteWorkflow(BatchEnrichWorkflow, BatchEnrichParams{
		Companies:   companies,
		Concurrency: 10,
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result BatchEnrichResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 1, result.Succeeded)
	require.Equal(t, 1, result.Failed)
}

func TestBatchEnrichWorkflow_ProgressQuery(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	companies := []model.Company{
		{URL: "a.com"},
	}

	env.OnWorkflow(EnrichCompanyWorkflow, mock.Anything, mock.Anything).
		Return(&EnrichCompanyResult{Score: 0.8, Answers: 10}, nil)

	env.ExecuteWorkflow(BatchEnrichWorkflow, BatchEnrichParams{
		Companies:   companies,
		Concurrency: 2,
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	// Query after completion to verify handler was registered.
	result, err := env.QueryWorkflow("batch_progress")
	require.NoError(t, err)
	var progress BatchProgress
	require.NoError(t, result.Get(&progress))
	require.Equal(t, 1, progress.Total)
	require.Equal(t, 1, progress.Completed)
}
