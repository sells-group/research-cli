package geo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

func TestBackfillWorkflow_NoRecords(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	env.OnActivity((*Activities).QueryUnlinkedRecords, mock.Anything, mock.Anything, mock.Anything).
		Return(&QueryUnlinkedResult{Records: nil}, nil)

	env.ExecuteWorkflow(BackfillWorkflow, BackfillParams{
		Source:    "adv",
		Limit:     100,
		BatchSize: 50,
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result BackfillResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 0, result.TotalRecords)
}

func TestBackfillWorkflow_SingleBatch(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	records := []UnlinkedRecord{
		{Key: "100", Name: "Firm A", City: "Boston", State: "MA"},
		{Key: "200", Name: "Firm B", City: "Chicago", State: "IL"},
	}

	env.OnActivity((*Activities).QueryUnlinkedRecords, mock.Anything, mock.Anything, mock.Anything).
		Return(&QueryUnlinkedResult{Records: records}, nil)

	env.OnActivity((*Activities).ProcessGeoBackfillBatch, mock.Anything, mock.Anything, mock.Anything).
		Return(&ProcessBatchResult{Created: 2, Geocoded: 2, Linked: 2}, nil)

	env.ExecuteWorkflow(BackfillWorkflow, BackfillParams{
		Source:    "adv",
		Limit:     100,
		BatchSize: 50,
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result BackfillResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 2, result.TotalRecords)
	require.Equal(t, 2, result.Created)
	require.Equal(t, 2, result.Geocoded)
	require.Equal(t, 2, result.Linked)
}

func TestBackfillWorkflow_MultipleBatches(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	records := make([]UnlinkedRecord, 5)
	for i := range records {
		records[i] = UnlinkedRecord{Key: string(rune('1' + i)), Name: "Firm"}
	}

	env.OnActivity((*Activities).QueryUnlinkedRecords, mock.Anything, mock.Anything, mock.Anything).
		Return(&QueryUnlinkedResult{Records: records}, nil)

	// Should be called 3 times: batches of 2, 2, 1.
	env.OnActivity((*Activities).ProcessGeoBackfillBatch, mock.Anything, mock.Anything, mock.Anything).
		Return(&ProcessBatchResult{Created: 1, Geocoded: 1, Linked: 1}, nil)

	env.ExecuteWorkflow(BackfillWorkflow, BackfillParams{
		Source:    "adv",
		Limit:     100,
		BatchSize: 2,
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result BackfillResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 5, result.TotalRecords)
	require.Equal(t, 3, result.Created) // 1 per batch * 3 batches
	require.Equal(t, 3, result.Linked)
}

func TestBackfillWorkflow_BatchFailureContinues(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	records := make([]UnlinkedRecord, 4)
	for i := range records {
		records[i] = UnlinkedRecord{Key: string(rune('1' + i)), Name: "Firm"}
	}

	env.OnActivity((*Activities).QueryUnlinkedRecords, mock.Anything, mock.Anything, mock.Anything).
		Return(&QueryUnlinkedResult{Records: records}, nil)

	// First batch succeeds, second fails (non-retryable so it stays failed).
	callCount := 0
	env.OnActivity((*Activities).ProcessGeoBackfillBatch, mock.Anything, mock.Anything, mock.Anything).
		Return(func(_ *Activities, _ context.Context, _ ProcessBatchParams) (*ProcessBatchResult, error) {
			callCount++
			if callCount == 2 {
				return nil, temporal.NewNonRetryableApplicationError(
					"batch processing failed", "BATCH_ERROR", nil)
			}
			return &ProcessBatchResult{Created: 2, Geocoded: 2, Linked: 2}, nil
		})

	env.ExecuteWorkflow(BackfillWorkflow, BackfillParams{
		Source:    "adv",
		Limit:     100,
		BatchSize: 2,
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result BackfillResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 2, result.Created)
	require.True(t, result.Failed > 0)
}

func TestBackfillWorkflow_SBASource(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	records := []UnlinkedRecord{
		{Key: "1234567", Name: "SBA Borrower LLC", City: "Dallas", State: "TX"},
	}

	env.OnActivity((*Activities).QueryUnlinkedRecords, mock.Anything, mock.Anything, mock.Anything).
		Return(&QueryUnlinkedResult{Records: records}, nil)

	env.OnActivity((*Activities).ProcessGeoBackfillBatch, mock.Anything, mock.Anything, mock.Anything).
		Return(&ProcessBatchResult{Created: 1, Geocoded: 1, Linked: 1}, nil)

	env.ExecuteWorkflow(BackfillWorkflow, BackfillParams{
		Source:    "sba",
		Limit:     100,
		BatchSize: 50,
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result BackfillResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 1, result.TotalRecords)
	require.Equal(t, 1, result.Created)
}

func TestBackfillWorkflow_AddressSource(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	records := []UnlinkedRecord{
		{Key: "42", Name: "123 Main St", City: "Boston", State: "MA"},
	}

	env.OnActivity((*Activities).QueryUnlinkedRecords, mock.Anything, mock.Anything, mock.Anything).
		Return(&QueryUnlinkedResult{Records: records}, nil)

	env.OnActivity((*Activities).ProcessGeoBackfillBatch, mock.Anything, mock.Anything, mock.Anything).
		Return(&ProcessBatchResult{Geocoded: 1}, nil)

	env.ExecuteWorkflow(BackfillWorkflow, BackfillParams{
		Source:    "address",
		Limit:     100,
		BatchSize: 50,
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result BackfillResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 1, result.TotalRecords)
	require.Equal(t, 1, result.Geocoded)
}

func TestBackfillWorkflow_ProgressQuery(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	records := []UnlinkedRecord{
		{Key: "100", Name: "Firm A"},
	}

	env.OnActivity((*Activities).QueryUnlinkedRecords, mock.Anything, mock.Anything, mock.Anything).
		Return(&QueryUnlinkedResult{Records: records}, nil)

	env.OnActivity((*Activities).ProcessGeoBackfillBatch, mock.Anything, mock.Anything, mock.Anything).
		Return(&ProcessBatchResult{Created: 1, Linked: 1}, nil)

	env.ExecuteWorkflow(BackfillWorkflow, BackfillParams{
		Source:    "adv",
		Limit:     100,
		BatchSize: 50,
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	// Query after completion to verify handler was registered.
	result, err := env.QueryWorkflow("geo_progress")
	require.NoError(t, err)
	var progress BackfillProgress
	require.NoError(t, result.Get(&progress))
	require.Equal(t, "adv", progress.Source)
	require.Equal(t, 1, progress.TotalRecords)
}
