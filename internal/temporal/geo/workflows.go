package geo

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// BackfillParams is the input for BackfillWorkflow.
type BackfillParams struct {
	Source    string `json:"source"` // "adv", "5500", "990", "fdic"
	Limit     int    `json:"limit"`
	BatchSize int    `json:"batch_size"`
	SkipMSA   bool   `json:"skip_msa"`
}

// BackfillResult is the output of BackfillWorkflow.
type BackfillResult struct {
	TotalRecords int `json:"total_records"`
	Created      int `json:"created"`
	Geocoded     int `json:"geocoded"`
	Linked       int `json:"linked"`
	MSAs         int `json:"msas"`
	Branches     int `json:"branches"`
	Failed       int `json:"failed"`
}

// BackfillProgress is returned by the geo_progress query.
type BackfillProgress struct {
	Source       string `json:"source"`
	TotalRecords int    `json:"total_records"`
	BatchesDone  int    `json:"batches_done"`
	TotalBatches int    `json:"total_batches"`
	Created      int    `json:"created"`
	Geocoded     int    `json:"geocoded"`
	Linked       int    `json:"linked"`
	Failed       int    `json:"failed"`
}

// BackfillWorkflow orchestrates geo backfill for a given source.
// It queries unlinked records, splits them into batches, and processes
// each batch as an activity.
func BackfillWorkflow(ctx workflow.Context, params BackfillParams) (*BackfillResult, error) {
	if params.BatchSize <= 0 {
		params.BatchSize = 100
	}

	// Query activity options.
	queryCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	})

	// Query unlinked records.
	var queryResult QueryUnlinkedResult
	err := workflow.ExecuteActivity(queryCtx, (*Activities).QueryUnlinkedRecords, QueryUnlinkedParams{
		Source: params.Source,
		Limit:  params.Limit,
	}).Get(ctx, &queryResult)
	if err != nil {
		return nil, fmt.Errorf("query unlinked %s records: %w", params.Source, err)
	}

	if len(queryResult.Records) == 0 {
		return &BackfillResult{}, nil
	}

	// Track progress.
	totalBatches := (len(queryResult.Records) + params.BatchSize - 1) / params.BatchSize
	progress := &BackfillProgress{
		Source:       params.Source,
		TotalRecords: len(queryResult.Records),
		TotalBatches: totalBatches,
	}

	err = workflow.SetQueryHandler(ctx, "geo_progress", func() (*BackfillProgress, error) {
		return progress, nil
	})
	if err != nil {
		return nil, fmt.Errorf("register query handler: %w", err)
	}

	// Process activity options (longer timeout, heartbeating).
	processCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Minute,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    1 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    3,
		},
	})

	// Process in batches sequentially (ON CONFLICT makes retries idempotent).
	result := &BackfillResult{
		TotalRecords: len(queryResult.Records),
	}

	for i := 0; i < len(queryResult.Records); i += params.BatchSize {
		end := i + params.BatchSize
		if end > len(queryResult.Records) {
			end = len(queryResult.Records)
		}
		batch := queryResult.Records[i:end]

		var batchResult ProcessBatchResult
		err := workflow.ExecuteActivity(processCtx, (*Activities).ProcessGeoBackfillBatch, ProcessBatchParams{
			Source:  params.Source,
			Records: batch,
			SkipMSA: params.SkipMSA,
		}).Get(ctx, &batchResult)

		if err != nil {
			// Log batch failure but continue with remaining batches.
			progress.Failed += len(batch)
			result.Failed += len(batch)
		} else {
			result.Created += batchResult.Created
			result.Geocoded += batchResult.Geocoded
			result.Linked += batchResult.Linked
			result.MSAs += batchResult.MSAs
			result.Branches += batchResult.Branches
			result.Failed += batchResult.Failed

			progress.Created += batchResult.Created
			progress.Geocoded += batchResult.Geocoded
			progress.Linked += batchResult.Linked
			progress.Failed += batchResult.Failed
		}
		progress.BatchesDone++
	}

	return result, nil
}
