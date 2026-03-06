package sdk

import "go.temporal.io/sdk/workflow"

// SequentialBatch processes totalItems in sequential batches of batchSize.
// processFn is called for each batch with the workflow context and batch
// boundaries [start, end). The caller handles activity execution and result
// accumulation inside processFn. Returns the number of batches processed.
// If processFn returns an error, iteration stops immediately.
// For continue-on-error semantics, handle errors inside processFn and return nil.
func SequentialBatch(
	ctx workflow.Context,
	totalItems int,
	batchSize int,
	processFn func(ctx workflow.Context, start, end int) error,
) (int, error) {
	if batchSize <= 0 {
		batchSize = 100
	}
	batches := 0
	for start := 0; start < totalItems; start += batchSize {
		end := start + batchSize
		if end > totalItems {
			end = totalItems
		}
		if err := processFn(ctx, start, end); err != nil {
			return batches, err
		}
		batches++
	}
	return batches, nil
}
