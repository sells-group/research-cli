package salesforce

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBulkUpdateAccounts(t *testing.T) {
	t.Run("empty updates returns nil", func(t *testing.T) {
		mock := &mockClient{}
		results, err := BulkUpdateAccounts(context.Background(), mock, nil)
		require.NoError(t, err)
		assert.Nil(t, results)
	})

	t.Run("single batch under 200", func(t *testing.T) {
		var callCount int
		mock := &mockClient{
			updateCollectionFn: func(_ context.Context, sObject string, records []CollectionRecord) ([]CollectionResult, error) {
				callCount++
				assert.Equal(t, "Account", sObject)
				results := make([]CollectionResult, len(records))
				for i, r := range records {
					results[i] = CollectionResult{ID: r.ID, Success: true}
				}
				return results, nil
			},
		}

		updates := make([]AccountUpdate, 50)
		for i := range updates {
			updates[i] = AccountUpdate{
				ID:     "001xx" + string(rune('A'+i)),
				Fields: map[string]any{"Name": "test"},
			}
		}

		results, err := BulkUpdateAccounts(context.Background(), mock, updates)
		require.NoError(t, err)
		assert.Len(t, results, 50)
		assert.Equal(t, 1, callCount)
	})

	t.Run("exact 200 is single batch", func(t *testing.T) {
		var callCount int
		mock := &mockClient{
			updateCollectionFn: func(_ context.Context, _ string, records []CollectionRecord) ([]CollectionResult, error) {
				callCount++
				assert.Len(t, records, 200)
				results := make([]CollectionResult, len(records))
				for i, r := range records {
					results[i] = CollectionResult{ID: r.ID, Success: true}
				}
				return results, nil
			},
		}

		updates := makeUpdates(200)
		results, err := BulkUpdateAccounts(context.Background(), mock, updates)
		require.NoError(t, err)
		assert.Len(t, results, 200)
		assert.Equal(t, 1, callCount)
	})

	t.Run("splits into batches of 200", func(t *testing.T) {
		var batchSizes []int
		mock := &mockClient{
			updateCollectionFn: func(_ context.Context, _ string, records []CollectionRecord) ([]CollectionResult, error) {
				batchSizes = append(batchSizes, len(records))
				results := make([]CollectionResult, len(records))
				for i, r := range records {
					results[i] = CollectionResult{ID: r.ID, Success: true}
				}
				return results, nil
			},
		}

		updates := makeUpdates(450)
		results, err := BulkUpdateAccounts(context.Background(), mock, updates)
		require.NoError(t, err)
		assert.Len(t, results, 450)
		require.Len(t, batchSizes, 3)
		assert.Equal(t, 200, batchSizes[0])
		assert.Equal(t, 200, batchSizes[1])
		assert.Equal(t, 50, batchSizes[2])
	})

	t.Run("201 splits into two batches", func(t *testing.T) {
		var batchSizes []int
		mock := &mockClient{
			updateCollectionFn: func(_ context.Context, _ string, records []CollectionRecord) ([]CollectionResult, error) {
				batchSizes = append(batchSizes, len(records))
				results := make([]CollectionResult, len(records))
				for i, r := range records {
					results[i] = CollectionResult{ID: r.ID, Success: true}
				}
				return results, nil
			},
		}

		updates := makeUpdates(201)
		results, err := BulkUpdateAccounts(context.Background(), mock, updates)
		require.NoError(t, err)
		assert.Len(t, results, 201)
		require.Len(t, batchSizes, 2)
		assert.Equal(t, 200, batchSizes[0])
		assert.Equal(t, 1, batchSizes[1])
	})

	t.Run("error in second batch returns partial results", func(t *testing.T) {
		callCount := 0
		mock := &mockClient{
			updateCollectionFn: func(_ context.Context, _ string, records []CollectionRecord) ([]CollectionResult, error) {
				callCount++
				if callCount == 2 {
					return nil, errors.New("rate limit exceeded")
				}
				results := make([]CollectionResult, len(records))
				for i, r := range records {
					results[i] = CollectionResult{ID: r.ID, Success: true}
				}
				return results, nil
			},
		}

		updates := makeUpdates(250)
		results, err := BulkUpdateAccounts(context.Background(), mock, updates)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "bulk update accounts")
		assert.Len(t, results, 200) // first batch succeeded
	})
}

func TestMaxBatchSizeConstant(t *testing.T) {
	assert.Equal(t, 200, maxBatchSize)
}

func makeUpdates(n int) []AccountUpdate {
	updates := make([]AccountUpdate, n)
	for i := range updates {
		updates[i] = AccountUpdate{
			ID:     "001xx" + string(rune(i)),
			Fields: map[string]any{"Industry": "Tech"},
		}
	}
	return updates
}
