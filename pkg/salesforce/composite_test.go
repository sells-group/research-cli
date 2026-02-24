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

func TestBulkCreateAccounts(t *testing.T) {
	t.Run("empty records returns nil", func(t *testing.T) {
		mock := &mockClient{}
		results, err := BulkCreateAccounts(context.Background(), mock, nil)
		require.NoError(t, err)
		assert.Nil(t, results)
	})

	t.Run("single batch under 200", func(t *testing.T) {
		var callCount int
		mock := &mockClient{
			insertCollectionFn: func(_ context.Context, sObject string, records []map[string]any) ([]CollectionResult, error) {
				callCount++
				assert.Equal(t, "Account", sObject)
				results := make([]CollectionResult, len(records))
				for i := range records {
					results[i] = CollectionResult{ID: "001NEW" + string(rune('A'+i)), Success: true}
				}
				return results, nil
			},
		}

		records := makeRecords(50)
		results, err := BulkCreateAccounts(context.Background(), mock, records)
		require.NoError(t, err)
		assert.Len(t, results, 50)
		assert.Equal(t, 1, callCount)
		// Verify IDs are returned.
		assert.Equal(t, "001NEWA", results[0].ID)
		assert.True(t, results[0].Success)
	})

	t.Run("exact 200 is single batch", func(t *testing.T) {
		var callCount int
		mock := &mockClient{
			insertCollectionFn: func(_ context.Context, _ string, records []map[string]any) ([]CollectionResult, error) {
				callCount++
				assert.Len(t, records, 200)
				results := make([]CollectionResult, len(records))
				for i := range records {
					results[i] = CollectionResult{ID: "001xx", Success: true}
				}
				return results, nil
			},
		}

		results, err := BulkCreateAccounts(context.Background(), mock, makeRecords(200))
		require.NoError(t, err)
		assert.Len(t, results, 200)
		assert.Equal(t, 1, callCount)
	})

	t.Run("201 splits into two batches", func(t *testing.T) {
		var batchSizes []int
		mock := &mockClient{
			insertCollectionFn: func(_ context.Context, _ string, records []map[string]any) ([]CollectionResult, error) {
				batchSizes = append(batchSizes, len(records))
				results := make([]CollectionResult, len(records))
				for i := range records {
					results[i] = CollectionResult{ID: "001xx", Success: true}
				}
				return results, nil
			},
		}

		results, err := BulkCreateAccounts(context.Background(), mock, makeRecords(201))
		require.NoError(t, err)
		assert.Len(t, results, 201)
		require.Len(t, batchSizes, 2)
		assert.Equal(t, 200, batchSizes[0])
		assert.Equal(t, 1, batchSizes[1])
	})

	t.Run("error in second batch returns partial results", func(t *testing.T) {
		callCount := 0
		mock := &mockClient{
			insertCollectionFn: func(_ context.Context, _ string, records []map[string]any) ([]CollectionResult, error) {
				callCount++
				if callCount == 2 {
					return nil, errors.New("rate limit exceeded")
				}
				results := make([]CollectionResult, len(records))
				for i := range records {
					results[i] = CollectionResult{ID: "001xx", Success: true}
				}
				return results, nil
			},
		}

		results, err := BulkCreateAccounts(context.Background(), mock, makeRecords(250))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "bulk insert Account")
		assert.Len(t, results, 200) // First batch succeeded.
	})
}

func TestBulkCreateContacts(t *testing.T) {
	t.Run("empty records returns nil", func(t *testing.T) {
		mock := &mockClient{}
		results, err := BulkCreateContacts(context.Background(), mock, nil)
		require.NoError(t, err)
		assert.Nil(t, results)
	})

	t.Run("sends to Contact sObject", func(t *testing.T) {
		mock := &mockClient{
			insertCollectionFn: func(_ context.Context, sObject string, records []map[string]any) ([]CollectionResult, error) {
				assert.Equal(t, "Contact", sObject)
				// Verify AccountId is present in records.
				for _, r := range records {
					assert.NotEmpty(t, r["AccountId"])
				}
				results := make([]CollectionResult, len(records))
				for i := range records {
					results[i] = CollectionResult{ID: "003NEW", Success: true}
				}
				return results, nil
			},
		}

		records := []map[string]any{
			{"AccountId": "001xx", "LastName": "Smith", "FirstName": "John"},
			{"AccountId": "001yy", "LastName": "Doe", "FirstName": "Jane"},
		}
		results, err := BulkCreateContacts(context.Background(), mock, records)
		require.NoError(t, err)
		assert.Len(t, results, 2)
	})
}

func TestMaxBatchSizeConstant(t *testing.T) {
	assert.Equal(t, 200, maxBatchSize)
}

func makeRecords(n int) []map[string]any {
	records := make([]map[string]any, n)
	for i := range records {
		records[i] = map[string]any{"Name": "Test " + string(rune('A'+i%26))}
	}
	return records
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
