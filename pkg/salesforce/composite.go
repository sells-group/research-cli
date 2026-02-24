package salesforce

import (
	"context"
	"fmt"

	"github.com/rotisserie/eris"
)

// maxBatchSize is the Salesforce Collections API limit per request.
const maxBatchSize = 200

// AccountUpdate holds an account ID and the fields to update.
type AccountUpdate struct {
	ID     string
	Fields map[string]any
}

// BulkUpdateAccounts splits updates into batches of 200 (SF Collections API limit)
// and sends them via UpdateCollection.
func BulkUpdateAccounts(ctx context.Context, c Client, updates []AccountUpdate) ([]CollectionResult, error) {
	if len(updates) == 0 {
		return nil, nil
	}

	var allResults []CollectionResult

	for start := 0; start < len(updates); start += maxBatchSize {
		end := start + maxBatchSize
		if end > len(updates) {
			end = len(updates)
		}
		batch := updates[start:end]

		records := make([]CollectionRecord, len(batch))
		for i, u := range batch {
			records[i] = CollectionRecord(u)
		}

		results, err := c.UpdateCollection(ctx, "Account", records)
		if err != nil {
			return allResults, eris.Wrap(err, fmt.Sprintf("sf: bulk update accounts batch %d-%d", start, end))
		}
		allResults = append(allResults, results...)
	}

	return allResults, nil
}
