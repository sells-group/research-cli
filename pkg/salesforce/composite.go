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

// bulkInsert splits records into batches of 200 and creates them via InsertCollection.
// Returns accumulated results and stops on first batch error.
func bulkInsert(ctx context.Context, c Client, sObjectName string, records []map[string]any) ([]CollectionResult, error) {
	if len(records) == 0 {
		return nil, nil
	}

	var allResults []CollectionResult

	for start := 0; start < len(records); start += maxBatchSize {
		end := start + maxBatchSize
		if end > len(records) {
			end = len(records)
		}

		results, err := c.InsertCollection(ctx, sObjectName, records[start:end])
		if err != nil {
			return allResults, eris.Wrap(err, fmt.Sprintf("sf: bulk insert %s batch %d-%d", sObjectName, start, end))
		}
		allResults = append(allResults, results...)
	}

	return allResults, nil
}

// BulkCreateAccounts creates Accounts in batches of 200 via the Collections API.
// Each record is a field map (must include "Name" at minimum).
// Returns CollectionResults with new SF IDs.
func BulkCreateAccounts(ctx context.Context, c Client, records []map[string]any) ([]CollectionResult, error) {
	return bulkInsert(ctx, c, "Account", records)
}

// BulkCreateContacts creates Contacts in batches of 200 via the Collections API.
// Each record must include "AccountId" and "LastName" at minimum.
// Returns CollectionResults with new SF IDs.
func BulkCreateContacts(ctx context.Context, c Client, records []map[string]any) ([]CollectionResult, error) {
	return bulkInsert(ctx, c, "Contact", records)
}
