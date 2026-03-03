package pipeline

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	notionmocks "github.com/sells-group/research-cli/pkg/notion/mocks"
	"github.com/sells-group/research-cli/pkg/salesforce"
	salesforcemocks "github.com/sells-group/research-cli/pkg/salesforce/mocks"
)

// ==========================================================================
// SalesforceExporter Tests
// ==========================================================================

func TestSalesforceExporter_Name(t *testing.T) {
	exp := NewSalesforceExporter(nil, nil, nil, nil, false)
	assert.Equal(t, "salesforce", exp.Name())
}

func TestSalesforceExporter_ImmediateMode(t *testing.T) {
	ctx := context.Background()

	sfClient := salesforcemocks.NewMockClient(t)
	sfClient.On("UpdateOne", mock.Anything, "Account", "001ABC", mock.AnythingOfType("map[string]interface {}")).
		Return(nil)

	notionClient := notionmocks.NewMockClient(t)
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry"},
	})
	cfg := &config.Config{}

	exp := NewSalesforceExporter(sfClient, notionClient, fields, cfg, false)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme", SalesforceID: "001ABC"},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech"},
		},
	}
	gate := &GateResult{Passed: true}

	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)
	sfClient.AssertExpectations(t)
}

func TestSalesforceExporter_DeferredMode(t *testing.T) {
	ctx := context.Background()

	sfClient := salesforcemocks.NewMockClient(t)
	notionClient := notionmocks.NewMockClient(t)
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry"},
	})
	cfg := &config.Config{}

	exp := NewSalesforceExporter(sfClient, notionClient, fields, cfg, true)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme", SalesforceID: "001ABC"},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech"},
		},
	}
	gate := &GateResult{Passed: true}

	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)

	assert.Len(t, exp.intents, 1)
	assert.Equal(t, "update", exp.intents[0].AccountOp)
	assert.Equal(t, "001ABC", exp.intents[0].AccountID)
}

func TestSalesforceExporter_SkipsWhenNotPassed(t *testing.T) {
	ctx := context.Background()

	sfClient := salesforcemocks.NewMockClient(t)
	notionClient := notionmocks.NewMockClient(t)
	fields := model.NewFieldRegistry(nil)
	cfg := &config.Config{}

	exp := NewSalesforceExporter(sfClient, notionClient, fields, cfg, false)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme"},
	}
	gate := &GateResult{Passed: false}

	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)
}

func TestSalesforceExporter_SkipsNilClient(t *testing.T) {
	ctx := context.Background()
	fields := model.NewFieldRegistry(nil)
	cfg := &config.Config{}

	exp := NewSalesforceExporter(nil, nil, fields, cfg, false)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme"},
	}
	gate := &GateResult{Passed: true}

	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)
}

func TestSalesforceExporter_SetDeferredMode(t *testing.T) {
	sfClient := salesforcemocks.NewMockClient(t)
	notionClient := notionmocks.NewMockClient(t)
	fields := model.NewFieldRegistry(nil)
	cfg := &config.Config{}

	exp := NewSalesforceExporter(sfClient, notionClient, fields, cfg, false)
	assert.False(t, exp.deferred)

	exp.SetDeferredMode(true)
	assert.True(t, exp.deferred)

	exp.SetDeferredMode(false)
	assert.False(t, exp.deferred)
}

func TestSalesforceExporter_DeferredNoSFID_WithURL(t *testing.T) {
	ctx := context.Background()

	sfClient := salesforcemocks.NewMockClient(t)
	// Mock Query for FindAccountByWebsite — return no match.
	sfClient.On("Query", mock.Anything, mock.MatchedBy(func(_ string) bool {
		return true
	}), mock.Anything).Return(nil)

	notionClient := notionmocks.NewMockClient(t)
	fields := model.NewFieldRegistry(nil)
	cfg := &config.Config{}

	exp := NewSalesforceExporter(sfClient, notionClient, fields, cfg, true)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme", URL: "https://acme.com"},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech"},
		},
	}
	gate := &GateResult{Passed: true}

	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)

	require.Len(t, exp.intents, 1)
	assert.Equal(t, "create", exp.intents[0].AccountOp)
}

func TestSalesforceExporter_DeferredNoSFID_DedupMatch(t *testing.T) {
	ctx := context.Background()

	sfClient := salesforcemocks.NewMockClient(t)
	// Mock Query for FindAccountByWebsite — return a match.
	sfClient.On("Query", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			out := args.Get(2).(*[]salesforce.Account)
			*out = []salesforce.Account{{ID: "001EXISTING", Name: "Existing Corp"}}
		}).
		Return(nil)

	notionClient := notionmocks.NewMockClient(t)
	fields := model.NewFieldRegistry(nil)
	cfg := &config.Config{}

	exp := NewSalesforceExporter(sfClient, notionClient, fields, cfg, true)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme", URL: "https://acme.com"},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech"},
		},
	}
	gate := &GateResult{Passed: true}

	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)

	require.Len(t, exp.intents, 1)
	assert.Equal(t, "update", exp.intents[0].AccountOp)
	assert.Equal(t, "001EXISTING", exp.intents[0].AccountID)
	assert.True(t, exp.intents[0].DedupMatch)
	assert.Equal(t, "001EXISTING", result.Company.SalesforceID)
}

func TestSalesforceExporter_DeferredNoSFID_NoURL(t *testing.T) {
	ctx := context.Background()

	sfClient := salesforcemocks.NewMockClient(t)
	notionClient := notionmocks.NewMockClient(t)
	fields := model.NewFieldRegistry(nil)
	cfg := &config.Config{}

	exp := NewSalesforceExporter(sfClient, notionClient, fields, cfg, true)

	result := &model.EnrichmentResult{
		Company:     model.Company{Name: "Acme"},
		FieldValues: map[string]model.FieldValue{},
	}
	gate := &GateResult{Passed: true}

	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)

	require.Len(t, exp.intents, 1)
	assert.Equal(t, "create", exp.intents[0].AccountOp)
}

func TestSalesforceExporter_DeferredDedupLookupFails(t *testing.T) {
	ctx := context.Background()

	sfClient := salesforcemocks.NewMockClient(t)
	sfClient.On("Query", mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("sf query error"))

	notionClient := notionmocks.NewMockClient(t)
	fields := model.NewFieldRegistry(nil)
	cfg := &config.Config{}

	exp := NewSalesforceExporter(sfClient, notionClient, fields, cfg, true)

	result := &model.EnrichmentResult{
		Company:     model.Company{Name: "Acme", URL: "https://acme.com"},
		FieldValues: map[string]model.FieldValue{},
	}
	gate := &GateResult{Passed: true}

	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)

	require.Len(t, exp.intents, 1)
	// Falls through to create when dedup lookup fails.
	assert.Equal(t, "create", exp.intents[0].AccountOp)
}

func TestSalesforceExporter_ImmediateNoSFID_CreateAccount(t *testing.T) {
	ctx := context.Background()

	sfClient := salesforcemocks.NewMockClient(t)
	// FindAccountByWebsite returns no match.
	sfClient.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	// CreateAccount: InsertOne called for "Account".
	sfClient.On("InsertOne", mock.Anything, "Account", mock.AnythingOfType("map[string]interface {}")).
		Return("001NEWID", nil)

	notionClient := notionmocks.NewMockClient(t)
	fields := model.NewFieldRegistry(nil)
	cfg := &config.Config{}

	exp := NewSalesforceExporter(sfClient, notionClient, fields, cfg, false)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "TestCo", URL: "https://testco.com"},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech"},
		},
	}
	gate := &GateResult{Passed: true}

	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)
	assert.Equal(t, "001NEWID", result.Company.SalesforceID)
}

func TestSalesforceExporter_ImmediateUpdateError(t *testing.T) {
	ctx := context.Background()

	sfClient := salesforcemocks.NewMockClient(t)
	sfClient.On("UpdateOne", mock.Anything, "Account", "001BAD", mock.Anything).
		Return(errors.New("sf update failed"))

	notionClient := notionmocks.NewMockClient(t)
	fields := model.NewFieldRegistry(nil)
	cfg := &config.Config{}

	exp := NewSalesforceExporter(sfClient, notionClient, fields, cfg, false)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme", SalesforceID: "001BAD"},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech"},
		},
	}
	gate := &GateResult{Passed: true}

	err := exp.ExportResult(ctx, result, gate)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sf update")
}

func TestSalesforceExporter_ImmediateWithContacts(t *testing.T) {
	ctx := context.Background()

	sfClient := salesforcemocks.NewMockClient(t)
	sfClient.On("UpdateOne", mock.Anything, "Account", "001ABC", mock.AnythingOfType("map[string]interface {}")).
		Return(nil)
	// Query for existing contacts.
	sfClient.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	// Create contact.
	sfClient.On("InsertOne", mock.Anything, "Contact", mock.AnythingOfType("map[string]interface {}")).
		Return("003NEW", nil)

	notionClient := notionmocks.NewMockClient(t)
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry"},
		{Key: "owner_last_name", SFField: "LastName", SFObject: "Contact"},
	})
	cfg := &config.Config{}

	exp := NewSalesforceExporter(sfClient, notionClient, fields, cfg, false)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme", SalesforceID: "001ABC"},
		FieldValues: map[string]model.FieldValue{
			"industry":        {FieldKey: "industry", SFField: "Industry", Value: "Tech"},
			"owner_last_name": {FieldKey: "owner_last_name", SFField: "LastName", Value: "Smith"},
		},
	}
	gate := &GateResult{Passed: true}

	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)
	sfClient.AssertExpectations(t)
}

func TestSalesforceExporter_FlushEmpty(t *testing.T) {
	ctx := context.Background()

	sfClient := salesforcemocks.NewMockClient(t)
	notionClient := notionmocks.NewMockClient(t)
	fields := model.NewFieldRegistry(nil)
	cfg := &config.Config{}

	exp := NewSalesforceExporter(sfClient, notionClient, fields, cfg, true)

	err := exp.Flush(ctx)
	assert.NoError(t, err)
}

func TestSalesforceExporter_FlushWithIntents(t *testing.T) {
	ctx := context.Background()

	sfClient := salesforcemocks.NewMockClient(t)
	// BulkUpdateAccounts uses UpdateCollection.
	sfClient.On("UpdateCollection", mock.Anything, "Account", mock.Anything).
		Return([]salesforce.CollectionResult{
			{ID: "001ABC", Success: true},
		}, nil)

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-1", mock.Anything).Return(nil, nil)

	fields := model.NewFieldRegistry(nil)
	cfg := &config.Config{}

	exp := NewSalesforceExporter(sfClient, notionClient, fields, cfg, true)
	exp.intents = []*SFWriteIntent{
		{
			AccountOp:     "update",
			AccountID:     "001ABC",
			AccountFields: map[string]any{"Industry": "Tech"},
			NotionPageID:  "page-1",
			Result: &model.EnrichmentResult{
				Company: model.Company{Name: "Acme", SalesforceID: "001ABC", NotionPageID: "page-1"},
			},
		},
	}

	err := exp.Flush(ctx)
	assert.NoError(t, err)
	assert.Empty(t, exp.intents)
	sfClient.AssertExpectations(t)
}

// ==========================================================================
// NotionExporter Tests
// ==========================================================================

func TestNotionExporter_Name(t *testing.T) {
	exp := NewNotionExporter(nil)
	assert.Equal(t, "notion", exp.Name())
}

func TestNotionExporter_Enriched(t *testing.T) {
	ctx := context.Background()

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-123", mock.Anything).Return(nil, nil)

	exp := NewNotionExporter(notionClient)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme", NotionPageID: "page-123"},
	}
	gate := &GateResult{Passed: true}

	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)
	notionClient.AssertExpectations(t)
}

func TestNotionExporter_ManualReview(t *testing.T) {
	ctx := context.Background()

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-123", mock.Anything).Return(nil, nil)

	exp := NewNotionExporter(notionClient)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme", NotionPageID: "page-123"},
	}
	gate := &GateResult{Passed: false}

	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)
	notionClient.AssertExpectations(t)
}

func TestNotionExporter_SkipsNoPageID(t *testing.T) {
	ctx := context.Background()

	notionClient := notionmocks.NewMockClient(t)

	exp := NewNotionExporter(notionClient)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme", NotionPageID: ""},
	}
	gate := &GateResult{Passed: true}

	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)
}

func TestNotionExporter_SkipsNilClient(t *testing.T) {
	ctx := context.Background()

	exp := NewNotionExporter(nil)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme", NotionPageID: "page-123"},
	}
	gate := &GateResult{Passed: true}

	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)
}

func TestNotionExporter_RetryOnFirstFailure(t *testing.T) {
	ctx := context.Background()

	notionClient := notionmocks.NewMockClient(t)
	// First call fails, second succeeds.
	notionClient.On("UpdatePage", mock.Anything, "page-123", mock.Anything).
		Return(nil, errors.New("temporary error")).Once()
	notionClient.On("UpdatePage", mock.Anything, "page-123", mock.Anything).
		Return(nil, nil).Once()

	exp := NewNotionExporter(notionClient)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme", NotionPageID: "page-123"},
	}
	gate := &GateResult{Passed: true}

	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)
	notionClient.AssertExpectations(t)
}

func TestNotionExporter_BothCallsFail(t *testing.T) {
	ctx := context.Background()

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-123", mock.Anything).
		Return(nil, errors.New("persistent error"))

	exp := NewNotionExporter(notionClient)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme", NotionPageID: "page-123"},
	}
	gate := &GateResult{Passed: true}

	// Should not return error — failures are logged, not propagated.
	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)
	notionClient.AssertExpectations(t)
}

func TestNotionExporter_Flush(t *testing.T) {
	ctx := context.Background()
	exp := NewNotionExporter(nil)
	err := exp.Flush(ctx)
	assert.NoError(t, err)
}

// ==========================================================================
// WebhookExporter Tests
// ==========================================================================

func TestWebhookExporter_Name(t *testing.T) {
	exp := NewWebhookExporter("http://example.com")
	assert.Equal(t, "webhook", exp.Name())
}

func TestWebhookExporter_SkipsWhenPassed(t *testing.T) {
	ctx := context.Background()

	exp := NewWebhookExporter("http://localhost:9999/webhook")

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme"},
	}
	gate := &GateResult{Passed: true}

	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)
}

func TestWebhookExporter_SkipsEmptyURL(t *testing.T) {
	ctx := context.Background()

	exp := NewWebhookExporter("")

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme"},
	}
	gate := &GateResult{Passed: false}

	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)
}

func TestWebhookExporter_FiresOnFailedGate(t *testing.T) {
	ctx := context.Background()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	exp := NewWebhookExporter(ts.URL)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme"},
	}
	gate := &GateResult{Passed: false}

	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)
}

func TestWebhookExporter_WebhookError(t *testing.T) {
	ctx := context.Background()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	exp := NewWebhookExporter(ts.URL)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme"},
	}
	gate := &GateResult{Passed: false}

	// Webhook errors are logged, not returned.
	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)
}

func TestWebhookExporter_Flush(t *testing.T) {
	ctx := context.Background()
	exp := NewWebhookExporter("http://example.com")
	err := exp.Flush(ctx)
	assert.NoError(t, err)
}

// ==========================================================================
// JSONExporter Tests
// ==========================================================================

func TestJSONExporter_Name(t *testing.T) {
	exp := NewJSONExporter("out.json")
	assert.Equal(t, "json", exp.Name())
}

func TestJSONExporter_ExportAndFlush(t *testing.T) {
	ctx := context.Background()
	outPath := filepath.Join(t.TempDir(), "results.json")

	exp := NewJSONExporter(outPath)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme", URL: "https://acme.com"},
		Score:   0.85,
	}
	gate := &GateResult{Passed: true}

	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)

	err = exp.Flush(ctx)
	assert.NoError(t, err)

	data, err := os.ReadFile(outPath)
	require.NoError(t, err)

	var results []model.EnrichmentResult
	err = json.Unmarshal(data, &results)
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "Acme", results[0].Company.Name)
}

func TestJSONExporter_FlushEmpty(t *testing.T) {
	ctx := context.Background()
	exp := NewJSONExporter("should-not-create.json")

	err := exp.Flush(ctx)
	assert.NoError(t, err)
}

func TestJSONExporter_FlushMultipleResults(t *testing.T) {
	ctx := context.Background()
	outPath := filepath.Join(t.TempDir(), "multi.json")

	exp := NewJSONExporter(outPath)

	for i := range 3 {
		result := &model.EnrichmentResult{
			Company: model.Company{Name: fmt.Sprintf("Company%d", i)},
		}
		err := exp.ExportResult(ctx, result, &GateResult{Passed: true})
		require.NoError(t, err)
	}

	err := exp.Flush(ctx)
	assert.NoError(t, err)

	data, err := os.ReadFile(outPath)
	require.NoError(t, err)

	var results []model.EnrichmentResult
	require.NoError(t, json.Unmarshal(data, &results))
	assert.Len(t, results, 3)
}

func TestJSONExporter_FlushBadPath(t *testing.T) {
	ctx := context.Background()

	exp := NewJSONExporter("/nonexistent/dir/out.json")
	exp.results = []*model.EnrichmentResult{
		{Company: model.Company{Name: "Acme"}},
	}

	err := exp.Flush(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "json export")
}

// ==========================================================================
// CSVExporter Tests
// ==========================================================================

func TestCSVExporter_Name(t *testing.T) {
	exp := NewCSVExporter(ExportFormatSFReport, "out.csv", nil)
	assert.Equal(t, "csv-sf-report-csv", exp.Name())

	exp2 := NewCSVExporter(ExportFormatGrata, "out.csv", nil)
	assert.Equal(t, "csv-grata-csv", exp2.Name())
}

func TestCSVExporter_ExportResult(t *testing.T) {
	ctx := context.Background()
	exp := NewCSVExporter(ExportFormatGrata, "out.csv", nil)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme"},
	}
	gate := &GateResult{Passed: true}

	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)
	assert.Len(t, exp.results, 1)
}

func TestCSVExporter_FlushEmpty(t *testing.T) {
	ctx := context.Background()
	exp := NewCSVExporter(ExportFormatGrata, "should-not-create.csv", nil)

	err := exp.Flush(ctx)
	assert.NoError(t, err)
}

func TestCSVExporter_FlushUnknownFormat(t *testing.T) {
	ctx := context.Background()
	exp := NewCSVExporter("unknown-format", "out.csv", nil)
	exp.results = []*model.EnrichmentResult{
		{Company: model.Company{Name: "Acme"}},
	}

	err := exp.Flush(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown format")
}

func TestCSVExporter_FlushGrata(t *testing.T) {
	ctx := context.Background()
	outPath := filepath.Join(t.TempDir(), "grata.csv")

	exp := NewCSVExporter(ExportFormatGrata, outPath, nil)
	exp.results = []*model.EnrichmentResult{
		{
			Company: model.Company{Name: "Acme", URL: "https://acme.com"},
			FieldValues: map[string]model.FieldValue{
				"industry": {FieldKey: "industry", Value: "Tech"},
			},
		},
	}

	err := exp.Flush(ctx)
	assert.NoError(t, err)

	data, err := os.ReadFile(outPath)
	require.NoError(t, err)
	assert.Contains(t, string(data), "Acme")
}

func TestCSVExporter_FlushSFReport(t *testing.T) {
	ctx := context.Background()
	outPath := filepath.Join(t.TempDir(), "sfreport.csv")

	exp := NewCSVExporter(ExportFormatSFReport, outPath, nil)
	exp.results = []*model.EnrichmentResult{
		{
			Company: model.Company{Name: "Acme", URL: "https://acme.com"},
			FieldValues: map[string]model.FieldValue{
				"industry": {FieldKey: "industry", Value: "Tech"},
			},
		},
	}

	err := exp.Flush(ctx)
	assert.NoError(t, err)

	data, err := os.ReadFile(outPath)
	require.NoError(t, err)
	assert.Contains(t, string(data), "Acme")
}

// ==========================================================================
// ProvenanceCSVExporter Tests
// ==========================================================================

func TestProvenanceCSVExporter_Name(t *testing.T) {
	exp := NewProvenanceCSVExporter("prov.csv")
	assert.Equal(t, "provenance-csv", exp.Name())
}

func TestProvenanceCSVExporter_ExportResult(t *testing.T) {
	ctx := context.Background()
	exp := NewProvenanceCSVExporter("prov.csv")

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme"},
	}
	gate := &GateResult{Passed: true}

	err := exp.ExportResult(ctx, result, gate)
	assert.NoError(t, err)
	assert.Len(t, exp.results, 1)
}

func TestProvenanceCSVExporter_FlushEmpty(t *testing.T) {
	ctx := context.Background()
	exp := NewProvenanceCSVExporter("should-not-create.csv")

	err := exp.Flush(ctx)
	assert.NoError(t, err)
}

func TestProvenanceCSVExporter_Flush(t *testing.T) {
	ctx := context.Background()
	outPath := filepath.Join(t.TempDir(), "prov.csv")

	exp := NewProvenanceCSVExporter(outPath)
	exp.results = []*model.EnrichmentResult{
		{
			Company: model.Company{
				Name:         "Acme",
				SalesforceID: "001ABC",
				URL:          "https://acme.com",
			},
			FieldValues: map[string]model.FieldValue{
				"industry": {
					FieldKey:   "industry",
					Value:      "Technology",
					Confidence: 0.9,
					Tier:       1,
					Source:     "https://acme.com/about",
					Reasoning:  "Found on about page",
				},
				"employees": {
					FieldKey:   "employees",
					Value:      200,
					Confidence: 0.75,
					Tier:       2,
					Source:     "https://acme.com/team",
					Reasoning:  "Team page listing",
				},
			},
		},
	}

	err := exp.Flush(ctx)
	assert.NoError(t, err)

	f, err := os.Open(outPath)
	require.NoError(t, err)
	defer f.Close() //nolint:errcheck

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	require.NoError(t, err)

	// Header + 2 field rows.
	assert.Len(t, records, 3)
	assert.Equal(t, provenanceColumns, records[0])
	assert.Equal(t, "Acme", records[1][0])
	assert.Equal(t, "001ABC", records[1][1])
}

func TestProvenanceCSVExporter_FlushBadPath(t *testing.T) {
	ctx := context.Background()

	exp := NewProvenanceCSVExporter("/nonexistent/dir/prov.csv")
	exp.results = []*model.EnrichmentResult{
		{Company: model.Company{Name: "Acme"}},
	}

	err := exp.Flush(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "provenance export")
}

// ==========================================================================
// Pipeline.FlushExporters Tests
// ==========================================================================

// mockExporter is a simple mock for testing FlushExporters.
type mockExporter struct {
	name     string
	flushed  bool
	flushErr error
}

func (m *mockExporter) Name() string { return m.name }
func (m *mockExporter) ExportResult(_ context.Context, _ *model.EnrichmentResult, _ *GateResult) error {
	return nil
}
func (m *mockExporter) Flush(_ context.Context) error {
	m.flushed = true
	return m.flushErr
}

func TestFlushExporters_Success(t *testing.T) {
	ctx := context.Background()
	p := &Pipeline{}

	e1 := &mockExporter{name: "exporter-1"}
	e2 := &mockExporter{name: "exporter-2"}
	p.AddExporter(e1)
	p.AddExporter(e2)

	err := p.FlushExporters(ctx)
	assert.NoError(t, err)
	assert.True(t, e1.flushed)
	assert.True(t, e2.flushed)
}

func TestFlushExporters_Error(t *testing.T) {
	ctx := context.Background()
	p := &Pipeline{}

	e1 := &mockExporter{name: "exporter-1"}
	e2 := &mockExporter{name: "exporter-2", flushErr: errors.New("flush failed")}
	e3 := &mockExporter{name: "exporter-3"}
	p.AddExporter(e1)
	p.AddExporter(e2)
	p.AddExporter(e3)

	err := p.FlushExporters(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exporter-2")
	assert.True(t, e1.flushed)
	assert.True(t, e2.flushed)
	// Stops after first error.
	assert.False(t, e3.flushed)
}

func TestFlushExporters_Empty(t *testing.T) {
	ctx := context.Background()
	p := &Pipeline{}

	err := p.FlushExporters(ctx)
	assert.NoError(t, err)
}

// ==========================================================================
// ExporterByName Tests
// ==========================================================================

func TestJSONExporter_Flush_BadPath(t *testing.T) {
	ctx := context.Background()
	exp := NewJSONExporter("/nonexistent/path/out.json")
	_ = exp.ExportResult(ctx, &model.EnrichmentResult{
		Company: model.Company{Name: "Test"},
	}, &GateResult{Passed: true})

	err := exp.Flush(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "json export")
}

func TestJSONExporter_Flush_EmptyResults(t *testing.T) {
	ctx := context.Background()
	exp := NewJSONExporter("/tmp/unused.json")
	err := exp.Flush(ctx)
	assert.NoError(t, err)
}

func TestProvenanceCSVExporter_Flush_BadPath(t *testing.T) {
	ctx := context.Background()
	exp := NewProvenanceCSVExporter("/nonexistent/dir/provenance.csv")
	_ = exp.ExportResult(ctx, &model.EnrichmentResult{
		Company:     model.Company{Name: "Test"},
		FieldValues: map[string]model.FieldValue{"industry": {FieldKey: "industry", Value: "Tech", Confidence: 0.9}},
	}, &GateResult{Passed: true})

	err := exp.Flush(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "provenance export")
}

func TestProvenanceCSVExporter_Flush_EmptyResults(t *testing.T) {
	ctx := context.Background()
	exp := NewProvenanceCSVExporter("/tmp/unused.csv")
	err := exp.Flush(ctx)
	assert.NoError(t, err)
}

func TestSalesforceExporter_Flush_EmptyIntents(t *testing.T) {
	ctx := context.Background()
	sfClient := salesforcemocks.NewMockClient(t)
	exp := NewSalesforceExporter(sfClient, nil, model.NewFieldRegistry(nil), &config.Config{}, true)
	err := exp.Flush(ctx)
	assert.NoError(t, err)
}

func TestExporterByName(t *testing.T) {
	p := &Pipeline{}
	p.AddExporter(NewNotionExporter(nil))
	p.AddExporter(NewWebhookExporter("http://example.com"))

	notion := p.ExporterByName("notion")
	assert.NotNil(t, notion)
	assert.Equal(t, "notion", notion.Name())

	webhook := p.ExporterByName("webhook")
	assert.NotNil(t, webhook)

	missing := p.ExporterByName("nonexistent")
	assert.Nil(t, missing)
}
