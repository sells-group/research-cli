package pipeline

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	notionmocks "github.com/sells-group/research-cli/pkg/notion/mocks"
	salesforcemocks "github.com/sells-group/research-cli/pkg/salesforce/mocks"
)

func TestQualityGate_SFUpdateFails(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry", Required: true},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "SF Fail Co",
			SalesforceID: "001ERR",
			NotionPageID: "page-err",
		},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech", Confidence: 0.9},
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	sfClient.On("UpdateOne", mock.Anything, "Account", "001ERR", mock.AnythingOfType("map[string]interface {}")).
		Return(errors.New("sf connection error"))

	notionClient := notionmocks.NewMockClient(t)
	// Notion runs concurrently with SF; it may or may not execute before SF error cancels context.
	notionClient.On("UpdatePage", mock.Anything, "page-err", mock.Anything).Return(nil, nil).Maybe()

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{
			QualityScoreThreshold: 0.5,
		},
	}

	gate, err := QualityGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "gate: sf update")
	assert.True(t, gate.Passed)
	assert.False(t, gate.SFUpdated)
	sfClient.AssertExpectations(t)
}

func TestQualityGate_NoNotionPageID(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry"},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "No Notion",
			SalesforceID: "001ABC",
			NotionPageID: "", // no page ID
		},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech", Confidence: 0.9},
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	sfClient.On("UpdateOne", mock.Anything, "Account", "001ABC", mock.AnythingOfType("map[string]interface {}")).
		Return(nil)

	notionClient := notionmocks.NewMockClient(t)
	// UpdatePage should NOT be called since NotionPageID is empty.

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{QualityScoreThreshold: 0.5},
	}

	gate, err := QualityGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	assert.NoError(t, err)
	assert.True(t, gate.Passed)
	assert.True(t, gate.SFUpdated)
	sfClient.AssertExpectations(t)
	notionClient.AssertExpectations(t)
}

func TestQualityGate_PassNoSFFieldValues(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry"},
	})

	// Field values exist but none have SF field mapping. However,
	// ensureMinimumSFFields still adds Name from Company, so UpdateAccount
	// is called with at least the company name.
	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "No SF Fields",
			SalesforceID: "001ABC",
			NotionPageID: "page-123",
		},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "", Value: "Tech", Confidence: 0.9},
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	// UpdateOne IS called because ensureMinimumSFFields adds Name.
	sfClient.On("UpdateOne", mock.Anything, "Account", "001ABC", mock.AnythingOfType("map[string]interface {}")).
		Return(nil)

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-123", mock.Anything).Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{QualityScoreThreshold: 0.5},
	}

	gate, err := QualityGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	assert.NoError(t, err)
	assert.True(t, gate.Passed)
	assert.True(t, gate.SFUpdated)
	sfClient.AssertExpectations(t)
}

func TestQualityGate_FailNoWebhook(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry", Required: true},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "Bad Co",
			NotionPageID: "page-789",
		},
		FieldValues: map[string]model.FieldValue{}, // No fields.
	}

	sfClient := salesforcemocks.NewMockClient(t)
	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-789", mock.Anything).Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{QualityScoreThreshold: 0.6},
		ToolJet:  config.ToolJetConfig{WebhookURL: ""}, // No webhook configured.
	}

	gate, err := QualityGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	assert.NoError(t, err)
	assert.False(t, gate.Passed)
	assert.False(t, gate.ManualReview) // No webhook -> no manual review.
}

func TestQualityGate_NotionUpdateFails(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry"},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "Notion Fail",
			SalesforceID: "001ABC",
			NotionPageID: "page-fail",
		},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech", Confidence: 0.9},
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	sfClient.On("UpdateOne", mock.Anything, "Account", "001ABC", mock.AnythingOfType("map[string]interface {}")).
		Return(nil)

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-fail", mock.Anything).
		Return(nil, errors.New("notion api error"))

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{QualityScoreThreshold: 0.5},
	}

	gate, err := QualityGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	// Notion failure is a warning, not an error.
	assert.NoError(t, err)
	assert.True(t, gate.Passed)
	assert.True(t, gate.SFUpdated)
}

func TestQualityGate_WithReportInSFFields(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry"},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "Report Co",
			SalesforceID: "001REP",
			NotionPageID: "page-rep",
		},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech", Confidence: 0.9},
		},
		Report: "# Enrichment Report\nThis is a test report.",
	}

	sfClient := salesforcemocks.NewMockClient(t)
	sfClient.On("UpdateOne", mock.Anything, "Account", "001REP", mock.MatchedBy(func(fields map[string]any) bool {
		_, hasReport := fields["Enrichment_Report__c"]
		return hasReport
	})).Return(nil)

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-rep", mock.Anything).Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{QualityScoreThreshold: 0.5},
	}

	gate, err := QualityGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	assert.NoError(t, err)
	assert.True(t, gate.SFUpdated)
	sfClient.AssertExpectations(t)
}

func TestSendToToolJet_ErrorStatus(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Test"},
	}

	err := sendToToolJet(context.Background(), result, ts.URL)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tooljet returned status 500")
}

func TestSendToToolJet_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Test"},
	}

	err := sendToToolJet(context.Background(), result, ts.URL)
	assert.NoError(t, err)
}

func TestSendToToolJet_ConnectionError(t *testing.T) {
	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Test"},
	}

	err := sendToToolJet(context.Background(), result, "http://localhost:1/bad")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tooljet request failed")
}

func TestBuildSFFields_Empty(t *testing.T) {
	fields := buildSFFields(nil)
	assert.Empty(t, fields)
}

func TestBuildSFFields_AllEmpty(t *testing.T) {
	fieldValues := map[string]model.FieldValue{
		"a": {SFField: "", Value: "ignored"},
		"b": {SFField: "", Value: "also ignored"},
	}

	fields := buildSFFields(fieldValues)
	assert.Empty(t, fields)
}

// TestQualityGate_SFSuccessNotionFailRetries verifies that when SF succeeds
// but Notion fails on the first attempt, the gate retries Notion once and
// succeeds on the retry. The overall gate should return no error.
func TestQualityGate_SFSuccessNotionFailRetries(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry", Required: true},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "RetryOK Corp",
			SalesforceID: "001RETRY",
			NotionPageID: "page-retry-ok",
		},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Finance", Confidence: 0.95},
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	sfClient.On("UpdateOne", mock.Anything, "Account", "001RETRY", mock.AnythingOfType("map[string]interface {}")).
		Return(nil)

	notionClient := notionmocks.NewMockClient(t)
	// First call fails, second call (retry) succeeds.
	notionClient.On("UpdatePage", mock.Anything, "page-retry-ok", mock.Anything).
		Return(nil, errors.New("notion: rate limited")).Once()
	notionClient.On("UpdatePage", mock.Anything, "page-retry-ok", mock.Anything).
		Return(nil, nil).Once()

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{
			QualityScoreThreshold: 0.5,
		},
	}

	gate, err := QualityGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	assert.NoError(t, err)
	assert.True(t, gate.Passed)
	assert.True(t, gate.SFUpdated)
	// Notion UpdatePage should have been called exactly twice (initial + retry).
	notionClient.AssertNumberOfCalls(t, "UpdatePage", 2)
	sfClient.AssertExpectations(t)
	notionClient.AssertExpectations(t)
}

// TestQualityGate_SFSuccessNotionFailRetryExhausted verifies that when SF
// succeeds but Notion fails on both the initial attempt and the retry, the
// gate still returns without a top-level error (Notion failure is logged but
// non-blocking since the Notion goroutine always returns nil to errgroup).
func TestQualityGate_SFSuccessNotionFailRetryExhausted(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry", Required: true},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "RetryFail Corp",
			SalesforceID: "001EXHAUST",
			NotionPageID: "page-retry-fail",
		},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Healthcare", Confidence: 0.85},
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	sfClient.On("UpdateOne", mock.Anything, "Account", "001EXHAUST", mock.AnythingOfType("map[string]interface {}")).
		Return(nil)

	notionClient := notionmocks.NewMockClient(t)
	// Both initial and retry calls fail.
	notionClient.On("UpdatePage", mock.Anything, "page-retry-fail", mock.Anything).
		Return(nil, errors.New("notion: service unavailable"))

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{
			QualityScoreThreshold: 0.5,
		},
	}

	gate, err := QualityGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	// Gate should still return without error — Notion failure is non-blocking.
	assert.NoError(t, err)
	assert.True(t, gate.Passed)
	assert.True(t, gate.SFUpdated)
	// Notion UpdatePage called twice: initial attempt + one retry.
	notionClient.AssertNumberOfCalls(t, "UpdatePage", 2)
	sfClient.AssertExpectations(t)
}

// TestQualityGate_NotionSuccessSFFailLogsInconsistency verifies that when
// Salesforce fails but Notion succeeds, the gate returns the SF error.
// This tests the inconsistent state path where Notion is updated but SF is not.
func TestQualityGate_NotionSuccessSFFailLogsInconsistency(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry", Required: true},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "SFFail Corp",
			SalesforceID: "001INCON",
			NotionPageID: "page-inconsistent",
		},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Energy", Confidence: 0.9},
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	sfClient.On("UpdateOne", mock.Anything, "Account", "001INCON", mock.AnythingOfType("map[string]interface {}")).
		Return(errors.New("sf: 500 internal server error"))

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-inconsistent", mock.Anything).
		Return(nil, nil).Maybe()

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{
			QualityScoreThreshold: 0.5,
		},
	}

	gate, err := QualityGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	// The SF error should propagate from errgroup.Wait().
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sf")
	assert.True(t, gate.Passed)
	assert.False(t, gate.SFUpdated)
	sfClient.AssertExpectations(t)
}

// TestSendToToolJet_Timeout verifies that the webhook call to ToolJet respects
// the webhookClient timeout and returns an error when the server is too slow.
func TestSendToToolJet_Timeout(t *testing.T) {
	// Save original client and restore after test.
	origClient := webhookClient
	t.Cleanup(func() { webhookClient = origClient })

	// Use a very short timeout for the test.
	webhookClient = &http.Client{Timeout: 100 * time.Millisecond}

	// Server that blocks longer than the client timeout. Use a done channel so
	// the handler exits promptly when the test finishes, avoiding slow Close().
	done := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-done
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	defer close(done) // unblock handler before ts.Close() waits for connections

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name: "Slow Webhook Corp",
		},
		FieldValues: map[string]model.FieldValue{},
	}

	start := time.Now()
	err := sendToToolJet(context.Background(), result, ts.URL)
	elapsed := time.Since(start)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tooljet request failed")
	// Should have returned well before 2s, respecting the 100ms timeout.
	assert.Less(t, elapsed, 2*time.Second, "should timeout quickly, not wait for server")
}

// TestWebhookClient_HasTimeout verifies that the package-level webhookClient
// is configured with the expected 10-second timeout.
func TestWebhookClient_HasTimeout(t *testing.T) {
	assert.Equal(t, 10*time.Second, webhookClient.Timeout)
}
