package pipeline

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

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

	gate, err := QualityGate(ctx, result, fields, sfClient, notionClient, cfg)

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

	gate, err := QualityGate(ctx, result, fields, sfClient, notionClient, cfg)

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

	// Field values exist but none have SF field mapping.
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
	// UpdateOne should NOT be called since no SF fields are populated.

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-123", mock.Anything).Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{QualityScoreThreshold: 0.5},
	}

	gate, err := QualityGate(ctx, result, fields, sfClient, notionClient, cfg)

	assert.NoError(t, err)
	assert.True(t, gate.Passed)
	assert.False(t, gate.SFUpdated) // No SF fields to update
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

	gate, err := QualityGate(ctx, result, fields, sfClient, notionClient, cfg)

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

	gate, err := QualityGate(ctx, result, fields, sfClient, notionClient, cfg)

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

	gate, err := QualityGate(ctx, result, fields, sfClient, notionClient, cfg)

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
