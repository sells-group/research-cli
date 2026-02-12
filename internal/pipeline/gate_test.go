package pipeline

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
)

func TestQualityGate_PassesAndUpdatesSF(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry", Required: true},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "Acme",
			SalesforceID: "001ABC",
			NotionPageID: "page-123",
		},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech", Confidence: 0.9},
		},
	}

	sfClient := &mockSalesforceClient{}
	sfClient.On("UpdateOne", ctx, "Account", "001ABC", mock.AnythingOfType("map[string]interface {}")).
		Return(nil)

	notionClient := &mockNotionClient{}
	notionClient.On("UpdatePage", ctx, "page-123", mock.Anything).
		Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{
			QualityScoreThreshold: 0.5,
		},
	}

	gate, err := QualityGate(ctx, result, fields, sfClient, notionClient, cfg)

	assert.NoError(t, err)
	assert.True(t, gate.Passed)
	assert.True(t, gate.SFUpdated)
	assert.False(t, gate.ManualReview)
	sfClient.AssertExpectations(t)
	notionClient.AssertExpectations(t)
}

func TestQualityGate_FailsSendsToManualReview(t *testing.T) {
	ctx := context.Background()

	// Set up a ToolJet webhook server.
	webhookCalled := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webhookCalled = true
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry", Required: true},
		{Key: "revenue", SFField: "AnnualRevenue", Required: true},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "Empty Co",
			NotionPageID: "page-456",
		},
		FieldValues: map[string]model.FieldValue{}, // No fields found.
	}

	sfClient := &mockSalesforceClient{}
	notionClient := &mockNotionClient{}
	notionClient.On("UpdatePage", ctx, "page-456", mock.Anything).
		Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{
			QualityScoreThreshold: 0.6,
		},
		ToolJet: config.ToolJetConfig{
			WebhookURL: ts.URL,
		},
	}

	gate, err := QualityGate(ctx, result, fields, sfClient, notionClient, cfg)

	assert.NoError(t, err)
	assert.False(t, gate.Passed)
	assert.False(t, gate.SFUpdated)
	assert.True(t, gate.ManualReview)
	assert.True(t, webhookCalled)
	notionClient.AssertExpectations(t)
}

func TestQualityGate_NoSalesforceID(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry"},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "No SF",
			NotionPageID: "page-789",
		},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech", Confidence: 0.9},
		},
	}

	sfClient := &mockSalesforceClient{}
	notionClient := &mockNotionClient{}
	notionClient.On("UpdatePage", ctx, "page-789", mock.Anything).
		Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{QualityScoreThreshold: 0.5},
	}

	gate, err := QualityGate(ctx, result, fields, sfClient, notionClient, cfg)

	assert.NoError(t, err)
	assert.True(t, gate.Passed)
	assert.False(t, gate.SFUpdated) // No SF ID, so no SF update.
	notionClient.AssertExpectations(t)
}

func TestBuildSFFields(t *testing.T) {
	fieldValues := map[string]model.FieldValue{
		"industry":  {SFField: "Industry", Value: "Tech"},
		"employees": {SFField: "NumberOfEmployees", Value: 200},
		"no_sf":     {SFField: "", Value: "ignored"}, // No SF field name.
	}

	sfFields := buildSFFields(fieldValues)

	assert.Len(t, sfFields, 2)
	assert.Equal(t, "Tech", sfFields["Industry"])
	assert.Equal(t, 200, sfFields["NumberOfEmployees"])
}
