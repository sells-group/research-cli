package pipeline

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jomei/notionapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	notionmocks "github.com/sells-group/research-cli/pkg/notion/mocks"
	salesforcemocks "github.com/sells-group/research-cli/pkg/salesforce/mocks"
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

	sfClient := salesforcemocks.NewMockClient(t)
	sfClient.On("UpdateOne", mock.Anything, "Account", "001ABC", mock.AnythingOfType("map[string]interface {}")).
		Return(nil)

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-123", mock.Anything).
		Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{
			QualityScoreThreshold: 0.5,
		},
	}

	gate, err := QualityGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

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

	sfClient := salesforcemocks.NewMockClient(t)
	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-456", mock.Anything).
		Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{
			QualityScoreThreshold: 0.6,
		},
		ToolJet: config.ToolJetConfig{
			WebhookURL: ts.URL,
		},
	}

	gate, err := QualityGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	assert.NoError(t, err)
	assert.False(t, gate.Passed)
	assert.False(t, gate.SFUpdated)
	assert.True(t, gate.ManualReview)
	assert.True(t, webhookCalled)
	notionClient.AssertExpectations(t)
}

func TestQualityGate_NoSalesforceID_CreatesAccount(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry"},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "New Co",
			URL:          "https://newco.com",
			NotionPageID: "page-789",
		},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech", Confidence: 0.9},
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	// CreateAccount → InsertOne("Account", ...) returns new ID.
	sfClient.On("InsertOne", mock.Anything, "Account", mock.AnythingOfType("map[string]interface {}")).
		Return("001NEW", nil)

	notionClient := notionmocks.NewMockClient(t)
	// Status update + SalesforceID writeback = 2 UpdatePage calls.
	notionClient.On("UpdatePage", mock.Anything, "page-789", mock.Anything).
		Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{QualityScoreThreshold: 0.5},
	}

	gate, err := QualityGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	assert.NoError(t, err)
	assert.True(t, gate.Passed)
	assert.True(t, gate.SFUpdated)
	assert.Equal(t, "001NEW", result.Company.SalesforceID)
	sfClient.AssertExpectations(t)
	notionClient.AssertExpectations(t)
}

func TestQualityGate_NoSFClient(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry"},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "No SF Client",
			NotionPageID: "page-000",
		},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech", Confidence: 0.9},
		},
	}

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-000", mock.Anything).
		Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{QualityScoreThreshold: 0.5},
	}

	// Pass nil sfClient — should not panic or attempt SF operations.
	gate, err := QualityGate(ctx, result, fields, nil, nil, notionClient, cfg)

	assert.NoError(t, err)
	assert.True(t, gate.Passed)
	assert.False(t, gate.SFUpdated)
	notionClient.AssertExpectations(t)
}

func TestQualityGate_ContactCreation(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry", SFObject: "Account"},
		{Key: "owner_last_name", SFField: "LastName", SFObject: "Contact"},
		{Key: "owner_first_name", SFField: "FirstName", SFObject: "Contact"},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "Contact Co",
			URL:          "https://contactco.com",
			SalesforceID: "001EXIST",
			NotionPageID: "page-contact",
		},
		FieldValues: map[string]model.FieldValue{
			"industry":         {FieldKey: "industry", SFField: "Industry", Value: "Finance", Confidence: 0.9},
			"owner_last_name":  {FieldKey: "owner_last_name", SFField: "LastName", Value: "Smith", Confidence: 0.8},
			"owner_first_name": {FieldKey: "owner_first_name", SFField: "FirstName", Value: "John", Confidence: 0.8},
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	// Account update.
	sfClient.On("UpdateOne", mock.Anything, "Account", "001EXIST", mock.AnythingOfType("map[string]interface {}")).
		Return(nil)
	// Contact creation.
	sfClient.On("InsertOne", mock.Anything, "Contact", mock.AnythingOfType("map[string]interface {}")).
		Return("003NEW", nil)

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-contact", mock.Anything).
		Return(nil, nil)

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

func TestBuildSFFieldsByObject(t *testing.T) {
	registry := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry", SFObject: "Account"},
		{Key: "owner_last_name", SFField: "LastName", SFObject: "Contact"},
		{Key: "revenue", SFField: "AnnualRevenue"}, // No SFObject → defaults to Account.
	})

	fieldValues := map[string]model.FieldValue{
		"industry":        {FieldKey: "industry", SFField: "Industry", Value: "Tech"},
		"owner_last_name": {FieldKey: "owner_last_name", SFField: "LastName", Value: "Smith"},
		"revenue":         {FieldKey: "revenue", SFField: "AnnualRevenue", Value: 5000000},
	}

	accountFields, contactFields := buildSFFieldsByObject(fieldValues, registry)

	assert.Len(t, accountFields, 2)
	assert.Equal(t, "Tech", accountFields["Industry"])
	assert.Equal(t, 5000000, accountFields["AnnualRevenue"])

	assert.Len(t, contactFields, 1)
	assert.Equal(t, "Smith", contactFields["LastName"])
}

func TestEnsureMinimumSFFields(t *testing.T) {
	t.Run("fills missing Name and Website", func(t *testing.T) {
		fields := map[string]any{"Industry": "Tech"}
		company := model.Company{Name: "Acme", URL: "https://acme.com"}
		ensureMinimumSFFields(fields, company)
		assert.Equal(t, "Acme", fields["Name"])
		assert.Equal(t, "https://acme.com", fields["Website"])
	})

	t.Run("does not overwrite existing values", func(t *testing.T) {
		fields := map[string]any{"Name": "Custom Name", "Website": "https://custom.com"}
		company := model.Company{Name: "Acme", URL: "https://acme.com"}
		ensureMinimumSFFields(fields, company)
		assert.Equal(t, "Custom Name", fields["Name"])
		assert.Equal(t, "https://custom.com", fields["Website"])
	})

	t.Run("empty company leaves fields unchanged", func(t *testing.T) {
		fields := map[string]any{"Industry": "Tech"}
		ensureMinimumSFFields(fields, model.Company{})
		_, hasName := fields["Name"]
		_, hasWebsite := fields["Website"]
		assert.False(t, hasName)
		assert.False(t, hasWebsite)
	})
}

func TestQualityGate_CreateAccountFails(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry"},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "Fail Co",
			URL:          "https://failco.com",
			NotionPageID: "page-fail",
		},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech", Confidence: 0.9},
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	sfClient.On("InsertOne", mock.Anything, "Account", mock.AnythingOfType("map[string]interface {}")).
		Return("", assert.AnError)

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-fail", mock.Anything).
		Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{QualityScoreThreshold: 0.5},
	}

	gate, err := QualityGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	assert.Error(t, err)
	assert.True(t, gate.Passed) // Score passed threshold.
	assert.False(t, gate.SFUpdated)
	assert.Equal(t, "", result.Company.SalesforceID) // Not set on failure.
	sfClient.AssertExpectations(t)
}

func TestQualityGate_ContactCreationFails(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry", SFObject: "Account"},
		{Key: "owner_last_name", SFField: "LastName", SFObject: "Contact"},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "Contact Fail Co",
			SalesforceID: "001EXIST",
			NotionPageID: "page-cf",
		},
		FieldValues: map[string]model.FieldValue{
			"industry":        {FieldKey: "industry", SFField: "Industry", Value: "Tech", Confidence: 0.9},
			"owner_last_name": {FieldKey: "owner_last_name", SFField: "LastName", Value: "Smith", Confidence: 0.8},
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	// Account update succeeds.
	sfClient.On("UpdateOne", mock.Anything, "Account", "001EXIST", mock.AnythingOfType("map[string]interface {}")).
		Return(nil)
	// Contact creation fails — should be logged but not fatal.
	sfClient.On("InsertOne", mock.Anything, "Contact", mock.AnythingOfType("map[string]interface {}")).
		Return("", assert.AnError)

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-cf", mock.Anything).
		Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{QualityScoreThreshold: 0.5},
	}

	gate, err := QualityGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	// Contact failure is non-fatal.
	assert.NoError(t, err)
	assert.True(t, gate.Passed)
	assert.True(t, gate.SFUpdated) // Account update succeeded.
	sfClient.AssertExpectations(t)
}

func TestQualityGate_CreateAccount_MinimumFieldsOnly(t *testing.T) {
	ctx := context.Background()

	// No enriched fields map to SF, but ensureMinimumSFFields adds Name+Website.
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "notes", SFField: ""}, // No SF mapping.
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "Minimum Co",
			URL:          "https://minimum.com",
			NotionPageID: "page-min",
		},
		FieldValues: map[string]model.FieldValue{
			"notes": {FieldKey: "notes", SFField: "", Value: "some notes", Confidence: 0.9},
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	var capturedFields map[string]any
	sfClient.On("InsertOne", mock.Anything, "Account", mock.AnythingOfType("map[string]interface {}")).
		Run(func(args mock.Arguments) {
			capturedFields = args.Get(2).(map[string]any)
		}).
		Return("001MIN", nil)

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-min", mock.Anything).
		Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{QualityScoreThreshold: 0.0}, // 0 threshold so empty fields pass.
	}

	gate, err := QualityGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	assert.NoError(t, err)
	assert.True(t, gate.SFUpdated)
	assert.Equal(t, "001MIN", result.Company.SalesforceID)
	// Verify minimum fields were set.
	assert.Equal(t, "Minimum Co", capturedFields["Name"])
	assert.Equal(t, "https://minimum.com", capturedFields["Website"])
	sfClient.AssertExpectations(t)
}

func TestWriteNotionSalesforceID(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		notionClient := notionmocks.NewMockClient(t)
		notionClient.On("UpdatePage", mock.Anything, "page-123", mock.MatchedBy(func(req *notionapi.PageUpdateRequest) bool {
			rtProp, ok := req.Properties["SalesforceID"].(notionapi.RichTextProperty)
			if !ok || len(rtProp.RichText) == 0 {
				return false
			}
			return rtProp.RichText[0].Text.Content == "001NEW"
		})).Return(nil, nil)

		err := writeNotionSalesforceID(context.Background(), notionClient, "page-123", "001NEW")
		assert.NoError(t, err)
		notionClient.AssertExpectations(t)
	})

	t.Run("error", func(t *testing.T) {
		notionClient := notionmocks.NewMockClient(t)
		notionClient.On("UpdatePage", mock.Anything, "page-err", mock.Anything).
			Return(nil, assert.AnError)

		err := writeNotionSalesforceID(context.Background(), notionClient, "page-err", "001NEW")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "write sf id to notion page")
	})
}
