package pipeline

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jomei/notionapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	notionmocks "github.com/sells-group/research-cli/pkg/notion/mocks"
	"github.com/sells-group/research-cli/pkg/salesforce"
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
			QualityWeights:        config.QualityWeights{Confidence: 1.0},
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
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
			QualityWeights:        config.QualityWeights{Confidence: 1.0},
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
	// Dedup lookup — no existing account.
	mockQueryNoMatch(sfClient)
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
	// Contact dedup lookup — no existing contacts.
	mockContactQueryEmpty(sfClient)
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
		ensureMinimumSFFields(fields, company, nil)
		assert.Equal(t, "Acme", fields["Name"])
		assert.Equal(t, "https://acme.com", fields["Website"])
	})

	t.Run("does not overwrite existing values", func(t *testing.T) {
		fields := map[string]any{"Name": "Custom Name", "Website": "https://custom.com"}
		company := model.Company{Name: "Acme", URL: "https://acme.com"}
		ensureMinimumSFFields(fields, company, nil)
		assert.Equal(t, "Custom Name", fields["Name"])
		assert.Equal(t, "https://custom.com", fields["Website"])
	})

	t.Run("empty company leaves fields unchanged", func(t *testing.T) {
		fields := map[string]any{"Industry": "Tech"}
		ensureMinimumSFFields(fields, model.Company{}, nil)
		_, hasName := fields["Name"]
		_, hasWebsite := fields["Website"]
		assert.False(t, hasName)
		assert.False(t, hasWebsite)
	})

	t.Run("falls back to fieldValues company_name when Name empty", func(t *testing.T) {
		fields := map[string]any{"Industry": "Tech"}
		company := model.Company{URL: "https://acme.com"} // No Name.
		fieldValues := map[string]model.FieldValue{
			"company_name": {FieldKey: "company_name", Value: "Extracted Acme Corp"},
		}
		ensureMinimumSFFields(fields, company, fieldValues)
		assert.Equal(t, "Extracted Acme Corp", fields["Name"])
		assert.Equal(t, "https://acme.com", fields["Website"])
	})

	t.Run("falls back to domain heuristic when all else empty", func(t *testing.T) {
		fields := map[string]any{"Industry": "Tech"}
		company := model.Company{URL: "https://acme-construction.com"} // No Name.
		ensureMinimumSFFields(fields, company, nil)
		assert.Equal(t, "Acme Construction", fields["Name"])
		assert.Equal(t, "https://acme-construction.com", fields["Website"])
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
	// Dedup lookup — no existing account.
	mockQueryNoMatch(sfClient)
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
	// Contact dedup lookup — no existing contacts.
	mockContactQueryEmpty(sfClient)
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
	// Dedup lookup — no existing account.
	mockQueryNoMatch(sfClient)
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
		Pipeline: config.PipelineConfig{QualityScoreThreshold: 0.0, QualityWeights: config.QualityWeights{Confidence: 1.0}}, // 0 threshold so empty fields pass.
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

func TestQualityGate_MultipleContacts(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry", SFObject: "Account"},
		{Key: "contacts", DataType: "json"},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "Multi Contact Co",
			SalesforceID: "001MULTI",
			NotionPageID: "page-multi",
		},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech", Confidence: 0.9},
			"contacts": {FieldKey: "contacts", Value: []map[string]string{
				{"first_name": "Jane", "last_name": "Doe", "title": "CEO", "email": "jane@acme.com"},
				{"first_name": "John", "last_name": "Smith", "title": "VP Ops"},
				{"first_name": "Bob", "last_name": "Jones", "title": "Director"},
			}, Confidence: 0.8},
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	// Account update.
	sfClient.On("UpdateOne", mock.Anything, "Account", "001MULTI", mock.AnythingOfType("map[string]interface {}")).
		Return(nil)
	// Contact dedup lookup — no existing contacts.
	mockContactQueryEmpty(sfClient)
	// 3 Contact creations.
	sfClient.On("InsertOne", mock.Anything, "Contact", mock.AnythingOfType("map[string]interface {}")).
		Return("003NEW", nil).Times(3)

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-multi", mock.Anything).
		Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{QualityScoreThreshold: 0.0, QualityWeights: config.QualityWeights{Confidence: 1.0}}, // Low threshold so it passes.
	}

	gate, err := QualityGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	assert.NoError(t, err)
	assert.True(t, gate.Passed)
	assert.True(t, gate.SFUpdated)
	sfClient.AssertExpectations(t)
}

func TestExtractContactsForSF_FromContacts(t *testing.T) {
	registry := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "contacts", DataType: "json"},
	})

	fieldValues := map[string]model.FieldValue{
		"contacts": {FieldKey: "contacts", Value: []map[string]string{
			{"first_name": "Jane", "last_name": "Doe", "title": "CEO", "email": "jane@acme.com", "linkedin_url": "https://linkedin.com/in/janedoe"},
			{"first_name": "John", "last_name": "Smith", "title": "VP"},
		}},
	}

	contacts := extractContactsForSF(fieldValues, registry)

	assert.Len(t, contacts, 2)
	assert.Equal(t, "Doe", contacts[0]["LastName"])
	assert.Equal(t, "Jane", contacts[0]["FirstName"])
	assert.Equal(t, "CEO", contacts[0]["Title"])
	assert.Equal(t, "jane@acme.com", contacts[0]["Email"])
	assert.Equal(t, "https://linkedin.com/in/janedoe", contacts[0]["LinkedIn_URL__c"])
	assert.Equal(t, "Smith", contacts[1]["LastName"])
}

func TestExtractContactsForSF_Fallback(t *testing.T) {
	registry := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "contacts", DataType: "json"},
	})

	// No contacts field → returns nil.
	fieldValues := map[string]model.FieldValue{
		"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech"},
	}

	contacts := extractContactsForSF(fieldValues, registry)
	assert.Nil(t, contacts)
}

func TestExtractContactsForSF_RequiresLastName(t *testing.T) {
	registry := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "contacts", DataType: "json"},
	})

	fieldValues := map[string]model.FieldValue{
		"contacts": {FieldKey: "contacts", Value: []map[string]string{
			{"first_name": "Jane", "last_name": "", "title": "CEO"},     // No last name — skipped.
			{"first_name": "John", "last_name": "Smith", "title": "VP"}, // Valid.
		}},
	}

	contacts := extractContactsForSF(fieldValues, registry)

	assert.Len(t, contacts, 1)
	assert.Equal(t, "Smith", contacts[0]["LastName"])
}

func TestExtractContactsForSF_CapsAt3(t *testing.T) {
	registry := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "contacts", DataType: "json"},
	})

	fieldValues := map[string]model.FieldValue{
		"contacts": {FieldKey: "contacts", Value: []map[string]string{
			{"first_name": "A", "last_name": "One", "title": "CEO"},
			{"first_name": "B", "last_name": "Two", "title": "VP"},
			{"first_name": "C", "last_name": "Three", "title": "Dir"},
			{"first_name": "D", "last_name": "Four", "title": "CTO"},
		}},
	}

	contacts := extractContactsForSF(fieldValues, registry)

	assert.Len(t, contacts, 3)
}

func TestExtractContactsForSF_HandlesAnyType(t *testing.T) {
	registry := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "contacts", DataType: "json"},
	})

	// Simulates JSON-unmarshaled data.
	fieldValues := map[string]model.FieldValue{
		"contacts": {FieldKey: "contacts", Value: []any{
			map[string]any{"first_name": "Jane", "last_name": "Doe", "title": "CEO"},
		}},
	}

	contacts := extractContactsForSF(fieldValues, registry)

	assert.Len(t, contacts, 1)
	assert.Equal(t, "Doe", contacts[0]["LastName"])
}

// --- UpsertContacts Tests ---

func TestUpsertContacts_NoExistingCreatesAll(t *testing.T) {
	ctx := context.Background()

	enriched := []map[string]any{
		{"FirstName": "Jane", "LastName": "Doe", "Email": "jane@acme.com"},
		{"FirstName": "John", "LastName": "Smith"},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	mockContactQueryEmpty(sfClient)
	// Both contacts should be created.
	sfClient.On("InsertOne", mock.Anything, "Contact", mock.AnythingOfType("map[string]interface {}")).
		Return("003X", nil).Times(2)

	upsertContacts(ctx, sfClient, "001ACC", enriched, "Test Co")
	sfClient.AssertExpectations(t)
}

func TestUpsertContacts_MatchByEmailUpdates(t *testing.T) {
	ctx := context.Background()

	enriched := []map[string]any{
		{"FirstName": "Jane", "LastName": "Doe", "Email": "jane@acme.com", "Title": "CEO"},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	// Existing contact with matching email.
	mockContactQueryMatch(sfClient, []salesforce.Contact{
		{ID: "003EXIST", FirstName: "Jane", LastName: "Doe", Email: "jane@acme.com"},
	})
	// Should update, not create.
	sfClient.On("UpdateOne", mock.Anything, "Contact", "003EXIST", mock.AnythingOfType("map[string]interface {}")).
		Return(nil)

	upsertContacts(ctx, sfClient, "001ACC", enriched, "Test Co")
	sfClient.AssertExpectations(t)
}

func TestUpsertContacts_MatchByNameUpdates(t *testing.T) {
	ctx := context.Background()

	// No email on enriched contact — should fall back to name matching.
	enriched := []map[string]any{
		{"FirstName": "John", "LastName": "Smith", "Title": "VP"},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	// Existing contact with matching name (different email).
	mockContactQueryMatch(sfClient, []salesforce.Contact{
		{ID: "003NAME", FirstName: "John", LastName: "Smith", Email: "john@other.com"},
	})
	// Should update by name match.
	sfClient.On("UpdateOne", mock.Anything, "Contact", "003NAME", mock.AnythingOfType("map[string]interface {}")).
		Return(nil)

	upsertContacts(ctx, sfClient, "001ACC", enriched, "Test Co")
	sfClient.AssertExpectations(t)
}

func TestUpsertContacts_MixedCreateAndUpdate(t *testing.T) {
	ctx := context.Background()

	enriched := []map[string]any{
		{"FirstName": "Jane", "LastName": "Doe", "Email": "jane@acme.com"}, // Matches existing.
		{"FirstName": "Bob", "LastName": "New"},                            // No match.
	}

	sfClient := salesforcemocks.NewMockClient(t)
	mockContactQueryMatch(sfClient, []salesforce.Contact{
		{ID: "003JANE", FirstName: "Jane", LastName: "Doe", Email: "jane@acme.com"},
	})
	// Jane: update existing.
	sfClient.On("UpdateOne", mock.Anything, "Contact", "003JANE", mock.AnythingOfType("map[string]interface {}")).
		Return(nil)
	// Bob: create new.
	sfClient.On("InsertOne", mock.Anything, "Contact", mock.AnythingOfType("map[string]interface {}")).
		Return("003BOB", nil)

	upsertContacts(ctx, sfClient, "001ACC", enriched, "Test Co")
	sfClient.AssertExpectations(t)
}

func TestUpsertContacts_QueryFailsFallsBackToCreate(t *testing.T) {
	ctx := context.Background()

	enriched := []map[string]any{
		{"FirstName": "Jane", "LastName": "Doe"},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	// Contact query fails.
	sfClient.On("Query", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("*[]salesforce.Contact")).
		Return(assert.AnError)
	// Should still create.
	sfClient.On("InsertOne", mock.Anything, "Contact", mock.AnythingOfType("map[string]interface {}")).
		Return("003X", nil)

	upsertContacts(ctx, sfClient, "001ACC", enriched, "Test Co")
	sfClient.AssertExpectations(t)
}

func TestUpsertContacts_EmptyInputsNoOp(t *testing.T) {
	ctx := context.Background()
	sfClient := salesforcemocks.NewMockClient(t)

	// No contacts.
	upsertContacts(ctx, sfClient, "001ACC", nil, "Test Co")
	upsertContacts(ctx, sfClient, "001ACC", []map[string]any{}, "Test Co")

	// No account ID.
	upsertContacts(ctx, sfClient, "", []map[string]any{{"LastName": "Doe"}}, "Test Co")
}

func TestUpsertContacts_EmailMatchCaseInsensitive(t *testing.T) {
	ctx := context.Background()

	enriched := []map[string]any{
		{"FirstName": "Jane", "LastName": "Doe", "Email": "JANE@ACME.COM"},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	mockContactQueryMatch(sfClient, []salesforce.Contact{
		{ID: "003CI", FirstName: "Jane", LastName: "Doe", Email: "jane@acme.com"},
	})
	// Should match case-insensitively and update.
	sfClient.On("UpdateOne", mock.Anything, "Contact", "003CI", mock.AnythingOfType("map[string]interface {}")).
		Return(nil)

	upsertContacts(ctx, sfClient, "001ACC", enriched, "Test Co")
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

// mockQueryNoMatch sets up a mock Query expectation that returns an empty result (no match).
func mockQueryNoMatch(sfClient *salesforcemocks.MockClient) {
	sfClient.On("Query", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("*[]salesforce.Account")).
		Run(func(args mock.Arguments) {
			accounts := args.Get(2).(*[]salesforce.Account)
			*accounts = []salesforce.Account{}
		}).
		Return(nil)
}

// mockQueryMatch sets up a mock Query expectation that returns an existing Account.
func mockQueryMatch(sfClient *salesforcemocks.MockClient, id, name string) {
	sfClient.On("Query", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("*[]salesforce.Account")).
		Run(func(args mock.Arguments) {
			accounts := args.Get(2).(*[]salesforce.Account)
			*accounts = []salesforce.Account{{ID: id, Name: name}}
		}).
		Return(nil)
}

// mockContactQueryEmpty sets up a mock Query expectation for Contact queries returning no results.
func mockContactQueryEmpty(sfClient *salesforcemocks.MockClient) {
	sfClient.On("Query", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("*[]salesforce.Contact")).
		Run(func(args mock.Arguments) {
			contacts := args.Get(2).(*[]salesforce.Contact)
			*contacts = []salesforce.Contact{}
		}).
		Return(nil)
}

// mockContactQueryMatch sets up a mock Query expectation for Contact queries
// returning the given existing contacts.
func mockContactQueryMatch(sfClient *salesforcemocks.MockClient, existing []salesforce.Contact) {
	sfClient.On("Query", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("*[]salesforce.Contact")).
		Run(func(args mock.Arguments) {
			contacts := args.Get(2).(*[]salesforce.Contact)
			*contacts = existing
		}).
		Return(nil)
}

func TestQualityGate_DedupMatchUpdatesExisting(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry"},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "Acme Corp",
			URL:          "https://acme.com",
			NotionPageID: "page-dedup",
		},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech", Confidence: 0.9},
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	// Dedup lookup — existing Account found.
	mockQueryMatch(sfClient, "001EXIST", "Acme Corporation")
	// Should update, not create.
	sfClient.On("UpdateOne", mock.Anything, "Account", "001EXIST", mock.AnythingOfType("map[string]interface {}")).
		Return(nil)

	notionClient := notionmocks.NewMockClient(t)
	// Status update + SalesforceID writeback.
	notionClient.On("UpdatePage", mock.Anything, "page-dedup", mock.Anything).
		Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{QualityScoreThreshold: 0.5},
	}

	gate, err := QualityGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	assert.NoError(t, err)
	assert.True(t, gate.Passed)
	assert.True(t, gate.SFUpdated)
	assert.True(t, gate.DedupMatch)
	assert.Equal(t, "001EXIST", result.Company.SalesforceID)
	sfClient.AssertExpectations(t)
	notionClient.AssertExpectations(t)
}

func TestQualityGate_DedupLookupFailsGracefully(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry"},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "Flaky Co",
			URL:          "https://flaky.com",
			NotionPageID: "page-flaky",
		},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech", Confidence: 0.9},
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	// Dedup lookup fails — should still proceed with create.
	sfClient.On("Query", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("*[]salesforce.Account")).
		Return(assert.AnError)
	sfClient.On("InsertOne", mock.Anything, "Account", mock.AnythingOfType("map[string]interface {}")).
		Return("001NEW", nil)

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-flaky", mock.Anything).
		Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{QualityScoreThreshold: 0.5},
	}

	gate, err := QualityGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	assert.NoError(t, err)
	assert.True(t, gate.Passed)
	assert.True(t, gate.SFUpdated)
	assert.False(t, gate.DedupMatch) // Lookup failed, so no dedup.
	assert.Equal(t, "001NEW", result.Company.SalesforceID)
	sfClient.AssertExpectations(t)
}

func TestQualityGate_DedupNoURLSkipsLookup(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry"},
	})

	// Company with no URL — dedup lookup should be skipped.
	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "No URL Co",
			NotionPageID: "page-nourl",
		},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech", Confidence: 0.9},
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	// No Query call expected — straight to create.
	sfClient.On("InsertOne", mock.Anything, "Account", mock.AnythingOfType("map[string]interface {}")).
		Return("001NOURL", nil)

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-nourl", mock.Anything).
		Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{QualityScoreThreshold: 0.5},
	}

	gate, err := QualityGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	assert.NoError(t, err)
	assert.True(t, gate.SFUpdated)
	assert.False(t, gate.DedupMatch)
	assert.Equal(t, "001NOURL", result.Company.SalesforceID)
	sfClient.AssertExpectations(t)
}

// --- PrepareGate Tests (Deferred SF Write Mode) ---

func TestPrepareGate_BuildsCreateIntent(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry"},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "New Co",
			URL:          "https://newco.com",
			NotionPageID: "page-prep",
		},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech", Confidence: 0.9},
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	// Dedup lookup — no match.
	mockQueryNoMatch(sfClient)

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-prep", mock.Anything).
		Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{QualityScoreThreshold: 0.5},
	}

	gate, intent, err := PrepareGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	assert.NoError(t, err)
	assert.True(t, gate.Passed)
	assert.False(t, gate.SFUpdated) // No SF writes in PrepareGate.

	// Verify intent.
	assert.NotNil(t, intent)
	assert.Equal(t, "create", intent.AccountOp)
	assert.Equal(t, "", intent.AccountID)
	assert.Equal(t, "Tech", intent.AccountFields["Industry"])
	assert.Equal(t, "New Co", intent.AccountFields["Name"])
	assert.Equal(t, "https://newco.com", intent.AccountFields["Website"])
	assert.Equal(t, "page-prep", intent.NotionPageID)
	assert.Same(t, result, intent.Result)
	sfClient.AssertExpectations(t)
}

func TestPrepareGate_BuildsUpdateIntent(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry"},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "Existing Co",
			SalesforceID: "001EXIST",
			NotionPageID: "page-upd",
		},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Finance", Confidence: 0.9},
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	// No dedup lookup needed — already has SF ID.

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-upd", mock.Anything).
		Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{QualityScoreThreshold: 0.5},
	}

	gate, intent, err := PrepareGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	assert.NoError(t, err)
	assert.True(t, gate.Passed)
	assert.NotNil(t, intent)
	assert.Equal(t, "update", intent.AccountOp)
	assert.Equal(t, "001EXIST", intent.AccountID)
	assert.Equal(t, "Finance", intent.AccountFields["Industry"])
}

func TestPrepareGate_DedupMatchBuildsUpdateIntent(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry"},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "Dup Co",
			URL:          "https://dup.com",
			NotionPageID: "page-dup",
		},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech", Confidence: 0.9},
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	// Dedup lookup finds existing account.
	mockQueryMatch(sfClient, "001DUP", "Duplicate Corp")

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-dup", mock.Anything).
		Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{QualityScoreThreshold: 0.5},
	}

	gate, intent, err := PrepareGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	assert.NoError(t, err)
	assert.True(t, gate.Passed)
	assert.True(t, gate.DedupMatch)
	assert.NotNil(t, intent)
	assert.Equal(t, "update", intent.AccountOp)
	assert.Equal(t, "001DUP", intent.AccountID)
	assert.True(t, intent.DedupMatch)
	assert.Equal(t, "001DUP", result.Company.SalesforceID)
}

func TestPrepareGate_FailingScoreReturnsNilIntent(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry", Required: true},
		{Key: "revenue", SFField: "AnnualRevenue", Required: true},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "Low Score Co",
			NotionPageID: "page-low",
		},
		FieldValues: map[string]model.FieldValue{},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-low", mock.Anything).
		Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{
			QualityScoreThreshold: 0.6,
			QualityWeights:        config.QualityWeights{Confidence: 1.0},
		},
	}

	gate, intent, err := PrepareGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	assert.NoError(t, err)
	assert.False(t, gate.Passed)
	assert.Nil(t, intent) // No intent for failing score.
}

func TestPrepareGate_CollectsContacts(t *testing.T) {
	ctx := context.Background()

	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry", SFObject: "Account"},
		{Key: "contacts", DataType: "json"},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:         "Contacts Co",
			SalesforceID: "001CC",
			NotionPageID: "page-cc",
		},
		FieldValues: map[string]model.FieldValue{
			"industry": {FieldKey: "industry", SFField: "Industry", Value: "Tech", Confidence: 0.9},
			"contacts": {FieldKey: "contacts", Value: []map[string]string{
				{"first_name": "Jane", "last_name": "Doe", "title": "CEO"},
				{"first_name": "John", "last_name": "Smith", "title": "VP"},
			}, Confidence: 0.8},
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-cc", mock.Anything).
		Return(nil, nil)

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{QualityScoreThreshold: 0.0, QualityWeights: config.QualityWeights{Confidence: 1.0}},
	}

	_, intent, err := PrepareGate(ctx, result, fields, nil, sfClient, notionClient, cfg)

	assert.NoError(t, err)
	assert.NotNil(t, intent)
	assert.Len(t, intent.Contacts, 2)
	assert.Equal(t, "Doe", intent.Contacts[0]["LastName"])
	assert.Equal(t, "Smith", intent.Contacts[1]["LastName"])
}

// --- FlushSFWrites Tests ---

func TestFlushSFWrites_EmptyIntents(t *testing.T) {
	ctx := context.Background()
	sfClient := salesforcemocks.NewMockClient(t)
	notionClient := notionmocks.NewMockClient(t)

	summary, err := FlushSFWrites(ctx, sfClient, notionClient, nil)
	assert.NoError(t, err)
	assert.NotNil(t, summary)
	assert.Equal(t, 0, summary.AccountsCreated)

	summary, err = FlushSFWrites(ctx, sfClient, notionClient, []*SFWriteIntent{})
	assert.NoError(t, err)
	assert.NotNil(t, summary)
}

func TestFlushSFWrites_BulkCreates(t *testing.T) {
	ctx := context.Background()

	result1 := &model.EnrichmentResult{
		Company: model.Company{Name: "Co1", NotionPageID: "p1"},
	}
	result2 := &model.EnrichmentResult{
		Company: model.Company{Name: "Co2", NotionPageID: "p2"},
	}

	intents := []*SFWriteIntent{
		{
			AccountOp:     "create",
			AccountFields: map[string]any{"Name": "Co1", "Website": "https://co1.com"},
			NotionPageID:  "p1",
			Result:        result1,
		},
		{
			AccountOp:     "create",
			AccountFields: map[string]any{"Name": "Co2", "Website": "https://co2.com"},
			NotionPageID:  "p2",
			Result:        result2,
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	// Bulk create: InsertCollection with 2 records.
	sfClient.On("InsertCollection", mock.Anything, "Account", mock.MatchedBy(func(records []map[string]any) bool {
		return len(records) == 2
	})).Return([]salesforce.CollectionResult{
		{ID: "001A", Success: true},
		{ID: "001B", Success: true},
	}, nil)

	notionClient := notionmocks.NewMockClient(t)
	// SF ID writeback for both.
	notionClient.On("UpdatePage", mock.Anything, "p1", mock.Anything).Return(nil, nil)
	notionClient.On("UpdatePage", mock.Anything, "p2", mock.Anything).Return(nil, nil)

	summary, err := FlushSFWrites(ctx, sfClient, notionClient, intents)

	assert.NoError(t, err)
	assert.Equal(t, "001A", result1.Company.SalesforceID)
	assert.Equal(t, "001B", result2.Company.SalesforceID)
	assert.Equal(t, 2, summary.AccountsCreated)
	assert.Equal(t, 0, summary.AccountsFailed)
	assert.Empty(t, summary.Failures)
	sfClient.AssertExpectations(t)
}

func TestFlushSFWrites_BulkUpdates(t *testing.T) {
	ctx := context.Background()

	result1 := &model.EnrichmentResult{
		Company: model.Company{Name: "Upd1", SalesforceID: "001X"},
	}

	intents := []*SFWriteIntent{
		{
			AccountOp:     "update",
			AccountID:     "001X",
			AccountFields: map[string]any{"Industry": "Tech"},
			Result:        result1,
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	sfClient.On("UpdateCollection", mock.Anything, "Account", mock.MatchedBy(func(records []salesforce.CollectionRecord) bool {
		return len(records) == 1 && records[0].ID == "001X"
	})).Return([]salesforce.CollectionResult{
		{ID: "001X", Success: true},
	}, nil)

	notionClient := notionmocks.NewMockClient(t)

	summary, err := FlushSFWrites(ctx, sfClient, notionClient, intents)

	assert.NoError(t, err)
	assert.Equal(t, 1, summary.AccountsUpdated)
	assert.Equal(t, 0, summary.UpdatesFailed)
	sfClient.AssertExpectations(t)
}

func TestFlushSFWrites_CreatesWithContacts(t *testing.T) {
	ctx := context.Background()

	result1 := &model.EnrichmentResult{
		Company: model.Company{Name: "Co1", NotionPageID: "p1"},
	}

	intents := []*SFWriteIntent{
		{
			AccountOp:     "create",
			AccountFields: map[string]any{"Name": "Co1"},
			Contacts: []map[string]any{
				{"LastName": "Doe", "FirstName": "Jane"},
				{"LastName": "Smith", "FirstName": "John"},
			},
			NotionPageID: "p1",
			Result:       result1,
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	// Account create.
	sfClient.On("InsertCollection", mock.Anything, "Account", mock.Anything).
		Return([]salesforce.CollectionResult{{ID: "001NEW", Success: true}}, nil)
	// upsertContacts: query existing contacts (empty), then create each.
	sfClient.On("Query", mock.Anything, mock.MatchedBy(func(soql string) bool {
		return strings.Contains(soql, "Contact") && strings.Contains(soql, "001NEW")
	}), mock.Anything).Return(nil)
	sfClient.On("InsertOne", mock.Anything, "Contact", mock.Anything).Return("003A", nil)

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "p1", mock.Anything).Return(nil, nil)

	summary, err := FlushSFWrites(ctx, sfClient, notionClient, intents)

	assert.NoError(t, err)
	assert.Equal(t, "001NEW", result1.Company.SalesforceID)
	assert.Equal(t, 1, summary.AccountsCreated)
	assert.Equal(t, 2, summary.ContactsCreated)
	sfClient.AssertExpectations(t)
}

func TestFlushSFWrites_MixedCreateAndUpdate(t *testing.T) {
	ctx := context.Background()

	resultCreate := &model.EnrichmentResult{
		Company: model.Company{Name: "New", NotionPageID: "pNew"},
	}
	resultUpdate := &model.EnrichmentResult{
		Company: model.Company{Name: "Existing", SalesforceID: "001OLD"},
	}

	intents := []*SFWriteIntent{
		{
			AccountOp:     "create",
			AccountFields: map[string]any{"Name": "New"},
			NotionPageID:  "pNew",
			Result:        resultCreate,
		},
		{
			AccountOp:     "update",
			AccountID:     "001OLD",
			AccountFields: map[string]any{"Industry": "Finance"},
			Result:        resultUpdate,
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	// Account create.
	sfClient.On("InsertCollection", mock.Anything, "Account", mock.Anything).
		Return([]salesforce.CollectionResult{{ID: "001CREATED", Success: true}}, nil)
	// Account update.
	sfClient.On("UpdateCollection", mock.Anything, "Account", mock.Anything).
		Return([]salesforce.CollectionResult{{ID: "001OLD", Success: true}}, nil)

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "pNew", mock.Anything).Return(nil, nil)

	summary, err := FlushSFWrites(ctx, sfClient, notionClient, intents)

	assert.NoError(t, err)
	assert.Equal(t, "001CREATED", resultCreate.Company.SalesforceID)
	assert.Equal(t, 1, summary.AccountsCreated)
	assert.Equal(t, 1, summary.AccountsUpdated)
	sfClient.AssertExpectations(t)
}

func TestFlushSFWrites_PartialCreateFailure(t *testing.T) {
	ctx := context.Background()

	result1 := &model.EnrichmentResult{
		Company: model.Company{Name: "OK Co", NotionPageID: "p1"},
	}
	result2 := &model.EnrichmentResult{
		Company: model.Company{Name: "Bad Co", NotionPageID: "p2"},
	}

	intents := []*SFWriteIntent{
		{
			AccountOp:     "create",
			AccountFields: map[string]any{"Name": "OK Co"},
			Contacts:      []map[string]any{{"LastName": "Doe"}},
			NotionPageID:  "p1",
			Result:        result1,
		},
		{
			AccountOp:     "create",
			AccountFields: map[string]any{"Name": "Bad Co"},
			Contacts:      []map[string]any{{"LastName": "Smith"}},
			NotionPageID:  "p2",
			Result:        result2,
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	// One succeeds, one fails.
	sfClient.On("InsertCollection", mock.Anything, "Account", mock.Anything).
		Return([]salesforce.CollectionResult{
			{ID: "001OK", Success: true},
			{ID: "", Success: false, Errors: []string{"DUPLICATE_VALUE"}},
		}, nil)
	// upsertContacts only runs for intent with AccountID "001OK" (failed one has empty ID).
	sfClient.On("Query", mock.Anything, mock.MatchedBy(func(soql string) bool {
		return strings.Contains(soql, "Contact") && strings.Contains(soql, "001OK")
	}), mock.Anything).Return(nil)
	sfClient.On("InsertOne", mock.Anything, "Contact", mock.Anything).Return("003A", nil)

	notionClient := notionmocks.NewMockClient(t)
	// Only successful account gets Notion writeback.
	notionClient.On("UpdatePage", mock.Anything, "p1", mock.Anything).Return(nil, nil)

	summary, err := FlushSFWrites(ctx, sfClient, notionClient, intents)

	assert.NoError(t, err)
	assert.Equal(t, "001OK", result1.Company.SalesforceID)
	assert.Equal(t, "", result2.Company.SalesforceID) // Failed create, no ID.
	assert.Equal(t, 1, summary.AccountsCreated)
	assert.Equal(t, 1, summary.AccountsFailed)
	require.Len(t, summary.Failures, 1)
	assert.Equal(t, "Bad Co", summary.Failures[0].Company)
	assert.Equal(t, "account_create", summary.Failures[0].Op)
	assert.Contains(t, summary.Failures[0].Error, "DUPLICATE_VALUE")
	// Contacts: 1 created for OK Co (Bad Co has empty AccountID).
	assert.Equal(t, 1, summary.ContactsCreated)
	sfClient.AssertExpectations(t)
}

// --- SetDeferredWrites Tests ---

func TestSetDeferredWrites_CollectsIntents(t *testing.T) {
	var collected []*SFWriteIntent

	p := &Pipeline{}
	p.SetDeferredWrites(func(intent *SFWriteIntent) {
		collected = append(collected, intent)
	})

	assert.True(t, p.deferSFWrites)
	assert.NotNil(t, p.onWriteIntent)

	// Simulate callback.
	p.onWriteIntent(&SFWriteIntent{AccountOp: "create"})
	p.onWriteIntent(&SFWriteIntent{AccountOp: "update"})

	assert.Len(t, collected, 2)
	assert.Equal(t, "create", collected[0].AccountOp)
	assert.Equal(t, "update", collected[1].AccountOp)
}

// --- Issue 1: validateRequiredFields tests ---

func TestValidateRequiredFields_AllPresent(t *testing.T) {
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry", Required: true},
		{Key: "revenue", SFField: "AnnualRevenue", Required: true},
		{Key: "website", SFField: "Website"},
	})

	fieldValues := map[string]model.FieldValue{
		"industry": {Value: "Tech", Confidence: 0.9},
		"revenue":  {Value: "$10M", Confidence: 0.8},
	}

	missing := validateRequiredFields(fieldValues, fields)
	assert.Empty(t, missing)
}

func TestValidateRequiredFields_OneMissing(t *testing.T) {
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry", Required: true},
		{Key: "revenue", SFField: "AnnualRevenue", Required: true},
	})

	fieldValues := map[string]model.FieldValue{
		"industry": {Value: "Tech", Confidence: 0.9},
		// revenue is missing
	}

	missing := validateRequiredFields(fieldValues, fields)
	assert.Equal(t, []string{"revenue"}, missing)
}

func TestValidateRequiredFields_NilValueTreatedAsMissing(t *testing.T) {
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry", Required: true},
	})

	fieldValues := map[string]model.FieldValue{
		"industry": {Value: nil, Confidence: 0.0},
	}

	missing := validateRequiredFields(fieldValues, fields)
	assert.Equal(t, []string{"industry"}, missing)
}

func TestValidateRequiredFields_NilRegistry(t *testing.T) {
	missing := validateRequiredFields(map[string]model.FieldValue{}, nil)
	assert.Nil(t, missing)
}

// --- Issue 3: extractContactsForSF truncation test ---

func TestExtractContactsForSF_TruncatesAt3(t *testing.T) {
	fieldValues := map[string]model.FieldValue{
		"contacts": {
			Value: []any{
				map[string]any{"first_name": "A", "last_name": "One", "title": "CEO"},
				map[string]any{"first_name": "B", "last_name": "Two", "title": "CTO"},
				map[string]any{"first_name": "C", "last_name": "Three", "title": "CFO"},
				map[string]any{"first_name": "D", "last_name": "Four", "title": "COO"},
				map[string]any{"first_name": "E", "last_name": "Five", "title": "VP"},
			},
		},
	}

	contacts := extractContactsForSF(fieldValues, nil)
	require.Len(t, contacts, 3)
	assert.Equal(t, "One", contacts[0]["LastName"])
	assert.Equal(t, "Three", contacts[2]["LastName"])
}

// --- Issue 6: min completeness floor test ---

func TestQualityGate_CompletenessFloorBlocksPass(t *testing.T) {
	ctx := context.Background()

	// 2 fields, only 1 populated → completeness = 0.5
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry", Required: true},
		{Key: "revenue", SFField: "AnnualRevenue", Required: true},
	})

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme", URL: "https://acme.com"},
		FieldValues: map[string]model.FieldValue{
			"industry": {Value: "Tech", Confidence: 0.95, SFField: "Industry"},
		},
		Answers: []model.ExtractionAnswer{
			{FieldKey: "industry", Value: "Tech", Confidence: 0.95, QuestionID: "q1", Tier: 1},
		},
	}

	questions := []model.Question{
		{ID: "q1", FieldKey: "industry"},
		{ID: "q2", FieldKey: "revenue"},
	}

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{
			QualityScoreThreshold:    0.3, // Low threshold — score would pass.
			MinCompletenessThreshold: 0.7, // But completeness floor blocks.
			QualityWeights: config.QualityWeights{
				Confidence: 1.0,
			},
		},
	}

	gate, err := QualityGate(ctx, result, fields, questions, nil, nil, cfg)
	require.NoError(t, err)
	assert.False(t, gate.Passed, "gate should fail due to completeness floor")
}

func TestInjectGeoFields(t *testing.T) {
	t.Run("nil geo data is no-op", func(t *testing.T) {
		fields := make(map[string]any)
		injectGeoFields(fields, nil)
		assert.Empty(t, fields)
	})

	t.Run("full geo data populates all fields", func(t *testing.T) {
		fields := make(map[string]any)
		gd := &model.GeoData{
			Latitude:       30.2672,
			Longitude:      -97.7431,
			MSAName:        "Austin-Round Rock-Georgetown, TX",
			CBSACode:       "12420",
			Classification: "urban_core",
			CentroidKM:     5.2,
			EdgeKM:         12.8,
			CountyFIPS:     "48453",
		}
		injectGeoFields(fields, gd)

		assert.Equal(t, 30.2672, fields["Latitude__c"])
		assert.Equal(t, -97.7431, fields["Longitude__c"])
		assert.Equal(t, "Austin-Round Rock-Georgetown, TX", fields["MSA_Name__c"])
		assert.Equal(t, "12420", fields["MSA_CBSA_Code__c"])
		assert.Equal(t, "urban_core", fields["Urban_Classification__c"])
		assert.Equal(t, 5.2, fields["Distance_to_MSA_Center_km__c"])
		assert.Equal(t, 12.8, fields["Distance_to_MSA_Edge_km__c"])
		assert.Equal(t, "48453", fields["County_FIPS__c"])
	})

	t.Run("partial geo data only sets available fields", func(t *testing.T) {
		fields := make(map[string]any)
		gd := &model.GeoData{
			Latitude:  30.2672,
			Longitude: -97.7431,
		}
		injectGeoFields(fields, gd)

		assert.Equal(t, 30.2672, fields["Latitude__c"])
		assert.Equal(t, -97.7431, fields["Longitude__c"])
		assert.Nil(t, fields["MSA_Name__c"])
		assert.Nil(t, fields["MSA_CBSA_Code__c"])
	})
}
