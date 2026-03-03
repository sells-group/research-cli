package pipeline

import (
	"context"
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

func TestBuildSFFields(t *testing.T) {
	fieldValues := map[string]model.FieldValue{
		"industry":       {SFField: "Industry", Value: "Tech"},
		"employee_count": {SFField: "NumberOfEmployees", Value: 200},
		"no_sf":          {SFField: "", Value: "ignored"}, // No SF field name.
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
	assert.Equal(t, "https://linkedin.com/in/janedoe", contacts[0]["LinkedIn__c"])
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

// --- validateRequiredFields tests ---

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

// --- extractContactsForSF truncation test ---

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

// --- resolveOrCreateAccount Tests ---

func TestResolveOrCreateAccount_DedupMatch(t *testing.T) {
	ctx := context.Background()

	sfClient := salesforcemocks.NewMockClient(t)
	// FindAccountByWebsite returns a match.
	sfClient.On("Query", mock.Anything, mock.MatchedBy(func(s string) bool {
		return strings.Contains(s, "Account") && strings.Contains(s, "Website")
	}), mock.Anything).Run(func(args mock.Arguments) {
		out := args.Get(2).(*[]salesforce.Account)
		*out = []salesforce.Account{{ID: "001EXISTING", Name: "Existing"}}
	}).Return(nil)
	// Update existing account.
	sfClient.On("UpdateOne", mock.Anything, "Account", "001EXISTING", mock.Anything).Return(nil)

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-1", mock.Anything).Return(nil, nil)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme", URL: "https://acme.com", NotionPageID: "page-1"},
	}
	gate := &GateResult{Passed: true}
	fields := map[string]any{"Industry": "Tech"}

	id, err := resolveOrCreateAccount(ctx, sfClient, notionClient, result, fields, gate)
	assert.NoError(t, err)
	assert.Equal(t, "001EXISTING", id)
	assert.True(t, gate.DedupMatch)
	assert.True(t, gate.SFUpdated)
	sfClient.AssertExpectations(t)
}

func TestResolveOrCreateAccount_CreateNew(t *testing.T) {
	ctx := context.Background()

	sfClient := salesforcemocks.NewMockClient(t)
	// FindAccountByWebsite returns no match.
	sfClient.On("Query", mock.Anything, mock.MatchedBy(func(s string) bool {
		return strings.Contains(s, "Account") && strings.Contains(s, "Website")
	}), mock.Anything).Return(nil)
	// CreateAccount.
	sfClient.On("InsertOne", mock.Anything, "Account", mock.Anything).Return("001NEW", nil)

	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-1", mock.Anything).Return(nil, nil)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "NewCo", URL: "https://newco.com", NotionPageID: "page-1"},
	}
	gate := &GateResult{Passed: true}
	fields := map[string]any{"Name": "NewCo", "Website": "https://newco.com"}

	id, err := resolveOrCreateAccount(ctx, sfClient, notionClient, result, fields, gate)
	assert.NoError(t, err)
	assert.Equal(t, "001NEW", id)
	assert.True(t, gate.SFUpdated)
	assert.False(t, gate.DedupMatch)
}

func TestResolveOrCreateAccount_NoURL(t *testing.T) {
	ctx := context.Background()

	sfClient := salesforcemocks.NewMockClient(t)
	// No URL → skip dedup → straight to create.
	sfClient.On("InsertOne", mock.Anything, "Account", mock.Anything).Return("001DIRECT", nil)

	notionClient := notionmocks.NewMockClient(t)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "NoURL Co"},
	}
	gate := &GateResult{Passed: true}
	fields := map[string]any{"Name": "NoURL Co"}

	id, err := resolveOrCreateAccount(ctx, sfClient, notionClient, result, fields, gate)
	assert.NoError(t, err)
	assert.Equal(t, "001DIRECT", id)
}

func TestResolveOrCreateAccount_DedupLookupFails(t *testing.T) {
	ctx := context.Background()

	sfClient := salesforcemocks.NewMockClient(t)
	// FindAccountByWebsite fails.
	sfClient.On("Query", mock.Anything, mock.MatchedBy(func(s string) bool {
		return strings.Contains(s, "Account")
	}), mock.Anything).Return(assert.AnError)
	// Falls through to create.
	sfClient.On("InsertOne", mock.Anything, "Account", mock.Anything).Return("001FALLBACK", nil)

	notionClient := notionmocks.NewMockClient(t)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme", URL: "https://acme.com"},
	}
	gate := &GateResult{Passed: true}
	fields := map[string]any{"Name": "Acme"}

	id, err := resolveOrCreateAccount(ctx, sfClient, notionClient, result, fields, gate)
	assert.NoError(t, err)
	assert.Equal(t, "001FALLBACK", id)
}

func TestResolveOrCreateAccount_CreateFails(t *testing.T) {
	ctx := context.Background()

	sfClient := salesforcemocks.NewMockClient(t)
	sfClient.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	sfClient.On("InsertOne", mock.Anything, "Account", mock.Anything).Return("", assert.AnError)

	notionClient := notionmocks.NewMockClient(t)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "FailCo", URL: "https://fail.com"},
	}
	gate := &GateResult{Passed: true}
	fields := map[string]any{"Name": "FailCo"}

	_, err := resolveOrCreateAccount(ctx, sfClient, notionClient, result, fields, gate)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sf create")
}

func TestResolveOrCreateAccount_UpdateDedupFails(t *testing.T) {
	ctx := context.Background()

	sfClient := salesforcemocks.NewMockClient(t)
	sfClient.On("Query", mock.Anything, mock.MatchedBy(func(s string) bool {
		return strings.Contains(s, "Account") && strings.Contains(s, "Website")
	}), mock.Anything).Run(func(args mock.Arguments) {
		out := args.Get(2).(*[]salesforce.Account)
		*out = []salesforce.Account{{ID: "001EXIST", Name: "Existing"}}
	}).Return(nil)
	sfClient.On("UpdateOne", mock.Anything, "Account", "001EXIST", mock.Anything).
		Return(assert.AnError)

	notionClient := notionmocks.NewMockClient(t)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme", URL: "https://acme.com"},
	}
	gate := &GateResult{Passed: true}
	fields := map[string]any{"Industry": "Tech"}

	_, err := resolveOrCreateAccount(ctx, sfClient, notionClient, result, fields, gate)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sf update (dedup)")
}

// --- updateNotionStatus Tests ---

func TestUpdateNotionStatus_Success(t *testing.T) {
	ctx := context.Background()
	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-1", mock.Anything).Return(nil, nil)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme"},
		Score:   0.85,
	}
	err := updateNotionStatus(ctx, notionClient, "page-1", "Enriched", result)
	assert.NoError(t, err)
	notionClient.AssertExpectations(t)
}

func TestUpdateNotionStatus_Error(t *testing.T) {
	ctx := context.Background()
	notionClient := notionmocks.NewMockClient(t)
	notionClient.On("UpdatePage", mock.Anything, "page-1", mock.Anything).
		Return(nil, assert.AnError)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme"},
	}
	err := updateNotionStatus(ctx, notionClient, "page-1", "Enriched", result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "update notion page")
}

// --- writeSFIDToNotion Tests ---

func TestWriteSFIDToNotion_SkipsNilClient(_ *testing.T) {
	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme", NotionPageID: "page-1"},
	}
	// Should not panic.
	writeSFIDToNotion(context.Background(), nil, result, "001ABC")
}

func TestWriteSFIDToNotion_SkipsEmptyPageID(t *testing.T) {
	notionClient := notionmocks.NewMockClient(t)
	// No UpdatePage call expected.
	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme"},
	}
	writeSFIDToNotion(context.Background(), notionClient, result, "001ABC")
}

// --- FlushSummary Tests ---

func TestFlushSummary_LogSummary(_ *testing.T) {
	s := &FlushSummary{
		AccountsCreated: 2,
		AccountsFailed:  1,
		AccountsUpdated: 3,
		Failures: []FlushFailure{
			{Company: "FailCo", Op: "account_create", Error: "dup"},
		},
	}
	// Should not panic.
	s.LogSummary()
}

func TestFlushSummary_LogSummary_NoFailures(_ *testing.T) {
	s := &FlushSummary{
		AccountsCreated: 5,
	}
	s.LogSummary()
}

// --- FlushSFWrites additional edge cases ---

func TestFlushSFWrites_NilIntentsFiltered(t *testing.T) {
	ctx := context.Background()

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Valid", SalesforceID: "001V"},
	}

	intents := []*SFWriteIntent{
		nil,                             // Should be skipped.
		{AccountOp: "", Result: result}, // Empty op — skipped.
		nil,                             // Another nil.
	}

	sfClient := salesforcemocks.NewMockClient(t)
	notionClient := notionmocks.NewMockClient(t)

	summary, err := FlushSFWrites(ctx, sfClient, notionClient, intents)
	assert.NoError(t, err)
	assert.Equal(t, 0, summary.AccountsCreated)
	assert.Equal(t, 0, summary.AccountsUpdated)
}

func TestFlushSFWrites_UpdatePartialFailure(t *testing.T) {
	ctx := context.Background()

	result1 := &model.EnrichmentResult{
		Company: model.Company{Name: "Good", SalesforceID: "001A"},
	}
	result2 := &model.EnrichmentResult{
		Company: model.Company{Name: "Bad", SalesforceID: "001B"},
	}

	intents := []*SFWriteIntent{
		{
			AccountOp:     "update",
			AccountID:     "001A",
			AccountFields: map[string]any{"Industry": "Tech"},
			Result:        result1,
		},
		{
			AccountOp:     "update",
			AccountID:     "001B",
			AccountFields: map[string]any{"Industry": "Finance"},
			Result:        result2,
		},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	sfClient.On("UpdateCollection", mock.Anything, "Account", mock.Anything).
		Return([]salesforce.CollectionResult{
			{ID: "001A", Success: true},
			{ID: "001B", Success: false, Errors: []string{"FIELD_INTEGRITY"}},
		}, nil)

	notionClient := notionmocks.NewMockClient(t)

	summary, err := FlushSFWrites(ctx, sfClient, notionClient, intents)
	assert.NoError(t, err)
	assert.Equal(t, 1, summary.AccountsUpdated)
	assert.Equal(t, 1, summary.UpdatesFailed)
	require.Len(t, summary.Failures, 1)
	assert.Equal(t, "Bad", summary.Failures[0].Company)
}

// --- Pipeline setter tests ---

func TestPipelineSetters(t *testing.T) {
	p := &Pipeline{}

	p.SetForceReExtract(true)
	assert.True(t, p.forceReExtract)

	p.SetForceReExtract(false)
	assert.False(t, p.forceReExtract)

	p.SetFedsyncPool(nil)
	assert.Nil(t, p.fedsyncPool)

	p.SetCompanyImporter(nil)
	assert.Nil(t, p.companyImporter)
}

// --- ComputeGateResult tests ---

func TestComputeGateResult_CompletenessFloorBlocksPass(t *testing.T) {
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

	gate := ComputeGateResult(result, fields, questions, cfg)
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

		assert.Equal(t, 30.2672, fields["Longitude_and_Lattitude__Latitude__s"])   //nolint:misspell // SF field name
		assert.Equal(t, -97.7431, fields["Longitude_and_Lattitude__Longitude__s"]) //nolint:misspell // SF field name
		assert.Equal(t, "Austin-Round Rock-Georgetown, TX", fields["Company_MSA__c"])
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

		assert.Equal(t, 30.2672, fields["Longitude_and_Lattitude__Latitude__s"])   //nolint:misspell // SF field name
		assert.Equal(t, -97.7431, fields["Longitude_and_Lattitude__Longitude__s"]) //nolint:misspell // SF field name
		assert.Nil(t, fields["Company_MSA__c"])
		assert.Nil(t, fields["MSA_CBSA_Code__c"])
	})
}

// --- writeSFIDToNotion additional coverage ---

func TestWriteSFIDToNotion_Success(t *testing.T) {
	ctx := context.Background()
	notionClient := notionmocks.NewMockClient(t)

	// Expect UpdatePage to be called with the correct pageID and SF ID content.
	notionClient.On("UpdatePage", mock.Anything, "page-abc", mock.MatchedBy(func(req *notionapi.PageUpdateRequest) bool {
		rtProp, ok := req.Properties["SalesforceID"].(notionapi.RichTextProperty)
		if !ok || len(rtProp.RichText) == 0 {
			return false
		}
		return rtProp.RichText[0].Text.Content == "001SFID"
	})).Return(nil, nil)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme Corp", NotionPageID: "page-abc"},
	}

	// Should call UpdatePage since notionClient != nil and PageID != "".
	writeSFIDToNotion(ctx, notionClient, result, "001SFID")
	notionClient.AssertExpectations(t)
}

func TestWriteSFIDToNotion_NilClient(_ *testing.T) {
	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme Corp", NotionPageID: "page-abc"},
	}
	// Should not panic with nil client.
	writeSFIDToNotion(context.Background(), nil, result, "001SFID")
}

func TestWriteSFIDToNotion_EmptyPageID(t *testing.T) {
	notionClient := notionmocks.NewMockClient(t)
	// No UpdatePage call expected since PageID is empty.
	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme Corp", NotionPageID: ""},
	}
	writeSFIDToNotion(context.Background(), notionClient, result, "001SFID")
	// Mock cleanup will assert no unexpected calls were made.
}

// --- upsertContacts additional coverage ---

func TestUpsertContacts_NameMatchEmailMismatch(t *testing.T) {
	ctx := context.Background()

	// Enriched contact has an email that doesn't match, but first+last name matches.
	enriched := []map[string]any{
		{"FirstName": "Alice", "LastName": "Wong", "Email": "alice.new@newco.com", "Title": "CTO"},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	// Existing contact has same name but different email.
	mockContactQueryMatch(sfClient, []salesforce.Contact{
		{ID: "003ALICE", FirstName: "Alice", LastName: "Wong", Email: "alice@oldco.com"},
	})
	// Should match by name and update existing contact.
	sfClient.On("UpdateOne", mock.Anything, "Contact", "003ALICE", mock.AnythingOfType("map[string]interface {}")).
		Return(nil)

	res := upsertContacts(ctx, sfClient, "001ACC", enriched, "Test Co")
	assert.Equal(t, 1, res.Updated)
	assert.Equal(t, 0, res.Created)
	assert.Equal(t, 0, res.Failed)
	sfClient.AssertExpectations(t)
}

func TestUpsertContacts_UpdateFailure(t *testing.T) {
	ctx := context.Background()

	enriched := []map[string]any{
		{"FirstName": "Bob", "LastName": "Jones", "Email": "bob@acme.com"},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	// Existing contact matches by email.
	mockContactQueryMatch(sfClient, []salesforce.Contact{
		{ID: "003BOB", FirstName: "Bob", LastName: "Jones", Email: "bob@acme.com"},
	})
	// Update fails.
	sfClient.On("UpdateOne", mock.Anything, "Contact", "003BOB", mock.AnythingOfType("map[string]interface {}")).
		Return(assert.AnError)

	res := upsertContacts(ctx, sfClient, "001ACC", enriched, "Test Co")
	assert.Equal(t, 0, res.Updated)
	assert.Equal(t, 0, res.Created)
	assert.Equal(t, 1, res.Failed)
	sfClient.AssertExpectations(t)
}

func TestUpsertContacts_CreateFailureAfterDedupSuccess(t *testing.T) {
	ctx := context.Background()

	// Enriched contact does not match any existing contact.
	enriched := []map[string]any{
		{"FirstName": "New", "LastName": "Person", "Email": "new@company.com"},
	}

	sfClient := salesforcemocks.NewMockClient(t)
	// Dedup lookup succeeds with existing contacts (none matching).
	mockContactQueryMatch(sfClient, []salesforce.Contact{
		{ID: "003OTHER", FirstName: "Other", LastName: "Person", Email: "other@company.com"},
	})
	// Create fails.
	sfClient.On("InsertOne", mock.Anything, "Contact", mock.AnythingOfType("map[string]interface {}")).
		Return("", assert.AnError)

	res := upsertContacts(ctx, sfClient, "001ACC", enriched, "Test Co")
	assert.Equal(t, 0, res.Updated)
	assert.Equal(t, 0, res.Created)
	assert.Equal(t, 1, res.Failed)
	sfClient.AssertExpectations(t)
}

// --- writeSFIDToNotion error path ---

func TestWriteSFIDToNotion_NotionUpdateError(t *testing.T) {
	ctx := context.Background()
	notionClient := notionmocks.NewMockClient(t)
	// UpdatePage returns an error.
	notionClient.On("UpdatePage", mock.Anything, "page-err", mock.Anything).
		Return(nil, assert.AnError)

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Acme Corp", NotionPageID: "page-err"},
	}

	// Should not panic — the error is logged but not propagated.
	writeSFIDToNotion(ctx, notionClient, result, "001SFID")
	notionClient.AssertExpectations(t)
}

// --- extractContactsForSF edge cases ---

func TestExtractContactsForSF_AnyMapStringAny(t *testing.T) {
	// Exercises the []any → map[string]any branch.
	fieldValues := map[string]model.FieldValue{
		"contacts": {
			FieldKey: "contacts",
			Value: []any{
				map[string]any{
					"first_name": "Jane",
					"last_name":  "Doe",
					"title":      "CEO",
					"email":      "jane@acme.com",
				},
			},
		},
	}

	contacts := extractContactsForSF(fieldValues, nil)
	assert.Len(t, contacts, 1)
	assert.Equal(t, "Jane", contacts[0]["FirstName"])
	assert.Equal(t, "Doe", contacts[0]["LastName"])
	assert.Equal(t, "CEO", contacts[0]["Title"])
	assert.Equal(t, "jane@acme.com", contacts[0]["Email"])
}

func TestExtractContactsForSF_UnsupportedValueType(t *testing.T) {
	// Value is not a slice → default branch returns nil.
	fieldValues := map[string]model.FieldValue{
		"contacts": {
			FieldKey: "contacts",
			Value:    "not a slice",
		},
	}

	contacts := extractContactsForSF(fieldValues, nil)
	assert.Nil(t, contacts)
}

func TestExtractContactsForSF_EmptyItems(t *testing.T) {
	fieldValues := map[string]model.FieldValue{
		"contacts": {
			FieldKey: "contacts",
			Value:    []map[string]string{},
		},
	}

	contacts := extractContactsForSF(fieldValues, nil)
	assert.Nil(t, contacts)
}

func TestExtractContactsForSF_NoLastName(t *testing.T) {
	// Contact without LastName should be skipped since SF requires it.
	fieldValues := map[string]model.FieldValue{
		"contacts": {
			FieldKey: "contacts",
			Value: []map[string]string{
				{"first_name": "Jane"}, // No last_name.
			},
		},
	}

	contacts := extractContactsForSF(fieldValues, nil)
	assert.Nil(t, contacts)
}

// --- buildSFFieldsByObject tests ---

func TestBuildSFFieldsByObject_ContactFields(t *testing.T) {
	registry := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry", SFObject: "Account"},
		{Key: "owner_email", SFField: "Email", SFObject: "Contact"},
		{Key: "employees", SFField: "NumberOfEmployees"},
	})

	fieldValues := map[string]model.FieldValue{
		"industry":    {FieldKey: "industry", SFField: "Industry", Value: "Tech"},
		"owner_email": {FieldKey: "owner_email", SFField: "Email", Value: "ceo@acme.com"},
		"employees":   {FieldKey: "employees", SFField: "NumberOfEmployees", Value: "200"},
	}

	accountFields, contactFields := buildSFFieldsByObject(fieldValues, registry)

	// Industry and employees → Account, email → Contact.
	assert.Equal(t, "Tech", accountFields["Industry"])
	assert.Equal(t, "200", accountFields["NumberOfEmployees"])
	assert.Equal(t, "ceo@acme.com", contactFields["Email"])
}

func TestBuildSFFieldsByObject_EmptySFField(t *testing.T) {
	registry := model.NewFieldRegistry(nil)
	fieldValues := map[string]model.FieldValue{
		"notes": {FieldKey: "notes", SFField: "", Value: "some notes"}, // No SF field.
	}

	accountFields, contactFields := buildSFFieldsByObject(fieldValues, registry)
	assert.Empty(t, accountFields)
	assert.Empty(t, contactFields)
}

// --- ensureMinimumSFFields tests ---

func TestEnsureMinimumSFFields_FallbackFromFieldValues(t *testing.T) {
	fields := make(map[string]any)
	company := model.Company{URL: "https://acme.com"} // No Name.
	fieldValues := map[string]model.FieldValue{
		"company_name": {FieldKey: "company_name", Value: "Acme Extracted"},
	}

	ensureMinimumSFFields(fields, company, fieldValues)

	assert.Equal(t, "Acme Extracted", fields["Name"])
	assert.Equal(t, "https://acme.com", fields["Website"])
}

func TestEnsureMinimumSFFields_FallbackFromDomain(t *testing.T) {
	fields := make(map[string]any)
	company := model.Company{URL: "https://acme.com"} // No Name.
	fieldValues := map[string]model.FieldValue{}      // No field values either.

	ensureMinimumSFFields(fields, company, fieldValues)

	// Should derive name from domain.
	assert.NotEmpty(t, fields["Name"])
	assert.Equal(t, "https://acme.com", fields["Website"])
}

func TestEnsureMinimumSFFields_ExistingFieldsNotOverwritten(t *testing.T) {
	fields := map[string]any{
		"Name":    "Already Set",
		"Website": "https://existing.com",
	}
	company := model.Company{Name: "Different", URL: "https://different.com"}

	ensureMinimumSFFields(fields, company, nil)

	assert.Equal(t, "Already Set", fields["Name"])
	assert.Equal(t, "https://existing.com", fields["Website"])
}

// --- injectGeoFields tests ---

func TestInjectGeoFields_Nil(t *testing.T) {
	fields := make(map[string]any)
	injectGeoFields(fields, nil)
	assert.Empty(t, fields)
}

func TestInjectGeoFields_AllFields(t *testing.T) {
	fields := make(map[string]any)
	gd := &model.GeoData{
		Latitude:       32.7767,
		Longitude:      -96.797,
		MSAName:        "Dallas-Fort Worth",
		CBSACode:       "19100",
		Classification: "Metropolitan",
		CentroidKM:     5.2,
		EdgeKM:         12.8,
		CountyFIPS:     "48113",
	}

	injectGeoFields(fields, gd)

	assert.Equal(t, 32.7767, fields["Longitude_and_Lattitude__Latitude__s"])  //nolint:misspell // SF field name
	assert.Equal(t, -96.797, fields["Longitude_and_Lattitude__Longitude__s"]) //nolint:misspell // SF field name
	assert.Equal(t, "Dallas-Fort Worth", fields["Company_MSA__c"])
	assert.Equal(t, "19100", fields["MSA_CBSA_Code__c"])
	assert.Equal(t, "Metropolitan", fields["Urban_Classification__c"])
	assert.Equal(t, 5.2, fields["Distance_to_MSA_Center_km__c"])
	assert.Equal(t, 12.8, fields["Distance_to_MSA_Edge_km__c"])
	assert.Equal(t, "48113", fields["County_FIPS__c"])
}

func TestInjectGeoFields_PartialData(t *testing.T) {
	fields := make(map[string]any)
	gd := &model.GeoData{
		Latitude:  32.7767,
		Longitude: -96.797,
		// No MSA, classification, etc.
	}

	injectGeoFields(fields, gd)

	assert.Equal(t, 32.7767, fields["Longitude_and_Lattitude__Latitude__s"])  //nolint:misspell // SF field name
	assert.Equal(t, -96.797, fields["Longitude_and_Lattitude__Longitude__s"]) //nolint:misspell // SF field name
	_, hasMSA := fields["Company_MSA__c"]
	assert.False(t, hasMSA)
}
