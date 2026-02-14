package salesforce

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSObjectField_AllFields(t *testing.T) {
	f := SObjectField{
		Name:       "Industry",
		Label:      "Industry",
		Type:       "picklist",
		Length:     255,
		Updateable: true,
	}
	assert.Equal(t, "Industry", f.Name)
	assert.Equal(t, "Industry", f.Label)
	assert.Equal(t, "picklist", f.Type)
	assert.Equal(t, 255, f.Length)
	assert.True(t, f.Updateable)
}

func TestSObjectDescription_AllFields(t *testing.T) {
	desc := SObjectDescription{
		Name:  "Account",
		Label: "Account",
		Fields: []SObjectField{
			{Name: "Id", Label: "Account ID", Type: "id", Length: 18, Updateable: false},
			{Name: "Name", Label: "Account Name", Type: "string", Length: 255, Updateable: true},
		},
	}
	assert.Equal(t, "Account", desc.Name)
	assert.Equal(t, "Account", desc.Label)
	require.Len(t, desc.Fields, 2)
}

func TestAccount_AllFields(t *testing.T) {
	a := Account{
		ID:                "001xx",
		Name:              "Acme Corp",
		Website:           "acme.com",
		Industry:          "Technology",
		Description:       "A test company",
		BillingCity:       "Austin",
		BillingState:      "TX",
		BillingCountry:    "US",
		BillingPostalCode: "78701",
		Phone:             "555-1234",
		NumberOfEmployees: 100,
		AnnualRevenue:     5000000.0,
		Type:              "Customer",
	}
	assert.Equal(t, "001xx", a.ID)
	assert.Equal(t, "Acme Corp", a.Name)
	assert.Equal(t, "acme.com", a.Website)
	assert.Equal(t, "Technology", a.Industry)
	assert.Equal(t, "A test company", a.Description)
	assert.Equal(t, "Austin", a.BillingCity)
	assert.Equal(t, "TX", a.BillingState)
	assert.Equal(t, "US", a.BillingCountry)
	assert.Equal(t, "78701", a.BillingPostalCode)
	assert.Equal(t, "555-1234", a.Phone)
	assert.Equal(t, 100, a.NumberOfEmployees)
	assert.Equal(t, 5000000.0, a.AnnualRevenue)
	assert.Equal(t, "Customer", a.Type)
}

func TestAccountUpdate_Fields(t *testing.T) {
	u := AccountUpdate{
		ID:     "001xx",
		Fields: map[string]any{"Industry": "Finance", "Phone": "555-9999"},
	}
	assert.Equal(t, "001xx", u.ID)
	assert.Equal(t, "Finance", u.Fields["Industry"])
}

func TestCollectionRecord_Fields(t *testing.T) {
	r := CollectionRecord{
		ID:     "001xx",
		Fields: map[string]any{"Name": "Updated"},
	}
	assert.Equal(t, "001xx", r.ID)
	assert.Equal(t, "Updated", r.Fields["Name"])
}

func TestAccountFields_AllPresent(t *testing.T) {
	expected := []string{
		"Id", "Name", "Website", "Industry", "Description",
		"BillingCity", "BillingState", "BillingCountry", "BillingPostalCode",
		"Phone", "NumberOfEmployees", "AnnualRevenue", "Type",
	}
	assert.Equal(t, expected, accountFields)
}

func TestQueryResult_GenericType(t *testing.T) {
	qr := QueryResult[Account]{
		Records: []Account{
			{ID: "001xx", Name: "Acme"},
			{ID: "002xx", Name: "Beta"},
		},
	}
	require.Len(t, qr.Records, 2)
	assert.Equal(t, "001xx", qr.Records[0].ID)
}

func TestMockClient_DefaultBehavior(t *testing.T) {
	mc := &mockClient{}

	// Query returns nil (no-op)
	err := mc.Query(context.Background(), "SELECT Id FROM Account", nil)
	assert.NoError(t, err)

	// InsertOne returns default ID
	id, err := mc.InsertOne(context.Background(), "Account", nil)
	assert.NoError(t, err)
	assert.Equal(t, "001000000000001", id)

	// UpdateOne returns nil
	err = mc.UpdateOne(context.Background(), "Account", "001xx", nil)
	assert.NoError(t, err)

	// DescribeSObject returns basic description
	desc, err := mc.DescribeSObject(context.Background(), "Account")
	assert.NoError(t, err)
	assert.Equal(t, "Account", desc.Name)
}

func TestMockClient_UpdateCollectionDefault(t *testing.T) {
	mc := &mockClient{}
	records := []CollectionRecord{
		{ID: "001xx", Fields: map[string]any{"Name": "A"}},
		{ID: "002xx", Fields: map[string]any{"Name": "B"}},
	}
	results, err := mc.UpdateCollection(context.Background(), "Account", records)
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.True(t, results[0].Success)
	assert.True(t, results[1].Success)
	assert.Equal(t, "001xx", results[0].ID)
	assert.Equal(t, "002xx", results[1].ID)
}

func TestFindAccountByWebsite_SOQLInjectionPrevented(t *testing.T) {
	var capturedSOQL string
	mc := &mockClient{
		queryFn: func(_ context.Context, soql string, out any) error {
			capturedSOQL = soql
			accounts := out.(*[]Account)
			*accounts = []Account{}
			return nil
		},
	}

	_, _ = FindAccountByWebsite(context.Background(), mc, "test'; DROP TABLE Account; --")
	assert.Contains(t, capturedSOQL, "test\\'; DROP TABLE Account; --")
	assert.NotContains(t, capturedSOQL, "test'; DROP")
}

func TestFindAccountByID_ErrorPropagation(t *testing.T) {
	mc := &mockClient{
		queryFn: func(_ context.Context, _ string, _ any) error {
			return errors.New("timeout")
		},
	}

	acct, err := FindAccountByID(context.Background(), mc, "001xx")
	assert.Error(t, err)
	assert.Nil(t, acct)
	assert.Contains(t, err.Error(), "find account by id")
}

func TestUpdateAccount_NilFields(t *testing.T) {
	mc := &mockClient{}
	err := UpdateAccount(context.Background(), mc, "001xx", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no fields to update")
}

func TestBulkUpdateAccounts_FieldsPassedCorrectly(t *testing.T) {
	var capturedRecords []CollectionRecord
	mc := &mockClient{
		updateCollectionFn: func(_ context.Context, sObject string, records []CollectionRecord) ([]CollectionResult, error) {
			assert.Equal(t, "Account", sObject)
			capturedRecords = records
			results := make([]CollectionResult, len(records))
			for i, r := range records {
				results[i] = CollectionResult{ID: r.ID, Success: true}
			}
			return results, nil
		},
	}

	updates := []AccountUpdate{
		{ID: "001xx", Fields: map[string]any{"Industry": "Tech", "Phone": "555"}},
		{ID: "002xx", Fields: map[string]any{"Name": "New Name"}},
	}

	results, err := BulkUpdateAccounts(context.Background(), mc, updates)
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Len(t, capturedRecords, 2)
	assert.Equal(t, "001xx", capturedRecords[0].ID)
	assert.Equal(t, "Tech", capturedRecords[0].Fields["Industry"])
	assert.Equal(t, "002xx", capturedRecords[1].ID)
	assert.Equal(t, "New Name", capturedRecords[1].Fields["Name"])
}
