package registry

import (
	"context"
	"testing"

	"github.com/jomei/notionapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestLoadFieldRegistry_Success(t *testing.T) {
	mc := new(mockNotionClient)
	ctx := context.Background()

	mc.On("QueryDatabase", ctx, "f-db", mock.AnythingOfType("*notionapi.DatabaseQueryRequest")).
		Return(&notionapi.DatabaseQueryResponse{
			Results: []notionapi.Page{
				makeFieldPage("f1", "company_name", "Name", "Account", "string", true, 255, "", "Active"),
				makeFieldPage("f2", "annual_revenue", "AnnualRevenue", "Account", "currency", false, 0, "^[0-9]+$", "Active"),
			},
			HasMore: false,
		}, nil).Once()

	reg, err := LoadFieldRegistry(ctx, mc, "f-db")
	assert.NoError(t, err)
	assert.Len(t, reg.Fields, 2)

	// Verify first field.
	f1 := reg.ByKey("company_name")
	assert.NotNil(t, f1)
	assert.Equal(t, "Name", f1.SFField)
	assert.Equal(t, "Account", f1.SFObject)
	assert.Equal(t, "string", f1.DataType)
	assert.True(t, f1.Required)
	assert.Equal(t, 255, f1.MaxLength)

	// Verify second field.
	f2 := reg.ByKey("annual_revenue")
	assert.NotNil(t, f2)
	assert.Equal(t, "AnnualRevenue", f2.SFField)
	assert.False(t, f2.Required)
	assert.Equal(t, "^[0-9]+$", f2.Validation)

	// Verify SF name index.
	assert.NotNil(t, reg.BySFName("Name"))
	assert.NotNil(t, reg.BySFName("AnnualRevenue"))
	assert.Nil(t, reg.BySFName("Nonexistent"))

	// Verify required.
	assert.Len(t, reg.Required(), 1)
	assert.Equal(t, "company_name", reg.Required()[0].Key)

	mc.AssertExpectations(t)
}

func TestLoadFieldRegistry_Pagination(t *testing.T) {
	mc := new(mockNotionClient)
	ctx := context.Background()

	mc.On("QueryDatabase", ctx, "f-db", mock.MatchedBy(func(req *notionapi.DatabaseQueryRequest) bool {
		return req.StartCursor == ""
	})).Return(&notionapi.DatabaseQueryResponse{
		Results:    []notionapi.Page{makeFieldPage("f1", "key1", "SF1", "Account", "string", false, 0, "", "Active")},
		HasMore:    true,
		NextCursor: "cursor-next",
	}, nil).Once()

	mc.On("QueryDatabase", ctx, "f-db", mock.MatchedBy(func(req *notionapi.DatabaseQueryRequest) bool {
		return req.StartCursor == "cursor-next"
	})).Return(&notionapi.DatabaseQueryResponse{
		Results: []notionapi.Page{makeFieldPage("f2", "key2", "SF2", "Account", "string", false, 0, "", "Active")},
		HasMore: false,
	}, nil).Once()

	reg, err := LoadFieldRegistry(ctx, mc, "f-db")
	assert.NoError(t, err)
	assert.Len(t, reg.Fields, 2)
	assert.NotNil(t, reg.ByKey("key1"))
	assert.NotNil(t, reg.ByKey("key2"))
	mc.AssertExpectations(t)
}

func TestLoadFieldRegistry_MalformedPage(t *testing.T) {
	mc := new(mockNotionClient)
	ctx := context.Background()

	mc.On("QueryDatabase", ctx, "f-db", mock.AnythingOfType("*notionapi.DatabaseQueryRequest")).
		Return(&notionapi.DatabaseQueryResponse{
			Results: []notionapi.Page{
				makeFieldPage("f1", "valid_key", "SF1", "Account", "string", false, 0, "", "Active"),
				makeFieldPage("f2", "", "SF2", "Account", "string", false, 0, "", "Active"), // empty Key
			},
			HasMore: false,
		}, nil).Once()

	reg, err := LoadFieldRegistry(ctx, mc, "f-db")
	assert.NoError(t, err)
	assert.Len(t, reg.Fields, 1)
	assert.NotNil(t, reg.ByKey("valid_key"))
	mc.AssertExpectations(t)
}

func TestLoadFieldRegistry_Empty(t *testing.T) {
	mc := new(mockNotionClient)
	ctx := context.Background()

	mc.On("QueryDatabase", ctx, "f-db", mock.AnythingOfType("*notionapi.DatabaseQueryRequest")).
		Return(&notionapi.DatabaseQueryResponse{
			Results: []notionapi.Page{},
			HasMore: false,
		}, nil).Once()

	reg, err := LoadFieldRegistry(ctx, mc, "f-db")
	assert.NoError(t, err)
	assert.Empty(t, reg.Fields)
	mc.AssertExpectations(t)
}

func TestLoadFieldRegistry_QueryError(t *testing.T) {
	mc := new(mockNotionClient)
	ctx := context.Background()

	mc.On("QueryDatabase", ctx, "f-db", mock.AnythingOfType("*notionapi.DatabaseQueryRequest")).
		Return(nil, assert.AnError).Once()

	reg, err := LoadFieldRegistry(ctx, mc, "f-db")
	assert.Error(t, err)
	assert.Nil(t, reg)
	mc.AssertExpectations(t)
}

func TestLoadFieldRegistry_IndexBuilding(t *testing.T) {
	mc := new(mockNotionClient)
	ctx := context.Background()

	mc.On("QueryDatabase", ctx, "f-db", mock.AnythingOfType("*notionapi.DatabaseQueryRequest")).
		Return(&notionapi.DatabaseQueryResponse{
			Results: []notionapi.Page{
				makeFieldPage("f1", "key_a", "FieldA", "Account", "string", true, 100, "regex", "Active"),
				makeFieldPage("f2", "key_b", "FieldB", "Contact", "number", true, 0, "", "Active"),
				makeFieldPage("f3", "key_c", "FieldC", "Account", "boolean", false, 0, "", "Active"),
			},
			HasMore: false,
		}, nil).Once()

	reg, err := LoadFieldRegistry(ctx, mc, "f-db")
	assert.NoError(t, err)

	// ByKey lookups.
	assert.NotNil(t, reg.ByKey("key_a"))
	assert.NotNil(t, reg.ByKey("key_b"))
	assert.NotNil(t, reg.ByKey("key_c"))
	assert.Nil(t, reg.ByKey("nonexistent"))

	// BySFName lookups.
	assert.Equal(t, "key_a", reg.BySFName("FieldA").Key)
	assert.Equal(t, "key_b", reg.BySFName("FieldB").Key)

	// Required fields.
	required := reg.Required()
	assert.Len(t, required, 2)
	mc.AssertExpectations(t)
}

// makeFieldPage builds a fake notionapi.Page with Field Registry properties.
func makeFieldPage(id, key, sfField, sfObject, dataType string, required bool, maxLength int, validation, status string) notionapi.Page {
	props := make(notionapi.Properties)

	props["Key"] = &notionapi.TitleProperty{
		Type: notionapi.PropertyTypeTitle,
		Title: []notionapi.RichText{
			{PlainText: key},
		},
	}

	props["SFField"] = &notionapi.RichTextProperty{
		Type: notionapi.PropertyTypeRichText,
		RichText: []notionapi.RichText{
			{PlainText: sfField},
		},
	}

	props["SFObject"] = &notionapi.SelectProperty{
		Type:   notionapi.PropertyTypeSelect,
		Select: notionapi.Option{Name: sfObject},
	}

	props["DataType"] = &notionapi.SelectProperty{
		Type:   notionapi.PropertyTypeSelect,
		Select: notionapi.Option{Name: dataType},
	}

	props["Required"] = &notionapi.CheckboxProperty{
		Type:     notionapi.PropertyTypeCheckbox,
		Checkbox: required,
	}

	props["MaxLength"] = &notionapi.NumberProperty{
		Type:   notionapi.PropertyTypeNumber,
		Number: float64(maxLength),
	}

	props["Validation"] = &notionapi.RichTextProperty{
		Type: notionapi.PropertyTypeRichText,
		RichText: []notionapi.RichText{
			{PlainText: validation},
		},
	}

	props["Status"] = &notionapi.StatusProperty{
		Type:   notionapi.PropertyTypeStatus,
		Status: notionapi.Status{Name: status},
	}

	return notionapi.Page{
		ID:         notionapi.ObjectID(id),
		Properties: props,
	}
}
