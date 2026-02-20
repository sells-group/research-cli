package registry

import (
	"context"
	"strconv"
	"testing"

	"github.com/jomei/notionapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/model"
	notionmocks "github.com/sells-group/research-cli/pkg/notion/mocks"
)

func init() {
	// Replace global logger with no-op for tests (suppress warning output).
	zap.ReplaceGlobals(zap.NewNop())
}

func TestLoadQuestionRegistry_Success(t *testing.T) {
	mc := notionmocks.NewMockClient(t)
	ctx := context.Background()

	mc.On("QueryDatabase", ctx, "q-db", mock.AnythingOfType("*notionapi.DatabaseQueryRequest")).
		Return(&notionapi.DatabaseQueryResponse{
			Results: []notionapi.Page{
				makeQuestionPage("q1", "What services?", 1, "services_offered", []string{"services", "homepage"}, "Extract list of services", "list", "Active"),
				makeQuestionPage("q2", "Company size?", 2, "company_size", []string{"about"}, "Determine employee count", "number", "Active"),
			},
			HasMore: false,
		}, nil).Once()

	questions, err := LoadQuestionRegistry(ctx, mc, "q-db")
	assert.NoError(t, err)
	assert.Len(t, questions, 2)

	assert.Equal(t, "q1", questions[0].ID)
	assert.Equal(t, "What services?", questions[0].Text)
	assert.Equal(t, 1, questions[0].Tier)
	assert.Equal(t, "services_offered", questions[0].FieldKey)
	assert.Equal(t, []model.PageType{"services", "homepage"}, questions[0].PageTypes)
	assert.Equal(t, "Extract list of services", questions[0].Instructions)
	assert.Equal(t, "list", questions[0].OutputFormat)
	assert.Equal(t, "Active", questions[0].Status)

	assert.Equal(t, "q2", questions[1].ID)
	assert.Equal(t, 2, questions[1].Tier)
	mc.AssertExpectations(t)
}

func TestLoadQuestionRegistry_Pagination(t *testing.T) {
	mc := notionmocks.NewMockClient(t)
	ctx := context.Background()

	// First page.
	mc.On("QueryDatabase", ctx, "q-db", mock.MatchedBy(func(req *notionapi.DatabaseQueryRequest) bool {
		return req.StartCursor == ""
	})).Return(&notionapi.DatabaseQueryResponse{
		Results:    []notionapi.Page{makeQuestionPage("q1", "Question 1", 1, "f1", nil, "", "text", "Active")},
		HasMore:    true,
		NextCursor: "cursor-2",
	}, nil).Once()

	// Second page.
	mc.On("QueryDatabase", ctx, "q-db", mock.MatchedBy(func(req *notionapi.DatabaseQueryRequest) bool {
		return req.StartCursor == "cursor-2"
	})).Return(&notionapi.DatabaseQueryResponse{
		Results: []notionapi.Page{makeQuestionPage("q2", "Question 2", 2, "f2", nil, "", "text", "Active")},
		HasMore: false,
	}, nil).Once()

	questions, err := LoadQuestionRegistry(ctx, mc, "q-db")
	assert.NoError(t, err)
	assert.Len(t, questions, 2)
	assert.Equal(t, "q1", questions[0].ID)
	assert.Equal(t, "q2", questions[1].ID)
	mc.AssertExpectations(t)
}

func TestLoadQuestionRegistry_MalformedPage(t *testing.T) {
	mc := notionmocks.NewMockClient(t)
	ctx := context.Background()

	// One good page, one with missing Text (will be skipped with warning).
	mc.On("QueryDatabase", ctx, "q-db", mock.AnythingOfType("*notionapi.DatabaseQueryRequest")).
		Return(&notionapi.DatabaseQueryResponse{
			Results: []notionapi.Page{
				makeQuestionPage("q1", "Valid question", 1, "f1", nil, "", "text", "Active"),
				makeQuestionPage("q2", "", 1, "f2", nil, "", "text", "Active"), // empty Text
			},
			HasMore: false,
		}, nil).Once()

	questions, err := LoadQuestionRegistry(ctx, mc, "q-db")
	assert.NoError(t, err) // malformed pages are warnings, not errors
	assert.Len(t, questions, 1)
	assert.Equal(t, "q1", questions[0].ID)
	mc.AssertExpectations(t)
}

func TestLoadQuestionRegistry_Empty(t *testing.T) {
	mc := notionmocks.NewMockClient(t)
	ctx := context.Background()

	mc.On("QueryDatabase", ctx, "q-db", mock.AnythingOfType("*notionapi.DatabaseQueryRequest")).
		Return(&notionapi.DatabaseQueryResponse{
			Results: []notionapi.Page{},
			HasMore: false,
		}, nil).Once()

	questions, err := LoadQuestionRegistry(ctx, mc, "q-db")
	assert.NoError(t, err)
	assert.Empty(t, questions)
	mc.AssertExpectations(t)
}

func TestLoadQuestionRegistry_QueryError(t *testing.T) {
	mc := notionmocks.NewMockClient(t)
	ctx := context.Background()

	mc.On("QueryDatabase", ctx, "q-db", mock.AnythingOfType("*notionapi.DatabaseQueryRequest")).
		Return(nil, assert.AnError).Once()

	questions, err := LoadQuestionRegistry(ctx, mc, "q-db")
	assert.Error(t, err)
	assert.Nil(t, questions)
	mc.AssertExpectations(t)
}

// makeQuestionPage builds a fake notionapi.Page with Question Registry properties.
// Property names match the existing Research Prompts Notion DB.
func makeQuestionPage(id, text string, tier int, fieldKey string, pageTypes []string, instructions, outputFormat, status string) notionapi.Page {
	props := make(notionapi.Properties)

	props["Question Key"] = &notionapi.TitleProperty{
		Type: notionapi.PropertyTypeTitle,
		Title: []notionapi.RichText{
			{PlainText: text},
		},
	}

	props["Tier"] = &notionapi.SelectProperty{
		Type:   notionapi.PropertyTypeSelect,
		Select: notionapi.Option{Name: strconv.Itoa(tier)},
	}

	props["Target SF Fields"] = &notionapi.RichTextProperty{
		Type: notionapi.PropertyTypeRichText,
		RichText: []notionapi.RichText{
			{PlainText: fieldKey},
		},
	}

	if len(pageTypes) > 0 {
		opts := make([]notionapi.Option, len(pageTypes))
		for i, pt := range pageTypes {
			opts[i] = notionapi.Option{Name: pt}
		}
		props["Relevant Page Types"] = &notionapi.MultiSelectProperty{
			Type:        notionapi.PropertyTypeMultiSelect,
			MultiSelect: opts,
		}
	}

	props["Instructions"] = &notionapi.RichTextProperty{
		Type: notionapi.PropertyTypeRichText,
		RichText: []notionapi.RichText{
			{PlainText: instructions},
		},
	}

	props["Output Schema"] = &notionapi.RichTextProperty{
		Type: notionapi.PropertyTypeRichText,
		RichText: []notionapi.RichText{
			{PlainText: outputFormat},
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
