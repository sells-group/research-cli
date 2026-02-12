package notion

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jomei/notionapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestCSVMapper_MapRow(t *testing.T) {
	m := CSVMapper{}

	headers := []string{"Name", "URL", "Industry"}
	row := []string{"Acme Corp", "https://acme.com", "SaaS"}

	result := m.MapRow(headers, row)
	assert.Equal(t, "Acme Corp", result["Name"])
	assert.Equal(t, "https://acme.com", result["URL"])
	assert.Equal(t, "SaaS", result["Industry"])
}

func TestCSVMapper_MapRow_ShortRow(t *testing.T) {
	m := CSVMapper{}

	headers := []string{"Name", "URL", "Industry"}
	row := []string{"Acme Corp"}

	result := m.MapRow(headers, row)
	assert.Equal(t, "Acme Corp", result["Name"])
	assert.Equal(t, "", result["URL"])
	assert.Equal(t, "", result["Industry"])
}

func TestCSVMapper_MapRow_EmptyHeaders(t *testing.T) {
	m := CSVMapper{}

	result := m.MapRow(nil, []string{"val"})
	assert.Empty(t, result)
}

func TestImportCSV_Basic(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	csvContent := "Name,URL,Industry\nAcme,https://acme.com,SaaS\nBeta,https://beta.io,Fintech\n"
	csvPath := writeTempCSV(t, csvContent)

	mc.On("CreatePage", ctx, mock.AnythingOfType("*notionapi.PageCreateRequest")).
		Return(&notionapi.Page{ID: "new"}, nil).Times(2)

	count, err := ImportCSV(ctx, mc, "db-1", csvPath)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
	mc.AssertExpectations(t)
}

func TestImportCSV_Deduplication(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	csvContent := "Name,URL\nAcme,https://acme.com\nAcme Dup,https://acme.com\nBeta,https://beta.io\n"
	csvPath := writeTempCSV(t, csvContent)

	mc.On("CreatePage", ctx, mock.AnythingOfType("*notionapi.PageCreateRequest")).
		Return(&notionapi.Page{ID: "new"}, nil).Times(2)

	count, err := ImportCSV(ctx, mc, "db-1", csvPath)
	assert.NoError(t, err)
	assert.Equal(t, 2, count) // duplicate URL skipped
	mc.AssertExpectations(t)
}

func TestImportCSV_SkipsEmptyURL(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	csvContent := "Name,URL\nAcme,https://acme.com\nNoURL,\nBeta,https://beta.io\n"
	csvPath := writeTempCSV(t, csvContent)

	mc.On("CreatePage", ctx, mock.AnythingOfType("*notionapi.PageCreateRequest")).
		Return(&notionapi.Page{ID: "new"}, nil).Times(2)

	count, err := ImportCSV(ctx, mc, "db-1", csvPath)
	assert.NoError(t, err)
	assert.Equal(t, 2, count) // empty URL row skipped
	mc.AssertExpectations(t)
}

func TestImportCSV_EmptyCSV(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	csvContent := "Name,URL\n"
	csvPath := writeTempCSV(t, csvContent)

	count, err := ImportCSV(ctx, mc, "db-1", csvPath)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestImportCSV_HeaderOnly(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	csvContent := "Name,URL"
	csvPath := writeTempCSV(t, csvContent)

	count, err := ImportCSV(ctx, mc, "db-1", csvPath)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestImportCSV_CreateError(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	csvContent := "Name,URL\nAcme,https://acme.com\nBeta,https://beta.io\n"
	csvPath := writeTempCSV(t, csvContent)

	mc.On("CreatePage", ctx, mock.AnythingOfType("*notionapi.PageCreateRequest")).
		Return(nil, assert.AnError).Once()

	count, err := ImportCSV(ctx, mc, "db-1", csvPath)
	assert.Error(t, err)
	assert.Equal(t, 0, count)
	mc.AssertExpectations(t)
}

func TestImportCSV_FileNotFound(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	count, err := ImportCSV(ctx, mc, "db-1", "/nonexistent/file.csv")
	assert.Error(t, err)
	assert.Equal(t, 0, count)
}

func TestImportCSV_NoURLColumn(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	// No URL column means no deduplication; all rows are imported.
	csvContent := "Name,Industry\nAcme,SaaS\nBeta,Fintech\n"
	csvPath := writeTempCSV(t, csvContent)

	mc.On("CreatePage", ctx, mock.AnythingOfType("*notionapi.PageCreateRequest")).
		Return(&notionapi.Page{ID: "new"}, nil).Times(2)

	count, err := ImportCSV(ctx, mc, "db-1", csvPath)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
	mc.AssertExpectations(t)
}

func TestImportCSV_PageProperties(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	csvContent := "Name,URL,Industry\nAcme,https://acme.com,SaaS\n"
	csvPath := writeTempCSV(t, csvContent)

	var captured *notionapi.PageCreateRequest
	mc.On("CreatePage", ctx, mock.AnythingOfType("*notionapi.PageCreateRequest")).
		Run(func(args mock.Arguments) {
			captured = args.Get(1).(*notionapi.PageCreateRequest)
		}).
		Return(&notionapi.Page{ID: "new"}, nil).Once()

	_, err := ImportCSV(ctx, mc, "db-1", csvPath)
	assert.NoError(t, err)

	// Verify parent.
	assert.Equal(t, notionapi.DatabaseID("db-1"), captured.Parent.DatabaseID)

	// Verify Name is a title property.
	nameProp, ok := captured.Properties["Name"]
	assert.True(t, ok)
	tp, ok := nameProp.(notionapi.TitleProperty)
	assert.True(t, ok)
	assert.Len(t, tp.Title, 1)
	assert.Equal(t, "Acme", tp.Title[0].Text.Content)

	// Verify URL is a URL property.
	urlProp, ok := captured.Properties["URL"]
	assert.True(t, ok)
	up, ok := urlProp.(notionapi.URLProperty)
	assert.True(t, ok)
	assert.Equal(t, "https://acme.com", up.URL)

	// Verify Industry is a rich_text property.
	indProp, ok := captured.Properties["Industry"]
	assert.True(t, ok)
	rtp, ok := indProp.(notionapi.RichTextProperty)
	assert.True(t, ok)
	assert.Len(t, rtp.RichText, 1)
	assert.Equal(t, "SaaS", rtp.RichText[0].Text.Content)

	mc.AssertExpectations(t)
}

func TestBuildPageProperties(t *testing.T) {
	row := map[string]string{
		"Name":     "Test Co",
		"URL":      "https://test.co",
		"Industry": "Tech",
	}

	props := buildPageProperties(row)

	// Name -> TitleProperty
	tp, ok := props["Name"].(notionapi.TitleProperty)
	assert.True(t, ok)
	assert.Equal(t, notionapi.PropertyTypeTitle, tp.Type)
	assert.Equal(t, "Test Co", tp.Title[0].Text.Content)

	// URL -> URLProperty
	up, ok := props["URL"].(notionapi.URLProperty)
	assert.True(t, ok)
	assert.Equal(t, notionapi.PropertyTypeURL, up.Type)
	assert.Equal(t, "https://test.co", up.URL)

	// Industry -> RichTextProperty
	rtp, ok := props["Industry"].(notionapi.RichTextProperty)
	assert.True(t, ok)
	assert.Equal(t, notionapi.PropertyTypeRichText, rtp.Type)
	assert.Equal(t, "Tech", rtp.RichText[0].Text.Content)
}

func writeTempCSV(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.csv")
	err := os.WriteFile(path, []byte(content), 0644)
	if err != nil {
		t.Fatal(err)
	}
	return path
}
