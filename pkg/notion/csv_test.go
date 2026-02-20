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

// --- Grata CSV tests ---

func TestIsGrataCSV(t *testing.T) {
	assert.True(t, isGrataCSV([]string{"Name", "Domain", "Grata Link", "City", "State"}))
	assert.True(t, isGrataCSV([]string{"Name", "domain", "grata link"})) // case insensitive
	assert.False(t, isGrataCSV([]string{"Name", "URL", "Industry"}))     // generic CSV
	assert.False(t, isGrataCSV([]string{"Name", "Domain"}))              // Domain but no Grata Link
	assert.False(t, isGrataCSV([]string{"Name", "Grata Link"}))          // Grata Link but no Domain
}

func TestNormalizeURL(t *testing.T) {
	assert.Equal(t, "https://acme.com", normalizeURL("acme.com"))
	assert.Equal(t, "https://acme.com", normalizeURL("https://acme.com"))
	assert.Equal(t, "http://acme.com", normalizeURL("http://acme.com"))
	assert.Equal(t, "", normalizeURL(""))
	assert.Equal(t, "https://acme.com", normalizeURL("  acme.com  "))
}

func TestBuildGrataProperties_DomainToURL(t *testing.T) {
	row := map[string]string{
		"Name":       "Acme Corp",
		"Domain":     "acme.com",
		"Grata Link": "https://grata.com/acme",
	}

	props := buildGrataProperties(row)

	// Domain → URL with https:// prefix.
	up, ok := props["URL"].(notionapi.URLProperty)
	assert.True(t, ok)
	assert.Equal(t, "https://acme.com", up.URL)

	// Name → title.
	tp, ok := props["Name"].(notionapi.TitleProperty)
	assert.True(t, ok)
	assert.Equal(t, "Acme Corp", tp.Title[0].Text.Content)

	// Status = Queued.
	sp, ok := props["Status"].(notionapi.StatusProperty)
	assert.True(t, ok)
	assert.Equal(t, "Queued", sp.Status.Name)

	// Grata Link passed through as rich_text.
	_, ok = props["Grata Link"].(notionapi.RichTextProperty)
	assert.True(t, ok)
}

func TestBuildGrataProperties_CityStateToLocation(t *testing.T) {
	row := map[string]string{
		"Name":       "Acme Corp",
		"Domain":     "acme.com",
		"Grata Link": "https://grata.com/acme",
		"City":       "Denver",
		"State":      "CO",
	}

	props := buildGrataProperties(row)

	// City + State → Location.
	lp, ok := props["Location"].(notionapi.RichTextProperty)
	assert.True(t, ok)
	assert.Equal(t, "Denver, CO", lp.RichText[0].Text.Content)

	// City and State should NOT appear as separate properties.
	_, hasCity := props["City"]
	_, hasState := props["State"]
	assert.False(t, hasCity, "City should be consumed by Location")
	assert.False(t, hasState, "State should be consumed by Location")
}

func TestBuildGrataProperties_CityOnly(t *testing.T) {
	row := map[string]string{
		"Name":       "Test",
		"Domain":     "test.com",
		"Grata Link": "https://grata.com/test",
		"City":       "Denver",
		"State":      "",
	}

	props := buildGrataProperties(row)

	lp, ok := props["Location"].(notionapi.RichTextProperty)
	assert.True(t, ok)
	assert.Equal(t, "Denver", lp.RichText[0].Text.Content)
}

func TestBuildGrataProperties_StateOnly(t *testing.T) {
	row := map[string]string{
		"Name":       "Test",
		"Domain":     "test.com",
		"Grata Link": "https://grata.com/test",
		"City":       "",
		"State":      "CO",
	}

	props := buildGrataProperties(row)

	lp, ok := props["Location"].(notionapi.RichTextProperty)
	assert.True(t, ok)
	assert.Equal(t, "CO", lp.RichText[0].Text.Content)
}

func TestBuildGrataProperties_NameStripQuotes(t *testing.T) {
	row := map[string]string{
		"Name":       `"Acme Corp"`,
		"Domain":     "acme.com",
		"Grata Link": "https://grata.com/acme",
	}

	props := buildGrataProperties(row)

	tp, ok := props["Name"].(notionapi.TitleProperty)
	assert.True(t, ok)
	assert.Equal(t, "Acme Corp", tp.Title[0].Text.Content)
}

func TestBuildGrataProperties_PassThroughColumns(t *testing.T) {
	row := map[string]string{
		"Name":         "Acme",
		"Domain":       "acme.com",
		"Grata Link":   "https://grata.com/acme",
		"Revenue":      "$10M",
		"Employee Count": "150",
		"NAICS":        "541511",
	}

	props := buildGrataProperties(row)

	// Extra columns should be rich_text.
	for _, col := range []string{"Revenue", "Employee Count", "NAICS"} {
		rtp, ok := props[col].(notionapi.RichTextProperty)
		assert.True(t, ok, "column %s should be rich_text", col)
		assert.NotEmpty(t, rtp.RichText)
	}
}

func TestBuildGrataProperties_EmptyColumnsSkipped(t *testing.T) {
	row := map[string]string{
		"Name":       "Acme",
		"Domain":     "acme.com",
		"Grata Link": "https://grata.com/acme",
		"Revenue":    "",
		"Phone":      "",
	}

	props := buildGrataProperties(row)

	_, hasRevenue := props["Revenue"]
	_, hasPhone := props["Phone"]
	assert.False(t, hasRevenue, "empty Revenue should be skipped")
	assert.False(t, hasPhone, "empty Phone should be skipped")
}

func TestImportCSV_GrataAutoDetect(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	csvContent := "Name,Domain,Grata Link,City,State,Revenue\nAcme,acme.com,https://grata.com/acme,Denver,CO,$10M\n"
	csvPath := writeTempCSV(t, csvContent)

	var captured *notionapi.PageCreateRequest
	mc.On("CreatePage", ctx, mock.AnythingOfType("*notionapi.PageCreateRequest")).
		Run(func(args mock.Arguments) {
			captured = args.Get(1).(*notionapi.PageCreateRequest)
		}).
		Return(&notionapi.Page{ID: "new"}, nil).Once()

	count, err := ImportCSV(ctx, mc, "db-1", csvPath)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// Should use Grata properties: URL from Domain, Location from City+State.
	up, ok := captured.Properties["URL"].(notionapi.URLProperty)
	assert.True(t, ok)
	assert.Equal(t, "https://acme.com", up.URL)

	lp, ok := captured.Properties["Location"].(notionapi.RichTextProperty)
	assert.True(t, ok)
	assert.Equal(t, "Denver, CO", lp.RichText[0].Text.Content)

	sp, ok := captured.Properties["Status"].(notionapi.StatusProperty)
	assert.True(t, ok)
	assert.Equal(t, "Queued", sp.Status.Name)

	mc.AssertExpectations(t)
}

func TestImportCSV_GrataDedupOnDomain(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	csvContent := "Name,Domain,Grata Link\nAcme,acme.com,https://grata.com/acme\nAcme Dup,acme.com,https://grata.com/acme2\nBeta,beta.io,https://grata.com/beta\n"
	csvPath := writeTempCSV(t, csvContent)

	mc.On("CreatePage", ctx, mock.AnythingOfType("*notionapi.PageCreateRequest")).
		Return(&notionapi.Page{ID: "new"}, nil).Times(2)

	count, err := ImportCSV(ctx, mc, "db-1", csvPath)
	assert.NoError(t, err)
	assert.Equal(t, 2, count) // duplicate domain skipped
	mc.AssertExpectations(t)
}
