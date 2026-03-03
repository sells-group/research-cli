package pipeline

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/model"
)

func TestExportSFReportCSV(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "sfreport.csv")

	results := []*model.EnrichmentResult{
		{
			Company: model.Company{
				Name:         "Acme Corp",
				SalesforceID: "001ABC",
				URL:          "https://acme.com",
				City:         "Austin",
				State:        "TX",
			},
			FieldValues: map[string]model.FieldValue{
				"year_established": {FieldKey: "year_established", Value: "2010"},
				"employees":        {FieldKey: "employees", Value: "150"},
				"description":      {FieldKey: "description", Value: "A tech company."},
				"end_markets":      {FieldKey: "end_markets", Value: "Enterprise Software"},
			},
		},
	}

	originals := []SFReportCompany{
		{AccountID: "001ABC", Ownership: "Private"},
	}

	err := ExportSFReportCSV(results, originals, outPath)
	assert.NoError(t, err)

	f, err := os.Open(outPath)
	require.NoError(t, err)
	defer f.Close() //nolint:errcheck

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	require.NoError(t, err)

	// Header + 1 data row.
	require.Len(t, records, 2)
	assert.Equal(t, sfReportColumns, records[0])

	row := records[1]
	assert.Equal(t, "Acme Corp", row[0])            // Account Name
	assert.Equal(t, "001ABC", row[1])               // Account ID
	assert.Equal(t, "acme.com", row[2])             // Website (stripped scheme)
	assert.Equal(t, "2010", row[3])                 // Year Founded
	assert.Equal(t, "150", row[5])                  // Employees
	assert.Equal(t, "TX", row[6])                   // State
	assert.Equal(t, "Austin", row[8])               // Shipping City
	assert.Equal(t, "Private", row[13])             // Ownership
	assert.Equal(t, "Enterprise Software", row[14]) // End Markets
}

func TestExportSFReportCSV_BadPath(t *testing.T) {
	results := []*model.EnrichmentResult{
		{Company: model.Company{Name: "Acme"}},
	}
	err := ExportSFReportCSV(results, nil, "/nonexistent/dir/out.csv")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sf-report export")
}

func TestExportSFReportCSV_NoOriginal(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "sfreport.csv")

	results := []*model.EnrichmentResult{
		{
			Company: model.Company{
				Name:         "NewCo",
				URL:          "https://newco.com",
				SalesforceID: "001NEW",
			},
			FieldValues: map[string]model.FieldValue{},
		},
	}

	err := ExportSFReportCSV(results, nil, outPath)
	assert.NoError(t, err)

	f, err := os.Open(outPath)
	require.NoError(t, err)
	defer f.Close() //nolint:errcheck

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 2)
	// Account ID should be empty since no original found.
	assert.Equal(t, "", records[1][1])
}

func TestBuildSFReportRow(t *testing.T) {
	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:  "TestCo",
			URL:   "https://testco.com",
			City:  "Dallas",
			State: "TX",
		},
		FieldValues: map[string]model.FieldValue{
			"year_established": {FieldKey: "year_established", Value: "2015"},
			"locations":        {FieldKey: "locations", Value: "3"},
			"employees":        {FieldKey: "employees", Value: "500"},
			"end_markets":      {FieldKey: "end_markets", Value: "Healthcare"},
			"hq_city":          {FieldKey: "hq_city", Value: "Dallas"},
			"hq_state":         {FieldKey: "hq_state", Value: "Texas"},
			"office_2_city":    {FieldKey: "office_2_city", Value: "Houston"},
			"office_3_city":    {FieldKey: "office_3_city", Value: "San Antonio"},
		},
	}

	original := &SFReportCompany{
		AccountID: "001X",
		Ownership: "Public",
	}

	row := buildSFReportRow(result, original)

	assert.Equal(t, "TestCo", row[0])       // Account Name
	assert.Equal(t, "001X", row[1])         // Account ID
	assert.Equal(t, "testco.com", row[2])   // Website
	assert.Equal(t, "2015", row[3])         // Year Founded
	assert.Equal(t, "3", row[4])            // Locations
	assert.Equal(t, "500", row[5])          // Employees
	assert.Equal(t, "TX", row[6])           // State (abbreviated)
	assert.Equal(t, "Dallas", row[8])       // City (title case)
	assert.Equal(t, "Houston", row[9])      // Office #2 City
	assert.Equal(t, "San Antonio", row[10]) // Office #3 City
	assert.Equal(t, "Public", row[13])      // Ownership
	assert.Equal(t, "Healthcare", row[14])  // End Markets
}

func TestBuildSFReportRow_FallbackToCityState(t *testing.T) {
	result := &model.EnrichmentResult{
		Company: model.Company{
			Name:  "TestCo",
			URL:   "https://testco.com",
			City:  "Denver",
			State: "CO",
		},
		FieldValues: map[string]model.FieldValue{},
	}

	row := buildSFReportRow(result, nil)
	assert.Equal(t, "CO", row[6])     // State from company
	assert.Equal(t, "Denver", row[8]) // City from company
}

func TestExportSFReportCSV_EmptyResults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.csv")
	err := ExportSFReportCSV(nil, nil, path)
	assert.NoError(t, err)

	content, readErr := os.ReadFile(path)
	assert.NoError(t, readErr)
	// Should have just the header.
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	assert.Equal(t, 1, len(lines))
}

func TestBuildSFReportRow_EmptyFieldValues(t *testing.T) {
	result := &model.EnrichmentResult{
		Company:     model.Company{Name: "Acme Corp", URL: "https://acme.com"},
		FieldValues: map[string]model.FieldValue{},
	}
	row := buildSFReportRow(result, nil)
	assert.Equal(t, "Acme Corp", row[0]) // name
	assert.NotEmpty(t, row)
}

func TestExportSFReportCSV_MultipleResults(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "sfreport_multi.csv")

	results := []*model.EnrichmentResult{
		{
			Company:     model.Company{Name: "Acme Corp", URL: "https://acme.com", SalesforceID: "001A"},
			FieldValues: map[string]model.FieldValue{"hq_city": {Value: "austin"}, "hq_state": {Value: "Texas"}},
		},
		{
			Company:     model.Company{Name: "Beta Inc", URL: "https://beta.com", SalesforceID: "001B"},
			FieldValues: map[string]model.FieldValue{"employees": {Value: "200"}, "office_4_city": {Value: "Denver"}},
		},
	}

	originals := []SFReportCompany{
		{AccountID: "001A", Ownership: "Private"},
		{AccountID: "001B", Ownership: "Public"},
	}

	err := ExportSFReportCSV(results, originals, outPath)
	assert.NoError(t, err)

	f, err := os.Open(outPath)
	require.NoError(t, err)
	defer f.Close() //nolint:errcheck

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 3)                // header + 2 rows
	assert.Equal(t, "Austin", records[1][8])  // hq_city title-cased
	assert.Equal(t, "TX", records[1][6])      // hq_state abbreviated
	assert.Equal(t, "Denver", records[2][11]) // office_4_city
	assert.Equal(t, "Public", records[2][13]) // ownership
}

func TestBuildResearchNotes(t *testing.T) {
	t.Run("all fields", func(t *testing.T) {
		result := &model.EnrichmentResult{
			FieldValues: map[string]model.FieldValue{
				"description":     {FieldKey: "description", Value: "A tech company."},
				"service_mix":     {FieldKey: "service_mix", Value: "Consulting, Development"},
				"differentiators": {FieldKey: "differentiators", Value: "AI-powered analytics"},
				"business_model":  {FieldKey: "business_model", Value: "B2B SaaS"},
				"customer_types":  {FieldKey: "customer_types", Value: "Enterprise"},
			},
		}

		notes := buildResearchNotes(result)
		assert.Contains(t, notes, "A tech company.")
		assert.Contains(t, notes, "Services: Consulting, Development.")
		assert.Contains(t, notes, "Differentiators: AI-powered analytics.")
		assert.Contains(t, notes, "Business Model: B2B SaaS.")
		assert.Contains(t, notes, "Customers: Enterprise.")
	})

	t.Run("partial fields", func(t *testing.T) {
		result := &model.EnrichmentResult{
			FieldValues: map[string]model.FieldValue{
				"description": {FieldKey: "description", Value: "Just a description."},
			},
		}

		notes := buildResearchNotes(result)
		assert.Equal(t, "Just a description.", notes)
	})

	t.Run("empty fields", func(t *testing.T) {
		result := &model.EnrichmentResult{
			FieldValues: map[string]model.FieldValue{},
		}

		notes := buildResearchNotes(result)
		assert.Equal(t, "", notes)
	})
}
