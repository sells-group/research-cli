package pipeline

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sells-group/research-cli/internal/model"
)

func TestParseGrataCSV_RealFile(t *testing.T) {
	// Path to the actual test CSV relative to this test file.
	csvPath := filepath.Join("..", "..", "TestListFeb13(Companies).csv")
	if _, err := os.Stat(csvPath); os.IsNotExist(err) {
		t.Skip("TestListFeb13(Companies).csv not found, skipping")
	}

	companies, err := ParseGrataCSV(csvPath)
	if err != nil {
		t.Fatalf("ParseGrataCSV() error: %v", err)
	}

	if len(companies) != 10 {
		t.Fatalf("expected 10 companies, got %d", len(companies))
	}

	// Verify first company.
	c := companies[0]
	if c.URL != "https://v3construction.com" {
		t.Errorf("expected URL https://v3construction.com, got %s", c.URL)
	}
	if c.Name != "V 3 Construction, Inc." {
		t.Errorf("expected name 'V 3 Construction, Inc.', got %q", c.Name)
	}
	if c.Location != "West Jordan, UT" {
		t.Errorf("expected location 'West Jordan, UT', got %q", c.Location)
	}

	// Verify state abbreviation works for various states.
	stateChecks := map[string]string{
		"https://lonestarsolarservices.com": "Houston, TX",
		"https://screenbuildersinc.com":     "West Palm Beach, FL",
		"https://davisroofing.com":          "Lombard, IL",
	}
	for _, co := range companies {
		if expected, ok := stateChecks[co.URL]; ok {
			if co.Location != expected {
				t.Errorf("company %s: expected location %q, got %q", co.URL, expected, co.Location)
			}
		}
	}

	// Verify all companies have URLs starting with https://
	for _, co := range companies {
		if co.URL[:8] != "https://" {
			t.Errorf("company %s: URL doesn't start with https://", co.Name)
		}
	}

	// Verify no SalesforceID or NotionPageID set.
	for _, co := range companies {
		if co.SalesforceID != "" {
			t.Errorf("company %s: unexpected SalesforceID %q", co.Name, co.SalesforceID)
		}
		if co.NotionPageID != "" {
			t.Errorf("company %s: unexpected NotionPageID %q", co.Name, co.NotionPageID)
		}
	}
}

func TestParseGrataCSV_Synthetic(t *testing.T) {
	content := `Domain,Name,City,State
example.com,Example Inc,NEW YORK,NEW YORK
test.io,Test Corp,SAN FRANCISCO,CALIFORNIA
,Empty Domain,,
example.com,Duplicate Example,,
`
	path := filepath.Join(t.TempDir(), "test.csv")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	companies, err := ParseGrataCSV(path)
	if err != nil {
		t.Fatalf("ParseGrataCSV() error: %v", err)
	}

	// Should have 2 companies (empty domain skipped, duplicate skipped).
	if len(companies) != 2 {
		t.Fatalf("expected 2 companies, got %d", len(companies))
	}

	if companies[0].URL != "https://example.com" {
		t.Errorf("expected https://example.com, got %s", companies[0].URL)
	}
	if companies[0].Location != "New York, NY" {
		t.Errorf("expected 'New York, NY', got %q", companies[0].Location)
	}

	if companies[1].URL != "https://test.io" {
		t.Errorf("expected https://test.io, got %s", companies[1].URL)
	}
	if companies[1].Location != "San Francisco, CA" {
		t.Errorf("expected 'San Francisco, CA', got %q", companies[1].Location)
	}
}

func TestParseGrataCSV_MissingDomainColumn(t *testing.T) {
	content := `Name,City,State
Example Inc,NEW YORK,NEW YORK
`
	path := filepath.Join(t.TempDir(), "nodomain.csv")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := ParseGrataCSV(path)
	if err == nil {
		t.Fatal("expected error for missing Domain column")
	}
}

func TestParseGrataCSV_NoDataRows(t *testing.T) {
	content := `Domain,Name,City,State
`
	path := filepath.Join(t.TempDir(), "empty.csv")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := ParseGrataCSV(path)
	if err == nil {
		t.Fatal("expected error for no data rows")
	}
}

func TestParseGrataCSV_NotFound(t *testing.T) {
	_, err := ParseGrataCSV("/nonexistent/file.csv")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestTitleCase(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"WEST JORDAN", "West Jordan"},
		{"NEW PORT RICHEY", "New Port Richey"},
		{"houston", "Houston"},
		{"SAN FRANCISCO", "San Francisco"},
		{"", ""},
		{"  BATON ROUGE  ", "Baton Rouge"},
	}
	for _, tc := range tests {
		got := titleCase(tc.input)
		if got != tc.want {
			t.Errorf("titleCase(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestStateAbbreviation(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"UTAH", "UT"},
		{"FLORIDA", "FL"},
		{"CALIFORNIA", "CA"},
		{"TEXAS", "TX"},
		{"PENNSYLVANIA", "PA"},
		{"ILLINOIS", "IL"},
		{"LOUISIANA", "LA"},
		{"TX", "TX"},
		{"ca", "CA"},
		{"", ""},
		{"NEW YORK", "NY"},
	}
	for _, tc := range tests {
		got := stateAbbreviation(tc.input)
		if got != tc.want {
			t.Errorf("stateAbbreviation(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestParseGrataCSVFull_AllColumns(t *testing.T) {
	content := `Domain,Name,Description,Revenue Estimate,Employee Estimate,Employees - LinkedIn,Total Review Count,Aggregate Rating,NAICS 6,Business Model,Primary Email,Primary Phone,Executive First Name,Executive Last Name,Executive Title,Executive Linkedin,Key People,Year Founded,Ownership,City,State,Zip Code,Mailing Address
example.com,Example Inc,A great company,"$5,000,000",50,55,120,4.7,541512,Services,info@example.com,555-1234,John,Doe,CEO,https://linkedin.com/in/jdoe,"John Doe (CEO), Jane Doe (COO)",2010,Bootstrapped,NEW YORK,NEW YORK,10001,123 Main St
test.io,Test Corp,Testing stuff,,25,,,3.2,,,test@test.io,,,,,,,,SAN FRANCISCO,CALIFORNIA,,
`
	path := filepath.Join(t.TempDir(), "full.csv")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	companies, err := ParseGrataCSVFull(path)
	if err != nil {
		t.Fatalf("ParseGrataCSVFull() error: %v", err)
	}

	if len(companies) != 2 {
		t.Fatalf("expected 2 companies, got %d", len(companies))
	}

	// Verify first company has all fields.
	c := companies[0]
	if c.Name != "Example Inc" {
		t.Errorf("Name = %q, want %q", c.Name, "Example Inc")
	}
	if c.Description != "A great company" {
		t.Errorf("Description = %q, want %q", c.Description, "A great company")
	}
	if c.RevenueEstimate != "$5,000,000" {
		t.Errorf("RevenueEstimate = %q, want %q", c.RevenueEstimate, "$5,000,000")
	}
	if c.EmployeeEstimate != 50 {
		t.Errorf("EmployeeEstimate = %d, want %d", c.EmployeeEstimate, 50)
	}
	if c.EmployeesLinkedIn != 55 {
		t.Errorf("EmployeesLinkedIn = %d, want %d", c.EmployeesLinkedIn, 55)
	}
	if c.ReviewCount != 120 {
		t.Errorf("ReviewCount = %d, want %d", c.ReviewCount, 120)
	}
	if c.Rating != 4.7 {
		t.Errorf("Rating = %f, want %f", c.Rating, 4.7)
	}
	if c.NAICS6 != "541512" {
		t.Errorf("NAICS6 = %q, want %q", c.NAICS6, "541512")
	}
	if c.BusinessModel != "Services" {
		t.Errorf("BusinessModel = %q, want %q", c.BusinessModel, "Services")
	}
	if c.PrimaryEmail != "info@example.com" {
		t.Errorf("PrimaryEmail = %q, want %q", c.PrimaryEmail, "info@example.com")
	}
	if c.PrimaryPhone != "555-1234" {
		t.Errorf("PrimaryPhone = %q, want %q", c.PrimaryPhone, "555-1234")
	}
	if c.ExecFirstName != "John" {
		t.Errorf("ExecFirstName = %q, want %q", c.ExecFirstName, "John")
	}
	if c.ExecLastName != "Doe" {
		t.Errorf("ExecLastName = %q, want %q", c.ExecLastName, "Doe")
	}
	if c.ExecTitle != "CEO" {
		t.Errorf("ExecTitle = %q, want %q", c.ExecTitle, "CEO")
	}
	if c.ExecLinkedIn != "https://linkedin.com/in/jdoe" {
		t.Errorf("ExecLinkedIn = %q, want %q", c.ExecLinkedIn, "https://linkedin.com/in/jdoe")
	}
	if c.KeyPeople != "John Doe (CEO), Jane Doe (COO)" {
		t.Errorf("KeyPeople = %q, want %q", c.KeyPeople, "John Doe (CEO), Jane Doe (COO)")
	}
	if c.YearFounded != "2010" {
		t.Errorf("YearFounded = %q, want %q", c.YearFounded, "2010")
	}
	if c.Ownership != "Bootstrapped" {
		t.Errorf("Ownership = %q, want %q", c.Ownership, "Bootstrapped")
	}
	if c.City != "New York" {
		t.Errorf("City = %q, want %q", c.City, "New York")
	}
	if c.State != "NY" {
		t.Errorf("State = %q, want %q", c.State, "NY")
	}
	if c.ZipCode != "10001" {
		t.Errorf("ZipCode = %q, want %q", c.ZipCode, "10001")
	}
	if c.Street != "123 Main St" {
		t.Errorf("Street = %q, want %q", c.Street, "123 Main St")
	}

	// Verify second company has partial data.
	c2 := companies[1]
	if c2.EmployeeEstimate != 25 {
		t.Errorf("c2 EmployeeEstimate = %d, want %d", c2.EmployeeEstimate, 25)
	}
	if c2.ReviewCount != 0 {
		t.Errorf("c2 ReviewCount = %d, want %d", c2.ReviewCount, 0)
	}
	if c2.PrimaryEmail != "test@test.io" {
		t.Errorf("c2 PrimaryEmail = %q, want %q", c2.PrimaryEmail, "test@test.io")
	}
}

func TestParseGrataCSVFull_Deduplicates(t *testing.T) {
	content := `Domain,Name,Description
example.com,First,Desc1
example.com,Duplicate,Desc2
other.com,Other,Desc3
`
	path := filepath.Join(t.TempDir(), "dedup.csv")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	companies, err := ParseGrataCSVFull(path)
	if err != nil {
		t.Fatalf("ParseGrataCSVFull() error: %v", err)
	}

	if len(companies) != 2 {
		t.Fatalf("expected 2 companies (dedup), got %d", len(companies))
	}
	if companies[0].Name != "First" {
		t.Errorf("first company Name = %q, want %q", companies[0].Name, "First")
	}
}

func TestParseGrataCSVFull_MissingDomainColumn(t *testing.T) {
	content := `Name,Description
Example,Test
`
	path := filepath.Join(t.TempDir(), "nodomain.csv")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := ParseGrataCSVFull(path)
	if err == nil {
		t.Fatal("expected error for missing Domain column")
	}
}

func TestCompareResults_MatchAndMismatch(t *testing.T) {
	grataCompanies := []GrataCompany{
		{
			Company: model.Company{
				URL:   "https://example.com",
				Name:  "Example Inc",
				City:  "New York",
				State: "NY",
			},
			Description:      "A great company",
			EmployeeEstimate: 50,
			ReviewCount:      100,
			NAICS6:           "541512",
			PrimaryEmail:     "info@example.com",
			YearFounded:      "2010",
		},
	}

	results := []*model.EnrichmentResult{
		{
			Company: model.Company{
				URL:   "https://example.com",
				Name:  "Example Inc",
				City:  "New York",
				State: "NY",
			},
			FieldValues: map[string]model.FieldValue{
				"description":    {FieldKey: "description", Value: "A great company"},
				"employee_count": {FieldKey: "employee_count", Value: 50},
				"review_count":   {FieldKey: "review_count", Value: 200}, // Mismatch
				"naics_code":     {FieldKey: "naics_code", Value: "541512"},
				"email":          {FieldKey: "email", Value: "info@example.com"},
				"year_founded":   {FieldKey: "year_founded", Value: 2010},
			},
		},
	}

	comparisons := CompareResults(grataCompanies, results)

	if len(comparisons) != 1 {
		t.Fatalf("expected 1 comparison, got %d", len(comparisons))
	}

	comp := comparisons[0]
	if comp.Domain != "example.com" {
		t.Errorf("Domain = %q, want %q", comp.Domain, "example.com")
	}

	// Build lookup by field name.
	byField := make(map[string]FieldComparison, len(comp.Comparisons))
	for _, fc := range comp.Comparisons {
		byField[fc.Field] = fc
	}

	// Description should match.
	if fc, ok := byField["description"]; ok {
		if !fc.Match {
			t.Errorf("description should match: grata=%q, ours=%q", fc.GrataValue, fc.OurValue)
		}
	} else {
		t.Error("description comparison not found")
	}

	// Review count should not match.
	if fc, ok := byField["review_count"]; ok {
		if fc.Match {
			t.Errorf("review_count should not match: grata=%q, ours=%q", fc.GrataValue, fc.OurValue)
		}
	} else {
		t.Error("review_count comparison not found")
	}

	// NAICS should match.
	if fc, ok := byField["naics_code"]; ok {
		if !fc.Match {
			t.Errorf("naics_code should match: grata=%q, ours=%q", fc.GrataValue, fc.OurValue)
		}
	} else {
		t.Error("naics_code comparison not found")
	}

	// Match rate should be > 0 but < 1.
	if comp.MatchRate <= 0 || comp.MatchRate >= 1 {
		t.Errorf("MatchRate = %f, expected between 0 and 1 exclusive", comp.MatchRate)
	}
}

func TestCompareResults_NoMatchingDomain(t *testing.T) {
	grataCompanies := []GrataCompany{
		{
			Company: model.Company{URL: "https://nomatch.com"},
		},
	}

	results := []*model.EnrichmentResult{
		{
			Company:     model.Company{URL: "https://other.com"},
			FieldValues: map[string]model.FieldValue{},
		},
	}

	comparisons := CompareResults(grataCompanies, results)
	if len(comparisons) != 0 {
		t.Errorf("expected 0 comparisons for non-matching domains, got %d", len(comparisons))
	}
}

func TestCompareResults_EmptyGrataValues(t *testing.T) {
	grataCompanies := []GrataCompany{
		{
			Company: model.Company{URL: "https://sparse.com", City: "Boston", State: "MA"},
			// All other fields are zero values (empty).
		},
	}

	results := []*model.EnrichmentResult{
		{
			Company: model.Company{URL: "https://sparse.com", City: "Boston", State: "MA"},
			FieldValues: map[string]model.FieldValue{
				"description": {FieldKey: "description", Value: "Something"},
			},
		},
	}

	comparisons := CompareResults(grataCompanies, results)
	if len(comparisons) != 1 {
		t.Fatalf("expected 1 comparison, got %d", len(comparisons))
	}

	comp := comparisons[0]
	// Should have comparisons for description (grata empty, ours not) and city/state (both set).
	if len(comp.Comparisons) == 0 {
		t.Error("expected some comparisons")
	}

	// City and state should match.
	byField := make(map[string]FieldComparison, len(comp.Comparisons))
	for _, fc := range comp.Comparisons {
		byField[fc.Field] = fc
	}
	if fc, ok := byField["city"]; ok {
		if !fc.Match {
			t.Errorf("city should match: grata=%q, ours=%q", fc.GrataValue, fc.OurValue)
		}
	}
	if fc, ok := byField["state"]; ok {
		if !fc.Match {
			t.Errorf("state should match: grata=%q, ours=%q", fc.GrataValue, fc.OurValue)
		}
	}
}
