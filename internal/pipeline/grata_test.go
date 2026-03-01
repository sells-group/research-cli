package pipeline

import (
	"math"
	"os"
	"path/filepath"
	"strings"
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
				"description":          {FieldKey: "description", Value: "A great company"},
				"employees":            {FieldKey: "employees", Value: 50},
				"google_reviews_count": {FieldKey: "google_reviews_count", Value: 200}, // Mismatch
				"naics_code":           {FieldKey: "naics_code", Value: "541512"},
				"email":                {FieldKey: "email", Value: "info@example.com"},
				"year_established":     {FieldKey: "year_established", Value: 2010},
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

	// Description should match (exact text → high overlap).
	if fc, ok := byField["description"]; ok {
		if !fc.Match {
			t.Errorf("description should match: grata=%q, ours=%q", fc.GrataValue, fc.OurValue)
		}
		if fc.Proximity < 0.5 {
			t.Errorf("description proximity = %f, want >= 0.5", fc.Proximity)
		}
	} else {
		t.Error("description comparison not found")
	}

	// Review count should not match (100 vs 200 → proximity 0.5).
	if fc, ok := byField["review_count"]; ok {
		if fc.Match {
			t.Errorf("review_count should not match: grata=%q, ours=%q", fc.GrataValue, fc.OurValue)
		}
		if fc.MatchType != "wrong" {
			t.Errorf("review_count MatchType = %q, want %q", fc.MatchType, "wrong")
		}
		if fc.Proximity < 0.4 || fc.Proximity > 0.6 {
			t.Errorf("review_count proximity = %f, want ~0.5", fc.Proximity)
		}
	} else {
		t.Error("review_count comparison not found")
	}

	// NAICS should match (exact string).
	if fc, ok := byField["naics_code"]; ok {
		if !fc.Match {
			t.Errorf("naics_code should match: grata=%q, ours=%q", fc.GrataValue, fc.OurValue)
		}
		if fc.MatchType != "exact" {
			t.Errorf("naics_code MatchType = %q, want %q", fc.MatchType, "exact")
		}
	} else {
		t.Error("naics_code comparison not found")
	}

	// CompanyName should be populated.
	if comp.CompanyName != "Example Inc" {
		t.Errorf("CompanyName = %q, want %q", comp.CompanyName, "Example Inc")
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

func TestNormalizePhone(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"(801) 874-7020", "8018747020"},
		{"18018747020", "18018747020"},
		{"+1-801-874-7020", "18018747020"},
		{"555.123.4567", "5551234567"},
		{"", ""},
		{"  (555)  123-4567  ", "5551234567"},
	}
	for _, tc := range tests {
		got := normalizePhone(tc.input)
		if got != tc.want {
			t.Errorf("normalizePhone(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestParseNumeric(t *testing.T) {
	tests := []struct {
		input  string
		want   float64
		wantOK bool
	}{
		{"$20,000,000", 20_000_000, true},
		{"$20M", 20_000_000, true},
		{"$20m", 20_000_000, true},
		{"$5B", 5_000_000_000, true},
		{"$500K", 500_000, true},
		{"82", 82, true},
		{"1,500", 1500, true},
		{"", 0, false},
		{"abc", 0, false},
		{"$", 0, false},
	}
	for _, tc := range tests {
		got, ok := parseNumeric(tc.input)
		if ok != tc.wantOK {
			t.Errorf("parseNumeric(%q) ok = %v, want %v", tc.input, ok, tc.wantOK)
			continue
		}
		if ok && math.Abs(got-tc.want) > 0.01 {
			t.Errorf("parseNumeric(%q) = %f, want %f", tc.input, got, tc.want)
		}
	}
}

func TestNumericProximity(t *testing.T) {
	tests := []struct {
		a, b    string
		wantMin float64
		wantMax float64
	}{
		{"82", "80", 0.95, 1.0},             // very close
		{"82", "18", 0.20, 0.25},            // very different
		{"$20M", "$20,000,000", 0.99, 1.01}, // same value, different format
		{"100", "100", 1.0, 1.01},           // identical
		{"0", "0", 1.0, 1.01},               // both zero
	}
	for _, tc := range tests {
		got := numericProximity(tc.a, tc.b)
		if got < tc.wantMin || got > tc.wantMax {
			t.Errorf("numericProximity(%q, %q) = %f, want [%f, %f]", tc.a, tc.b, got, tc.wantMin, tc.wantMax)
		}
	}
}

func TestStringOverlap(t *testing.T) {
	tests := []struct {
		a, b    string
		wantMin float64
	}{
		{"the quick brown fox", "the quick brown fox", 1.0},
		{"hello world", "hello earth", 0.3}, // 1 shared out of 3 unique
		{"completely different", "nothing alike", 0.0},
		{"", "", 1.0},
		{"something", "", 0.0},
	}
	for _, tc := range tests {
		got := stringOverlap(tc.a, tc.b)
		if got < tc.wantMin-0.01 {
			t.Errorf("stringOverlap(%q, %q) = %f, want >= %f", tc.a, tc.b, got, tc.wantMin)
		}
	}
}

func TestCompareResults_PhoneNormalization(t *testing.T) {
	grataCompanies := []GrataCompany{
		{
			Company:      model.Company{URL: "https://example.com", Name: "Test"},
			PrimaryPhone: "18018747020",
		},
	}

	results := []*model.EnrichmentResult{
		{
			Company: model.Company{URL: "https://example.com"},
			FieldValues: map[string]model.FieldValue{
				"phone": {FieldKey: "phone", Value: "(801) 874-7020", Confidence: 0.75},
			},
		},
	}

	comparisons := CompareResults(grataCompanies, results)
	if len(comparisons) != 1 {
		t.Fatalf("expected 1 comparison, got %d", len(comparisons))
	}

	byField := make(map[string]FieldComparison)
	for _, fc := range comparisons[0].Comparisons {
		byField[fc.Field] = fc
	}

	fc, ok := byField["phone"]
	if !ok {
		t.Fatal("phone comparison not found")
	}
	if !fc.Match {
		t.Errorf("phone should match after normalization: grata=%q, ours=%q", fc.GrataValue, fc.OurValue)
	}
	if fc.MatchType != "format" {
		t.Errorf("phone MatchType = %q, want %q", fc.MatchType, "format")
	}
	if fc.Confidence != 0.75 {
		t.Errorf("phone Confidence = %f, want 0.75", fc.Confidence)
	}
}

func TestCompareResults_NumericProximity(t *testing.T) {
	grataCompanies := []GrataCompany{
		{
			Company:          model.Company{URL: "https://example.com", Name: "Test"},
			EmployeeEstimate: 82,
		},
	}

	results := []*model.EnrichmentResult{
		{
			Company: model.Company{URL: "https://example.com"},
			FieldValues: map[string]model.FieldValue{
				"employees": {FieldKey: "employees", Value: 80, Confidence: 0.90},
			},
		},
	}

	comparisons := CompareResults(grataCompanies, results)
	if len(comparisons) != 1 {
		t.Fatalf("expected 1 comparison, got %d", len(comparisons))
	}

	byField := make(map[string]FieldComparison)
	for _, fc := range comparisons[0].Comparisons {
		byField[fc.Field] = fc
	}

	fc, ok := byField["employee_count"]
	if !ok {
		t.Fatal("employee_count comparison not found")
	}
	if !fc.Match {
		t.Errorf("employee_count should match (82 vs 80 is within threshold): grata=%q, ours=%q", fc.GrataValue, fc.OurValue)
	}
	if fc.MatchType != "close" {
		t.Errorf("employee_count MatchType = %q, want %q", fc.MatchType, "close")
	}
	if fc.Proximity < 0.95 {
		t.Errorf("employee_count Proximity = %f, want >= 0.95", fc.Proximity)
	}
}

func TestFormatComparisonReport_Output(t *testing.T) {
	comparisons := []CompanyComparison{
		{
			Domain:      "example.com",
			CompanyName: "Example Inc",
			MatchRate:   0.6,
			Comparisons: []FieldComparison{
				{Field: "description", GrataValue: "A great company", OurValue: "A great company", Match: true, Proximity: 1.0, MatchType: "exact", Confidence: 0.82},
				{Field: "revenue_estimate", GrataValue: "$20,000,000", OurValue: "", Match: false, Proximity: 0, MatchType: "gap"},
				{Field: "employee_count", GrataValue: "82", OurValue: "18", Match: false, Proximity: 0.22, MatchType: "wrong", Confidence: 0.95},
				{Field: "phone", GrataValue: "18018747020", OurValue: "(801) 874-7020", Match: true, Proximity: 1.0, MatchType: "format", Confidence: 0.75},
				{Field: "exec_first_name", GrataValue: "Kyle", OurValue: "Kyle", Match: true, Proximity: 1.0, MatchType: "exact", Confidence: 0.75},
			},
		},
	}

	report := FormatComparisonReport(comparisons)

	// Check report contains expected sections.
	if !strings.Contains(report, "=== ENRICHMENT vs GRATA COMPARISON ===") {
		t.Error("report missing header")
	}
	if !strings.Contains(report, "--- Example Inc (example.com) ---") {
		t.Error("report missing company section")
	}
	if !strings.Contains(report, "--- FIELD ACCURACY (all companies) ---") {
		t.Error("report missing aggregate section")
	}
	if !strings.Contains(report, "--- SUMMARY ---") {
		t.Error("report missing summary section")
	}
	if !strings.Contains(report, "GAP") {
		t.Error("report missing GAP indicator")
	}
	if !strings.Contains(report, "OK (fmt)") {
		t.Error("report missing format match indicator")
	}
	if !strings.Contains(report, "WRONG") {
		t.Error("report missing WRONG indicator")
	}
	if !strings.Contains(report, "revenue_estimate") {
		t.Error("report missing revenue_estimate in gaps")
	}
}

func TestLevenshtein(t *testing.T) {
	t.Parallel()
	tests := []struct {
		a, b string
		want int
	}{
		{"", "", 0},
		{"abc", "", 3},
		{"", "abc", 3},
		{"abc", "abc", 0},
		{"gonzalez", "gonzales", 1},
		{"kitten", "sitting", 3},
		{"saturday", "sunday", 3},
	}
	for _, tc := range tests {
		t.Run(tc.a+"_"+tc.b, func(t *testing.T) {
			t.Parallel()
			got := levenshtein(tc.a, tc.b)
			if got != tc.want {
				t.Errorf("levenshtein(%q, %q) = %d, want %d", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestNormalizeTitle(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{"president, ceo", "president ceo"},
		{"president & ceo", "president ceo"},
		{"ceo/founder", "ceo founder"},
		{"ceo and founder", "ceo founder"},
		{"chief executive officer", "chief executive officer"},
		{"  president ,  ceo  ", "president ceo"},
		{"", ""},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			got := normalizeTitle(tc.input)
			if got != tc.want {
				t.Errorf("normalizeTitle(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestCompareField_ExecTitle_PartialMatch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		grata     string
		ours      string
		wantMatch bool
		wantType  string
	}{
		{"exact", "CEO", "CEO", true, "exact"},
		{"case insensitive", "ceo", "CEO", true, "exact"},
		{"containment", "CEO", "President, CEO", true, "close"},
		{"containment reverse", "CEO & Founder", "CEO", true, "close"},
		{"word overlap", "CEO", "Chief Executive Officer, CEO", true, "close"},
		{"separator normalization comma vs ampersand", "President, CEO", "President & CEO", true, "close"},
		{"separator normalization slash", "CEO/Founder", "CEO & Founder", true, "close"},
		{"separator normalization and", "CEO and Founder", "CEO & Founder", true, "close"},
		{"no match", "CEO", "CFO", false, "wrong"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			match, _, matchType := compareField("exec_title", tc.grata, tc.ours, 0)
			if match != tc.wantMatch {
				t.Errorf("compareField exec_title(%q, %q) match = %v, want %v", tc.grata, tc.ours, match, tc.wantMatch)
			}
			if matchType != tc.wantType {
				t.Errorf("compareField exec_title(%q, %q) matchType = %q, want %q", tc.grata, tc.ours, matchType, tc.wantType)
			}
		})
	}
}

func TestCompareField_ExecName_Levenshtein(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		field     string
		grata     string
		ours      string
		wantMatch bool
		wantType  string
	}{
		{"exact match", "exec_last_name", "Gonzalez", "Gonzalez", true, "exact"},
		{"1 char diff", "exec_last_name", "Gonzalez", "Gonzales", true, "close"},
		{"case insensitive", "exec_first_name", "john", "John", true, "exact"},
		{"2 char diff", "exec_last_name", "Smith", "Smythe", false, "wrong"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			match, _, matchType := compareField(tc.field, tc.grata, tc.ours, 0)
			if match != tc.wantMatch {
				t.Errorf("compareField %s(%q, %q) match = %v, want %v", tc.field, tc.grata, tc.ours, match, tc.wantMatch)
			}
			if matchType != tc.wantType {
				t.Errorf("compareField %s(%q, %q) matchType = %q, want %q", tc.field, tc.grata, tc.ours, matchType, tc.wantType)
			}
		})
	}
}

func TestCompareField_Description_LoweredThreshold(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		grata     string
		ours      string
		wantMatch bool
	}{
		{
			"high overlap",
			"Acme Corp is a technology company providing innovative solutions",
			"Acme Corp is a technology company providing innovative solutions for enterprise",
			true,
		},
		{
			"company name containment bonus",
			"Acme Corp provides technology services",
			"Acme Corp delivers tech solutions and services",
			true,
		},
		{
			"completely different",
			"A plumbing company in Texas",
			"Software development firm based in San Francisco",
			false,
		},
		{
			"shared industry keywords — solar",
			"Lonestar Solar provides residential solar panel installation and energy services across Texas",
			"A company that specializes in solar energy solutions and renewable power installation for homeowners",
			true,
		},
		{
			"shared industry keywords — construction",
			"V3 Construction handles commercial building and general construction projects in Utah",
			"A construction company offering general contracting and building services for commercial clients",
			true,
		},
		{
			"too few shared keywords",
			"A large company that provides staffing solutions and consulting",
			"A small firm offering plumbing repair services in rural areas",
			false,
		},
		{
			"bigram overlap catches phrase similarity",
			"Screen Builders provides custom pool enclosures and screen rooms in South Florida",
			"Screen Builders Inc is a company that builds screen rooms and pool enclosures for residential customers",
			true,
		},
		{
			"editorial vs detailed descriptions match via shared keywords",
			"Davis Roofing is a commercial roofing contractor serving the greater Chicago area",
			"A full-service roofing company specializing in commercial roofing installation, repair, and maintenance for businesses in Illinois",
			true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			match, _, _ := compareField("description", tc.grata, tc.ours, 0)
			if match != tc.wantMatch {
				t.Errorf("compareField description(%q, %q) match = %v, want %v", tc.grata, tc.ours, match, tc.wantMatch)
			}
		})
	}
}

func TestBigramOverlap(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		a, b    string
		wantMin float64
	}{
		{"identical phrases", "solar panel installation services", "solar panel installation services", 1.0},
		{"shared bigrams", "custom pool enclosures and screen rooms", "screen rooms and pool enclosures for residential", 0.1},
		{"no shared bigrams", "plumbing company texas", "software firm california", 0.0},
		{"single word each", "hello", "world", 0.0},
		{"empty inputs", "", "", 0.0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			aWords := strings.Fields(strings.ToLower(tc.a))
			bWords := strings.Fields(strings.ToLower(tc.b))
			got := bigramOverlap(aWords, bWords)
			if got < tc.wantMin-0.01 {
				t.Errorf("bigramOverlap(%q, %q) = %f, want >= %f", tc.a, tc.b, got, tc.wantMin)
			}
		})
	}
}

func TestCompareField_EmployeeCount_SpecificThreshold(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		grata     string
		ours      string
		wantMatch bool
	}{
		{"within 0.60 — V3 82 vs 125", "82", "125", true},       // proximity ~0.66
		{"within 0.60 — close values", "82", "68", true},        // proximity ~0.83
		{"below 0.60 — Hire A Pro 70 vs 5", "70", "5", false},   // proximity ~0.07
		{"below 0.60 — Lonestar 78 vs 200", "78", "200", false}, // proximity ~0.39
		{"exactly 0.60", "100", "60", true},                     // proximity = 0.60
		{"just below 0.60", "100", "40", false},                 // proximity = 0.40 (100→60% off)
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			match, _, _ := compareField("employee_count", tc.grata, tc.ours, 0)
			if match != tc.wantMatch {
				t.Errorf("compareField employee_count(%q, %q) match = %v, want %v", tc.grata, tc.ours, match, tc.wantMatch)
			}
		})
	}
}

func TestCompareField_NumericThreshold_ReviewCount(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		grata     string
		ours      string
		wantMatch bool
	}{
		{"within 0.60", "100", "80", true},  // proximity = 0.80
		{"exactly 0.60", "100", "60", true}, // proximity = 0.60
		{"below 0.60", "100", "30", false},  // proximity = 0.30
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			match, _, _ := compareField("review_count", tc.grata, tc.ours, 0)
			if match != tc.wantMatch {
				t.Errorf("compareField review_count(%q, %q) match = %v, want %v", tc.grata, tc.ours, match, tc.wantMatch)
			}
		})
	}
}

func TestCompareField_NAICSCode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		grata     string
		ours      string
		wantMatch bool
		wantType  string
	}{
		{"exact match", "236115", "236115", true, "exact"},
		{"grata has description", "236115 New Single-Family Housing Construction", "236115", true, "close"},
		{"both have descriptions", "236115 Housing", "236115 New Housing", true, "close"},
		{"different codes", "221114 Solar Electric", "238220", false, "wrong"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			match, _, matchType := compareField("naics_code", tc.grata, tc.ours, 0)
			if match != tc.wantMatch {
				t.Errorf("compareField naics_code(%q, %q) match = %v, want %v", tc.grata, tc.ours, match, tc.wantMatch)
			}
			if matchType != tc.wantType {
				t.Errorf("compareField naics_code(%q, %q) matchType = %q, want %q", tc.grata, tc.ours, matchType, tc.wantType)
			}
		})
	}
}

func TestCompareField_BusinessModel(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		grata     string
		ours      string
		wantMatch bool
		wantType  string
	}{
		{"exact match", "Services", "Services", true, "exact"},
		{"canonical services", "Services", "B2B Service Provider", true, "canonical"},
		{"canonical manufacturer", "Manufacturer", "Manufacturing Company", true, "canonical"},
		{"canonical containment", "Services", "B2B Services", true, "canonical"},
		{"no match", "Manufacturer", "B2B Service Provider", false, "wrong"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			match, _, matchType := compareField("business_model", tc.grata, tc.ours, 0)
			if match != tc.wantMatch {
				t.Errorf("compareField business_model(%q, %q) match = %v, want %v", tc.grata, tc.ours, match, tc.wantMatch)
			}
			if matchType != tc.wantType {
				t.Errorf("compareField business_model(%q, %q) matchType = %q, want %q", tc.grata, tc.ours, matchType, tc.wantType)
			}
		})
	}
}

func TestCompareField_BusinessModel_Canonical(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		grata     string
		ours      string
		conf      float64
		wantMatch bool
		wantType  string
	}{
		{"canonical match ignores conf", "Manufacturer", "Manufacturing Company", 0.50, true, "canonical"},
		{"canonical match at high conf", "Manufacturer", "Manufacturing Company", 0.90, true, "canonical"},
		{"different categories reject at high conf", "Manufacturer", "B2B Service Provider", 0.95, false, "wrong"},
		{"different categories reject at low conf", "Manufacturer", "B2B Service Provider", 0.50, false, "wrong"},
		{"financial services canonical", "Financial Services", "Wealth Management Firm", 0, true, "canonical"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			match, _, matchType := compareField("business_model", tc.grata, tc.ours, tc.conf)
			if match != tc.wantMatch {
				t.Errorf("compareField business_model(%q, %q, conf=%.2f) match = %v, want %v", tc.grata, tc.ours, tc.conf, match, tc.wantMatch)
			}
			if matchType != tc.wantType {
				t.Errorf("compareField business_model(%q, %q, conf=%.2f) matchType = %q, want %q", tc.grata, tc.ours, tc.conf, matchType, tc.wantType)
			}
		})
	}
}

func TestCompareField_EmployeeCount_HighConf(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		grata     string
		ours      string
		conf      float64
		wantMatch bool
		wantType  string
	}{
		{"high conf accepts divergent count", "78", "200", 0.88, true, "high_conf"},
		{"high conf at threshold", "78", "200", 0.80, true, "high_conf"},
		{"low conf rejects divergent count", "78", "200", 0.79, false, "wrong"},
		{"zero conf rejects", "78", "200", 0, false, "wrong"},
		{"close values still match without high conf", "82", "80", 0.50, true, "close"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			match, _, matchType := compareField("employee_count", tc.grata, tc.ours, tc.conf)
			if match != tc.wantMatch {
				t.Errorf("compareField employee_count(%q, %q, conf=%.2f) match = %v, want %v", tc.grata, tc.ours, tc.conf, match, tc.wantMatch)
			}
			if matchType != tc.wantType {
				t.Errorf("compareField employee_count(%q, %q, conf=%.2f) matchType = %q, want %q", tc.grata, tc.ours, tc.conf, matchType, tc.wantType)
			}
		})
	}
}

func TestCompareField_ReviewCount_HighConf(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		grata     string
		ours      string
		conf      float64
		wantMatch bool
		wantType  string
	}{
		{"high conf accepts divergent count", "93", "170", 0.95, true, "high_conf"},
		{"high conf at threshold", "93", "170", 0.70, true, "high_conf"},
		{"low conf rejects divergent count", "93", "170", 0.69, false, "wrong"},
		{"close values match without high conf", "80", "82", 0.50, true, "close"},
		{"exact match", "127", "127", 0.60, true, "exact"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			match, _, matchType := compareField("review_count", tc.grata, tc.ours, tc.conf)
			if match != tc.wantMatch {
				t.Errorf("compareField review_count(%q, %q, conf=%.2f) match = %v, want %v", tc.grata, tc.ours, tc.conf, match, tc.wantMatch)
			}
			if matchType != tc.wantType {
				t.Errorf("compareField review_count(%q, %q, conf=%.2f) matchType = %q, want %q", tc.grata, tc.ours, tc.conf, matchType, tc.wantType)
			}
		})
	}
}

func TestCompareField_NAICS_Hierarchical(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		grata     string
		ours      string
		wantMatch bool
		wantType  string
	}{
		{"exact 6-digit match", "236115", "236115", true, "exact"},
		{"same 4-digit group", "236115", "236116", true, "close"},
		{"same 3-digit subsector", "236115", "236220", true, "close"},
		{"same 2-digit sector", "236115", "238170", true, "close"},
		{"different 2-digit sector", "221114", "238220", false, "wrong"},
		{"same 4-digit with description", "238220 Plumbing", "238290", true, "close"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			match, _, matchType := compareField("naics_code", tc.grata, tc.ours, 0)
			if match != tc.wantMatch {
				t.Errorf("compareField naics_code(%q, %q) match = %v, want %v", tc.grata, tc.ours, match, tc.wantMatch)
			}
			if matchType != tc.wantType {
				t.Errorf("compareField naics_code(%q, %q) matchType = %q, want %q", tc.grata, tc.ours, matchType, tc.wantType)
			}
		})
	}
}
