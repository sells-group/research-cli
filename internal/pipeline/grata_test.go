package pipeline

import (
	"os"
	"path/filepath"
	"testing"
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
