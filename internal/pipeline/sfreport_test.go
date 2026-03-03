package pipeline

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDetectCSVFormat_SFReport(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "report.csv")
	content := "Account Name,Account ID,Website,Ownership,Year Founded\nAcme Corp,001xx,acme.com,Private,2010\n"
	if err := os.WriteFile(csvPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	format, err := DetectCSVFormat(csvPath)
	if err != nil {
		t.Fatal(err)
	}
	if format != CSVFormatSFReport {
		t.Errorf("got format %q, want %q", format, CSVFormatSFReport)
	}
}

func TestDetectCSVFormat_Grata(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "grata.csv")
	content := "Domain,Name,City,State\nacme.com,Acme,NYC,NY\n"
	if err := os.WriteFile(csvPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	format, err := DetectCSVFormat(csvPath)
	if err != nil {
		t.Fatal(err)
	}
	if format != CSVFormatGrata {
		t.Errorf("got format %q, want %q", format, CSVFormatGrata)
	}
}

func TestDetectCSVFormat_Unknown(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "unknown.csv")
	content := "Foo,Bar,Baz\n1,2,3\n"
	if err := os.WriteFile(csvPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	format, err := DetectCSVFormat(csvPath)
	if err != nil {
		t.Fatal(err)
	}
	if format != CSVFormatUnknown {
		t.Errorf("got format %q, want %q", format, CSVFormatUnknown)
	}
}

func TestParseSFReportCSV(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "report.csv")
	content := `Account Name,Account ID,Website,Ownership,Year Founded
Acme Medical,001ABC,acme-medical.com,Private,2015
Beta Health,002DEF,betahealth.com,Public,2008
,003GHI,,Private,
`
	if err := os.WriteFile(csvPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	companies, err := ParseSFReportCSV(csvPath)
	if err != nil {
		t.Fatal(err)
	}

	// Third row has no Account ID after trim, but has website empty — should be skipped.
	if len(companies) != 2 {
		t.Fatalf("got %d companies, want 2", len(companies))
	}

	// First company.
	c := companies[0]
	if c.Name != "Acme Medical" {
		t.Errorf("company[0].Name = %q, want %q", c.Name, "Acme Medical")
	}
	if c.AccountID != "001ABC" {
		t.Errorf("company[0].AccountID = %q, want %q", c.AccountID, "001ABC")
	}
	if c.URL != "https://acme-medical.com" {
		t.Errorf("company[0].URL = %q, want %q", c.URL, "https://acme-medical.com")
	}
	if c.SalesforceID != "001ABC" {
		t.Errorf("company[0].SalesforceID = %q, want %q", c.SalesforceID, "001ABC")
	}
	if c.Ownership != "Private" {
		t.Errorf("company[0].Ownership = %q, want %q", c.Ownership, "Private")
	}
}

func TestParseSFReportCSV_Dedup(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "report.csv")
	content := `Account Name,Account ID,Website,Ownership
Acme One,001A,acme.com,Private
Acme Two,002B,ACME.COM,Public
`
	if err := os.WriteFile(csvPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	companies, err := ParseSFReportCSV(csvPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(companies) != 1 {
		t.Fatalf("got %d companies, want 1 (dedup by URL)", len(companies))
	}
}

func TestCompaniesFromSFReport(t *testing.T) {
	reports := []SFReportCompany{
		{Ownership: "Private", AccountID: "001"},
		{Ownership: "Public", AccountID: "002"},
	}
	reports[0].Name = "Acme"
	reports[0].URL = "https://acme.com"
	reports[1].Name = "Beta"
	reports[1].URL = "https://beta.com"

	companies := CompaniesFromSFReport(reports)
	if len(companies) != 2 {
		t.Fatalf("got %d companies, want 2", len(companies))
	}
	if companies[0].Name != "Acme" {
		t.Errorf("companies[0].Name = %q, want %q", companies[0].Name, "Acme")
	}
}

func TestNormalizeWebsite(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"acme.com", "https://acme.com"},
		{"https://acme.com", "https://acme.com"},
		{"http://acme.com", "http://acme.com"},
		{"  acme.com  ", "https://acme.com"},
		{"", ""},
	}

	for _, tt := range tests {
		got := normalizeWebsite(tt.input)
		if got != tt.want {
			t.Errorf("normalizeWebsite(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestDetectCSVFormat_FileNotFound(t *testing.T) {
	_, err := DetectCSVFormat("/nonexistent/path.csv")
	assert.Error(t, err)
}

func TestDetectCSVFormat_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.csv")
	assert.NoError(t, os.WriteFile(path, []byte(""), 0o644))

	_, err := DetectCSVFormat(path)
	assert.Error(t, err) // Can't read header
}

func TestParseSFReportCSV_FileNotFound(t *testing.T) {
	_, err := ParseSFReportCSV("/nonexistent/path.csv")
	assert.Error(t, err)
}

func TestParseSFReportCSV_NoDataRows(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "header_only.csv")
	assert.NoError(t, os.WriteFile(path, []byte("Account Name,Account ID,Website\n"), 0o644))

	_, err := ParseSFReportCSV(path)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no data rows")
}

func TestParseSFReportCSV_MissingRequiredColumn(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "missing_col.csv")
	// Missing "Website" column.
	content := "Account Name,Account ID\nAcme,001ABC\n"
	assert.NoError(t, os.WriteFile(path, []byte(content), 0o644))

	_, err := ParseSFReportCSV(path)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing required column")
}

func TestParseSFReportCSV_SkipsEmptyWebsite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty_website.csv")
	content := "Account Name,Account ID,Website\nAcme,001ABC,acme.com\nEmpty,002DEF,\n"
	assert.NoError(t, os.WriteFile(path, []byte(content), 0o644))

	companies, err := ParseSFReportCSV(path)
	assert.NoError(t, err)
	assert.Len(t, companies, 1) // Only Acme included.
	assert.Equal(t, "001ABC", companies[0].AccountID)
}

func TestParseSFReportCSV_SkipsEmptyAccountID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty_id.csv")
	content := "Account Name,Account ID,Website\nAcme,,acme.com\nReal,002DEF,real.com\n"
	assert.NoError(t, os.WriteFile(path, []byte(content), 0o644))

	companies, err := ParseSFReportCSV(path)
	assert.NoError(t, err)
	assert.Len(t, companies, 1)
	assert.Equal(t, "002DEF", companies[0].AccountID)
}

func TestParseSFReportCSV_WithOwnership(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "with_ownership.csv")
	content := "Account Name,Account ID,Website,Ownership\nAcme,001ABC,acme.com,Private\n"
	assert.NoError(t, os.WriteFile(path, []byte(content), 0o644))

	companies, err := ParseSFReportCSV(path)
	assert.NoError(t, err)
	assert.Len(t, companies, 1)
	assert.Equal(t, "Private", companies[0].Ownership)
}

func TestParseSFReportCSV_NoValidCompanies(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "no_valid.csv")
	// All rows have empty account ID or website.
	content := "Account Name,Account ID,Website\nAcme,,\n"
	assert.NoError(t, os.WriteFile(path, []byte(content), 0o644))

	_, err := ParseSFReportCSV(path)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no valid companies")
}
