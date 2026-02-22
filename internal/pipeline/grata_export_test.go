package pipeline

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"

	"github.com/sells-group/research-cli/internal/model"
)

func TestExportGrataCSV_ColumnOrder(t *testing.T) {
	results := []*model.EnrichmentResult{
		{
			Company: model.Company{
				URL:     "https://www.example.com",
				Name:    "Example Inc",
				City:    "New York",
				State:   "NY",
				ZipCode: "10001",
				Street:  "123 Main St",
			},
			FieldValues: map[string]model.FieldValue{
				"description":     {FieldKey: "description", Value: "A test company"},
				"email":           {FieldKey: "email", Value: "info@example.com"},
				"phone":           {FieldKey: "phone", Value: "555-1234"},
				"year_founded":    {FieldKey: "year_founded", Value: 2010},
				"employees":       {FieldKey: "employees", Value: 50},
				"naics_code":      {FieldKey: "naics_code", Value: "541512"},
				"business_model":  {FieldKey: "business_model", Value: "Services"},
				"exec_first_name": {FieldKey: "exec_first_name", Value: "John"},
				"exec_last_name":  {FieldKey: "exec_last_name", Value: "Doe"},
				"exec_title":      {FieldKey: "exec_title", Value: "CEO"},
				"review_count":    {FieldKey: "review_count", Value: 42},
				"review_rating":   {FieldKey: "review_rating", Value: 4.5},
			},
		},
	}

	outPath := filepath.Join(t.TempDir(), "export.csv")
	if err := ExportGrataCSV(results, outPath); err != nil {
		t.Fatalf("ExportGrataCSV() error: %v", err)
	}

	f, err := os.Open(outPath)
	if err != nil {
		t.Fatalf("open output: %v", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 rows (header + 1 data), got %d", len(records))
	}

	// Verify header matches grataColumns.
	header := records[0]
	if len(header) != len(grataColumns) {
		t.Fatalf("header length %d != grataColumns length %d", len(header), len(grataColumns))
	}
	for i, col := range grataColumns {
		if header[i] != col {
			t.Errorf("header[%d] = %q, want %q", i, header[i], col)
		}
	}

	// Verify data row values.
	row := records[1]
	checks := map[string]string{
		"Domain":               "www.example.com",
		"Name":                 "Example Inc",
		"Description":          "A test company",
		"Primary Email":        "info@example.com",
		"Primary Phone":        "555-1234",
		"Year Founded":         "2010",
		"Employee Estimate":    "50",
		"NAICS 6":              "541512",
		"Business Model":       "Services",
		"Executive First Name": "John",
		"Executive Last Name":  "Doe",
		"Executive Title":      "CEO",
		"Total Review Count":   "42",
		"Aggregate Rating":     "4.5",
		"Ownership":            "Bootstrapped",
		"City":                 "New York",
		"State":                "NY",
		"Zip Code":             "10001",
		"Mailing Address":      "123 Main St",
	}

	colIdx := make(map[string]int, len(header))
	for i, col := range header {
		colIdx[col] = i
	}

	for col, want := range checks {
		idx, ok := colIdx[col]
		if !ok {
			t.Errorf("column %q not found in header", col)
			continue
		}
		if row[idx] != want {
			t.Errorf("column %q = %q, want %q", col, row[idx], want)
		}
	}
}

func TestExportGrataCSV_RevenueFormatting(t *testing.T) {
	tests := []struct {
		name        string
		fieldValues map[string]model.FieldValue
		want        string
	}{
		{
			name: "revenue_estimate numeric",
			fieldValues: map[string]model.FieldValue{
				"revenue_estimate": {FieldKey: "revenue_estimate", Value: float64(1234567)},
			},
			want: "$1,234,567",
		},
		{
			name: "revenue_estimate integer",
			fieldValues: map[string]model.FieldValue{
				"revenue_estimate": {FieldKey: "revenue_estimate", Value: 5000000},
			},
			want: "$5,000,000",
		},
		{
			name: "falls back to revenue_range string",
			fieldValues: map[string]model.FieldValue{
				"revenue_range": {FieldKey: "revenue_range", Value: "$1M-$5M"},
			},
			want: "$1M-$5M",
		},
		{
			name:        "empty when no revenue fields",
			fieldValues: map[string]model.FieldValue{},
			want:        "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := formatRevenue(tc.fieldValues)
			if got != tc.want {
				t.Errorf("formatRevenue() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestExportGrataCSV_MissingFields(t *testing.T) {
	results := []*model.EnrichmentResult{
		{
			Company: model.Company{
				URL:  "https://sparse.com",
				Name: "Sparse Co",
			},
			FieldValues: map[string]model.FieldValue{},
		},
	}

	outPath := filepath.Join(t.TempDir(), "sparse.csv")
	if err := ExportGrataCSV(results, outPath); err != nil {
		t.Fatalf("ExportGrataCSV() error: %v", err)
	}

	f, err := os.Open(outPath)
	if err != nil {
		t.Fatalf("open output: %v", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(records))
	}

	row := records[1]
	// Domain and Name should be populated.
	if row[0] != "sparse.com" {
		t.Errorf("Domain = %q, want %q", row[0], "sparse.com")
	}
	if row[1] != "Sparse Co" {
		t.Errorf("Name = %q, want %q", row[1], "Sparse Co")
	}

	// All field-value columns should be empty strings.
	for i := 2; i < len(row); i++ {
		col := grataColumns[i]
		// Ownership always defaults to "Bootstrapped".
		if col == "Ownership" {
			if row[i] != "Bootstrapped" {
				t.Errorf("Ownership = %q, want %q", row[i], "Bootstrapped")
			}
			continue
		}
		if row[i] != "" {
			t.Errorf("column %q = %q, want empty for missing field", col, row[i])
		}
	}
}

func TestExportGrataCSV_DomainStripping(t *testing.T) {
	tests := []struct {
		url  string
		want string
	}{
		{"https://www.example.com", "www.example.com"},
		{"http://example.com", "example.com"},
		{"https://example.com/path", "example.com/path"},
		{"example.com", "example.com"},
	}

	for _, tc := range tests {
		got := stripScheme(tc.url)
		if got != tc.want {
			t.Errorf("stripScheme(%q) = %q, want %q", tc.url, got, tc.want)
		}
	}
}

func TestFormatDollars(t *testing.T) {
	tests := []struct {
		input float64
		want  string
	}{
		{1234567, "$1,234,567"},
		{5000000, "$5,000,000"},
		{999, "$999"},
		{0, "$0"},
		{100, "$100"},
		{1000, "$1,000"},
		{10000000, "$10,000,000"},
	}

	for _, tc := range tests {
		got := formatDollars(tc.input)
		if got != tc.want {
			t.Errorf("formatDollars(%v) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestExportGrataCSV_MultipleResults(t *testing.T) {
	results := []*model.EnrichmentResult{
		{
			Company: model.Company{URL: "https://a.com", Name: "Company A"},
			FieldValues: map[string]model.FieldValue{
				"description": {FieldKey: "description", Value: "First"},
			},
		},
		{
			Company: model.Company{URL: "https://b.com", Name: "Company B"},
			FieldValues: map[string]model.FieldValue{
				"description": {FieldKey: "description", Value: "Second"},
			},
		},
	}

	outPath := filepath.Join(t.TempDir(), "multi.csv")
	if err := ExportGrataCSV(results, outPath); err != nil {
		t.Fatalf("ExportGrataCSV() error: %v", err)
	}

	f, err := os.Open(outPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	// Header + 2 data rows.
	if len(records) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(records))
	}

	if records[1][0] != "a.com" {
		t.Errorf("row 1 Domain = %q, want %q", records[1][0], "a.com")
	}
	if records[2][0] != "b.com" {
		t.Errorf("row 2 Domain = %q, want %q", records[2][0], "b.com")
	}
}
