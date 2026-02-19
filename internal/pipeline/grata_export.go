package pipeline

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/model"
)

// grataColumns defines the ordered Grata CSV output columns.
var grataColumns = []string{
	"Domain",
	"Name",
	"Description",
	"Revenue Estimate",
	"Employee Estimate",
	"Total Review Count",
	"Aggregate Rating",
	"NAICS 6",
	"Business Model",
	"Primary Email",
	"Primary Phone",
	"Executive First Name",
	"Executive Last Name",
	"Executive Title",
	"Executive Linkedin",
	"Key People",
	"Year Founded",
	"Ownership",
	"City",
	"State",
	"Zip Code",
	"Mailing Address",
}

// ExportGrataCSV writes enrichment results as a Grata-format CSV file.
func ExportGrataCSV(results []*model.EnrichmentResult, outputPath string) error {
	f, err := os.Create(outputPath)
	if err != nil {
		return eris.Wrap(err, "grata export: create file")
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write(grataColumns); err != nil {
		return eris.Wrap(err, "grata export: write header")
	}

	for _, r := range results {
		row := buildGrataRow(r)
		if err := w.Write(row); err != nil {
			return eris.Wrap(err, "grata export: write row")
		}
	}

	return nil
}

// buildGrataRow maps an EnrichmentResult to a Grata CSV row.
func buildGrataRow(r *model.EnrichmentResult) []string {
	fv := r.FieldValues

	return []string{
		stripScheme(r.Company.URL),                     // Domain
		r.Company.Name,                                 // Name
		fieldStr(fv, "description"),                    // Description
		formatRevenue(fv),                              // Revenue Estimate
		fieldStr(fv, "employee_count"),                 // Employee Estimate
		fieldStr(fv, "review_count"),                   // Total Review Count
		fieldStr(fv, "review_rating"),                  // Aggregate Rating
		fieldStr(fv, "naics_code"),                     // NAICS 6
		fieldStr(fv, "business_model"),                 // Business Model
		fieldStr(fv, "email"),                          // Primary Email
		fieldStr(fv, "phone"),                          // Primary Phone
		fieldStr(fv, "exec_first_name"),                // Executive First Name
		fieldStr(fv, "exec_last_name"),                 // Executive Last Name
		fieldStr(fv, "exec_title"),                     // Executive Title
		fieldStr(fv, "exec_linkedin"),                  // Executive Linkedin
		fieldStr(fv, "key_people"),                     // Key People
		fieldStr(fv, "year_founded"),                   // Year Founded
		"Bootstrapped",                                 // Ownership (default)
		r.Company.City,                                 // City
		r.Company.State,                                // State
		r.Company.ZipCode,                              // Zip Code
		r.Company.Street,                               // Mailing Address
	}
}

// stripScheme removes "https://" or "http://" from a URL.
func stripScheme(u string) string {
	u = strings.TrimPrefix(u, "https://")
	u = strings.TrimPrefix(u, "http://")
	return u
}

// fieldStr extracts a field value as a string. Returns empty string if not found.
func fieldStr(fv map[string]model.FieldValue, key string) string {
	v, ok := fv[key]
	if !ok {
		return ""
	}
	if v.Value == nil {
		return ""
	}
	return fmt.Sprintf("%v", v.Value)
}

// formatRevenue formats the revenue field as "$X,XXX,XXX".
// It tries revenue_estimate first, then falls back to revenue_range.
func formatRevenue(fv map[string]model.FieldValue) string {
	// Try revenue_estimate first.
	if v, ok := fv["revenue_estimate"]; ok && v.Value != nil {
		if n, numOK := toFloat(v.Value); numOK && n > 0 {
			return formatDollars(n)
		}
		return fmt.Sprintf("%v", v.Value)
	}
	// Fall back to revenue_range.
	if v, ok := fv["revenue_range"]; ok && v.Value != nil {
		return fmt.Sprintf("%v", v.Value)
	}
	return ""
}

// formatDollars formats a number as "$1,234,567".
func formatDollars(n float64) string {
	intVal := int64(n)
	if intVal == 0 {
		return "$0"
	}

	negative := intVal < 0
	if negative {
		intVal = -intVal
	}

	// Format with commas.
	s := fmt.Sprintf("%d", intVal)
	var result []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}

	prefix := "$"
	if negative {
		prefix = "-$"
	}
	return prefix + string(result)
}
