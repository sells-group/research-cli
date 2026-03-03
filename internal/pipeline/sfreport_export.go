package pipeline

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/model"
)

// sfReportColumns defines the ordered SF report CSV output columns.
var sfReportColumns = []string{
	"Account Name",
	"Account ID",
	"Website",
	"Year Founded",
	"Locations",
	"Employees",
	"Shipping State/Province",
	"Company MSA",
	"Shipping City",
	"Office #2 City",
	"Office #3 City",
	"Office #4 City",
	"Company Research Notes",
	"Ownership",
	"End Markets",
	"Region",
	"Priority",
	"Annual Revenue",
}

// ExportSFReportCSV writes enrichment results as a SF report-format CSV file.
func ExportSFReportCSV(results []*model.EnrichmentResult, originals []SFReportCompany, outputPath string) error {
	f, err := os.Create(outputPath) // #nosec G304 -- path from CLI flag
	if err != nil {
		return eris.Wrap(err, "sf-report export: create file")
	}
	defer f.Close() //nolint:errcheck

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write(sfReportColumns); err != nil {
		return eris.Wrap(err, "sf-report export: write header")
	}

	// Index originals by SalesforceID for lookup.
	origByID := make(map[string]*SFReportCompany, len(originals))
	for i := range originals {
		origByID[originals[i].AccountID] = &originals[i]
	}

	for _, r := range results {
		orig := origByID[r.Company.SalesforceID]
		row := buildSFReportRow(r, orig)
		if err := w.Write(row); err != nil {
			return eris.Wrap(err, "sf-report export: write row")
		}
	}

	return nil
}

// buildSFReportRow maps an EnrichmentResult to a SF report CSV row.
func buildSFReportRow(r *model.EnrichmentResult, original *SFReportCompany) []string {
	fv := r.FieldValues

	// Resolve city and state from extraction or company.
	city := fieldStr(fv, "hq_city")
	if city == "" {
		city = r.Company.City
	}
	state := fieldStr(fv, "hq_state")
	if state == "" {
		state = r.Company.State
	}
	state = stateAbbreviation(state)

	// Locations: empty means unknown (no default to avoid masking real data).
	locations := fieldStr(fv, "locations")

	// Employees: canonical key is "employees" in Notion registry.
	employees := fieldStr(fv, "employees")

	// Pass-through fields from original.
	var accountID, ownership string
	if original != nil {
		accountID = original.AccountID
		ownership = original.Ownership
	}

	return []string{
		r.Company.Name,                       // Account Name
		accountID,                            // Account ID
		stripScheme(r.Company.URL),           // Website
		fieldStr(fv, "year_established"),     // Year Founded
		locations,                            // Locations
		employees,                            // Employees
		state,                                // Shipping State/Province
		MSAShortName(LookupMSA(city, state)), // Company MSA
		titleCase(city),                      // Shipping City
		fieldStr(fv, "office_2_city"),        // Office #2 City
		fieldStr(fv, "office_3_city"),        // Office #3 City
		fieldStr(fv, "office_4_city"),        // Office #4 City
		buildResearchNotes(r),                // Company Research Notes
		ownership,                            // Ownership
		fieldStr(fv, "end_markets"),          // End Markets
		StateToRegion(state),                 // Region
		"",                                   // Priority (blank for manual review)
		formatRevenue(fv),                    // Annual Revenue
	}
}

// buildResearchNotes combines description, services, differentiators, and
// business model into a rich paragraph for the Company Research Notes column.
func buildResearchNotes(r *model.EnrichmentResult) string {
	fv := r.FieldValues

	var parts []string

	if desc := fieldStr(fv, "description"); desc != "" {
		parts = append(parts, desc)
	}

	if services := fieldStr(fv, "service_mix"); services != "" {
		parts = append(parts, fmt.Sprintf("Services: %s.", services))
	}

	if diff := fieldStr(fv, "differentiators"); diff != "" {
		parts = append(parts, fmt.Sprintf("Differentiators: %s.", diff))
	}

	if bm := fieldStr(fv, "business_model"); bm != "" {
		parts = append(parts, fmt.Sprintf("Business Model: %s.", bm))
	}

	if customers := fieldStr(fv, "customer_types"); customers != "" {
		parts = append(parts, fmt.Sprintf("Customers: %s.", customers))
	}

	return strings.Join(parts, " ")
}
