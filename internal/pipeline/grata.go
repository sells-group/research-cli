package pipeline

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/model"
)

// ParseGrataCSV reads a Grata-exported CSV and returns parsed companies.
// It maps Grata columns (Domain, Name, City, State) to model.Company fields.
func ParseGrataCSV(csvPath string) ([]model.Company, error) {
	f, err := os.Open(csvPath)
	if err != nil {
		return nil, eris.Wrap(err, "grata: open csv")
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.LazyQuotes = true

	records, err := reader.ReadAll()
	if err != nil {
		return nil, eris.Wrap(err, "grata: read csv")
	}

	if len(records) < 2 {
		return nil, eris.New("grata: csv has no data rows")
	}

	header := records[0]
	colIdx := make(map[string]int, len(header))
	for i, col := range header {
		colIdx[strings.TrimSpace(col)] = i
	}

	// Verify required columns exist.
	requiredCols := []string{"Domain"}
	for _, col := range requiredCols {
		if _, ok := colIdx[col]; !ok {
			return nil, eris.Errorf("grata: missing required column %q", col)
		}
	}

	seen := make(map[string]bool)
	var companies []model.Company

	for _, row := range records[1:] {
		domain := getCol(row, colIdx, "Domain")
		if domain == "" {
			continue
		}

		// Deduplicate by domain.
		domainLower := strings.ToLower(strings.TrimSpace(domain))
		if seen[domainLower] {
			continue
		}
		seen[domainLower] = true

		// Build URL from domain.
		url := "https://" + domainLower

		name := getCol(row, colIdx, "Name")
		city := getCol(row, colIdx, "City")
		state := getCol(row, colIdx, "State")
		zipCode := getCol(row, colIdx, "Zip Code")
		street := getCol(row, colIdx, "Mailing Address")

		cityFormatted := titleCase(city)
		stateAbbr := stateAbbreviation(state)

		var location string
		if city != "" || state != "" {
			if cityFormatted != "" && stateAbbr != "" {
				location = cityFormatted + ", " + stateAbbr
			} else if cityFormatted != "" {
				location = cityFormatted
			} else {
				location = stateAbbr
			}
		}

		companies = append(companies, model.Company{
			URL:      url,
			Name:     strings.TrimSpace(name),
			Location: location,
			City:     cityFormatted,
			State:    stateAbbr,
			ZipCode:  strings.TrimSpace(zipCode),
			Street:   strings.TrimSpace(street),
		})
	}

	if len(companies) == 0 {
		return nil, eris.New("grata: no valid companies found in csv")
	}

	return companies, nil
}

// getCol safely retrieves a column value from a CSV row.
func getCol(row []string, colIdx map[string]int, col string) string {
	idx, ok := colIdx[col]
	if !ok || idx >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[idx])
}

// titleCase converts "WEST JORDAN" to "West Jordan".
func titleCase(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	words := strings.Fields(strings.ToLower(s))
	for i, w := range words {
		if len(w) > 0 {
			words[i] = strings.ToUpper(w[:1]) + w[1:]
		}
	}
	return strings.Join(words, " ")
}

// stateAbbreviation converts full state names to two-letter abbreviations.
// If the input is already a 2-letter abbreviation, it is returned as-is (uppercased).
func stateAbbreviation(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	upper := strings.ToUpper(s)
	if len(upper) == 2 {
		return upper
	}
	if abbr, ok := stateMap[upper]; ok {
		return abbr
	}
	// If not found, return as title case.
	return titleCase(s)
}

var stateMap = map[string]string{
	"ALABAMA":              "AL",
	"ALASKA":               "AK",
	"ARIZONA":              "AZ",
	"ARKANSAS":             "AR",
	"CALIFORNIA":           "CA",
	"COLORADO":             "CO",
	"CONNECTICUT":          "CT",
	"DELAWARE":             "DE",
	"FLORIDA":              "FL",
	"GEORGIA":              "GA",
	"HAWAII":               "HI",
	"IDAHO":                "ID",
	"ILLINOIS":             "IL",
	"INDIANA":              "IN",
	"IOWA":                 "IA",
	"KANSAS":               "KS",
	"KENTUCKY":             "KY",
	"LOUISIANA":            "LA",
	"MAINE":                "ME",
	"MARYLAND":             "MD",
	"MASSACHUSETTS":        "MA",
	"MICHIGAN":             "MI",
	"MINNESOTA":            "MN",
	"MISSISSIPPI":          "MS",
	"MISSOURI":             "MO",
	"MONTANA":              "MT",
	"NEBRASKA":             "NE",
	"NEVADA":               "NV",
	"NEW HAMPSHIRE":        "NH",
	"NEW JERSEY":           "NJ",
	"NEW MEXICO":           "NM",
	"NEW YORK":             "NY",
	"NORTH CAROLINA":       "NC",
	"NORTH DAKOTA":         "ND",
	"OHIO":                 "OH",
	"OKLAHOMA":             "OK",
	"OREGON":               "OR",
	"PENNSYLVANIA":         "PA",
	"RHODE ISLAND":         "RI",
	"SOUTH CAROLINA":       "SC",
	"SOUTH DAKOTA":         "SD",
	"TENNESSEE":            "TN",
	"TEXAS":                "TX",
	"UTAH":                 "UT",
	"VERMONT":              "VT",
	"VIRGINIA":             "VA",
	"WASHINGTON":           "WA",
	"WEST VIRGINIA":        "WV",
	"WISCONSIN":            "WI",
	"WYOMING":              "WY",
	"DISTRICT OF COLUMBIA": "DC",
}

// GrataCompany captures all Grata export CSV columns for full-fidelity parsing.
type GrataCompany struct {
	model.Company
	Description       string  `json:"description"`
	RevenueEstimate   string  `json:"revenue_estimate"`
	EmployeeEstimate  int     `json:"employee_estimate"`
	EmployeesLinkedIn int     `json:"employees_linkedin"`
	ReviewCount       int     `json:"review_count"`
	Rating            float64 `json:"rating"`
	NAICS6            string  `json:"naics_6"`
	BusinessModel     string  `json:"business_model"`
	PrimaryEmail      string  `json:"primary_email"`
	PrimaryPhone      string  `json:"primary_phone"`
	ExecFirstName     string  `json:"exec_first_name"`
	ExecLastName      string  `json:"exec_last_name"`
	ExecTitle         string  `json:"exec_title"`
	ExecLinkedIn      string  `json:"exec_linkedin"`
	KeyPeople         string  `json:"key_people"`
	YearFounded       string  `json:"year_founded"`
	Ownership         string  `json:"ownership"`
}

// ParseGrataCSVFull reads ALL Grata CSV columns into GrataCompany structs.
func ParseGrataCSVFull(csvPath string) ([]GrataCompany, error) {
	f, err := os.Open(csvPath)
	if err != nil {
		return nil, eris.Wrap(err, "grata: open csv")
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1 // tolerate variable field counts

	records, err := reader.ReadAll()
	if err != nil {
		return nil, eris.Wrap(err, "grata: read csv")
	}

	if len(records) < 2 {
		return nil, eris.New("grata: csv has no data rows")
	}

	header := records[0]
	colIdx := make(map[string]int, len(header))
	for i, col := range header {
		colIdx[strings.TrimSpace(col)] = i
	}

	if _, ok := colIdx["Domain"]; !ok {
		return nil, eris.New("grata: missing required column \"Domain\"")
	}

	seen := make(map[string]bool)
	var result []GrataCompany

	for _, row := range records[1:] {
		domain := getCol(row, colIdx, "Domain")
		if domain == "" {
			continue
		}

		domainLower := strings.ToLower(strings.TrimSpace(domain))
		if seen[domainLower] {
			continue
		}
		seen[domainLower] = true

		url := "https://" + domainLower
		name := getCol(row, colIdx, "Name")
		city := getCol(row, colIdx, "City")
		state := getCol(row, colIdx, "State")
		zipCode := getCol(row, colIdx, "Zip Code")
		street := getCol(row, colIdx, "Mailing Address")

		cityFormatted := titleCase(city)
		stateAbbr := stateAbbreviation(state)

		var location string
		if city != "" || state != "" {
			if cityFormatted != "" && stateAbbr != "" {
				location = cityFormatted + ", " + stateAbbr
			} else if cityFormatted != "" {
				location = cityFormatted
			} else {
				location = stateAbbr
			}
		}

		gc := GrataCompany{
			Company: model.Company{
				URL:      url,
				Name:     strings.TrimSpace(name),
				Location: location,
				City:     cityFormatted,
				State:    stateAbbr,
				ZipCode:  strings.TrimSpace(zipCode),
				Street:   strings.TrimSpace(street),
			},
			Description:       getCol(row, colIdx, "Description"),
			RevenueEstimate:   getCol(row, colIdx, "Revenue Estimate"),
			EmployeeEstimate:  getColInt(row, colIdx, "Employee Estimate"),
			EmployeesLinkedIn: getColInt(row, colIdx, "Employees - LinkedIn"),
			ReviewCount:       getColInt(row, colIdx, "Total Review Count"),
			Rating:            getColFloat(row, colIdx, "Aggregate Rating"),
			NAICS6:            getCol(row, colIdx, "NAICS 6"),
			BusinessModel:     getCol(row, colIdx, "Business Model"),
			PrimaryEmail:      getCol(row, colIdx, "Primary Email"),
			PrimaryPhone:      getCol(row, colIdx, "Primary Phone"),
			ExecFirstName:     getCol(row, colIdx, "Executive First Name"),
			ExecLastName:      getCol(row, colIdx, "Executive Last Name"),
			ExecTitle:         getCol(row, colIdx, "Executive Title"),
			ExecLinkedIn:      getCol(row, colIdx, "Executive Linkedin"),
			KeyPeople:         getCol(row, colIdx, "Key People"),
			YearFounded:       getCol(row, colIdx, "Year Founded"),
			Ownership:         getCol(row, colIdx, "Ownership"),
		}

		result = append(result, gc)
	}

	if len(result) == 0 {
		return nil, eris.New("grata: no valid companies found in csv")
	}

	return result, nil
}

// getColInt parses a column value as an integer, returning 0 on failure.
func getColInt(row []string, colIdx map[string]int, col string) int {
	s := getCol(row, colIdx, col)
	if s == "" {
		return 0
	}
	s = strings.ReplaceAll(s, ",", "")
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}

// getColFloat parses a column value as a float, returning 0 on failure.
func getColFloat(row []string, colIdx map[string]int, col string) float64 {
	s := getCol(row, colIdx, col)
	if s == "" {
		return 0
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return f
}

// FieldComparison holds a single field comparison between Grata and enrichment.
type FieldComparison struct {
	Field      string `json:"field"`
	GrataValue string `json:"grata_value"`
	OurValue   string `json:"our_value"`
	Match      bool   `json:"match"`
}

// CompanyComparison holds the comparison results for one company.
type CompanyComparison struct {
	Domain      string            `json:"domain"`
	Comparisons []FieldComparison `json:"comparisons"`
	MatchRate   float64           `json:"match_rate"`
}

// CompareResults compares Grata company data against enrichment results field by field.
func CompareResults(grataCompanies []GrataCompany, results []*model.EnrichmentResult) []CompanyComparison {
	// Index results by domain for lookup.
	resultsByDomain := make(map[string]*model.EnrichmentResult, len(results))
	for _, r := range results {
		domain := strings.ToLower(stripScheme(r.Company.URL))
		resultsByDomain[domain] = r
	}

	var comparisons []CompanyComparison

	for _, gc := range grataCompanies {
		domain := strings.ToLower(stripScheme(gc.Company.URL))
		r, ok := resultsByDomain[domain]
		if !ok {
			continue
		}

		fields := []struct {
			name  string
			grata string
			our   string
		}{
			{"description", gc.Description, fieldStr(r.FieldValues, "description")},
			{"revenue_estimate", gc.RevenueEstimate, fieldStr(r.FieldValues, "revenue_estimate")},
			{"employee_count", intStr(gc.EmployeeEstimate), fieldStr(r.FieldValues, "employee_count")},
			{"review_count", intStr(gc.ReviewCount), fieldStr(r.FieldValues, "review_count")},
			{"review_rating", floatStr(gc.Rating), fieldStr(r.FieldValues, "review_rating")},
			{"naics_code", gc.NAICS6, fieldStr(r.FieldValues, "naics_code")},
			{"business_model", gc.BusinessModel, fieldStr(r.FieldValues, "business_model")},
			{"email", gc.PrimaryEmail, fieldStr(r.FieldValues, "email")},
			{"phone", gc.PrimaryPhone, fieldStr(r.FieldValues, "phone")},
			{"exec_first_name", gc.ExecFirstName, fieldStr(r.FieldValues, "exec_first_name")},
			{"exec_last_name", gc.ExecLastName, fieldStr(r.FieldValues, "exec_last_name")},
			{"exec_title", gc.ExecTitle, fieldStr(r.FieldValues, "exec_title")},
			{"year_founded", gc.YearFounded, fieldStr(r.FieldValues, "year_founded")},
			{"city", gc.City, r.Company.City},
			{"state", gc.State, r.Company.State},
		}

		var fc []FieldComparison
		var matches int
		var comparable int

		for _, f := range fields {
			// Skip if both are empty.
			if f.grata == "" && f.our == "" {
				continue
			}
			comparable++
			match := strings.EqualFold(strings.TrimSpace(f.grata), strings.TrimSpace(f.our))
			if match {
				matches++
			}
			fc = append(fc, FieldComparison{
				Field:      f.name,
				GrataValue: f.grata,
				OurValue:   f.our,
				Match:      match,
			})
		}

		var matchRate float64
		if comparable > 0 {
			matchRate = float64(matches) / float64(comparable)
		}

		comparisons = append(comparisons, CompanyComparison{
			Domain:      domain,
			Comparisons: fc,
			MatchRate:   matchRate,
		})
	}

	return comparisons
}

// intStr converts an int to string, returning empty for zero.
func intStr(n int) string {
	if n == 0 {
		return ""
	}
	return fmt.Sprintf("%d", n)
}

// floatStr converts a float to string, returning empty for zero.
func floatStr(f float64) string {
	if f == 0 {
		return ""
	}
	return fmt.Sprintf("%g", f)
}
