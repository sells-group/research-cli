package pipeline

import (
	"encoding/csv"
	"os"
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
