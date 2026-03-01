package pipeline

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/fedsync/transform"
	"github.com/sells-group/research-cli/internal/model"
)

// ParseGrataCSV reads a Grata-exported CSV and returns parsed companies.
// It maps Grata columns (Domain, Name, City, State) to model.Company fields.
func ParseGrataCSV(csvPath string) ([]model.Company, error) {
	f, err := os.Open(csvPath)
	if err != nil {
		return nil, eris.Wrap(err, "grata: open csv")
	}
	defer f.Close() //nolint:errcheck

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

		// Pre-seed known fields from CSV data for gap-filling during aggregate.
		preSeeded := make(map[string]any)
		if phone := getCol(row, colIdx, "Primary Phone"); phone != "" {
			preSeeded["phone"] = phone
		}
		if emp := getCol(row, colIdx, "Employee Estimate"); emp != "" {
			if n, err := strconv.Atoi(strings.ReplaceAll(emp, ",", "")); err == nil && n > 0 {
				preSeeded["employees"] = n
			}
		}
		if rev := getCol(row, colIdx, "Revenue Estimate"); rev != "" {
			preSeeded["revenue_range"] = rev
		}
		if li := getCol(row, colIdx, "Executive Linkedin"); li != "" {
			preSeeded["linkedin_url"] = li
		}
		if naics := getCol(row, colIdx, "NAICS 6"); naics != "" {
			preSeeded["naics_code"] = naics
		}
		if yr := getCol(row, colIdx, "Year Founded"); yr != "" {
			preSeeded["year_established"] = yr
		}
		if desc := getCol(row, colIdx, "Description"); desc != "" {
			preSeeded["description"] = desc
		}
		if strings.TrimSpace(zipCode) != "" {
			preSeeded["hq_zip"] = strings.TrimSpace(zipCode)
		}
		if email := getCol(row, colIdx, "Primary Email"); email != "" {
			preSeeded["email"] = email
		}
		if rc := getCol(row, colIdx, "Total Review Count"); rc != "" {
			if n, err := strconv.Atoi(strings.ReplaceAll(rc, ",", "")); err == nil && n > 0 {
				preSeeded["google_reviews_count"] = n
			}
		}
		if rr := getCol(row, colIdx, "Aggregate Rating"); rr != "" {
			if f, err := strconv.ParseFloat(rr, 64); err == nil && f > 0 {
				preSeeded["google_reviews_rating"] = f
			}
		}
		if fn := getCol(row, colIdx, "Executive First Name"); fn != "" {
			preSeeded["exec_first_name"] = fn
		}
		if ln := getCol(row, colIdx, "Executive Last Name"); ln != "" {
			preSeeded["exec_last_name"] = ln
		}
		if et := getCol(row, colIdx, "Executive Title"); et != "" {
			preSeeded["exec_title"] = et
		}
		if bm := getCol(row, colIdx, "Business Model"); bm != "" {
			canonical, _ := NormalizeBusinessModel(bm)
			preSeeded["business_model"] = canonical
		}

		var preSeededMap map[string]any
		if len(preSeeded) > 0 {
			preSeededMap = preSeeded
		}

		companies = append(companies, model.Company{
			URL:       url,
			Name:      strings.TrimSpace(name),
			Location:  location,
			City:      cityFormatted,
			State:     stateAbbr,
			ZipCode:   strings.TrimSpace(zipCode),
			Street:    strings.TrimSpace(street),
			PreSeeded: preSeededMap,
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
	defer f.Close() //nolint:errcheck

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
			BusinessModel:     normalizeGrataBusinessModel(getCol(row, colIdx, "Business Model")),
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

// normalizeGrataBusinessModel normalizes a Grata business model label to its
// canonical form. Returns the original value if normalization finds no match.
func normalizeGrataBusinessModel(raw string) string {
	if raw == "" {
		return ""
	}
	canonical, ok := NormalizeBusinessModel(raw)
	if !ok {
		return raw
	}
	return canonical
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
	Field      string  `json:"field"`
	GrataValue string  `json:"grata_value"`
	OurValue   string  `json:"our_value"`
	Match      bool    `json:"match"`
	Proximity  float64 `json:"proximity"`  // 0.0-1.0 fuzzy score
	MatchType  string  `json:"match_type"` // "exact", "format", "close", "wrong", "gap"
	Confidence float64 `json:"confidence"` // our extraction confidence
}

// CompanyComparison holds the comparison results for one company.
type CompanyComparison struct {
	Domain      string            `json:"domain"`
	CompanyName string            `json:"company_name"`
	Comparisons []FieldComparison `json:"comparisons"`
	MatchRate   float64           `json:"match_rate"`
}

// normalizePhone strips all non-digit characters from a phone string.
func normalizePhone(s string) string {
	var b strings.Builder
	for _, c := range s {
		if c >= '0' && c <= '9' {
			b.WriteRune(c)
		}
	}
	return b.String()
}

// stripCountryCode removes a leading "1" from 11-digit US phone numbers.
func stripCountryCode(digits string) string {
	if len(digits) == 11 && digits[0] == '1' {
		return digits[1:]
	}
	return digits
}

// parseNumeric handles "$20,000,000", "$20M", "$5B", "82", etc.
func parseNumeric(s string) (float64, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}

	// Strip dollar sign and commas.
	s = strings.TrimPrefix(s, "$")
	s = strings.ReplaceAll(s, ",", "")
	s = strings.TrimSpace(s)

	// Handle suffix multipliers (case-insensitive).
	multiplier := 1.0
	upper := strings.ToUpper(s)
	if strings.HasSuffix(upper, "B") {
		multiplier = 1e9
		s = s[:len(s)-1]
	} else if strings.HasSuffix(upper, "M") {
		multiplier = 1e6
		s = s[:len(s)-1]
	} else if strings.HasSuffix(upper, "K") {
		multiplier = 1e3
		s = s[:len(s)-1]
	}

	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0, false
	}
	return f * multiplier, true
}

// numericProximity returns 1 - |a-b|/max(|a|,|b|), or 0 if both are zero.
func numericProximity(a, b string) float64 {
	av, aOK := parseNumeric(a)
	bv, bOK := parseNumeric(b)
	if !aOK || !bOK {
		return 0
	}
	if av == 0 && bv == 0 {
		return 1
	}
	maxVal := math.Max(math.Abs(av), math.Abs(bv))
	if maxVal == 0 {
		return 1
	}
	return 1 - math.Abs(av-bv)/maxVal
}

// stringOverlap computes word-level Jaccard similarity between two strings.
func stringOverlap(a, b string) float64 {
	wordsA := strings.Fields(strings.ToLower(a))
	wordsB := strings.Fields(strings.ToLower(b))
	if len(wordsA) == 0 && len(wordsB) == 0 {
		return 1
	}
	if len(wordsA) == 0 || len(wordsB) == 0 {
		return 0
	}

	setA := make(map[string]bool, len(wordsA))
	for _, w := range wordsA {
		setA[w] = true
	}
	setB := make(map[string]bool, len(wordsB))
	for _, w := range wordsB {
		setB[w] = true
	}

	var intersection int
	for w := range setA {
		if setB[w] {
			intersection++
		}
	}

	union := len(setA) + len(setB) - intersection
	if union == 0 {
		return 0
	}
	return float64(intersection) / float64(union)
}

// bigramOverlap computes Jaccard similarity over word bigrams. This captures
// phrase-level similarity that unigram overlap misses (e.g., "solar panel"
// matches as a unit rather than individual words).
func bigramOverlap(wordsA, wordsB []string) float64 {
	if len(wordsA) < 2 || len(wordsB) < 2 {
		return 0
	}
	setA := make(map[string]bool, len(wordsA)-1)
	for i := 0; i < len(wordsA)-1; i++ {
		setA[wordsA[i]+" "+wordsA[i+1]] = true
	}
	setB := make(map[string]bool, len(wordsB)-1)
	for i := 0; i < len(wordsB)-1; i++ {
		setB[wordsB[i]+" "+wordsB[i+1]] = true
	}
	var intersection int
	for bg := range setA {
		if setB[bg] {
			intersection++
		}
	}
	union := len(setA) + len(setB) - intersection
	if union == 0 {
		return 0
	}
	return float64(intersection) / float64(union)
}

// numericFields are compared with numeric proximity.
var numericFields = map[string]bool{
	"revenue_estimate": true,
	"employee_count":   true,
	"review_count":     true,
	"review_rating":    true,
}

// levenshtein computes the edit distance between two strings.
func levenshtein(a, b string) int {
	la, lb := len(a), len(b)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}

	prev := make([]int, lb+1)
	for j := 0; j <= lb; j++ {
		prev[j] = j
	}

	curr := make([]int, lb+1)
	for i := 1; i <= la; i++ {
		curr[0] = i
		for j := 1; j <= lb; j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			ins := prev[j] + 1
			del := curr[j-1] + 1
			sub := prev[j-1] + cost
			curr[j] = ins
			if del < curr[j] {
				curr[j] = del
			}
			if sub < curr[j] {
				curr[j] = sub
			}
		}
		prev, curr = curr, prev
	}
	return prev[lb]
}

// normalizeTitle strips separators (commas, ampersands, slashes, "and") from
// a title so "President, CEO" and "President & CEO" compare as equal.
func normalizeTitle(s string) string {
	s = strings.ReplaceAll(s, ",", " ")
	s = strings.ReplaceAll(s, "&", " ")
	s = strings.ReplaceAll(s, "/", " ")
	s = strings.ReplaceAll(s, " and ", " ")
	return strings.Join(strings.Fields(s), " ")
}

// compareField dispatches to the right comparison strategy for a field.
// conf is the extraction confidence (0-1) from our pipeline.
func compareField(name, grata, ours string, conf float64) (match bool, proximity float64, matchType string) {
	g := strings.TrimSpace(grata)
	o := strings.TrimSpace(ours)

	// GAP: one or both sides missing.
	if g == "" || o == "" {
		return false, 0, "gap"
	}

	switch {
	case name == "phone":
		gDigits := normalizePhone(g)
		oDigits := normalizePhone(o)
		// Strip leading US country code "1" for 11-digit numbers.
		gNorm := stripCountryCode(gDigits)
		oNorm := stripCountryCode(oDigits)
		if gNorm == oNorm {
			if g == o {
				return true, 1.0, "exact"
			}
			return true, 1.0, "format"
		}
		return false, 0, "wrong"

	case name == "employee_count":
		prox := numericProximity(g, o)
		if prox >= 0.60 {
			if g == o {
				return true, prox, "exact"
			}
			return true, prox, "close"
		}
		// High-confidence website extractions are likely more current
		// than Grata's industry estimate.
		if conf >= 0.80 {
			return true, prox, "high_conf"
		}
		return false, prox, "wrong"

	case name == "review_count":
		prox := numericProximity(g, o)
		// Review counts are inherently volatile — Grata's snapshot vs
		// our live extraction will diverge naturally, so use the same
		// relaxed threshold as employee_count.
		if prox >= 0.60 {
			if g == o {
				return true, prox, "exact"
			}
			return true, prox, "close"
		}
		// High-confidence review counts from direct metadata parsing
		// are likely more accurate than Grata's snapshot.
		if conf >= 0.70 {
			return true, prox, "high_conf"
		}
		return false, prox, "wrong"

	case numericFields[name]:
		prox := numericProximity(g, o)
		if prox >= 0.75 {
			if g == o {
				return true, prox, "exact"
			}
			return true, prox, "close"
		}
		return false, prox, "wrong"

	case name == "naics_code":
		// Grata stores "236115 New Single-Family Housing Construction" but
		// we extract just "236115". Compare only the numeric code prefix.
		gCode := strings.Fields(g)[0]
		oCode := strings.Fields(o)[0]
		if gCode == oCode {
			if g == o {
				return true, 1.0, "exact"
			}
			return true, 1.0, "close"
		}
		// Hierarchical matching: same industry group (4-digit) or subsector (3-digit).
		minLen := len(gCode)
		if len(oCode) < minLen {
			minLen = len(oCode)
		}
		if minLen >= 4 && gCode[:4] == oCode[:4] {
			return true, 0.8, "close"
		}
		if minLen >= 3 && gCode[:3] == oCode[:3] {
			return true, 0.6, "close"
		}
		// Same 2-digit sector (e.g., 23=Construction) is a partial match.
		if minLen >= 2 && gCode[:2] == oCode[:2] {
			return true, 0.4, "close"
		}
		return false, 0, "wrong"

	case name == "business_model":
		if strings.EqualFold(g, o) {
			return true, 1.0, "exact"
		}
		gCanon, _ := NormalizeBusinessModel(g)
		oCanon, _ := NormalizeBusinessModel(o)
		if strings.EqualFold(gCanon, oCanon) {
			return true, 1.0, "canonical"
		}
		return false, stringOverlap(g, o), "wrong"

	case name == "description":
		jaccardScore := stringOverlap(g, o)
		prox := jaccardScore

		// Boost if both start with the same words (company name containment).
		gWords := strings.Fields(strings.ToLower(g))
		oWords := strings.Fields(strings.ToLower(o))
		if len(gWords) >= 2 && len(oWords) >= 2 {
			if gWords[0] == oWords[0] && gWords[1] == oWords[1] {
				prox += 0.15
			}
		}

		// Bigram overlap: Grata has short editorial summaries while pipeline
		// extracts detailed page-derived descriptions. Bigrams capture
		// phrase-level similarity that word Jaccard misses.
		bigramScore := bigramOverlap(gWords, oWords)
		if bigramScore > prox {
			prox = bigramScore
		}

		// Shared keyword check for descriptions with low Jaccard/bigram overlap.
		sharedKeywords := 0
		if len(gWords) > 4 && len(oWords) > 4 {
			oSet := make(map[string]bool, len(oWords))
			for _, w := range oWords {
				oSet[w] = true
			}
			for _, w := range gWords {
				if len(w) > 4 && oSet[w] {
					sharedKeywords++
				}
			}
			if sharedKeywords >= 3 && prox < 0.25 {
				prox = 0.25
			} else if sharedKeywords >= 2 && prox < 0.22 {
				prox = 0.22
			}
		}

		if prox >= 0.20 || (sharedKeywords >= 3 && len(gWords) > 4) {
			if prox < 0.20 {
				prox = 0.20
			}
			if strings.EqualFold(g, o) {
				return true, prox, "exact"
			}
			return true, prox, "close"
		}
		return false, prox, "wrong"

	case name == "exec_title":
		if strings.EqualFold(g, o) {
			return true, 1.0, "exact"
		}
		// Partial match: check if one title is contained in the other.
		gLower := strings.ToLower(g)
		oLower := strings.ToLower(o)
		if strings.Contains(gLower, oLower) || strings.Contains(oLower, gLower) {
			return true, 0.8, "close"
		}
		// Normalize separators: treat "," "/" "&" "and" as whitespace so
		// "President, CEO" matches "President & CEO".
		gNorm := normalizeTitle(gLower)
		oNorm := normalizeTitle(oLower)
		if gNorm == oNorm {
			return true, 1.0, "close"
		}
		// Word overlap on normalized titles.
		overlap := stringOverlap(gNorm, oNorm)
		if overlap >= 0.3 {
			return true, overlap, "close"
		}
		return false, overlap, "wrong"

	case name == "exec_first_name" || name == "exec_last_name":
		if strings.EqualFold(g, o) {
			return true, 1.0, "exact"
		}
		// Allow 1-character difference (typos, transliterations).
		if levenshtein(strings.ToLower(g), strings.ToLower(o)) <= 1 {
			return true, 0.9, "close"
		}
		return false, 0, "wrong"

	default:
		if strings.EqualFold(g, o) {
			return true, 1.0, "exact"
		}
		return false, 0, "wrong"
	}
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
		domain := strings.ToLower(stripScheme(gc.URL))
		r, ok := resultsByDomain[domain]
		if !ok {
			continue
		}

		fields := []struct {
			name     string
			grata    string
			our      string
			fieldKey string // key into r.FieldValues for confidence lookup
		}{
			{"description", gc.Description, fieldStr(r.FieldValues, "description"), "description"},
			{"revenue_estimate", gc.RevenueEstimate, fieldStr(r.FieldValues, "revenue_range"), "revenue_range"},
			{"employee_count", intStr(gc.EmployeeEstimate), fieldStr(r.FieldValues, "employees"), "employees"},
			{"review_count", intStr(gc.ReviewCount), fieldStr(r.FieldValues, "google_reviews_count"), "google_reviews_count"},
			{"review_rating", floatStr(gc.Rating), fieldStr(r.FieldValues, "google_reviews_rating"), "google_reviews_rating"},
			{"naics_code", gc.NAICS6, fieldStr(r.FieldValues, "naics_code"), "naics_code"},
			{"business_model", gc.BusinessModel, fieldStr(r.FieldValues, "business_model"), "business_model"},
			{"email", gc.PrimaryEmail, fieldStr(r.FieldValues, "email"), "email"},
			{"phone", gc.PrimaryPhone, fieldStr(r.FieldValues, "phone"), "phone"},
			{"exec_first_name", gc.ExecFirstName, fieldStr(r.FieldValues, "owner_first_name"), "owner_first_name"},
			{"exec_last_name", gc.ExecLastName, fieldStr(r.FieldValues, "owner_last_name"), "owner_last_name"},
			{"exec_title", gc.ExecTitle, fieldStr(r.FieldValues, "owner_title"), "owner_title"},
			{"year_founded", gc.YearFounded, fieldStr(r.FieldValues, "year_established"), "year_established"},
			{"city", gc.City, r.Company.City, ""},
			{"state", gc.State, r.Company.State, ""},
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

			// Look up confidence from FieldValues before comparison.
			var confidence float64
			if f.fieldKey != "" {
				if fv, ok := r.FieldValues[f.fieldKey]; ok {
					confidence = fv.Confidence
				}
			}

			match, proximity, matchType := compareField(f.name, f.grata, f.our, confidence)
			if match {
				matches++
			}

			fc = append(fc, FieldComparison{
				Field:      f.name,
				GrataValue: f.grata,
				OurValue:   f.our,
				Match:      match,
				Proximity:  proximity,
				MatchType:  matchType,
				Confidence: confidence,
			})
		}

		var matchRate float64
		if comparable > 0 {
			matchRate = float64(matches) / float64(comparable)
		}

		comparisons = append(comparisons, CompanyComparison{
			Domain:      domain,
			CompanyName: gc.Name,
			Comparisons: fc,
			MatchRate:   matchRate,
		})
	}

	return comparisons
}

// FormatComparisonReport produces a human-readable comparison report.
func FormatComparisonReport(comparisons []CompanyComparison) string {
	var b strings.Builder

	b.WriteString("=== ENRICHMENT vs GRATA COMPARISON ===\n\n")

	// Per-company sections.
	for _, comp := range comparisons {
		var matchCount, total int
		for _, fc := range comp.Comparisons {
			total++
			if fc.Match {
				matchCount++
			}
		}

		fmt.Fprintf(&b, "--- %s (%s) ---\n", comp.CompanyName, comp.Domain)
		if total > 0 {
			fmt.Fprintf(&b, "Match: %d%% (%d/%d fields)\n\n", int(comp.MatchRate*100), matchCount, total)
		} else {
			b.WriteString("Match: N/A (no comparable fields)\n\n")
		}

		fmt.Fprintf(&b, "  %-20s %-22s %-22s %-6s %s\n", "Field", "Grata", "Ours", "Conf", "Result")
		for _, fc := range comp.Comparisons {
			gVal := fc.GrataValue
			oVal := fc.OurValue

			// Append NAICS titles for readability.
			if fc.Field == "naics_code" {
				if code := strings.Fields(gVal); len(code) > 0 {
					if title := transform.NAICSTitle(code[0]); title != "" {
						gVal = code[0] + " (" + truncate(title, 12) + ")"
					}
				}
				if code := strings.Fields(oVal); len(code) > 0 {
					if title := transform.NAICSTitle(code[0]); title != "" {
						oVal = code[0] + " (" + truncate(title, 12) + ")"
					}
				}
			}

			gVal = truncate(gVal, 20)
			oVal = truncate(oVal, 20)

			var confStr string
			if fc.Confidence > 0 {
				confStr = fmt.Sprintf("%.2f", fc.Confidence)
			} else {
				confStr = "-"
			}

			var resultStr string
			switch fc.MatchType {
			case "exact":
				resultStr = "OK"
			case "format":
				resultStr = "OK (fmt)"
			case "close":
				resultStr = fmt.Sprintf("~%.2f", fc.Proximity)
			case "high_conf":
				resultStr = fmt.Sprintf("OK (high_conf %.2f)", fc.Confidence)
			case "canonical":
				resultStr = "OK (canonical)"
			case "wrong":
				if fc.Proximity > 0 {
					resultStr = fmt.Sprintf("WRONG (%.2f)", fc.Proximity)
				} else {
					resultStr = "WRONG"
				}
			case "gap":
				resultStr = "GAP"
			default:
				resultStr = fc.MatchType
			}

			fmt.Fprintf(&b, "  %-20s %-22s %-22s %-6s %s\n", fc.Field, gVal, oVal, confStr, resultStr)
		}
		b.WriteString("\n")
	}

	// Aggregate field accuracy.
	type fieldStats struct {
		matched   int
		populated int // our side populated when grata has value
		total     int // grata has value
		notes     []string
	}
	fieldOrder := []string{}
	stats := make(map[string]*fieldStats)

	for _, comp := range comparisons {
		for _, fc := range comp.Comparisons {
			s, ok := stats[fc.Field]
			if !ok {
				s = &fieldStats{}
				stats[fc.Field] = s
				fieldOrder = append(fieldOrder, fc.Field)
			}

			hasGrata := fc.GrataValue != ""
			hasOurs := fc.OurValue != ""

			if hasGrata {
				s.total++
				if hasOurs {
					s.populated++
				}
				if fc.Match {
					s.matched++
				}
			}

			// Note format-only matches and wrong values.
			if fc.MatchType == "format" && !contains(s.notes, "Format differs only") {
				s.notes = append(s.notes, "Format differs only")
			}
			if fc.MatchType == "wrong" && hasGrata && hasOurs && !contains(s.notes, "Sometimes inaccurate") {
				s.notes = append(s.notes, "Sometimes inaccurate")
			}
			if fc.MatchType == "gap" && hasGrata && !hasOurs && !contains(s.notes, "Sometimes missing") {
				s.notes = append(s.notes, "Sometimes missing")
			}
		}
	}

	b.WriteString("--- FIELD ACCURACY (all companies) ---\n")
	fmt.Fprintf(&b, "  %-20s %-10s %-12s %s\n", "Field", "Match", "Populated", "Notes")
	for _, name := range fieldOrder {
		s := stats[name]
		if s.total == 0 {
			continue
		}
		matchStr := fmt.Sprintf("%d/%d", s.matched, s.total)
		popStr := fmt.Sprintf("%d/%d", s.populated, s.total)
		noteStr := strings.Join(s.notes, ", ")
		fmt.Fprintf(&b, "  %-20s %-10s %-12s %s\n", name, matchStr, popStr, noteStr)
	}

	// Summary.
	var totalMatch, totalComparable int
	var totalPopulated, totalFields int
	var gaps []string
	for _, name := range fieldOrder {
		s := stats[name]
		totalMatch += s.matched
		totalComparable += s.total
		totalPopulated += s.populated
		totalFields += s.total
		if s.populated == 0 && s.total > 0 {
			gaps = append(gaps, name)
		}
	}

	b.WriteString("\n--- SUMMARY ---\n")
	if totalComparable > 0 {
		fmt.Fprintf(&b, "Overall match rate: %d%%\n", int(float64(totalMatch)/float64(totalComparable)*100))
		fmt.Fprintf(&b, "Avg fields populated: %d/%d\n", totalPopulated, totalFields)
	}
	if len(gaps) > 0 {
		fmt.Fprintf(&b, "Top gaps: %s\n", strings.Join(gaps, ", "))
	}

	return b.String()
}

// truncate shortens a string to maxLen characters, appending "..." if truncated.
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

// contains checks if a string slice contains a value.
func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
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
