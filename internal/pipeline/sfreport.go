package pipeline

import (
	"encoding/csv"
	"os"
	"strings"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/model"
)

// CSVFormat identifies the input CSV format.
type CSVFormat string

const (
	// CSVFormatGrata is a Grata-exported CSV with "Domain" column.
	CSVFormatGrata CSVFormat = "grata"
	// CSVFormatSFReport is a Salesforce report CSV with "Account Name" + "Account ID".
	CSVFormatSFReport CSVFormat = "sf-report"
	// CSVFormatUnknown is an unrecognized CSV format.
	CSVFormatUnknown CSVFormat = "unknown"
)

// SFReportCompany holds parsed SF report row data alongside the core Company.
type SFReportCompany struct {
	model.Company
	AccountID string `json:"account_id"`
	Ownership string `json:"ownership"`
}

// DetectCSVFormat reads the header row of a CSV file and returns the detected format.
func DetectCSVFormat(csvPath string) (CSVFormat, error) {
	f, err := os.Open(csvPath) // #nosec G304 -- path from CLI flag
	if err != nil {
		return CSVFormatUnknown, eris.Wrap(err, "detect csv format: open")
	}
	defer f.Close() //nolint:errcheck

	reader := csv.NewReader(f)
	reader.LazyQuotes = true
	header, err := reader.Read()
	if err != nil {
		return CSVFormatUnknown, eris.Wrap(err, "detect csv format: read header")
	}

	cols := make(map[string]bool, len(header))
	for _, col := range header {
		cols[strings.TrimSpace(col)] = true
	}

	if cols["Account Name"] && cols["Account ID"] {
		return CSVFormatSFReport, nil
	}
	if cols["Domain"] {
		return CSVFormatGrata, nil
	}
	return CSVFormatUnknown, nil
}

// ParseSFReportCSV reads a Salesforce report CSV and returns parsed companies.
func ParseSFReportCSV(csvPath string) ([]SFReportCompany, error) {
	f, err := os.Open(csvPath) // #nosec G304 -- path from CLI flag
	if err != nil {
		return nil, eris.Wrap(err, "sf-report: open csv")
	}
	defer f.Close() //nolint:errcheck

	reader := csv.NewReader(f)
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1

	records, err := reader.ReadAll()
	if err != nil {
		return nil, eris.Wrap(err, "sf-report: read csv")
	}

	if len(records) < 2 {
		return nil, eris.New("sf-report: csv has no data rows")
	}

	header := records[0]
	colIdx := make(map[string]int, len(header))
	for i, col := range header {
		colIdx[strings.TrimSpace(col)] = i
	}

	requiredCols := []string{"Account Name", "Account ID", "Website"}
	for _, col := range requiredCols {
		if _, ok := colIdx[col]; !ok {
			return nil, eris.Errorf("sf-report: missing required column %q", col)
		}
	}

	seen := make(map[string]bool)
	var companies []SFReportCompany

	for _, row := range records[1:] {
		accountID := getCol(row, colIdx, "Account ID")
		if accountID == "" {
			continue
		}

		website := getCol(row, colIdx, "Website")
		if website == "" {
			continue
		}

		// Normalize URL.
		url := normalizeWebsite(website)

		// Deduplicate by URL.
		urlLower := strings.ToLower(url)
		if seen[urlLower] {
			continue
		}
		seen[urlLower] = true

		name := getCol(row, colIdx, "Account Name")
		ownership := getCol(row, colIdx, "Ownership")

		companies = append(companies, SFReportCompany{
			Company: model.Company{
				URL:          url,
				Name:         strings.TrimSpace(name),
				SalesforceID: accountID,
			},
			AccountID: accountID,
			Ownership: ownership,
		})
	}

	if len(companies) == 0 {
		return nil, eris.New("sf-report: no valid companies found in csv")
	}

	return companies, nil
}

// CompaniesFromSFReport extracts model.Company values from SFReportCompany slices.
func CompaniesFromSFReport(reports []SFReportCompany) []model.Company {
	companies := make([]model.Company, len(reports))
	for i, r := range reports {
		companies[i] = r.Company
	}
	return companies
}

// normalizeWebsite ensures a website string has an https:// scheme.
func normalizeWebsite(website string) string {
	website = strings.TrimSpace(website)
	if website == "" {
		return ""
	}
	lower := strings.ToLower(website)
	if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") {
		return website
	}
	return "https://" + website
}
