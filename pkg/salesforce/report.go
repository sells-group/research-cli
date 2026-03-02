package salesforce

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rotisserie/eris"
)

// ReportResult is the top-level response from the SF Analytics Reports API.
type ReportResult struct {
	ReportMetadata ReportMetadata             `json:"reportMetadata"`
	FactMap        map[string]json.RawMessage `json:"factMap"`
}

// ReportMetadata holds report identity and column layout.
type ReportMetadata struct {
	ID             string               `json:"id"`
	Name           string               `json:"name"`
	DetailColumns  []string             `json:"detailColumns"`
	ReportFormat   string               `json:"reportFormat"`
	GroupingsDown  []string             `json:"groupingsDown"`
	GroupingColumn []GroupingColumnInfo `json:"groupingColumnInfo,omitempty"`
}

// GroupingColumnInfo describes a grouping column in a SUMMARY/MATRIX report.
type GroupingColumnInfo struct {
	Name          string `json:"name"`
	GroupingLevel int    `json:"groupingLevel"`
}

// FactMapEntry holds rows for a single grouping key (e.g. "T!T" for tabular).
type FactMapEntry struct {
	Rows []ReportRow `json:"rows"`
}

// ReportRow is a single data row from the report.
type ReportRow struct {
	DataCells []ReportCell `json:"dataCells"`
}

// ReportCell is a single cell value in a report row.
type ReportCell struct {
	Label string `json:"label"`
	Value any    `json:"value"`
}

// ReportAccount holds the account fields extracted from a report row.
type ReportAccount struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Website string `json:"website"`
	City    string `json:"city"`
	State   string `json:"state"`
}

// RunReport fetches a Salesforce report with detail rows via the Analytics API.
func (c *sfClient) RunReport(ctx context.Context, reportID string) (*ReportResult, error) {
	if err := c.wait(ctx); err != nil {
		return nil, eris.Wrap(err, "sf: rate limit")
	}

	uri := fmt.Sprintf("/analytics/reports/%s?includeDetails=true", reportID)
	resp, err := c.sf.DoRequest("GET", uri, nil)
	if err != nil {
		return nil, eris.Wrap(err, fmt.Sprintf("sf: run report %s", reportID))
	}
	defer resp.Body.Close() //nolint:errcheck

	var result ReportResult
	if err := decodeJSON(resp.Body, &result); err != nil {
		return nil, eris.Wrap(err, fmt.Sprintf("sf: decode report %s", reportID))
	}
	return &result, nil
}

// ParseReportAccounts extracts account records from a report result by dynamically
// mapping detailColumns to known Salesforce API field names.
func ParseReportAccounts(result *ReportResult) ([]ReportAccount, error) {
	colIndex := buildColumnIndex(result.ReportMetadata.DetailColumns)

	idIdx, hasID := colIndex.lookup("Account.Id", "Id", "ACCOUNT_ID")
	websiteIdx, hasWebsite := colIndex.lookup("Account.Website", "Website", "URL")
	nameIdx, hasName := colIndex.lookup("Account.Name", "Name", "ACCOUNT.NAME")
	cityIdx, hasCity := colIndex.lookup("Account.BillingCity", "BillingCity", "ADDRESS2_CITY")
	stateIdx, hasState := colIndex.lookup("Account.BillingState", "BillingState", "ADDRESS2_STATE")

	var accounts []ReportAccount

	for key, raw := range result.FactMap {
		var entry FactMapEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			return nil, eris.Wrap(err, fmt.Sprintf("sf: unmarshal fact map entry %s", key))
		}

		for _, row := range entry.Rows {
			var acct ReportAccount

			// Extract ID from detail column if available.
			if hasID {
				acct.ID = cellString(row.DataCells, idIdx)
				if acct.ID == "" {
					continue
				}
			}

			if hasName {
				acct.Name = cellString(row.DataCells, nameIdx)
			}
			if hasWebsite {
				acct.Website = cellString(row.DataCells, websiteIdx)
			}
			if hasCity {
				acct.City = cellString(row.DataCells, cityIdx)
			}
			if hasState {
				acct.State = cellString(row.DataCells, stateIdx)
			}

			// For SUMMARY reports, the Account Name can serve as a fallback
			// identifier when ID is not in detail columns.
			if acct.ID == "" && acct.Name == "" {
				continue
			}

			accounts = append(accounts, acct)
		}
	}

	return accounts, nil
}

// columnIndex maps known API names to their position in detailColumns.
type columnIndex struct {
	m map[string]int
}

// buildColumnIndex creates a columnIndex from the report's detailColumns slice.
func buildColumnIndex(cols []string) columnIndex {
	m := make(map[string]int, len(cols))
	for i, col := range cols {
		m[col] = i
	}
	return columnIndex{m: m}
}

// lookup returns the index for the first matching column name.
func (ci columnIndex) lookup(names ...string) (int, bool) {
	for _, n := range names {
		if idx, ok := ci.m[n]; ok {
			return idx, true
		}
	}
	return -1, false
}

// cellString safely extracts a string from a cell slice by index.
func cellString(cells []ReportCell, idx int) string {
	if idx < 0 || idx >= len(cells) {
		return ""
	}
	return cells[idx].Label
}
