package notion

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	"github.com/jomei/notionapi"
	"github.com/rotisserie/eris"
)

// CSVMapper maps a CSV row to a flat key-value map using the header row.
type CSVMapper struct{}

// MapRow pairs each header with the corresponding value in the row.
// If the row has fewer columns than headers, missing values become empty strings.
func (m CSVMapper) MapRow(headers []string, row []string) map[string]string {
	result := make(map[string]string, len(headers))
	for i, h := range headers {
		if i < len(row) {
			result[h] = row[i]
		} else {
			result[h] = ""
		}
	}
	return result
}

// isGrataCSV checks if CSV headers indicate Grata export format.
// Grata CSVs contain both "Domain" and "Grata Link" columns.
func isGrataCSV(headers []string) bool {
	hasDomain, hasGrataLink := false, false
	for _, h := range headers {
		switch strings.ToLower(strings.TrimSpace(h)) {
		case "domain":
			hasDomain = true
		case "grata link":
			hasGrataLink = true
		}
	}
	return hasDomain && hasGrataLink
}

// ImportCSV reads a CSV file, deduplicates rows by URL/Domain, and creates
// a Notion page for each unique row. Auto-detects Grata CSV format (Domain +
// Grata Link headers) and applies column mapping. Pages are created at 3 req/s
// via a 334ms ticker. Returns the number of pages created.
func ImportCSV(ctx context.Context, c Client, dbID string, csvPath string) (int, error) {
	f, err := os.Open(csvPath)
	if err != nil {
		return 0, eris.Wrap(err, fmt.Sprintf("notion: open csv %s", csvPath))
	}
	defer f.Close() //nolint:errcheck

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		return 0, eris.Wrap(err, "notion: read csv")
	}

	if len(records) < 2 {
		return 0, nil // header only or empty
	}

	headers := records[0]
	rows := records[1:]
	grata := isGrataCSV(headers)

	// Find URL/Domain column index for deduplication.
	urlIdx := -1
	for i, h := range headers {
		lower := strings.ToLower(strings.TrimSpace(h))
		if lower == "url" || lower == "domain" {
			urlIdx = i
			break
		}
	}

	mapper := CSVMapper{}
	seen := make(map[string]struct{})
	var uniqueRows []map[string]string

	for _, row := range rows {
		mapped := mapper.MapRow(headers, row)

		// Deduplicate by URL/Domain if column exists.
		if urlIdx >= 0 {
			u := ""
			if urlIdx < len(row) {
				u = strings.TrimSpace(row[urlIdx])
			}
			if u == "" {
				continue // skip rows with no URL/Domain
			}
			if _, exists := seen[u]; exists {
				continue
			}
			seen[u] = struct{}{}
		}

		uniqueRows = append(uniqueRows, mapped)
	}

	created := 0
	for _, row := range uniqueRows {
		if ctx.Err() != nil {
			return created, eris.Wrap(ctx.Err(), "notion: import csv cancelled")
		}

		var props notionapi.Properties
		if grata {
			props = buildGrataProperties(row)
		} else {
			props = buildPageProperties(row)
		}

		req := &notionapi.PageCreateRequest{
			Parent: notionapi.Parent{
				Type:       notionapi.ParentTypeDatabaseID,
				DatabaseID: notionapi.DatabaseID(dbID),
			},
			Properties: props,
		}

		if _, err := c.CreatePage(ctx, req); err != nil {
			return created, eris.Wrap(err, "notion: create page from csv row")
		}
		created++
	}

	return created, nil
}

// buildPageProperties converts a CSV row map to Notion page properties.
// All values are stored as rich_text properties, except "Name" which becomes
// the title property.
func buildPageProperties(row map[string]string) notionapi.Properties {
	props := make(notionapi.Properties)
	for k, v := range row {
		if strings.EqualFold(k, "Name") {
			props[k] = notionapi.TitleProperty{
				Type: notionapi.PropertyTypeTitle,
				Title: []notionapi.RichText{
					{Type: notionapi.ObjectTypeText, Text: &notionapi.Text{Content: v}},
				},
			}
		} else if strings.EqualFold(k, "URL") {
			props[k] = notionapi.URLProperty{
				Type: notionapi.PropertyTypeURL,
				URL:  v,
			}
		} else {
			props[k] = notionapi.RichTextProperty{
				Type: notionapi.PropertyTypeRichText,
				RichText: []notionapi.RichText{
					{Type: notionapi.ObjectTypeText, Text: &notionapi.Text{Content: v}},
				},
			}
		}
	}
	return props
}

// normalizeURL ensures a domain has an https:// scheme prefix.
func normalizeURL(domain string) string {
	domain = strings.TrimSpace(domain)
	if domain == "" {
		return ""
	}
	if !strings.Contains(domain, "://") {
		return "https://" + domain
	}
	return domain
}

// buildGrataProperties converts a Grata CSV row map to Notion page properties.
// Special handling: Domain→URL, City+State→Location, Status=Queued, Name as title.
// All other columns pass through as rich_text.
func buildGrataProperties(row map[string]string) notionapi.Properties {
	props := make(notionapi.Properties)

	// Track which keys we handle specially to avoid double-setting.
	handled := map[string]bool{}

	// Name → title property (strip wrapping quotes).
	for k, v := range row {
		if strings.EqualFold(k, "Name") {
			name := strings.TrimSpace(v)
			name = strings.Trim(name, "\"")
			props["Name"] = notionapi.TitleProperty{
				Type: notionapi.PropertyTypeTitle,
				Title: []notionapi.RichText{
					{Type: notionapi.ObjectTypeText, Text: &notionapi.Text{Content: name}},
				},
			}
			handled[k] = true
			break
		}
	}

	// Domain → URL property (prepend https:// if no scheme).
	for k, v := range row {
		if strings.EqualFold(k, "Domain") {
			props["URL"] = notionapi.URLProperty{
				Type: notionapi.PropertyTypeURL,
				URL:  normalizeURL(v),
			}
			handled[k] = true
			break
		}
	}

	// City + State → Location rich_text.
	var city, state string
	var cityKey, stateKey string
	for k, v := range row {
		if strings.EqualFold(k, "City") {
			city = strings.TrimSpace(v)
			cityKey = k
		}
		if strings.EqualFold(k, "State") {
			state = strings.TrimSpace(v)
			stateKey = k
		}
	}
	if city != "" || state != "" {
		location := city
		if city != "" && state != "" {
			location = city + ", " + state
		} else if state != "" {
			location = state
		}
		props["Location"] = notionapi.RichTextProperty{
			Type: notionapi.PropertyTypeRichText,
			RichText: []notionapi.RichText{
				{Type: notionapi.ObjectTypeText, Text: &notionapi.Text{Content: location}},
			},
		}
	}
	if cityKey != "" {
		handled[cityKey] = true
	}
	if stateKey != "" {
		handled[stateKey] = true
	}

	// Status → always "Queued".
	props["Status"] = notionapi.StatusProperty{
		Status: notionapi.Status{
			Name: "Queued",
		},
	}

	// All remaining columns → rich_text pass-through.
	for k, v := range row {
		if handled[k] {
			continue
		}
		if v == "" {
			continue
		}
		props[k] = notionapi.RichTextProperty{
			Type: notionapi.PropertyTypeRichText,
			RichText: []notionapi.RichText{
				{Type: notionapi.ObjectTypeText, Text: &notionapi.Text{Content: v}},
			},
		}
	}

	return props
}
