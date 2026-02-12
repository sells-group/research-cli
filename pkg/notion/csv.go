package notion

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"time"

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

// ImportCSV reads a CSV file, deduplicates rows by the "URL" column, and creates
// a Notion page for each unique row. Pages are created at 3 req/s via a 334ms
// ticker. Returns the number of pages created.
func ImportCSV(ctx context.Context, c Client, dbID string, csvPath string) (int, error) {
	f, err := os.Open(csvPath)
	if err != nil {
		return 0, eris.Wrap(err, fmt.Sprintf("notion: open csv %s", csvPath))
	}
	defer f.Close()

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

	// Find URL column index for deduplication.
	urlIdx := -1
	for i, h := range headers {
		if strings.EqualFold(h, "URL") {
			urlIdx = i
			break
		}
	}

	mapper := CSVMapper{}
	seen := make(map[string]struct{})
	var uniqueRows []map[string]string

	for _, row := range rows {
		mapped := mapper.MapRow(headers, row)

		// Deduplicate by URL if column exists.
		if urlIdx >= 0 {
			u := ""
			if urlIdx < len(row) {
				u = strings.TrimSpace(row[urlIdx])
			}
			if u == "" {
				continue // skip rows with no URL
			}
			if _, exists := seen[u]; exists {
				continue
			}
			seen[u] = struct{}{}
		}

		uniqueRows = append(uniqueRows, mapped)
	}

	// Notion rate limit: 3 requests per second -> 334ms between requests.
	ticker := time.NewTicker(334 * time.Millisecond)
	defer ticker.Stop()

	created := 0
	for _, row := range uniqueRows {
		select {
		case <-ctx.Done():
			return created, eris.Wrap(ctx.Err(), "notion: import csv cancelled")
		case <-ticker.C:
		}

		props := buildPageProperties(row)

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
