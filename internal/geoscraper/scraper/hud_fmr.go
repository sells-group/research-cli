package scraper

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"github.com/tealeg/xlsx/v2"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

// fmrExclude lists XLSX columns stored in dedicated DB columns.
var fmrExclude = map[string]bool{
	"fips":       true,
	"state":      true,
	"countyname": true,
	"fmr_0":      true,
	"fmr_1":      true,
	"fmr_2":      true,
	"fmr_3":      true,
	"fmr_4":      true,
	"year":       true,
}

var fmrCols = []string{
	"fips", "state_fips", "county_name", "year",
	"fmr_0br", "fmr_1br", "fmr_2br", "fmr_3br", "fmr_4br",
	"source", "source_id", "properties",
}

var fmrConflictKeys = []string{"fips", "year"}

// HUDFMR scrapes Fair Market Rent data from HUD.
type HUDFMR struct {
	baseURL string // override for testing; empty uses default HUD endpoint
}

// Name implements GeoScraper.
func (s *HUDFMR) Name() string { return "hud_fmr" }

// Table implements GeoScraper.
func (s *HUDFMR) Table() string { return "geo.fair_market_rents" }

// Category implements GeoScraper.
func (s *HUDFMR) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (s *HUDFMR) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (s *HUDFMR) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}

// csvFMRInt parses a string to int, stripping any dollar signs and commas.
func csvFMRInt(s string) int {
	s = strings.ReplaceAll(s, "$", "")
	s = strings.ReplaceAll(s, ",", "")
	s = strings.TrimSpace(s)
	n, _ := strconv.Atoi(s)
	return n
}

// xlsxColIndex builds a header-name → column-index map from an XLSX row.
func xlsxColIndex(row *xlsx.Row) map[string]int {
	m := make(map[string]int)
	for i, cell := range row.Cells {
		m[strings.TrimSpace(cell.String())] = i
	}
	return m
}

// xlsxString safely extracts a trimmed string from an XLSX row by column index.
func xlsxString(row *xlsx.Row, idx int) string {
	if idx < 0 || idx >= len(row.Cells) {
		return ""
	}
	return strings.TrimSpace(row.Cells[idx].String())
}

// xlsxProperties builds a JSONB object from all non-excluded XLSX columns.
func xlsxProperties(row *xlsx.Row, header *xlsx.Row, exclude map[string]bool) json.RawMessage {
	m := make(map[string]string)
	for i, cell := range row.Cells {
		if i >= len(header.Cells) {
			break
		}
		key := strings.TrimSpace(header.Cells[i].String())
		if key == "" || exclude[key] {
			continue
		}
		val := strings.TrimSpace(cell.String())
		if val != "" {
			m[key] = val
		}
	}
	b, _ := json.Marshal(m)
	return b
}

// Sync implements GeoScraper.
func (s *HUDFMR) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting hud_fmr sync")

	url := s.baseURL
	if url == "" {
		url = "https://www.huduser.gov/portal/datasets/fmr/fmr2026/FY26_FMRs.xlsx"
	}

	xlsxPath := filepath.Join(tempDir, "fmr.xlsx")
	if _, err := f.DownloadToFile(ctx, url, xlsxPath); err != nil {
		return nil, eris.Wrap(err, "hud_fmr: download")
	}

	xlFile, err := xlsx.OpenFile(xlsxPath)
	if err != nil {
		return nil, eris.Wrap(err, "hud_fmr: open xlsx")
	}

	if len(xlFile.Sheets) == 0 {
		return nil, eris.New("hud_fmr: no sheets in xlsx")
	}
	sheet := xlFile.Sheets[0]

	if len(sheet.Rows) < 2 {
		return nil, eris.New("hud_fmr: no data rows in xlsx")
	}

	headerRow := sheet.Rows[0]
	cols := xlsxColIndex(headerRow)

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, uErr := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        s.Table(),
			Columns:      fmrCols,
			ConflictKeys: fmrConflictKeys,
		}, batch)
		if uErr != nil {
			return eris.Wrap(uErr, "hud_fmr: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	for i := 1; i < len(sheet.Rows); i++ {
		row := sheet.Rows[i]

		fips := xlsxString(row, cols["fips"])
		if fips == "" {
			continue
		}

		// Extract state FIPS from the "state" column (2-digit code).
		stateFIPS := xlsxString(row, cols["state"])
		if stateFIPS == "" && len(fips) >= 2 {
			stateFIPS = fips[:2]
		}

		year := 2026 // Current FMR year from the dataset
		sourceID := fips + "_" + strconv.Itoa(year)

		batch = append(batch, []any{
			fips,
			stateFIPS,
			xlsxString(row, cols["countyname"]),
			year,
			csvFMRInt(xlsxString(row, cols["fmr_0"])),
			csvFMRInt(xlsxString(row, cols["fmr_1"])),
			csvFMRInt(xlsxString(row, cols["fmr_2"])),
			csvFMRInt(xlsxString(row, cols["fmr_3"])),
			csvFMRInt(xlsxString(row, cols["fmr_4"])),
			"hud_fmr",
			sourceID,
			xlsxProperties(row, headerRow, fmrExclude),
		})

		if len(batch) >= hifldBatchSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("hud_fmr sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
