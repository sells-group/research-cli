package scraper

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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

// eiaPlantSource is the source identifier for EIA-860 plant data.
const eiaPlantSource = "eia"

// eiaPlantBatchSize is the number of rows per BulkUpsert batch.
const eiaPlantBatchSize = 5000

// eiaPlantYear is the default EIA-860 data year.
const eiaPlantYear = 2024

// eiaPlantExclude lists attribute keys stored in dedicated columns.
var eiaPlantExclude = map[string]bool{
	"Plant Code":  true,
	"Plant Name":  true,
	"Latitude":    true,
	"Longitude":   true,
	"Sector Name": true,
}

// EIAPlants scrapes power plant locations from the EIA-860 annual survey,
// replacing the ArcGIS-paginated HIFLD power plants source with a bulk
// XLSX download.
type EIAPlants struct {
	downloadURL string // override for testing; empty uses EIA default
	year        int    // override for testing; 0 uses eiaPlantYear
}

// Name implements GeoScraper.
func (e *EIAPlants) Name() string { return "eia_plants" }

// Table implements GeoScraper.
func (e *EIAPlants) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (e *EIAPlants) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (e *EIAPlants) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (e *EIAPlants) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}

// Sync implements GeoScraper.
func (e *EIAPlants) Sync(ctx context.Context, pool db.Pool, ft fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", e.Name()))
	log.Info("starting EIA-860 plants sync")

	year := eiaPlantYear
	if e.year > 0 {
		year = e.year
	}

	// Download the EIA-860 ZIP.
	url := e.buildURL(year)
	zipPath := filepath.Join(tempDir, fmt.Sprintf("eia860_%d.zip", year))

	if _, err := ft.DownloadToFile(ctx, url, zipPath); err != nil {
		return nil, eris.Wrap(err, "eia_plants: download")
	}

	// Extract ZIP.
	extractDir := filepath.Join(tempDir, fmt.Sprintf("eia860_%d", year))
	if err := os.MkdirAll(extractDir, 0o750); err != nil {
		return nil, eris.Wrap(err, "eia_plants: create extract dir")
	}

	if _, err := fetcher.ExtractZIP(zipPath, extractDir); err != nil {
		return nil, eris.Wrap(err, "eia_plants: extract ZIP")
	}

	// Find plant XLSX file (pattern: contains "Plant" in filename).
	plantFile, err := findEIAFile(extractDir, "plant")
	if err != nil {
		return nil, eris.Wrap(err, "eia_plants: find plant file")
	}

	// Parse plant data.
	plants, err := parseEIAPlantXLSX(plantFile)
	if err != nil {
		return nil, eris.Wrap(err, "eia_plants: parse plant XLSX")
	}

	// Find generator XLSX file to aggregate capacity and fuel type per plant.
	genFile, err := findEIAFile(extractDir, "generator")
	if err != nil {
		log.Warn("generator file not found, proceeding without capacity data", zap.Error(err))
	} else {
		genData, parseErr := parseEIAGeneratorXLSX(genFile)
		if parseErr != nil {
			log.Warn("failed to parse generator file", zap.Error(parseErr))
		} else {
			enrichPlantsWithGenerators(plants, genData)
		}
	}

	// Build rows and upsert.
	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, uErr := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        e.Table(),
			Columns:      infraCols,
			ConflictKeys: infraConflictKeys,
		}, batch)
		if uErr != nil {
			return eris.Wrap(uErr, "eia_plants: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	for _, p := range plants {
		if p.lat == 0 && p.lon == 0 {
			continue
		}

		row := []any{
			p.name,
			"power_plant",
			p.fuelType,
			p.capacity,
			p.lat,
			p.lon,
			eiaPlantSource,
			fmt.Sprintf("eia860/%d", p.code),
			p.properties(),
		}
		batch = append(batch, row)

		if len(batch) >= eiaPlantBatchSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("EIA-860 plants sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}

func (e *EIAPlants) buildURL(year int) string {
	if e.downloadURL != "" {
		return e.downloadURL
	}
	return fmt.Sprintf("https://www.eia.gov/electricity/data/eia860/xls/eia860%d.zip", year)
}

// eiaPlant holds parsed plant data.
type eiaPlant struct {
	code     int
	name     string
	state    string
	county   string
	lat      float64
	lon      float64
	sector   string
	fuelType string
	capacity float64
	extra    map[string]string
}

func (p *eiaPlant) properties() []byte {
	props := make(map[string]any)
	for k, v := range p.extra {
		if v != "" {
			props[k] = v
		}
	}
	if p.state != "" {
		props["state"] = p.state
	}
	if p.county != "" {
		props["county"] = p.county
	}
	if p.sector != "" {
		props["sector"] = p.sector
	}
	data, _ := json.Marshal(props)
	return data
}

// eiaGeneratorAgg holds aggregated generator data for a plant.
type eiaGeneratorAgg struct {
	totalCapacity float64
	primaryFuel   string
	maxCapFuel    float64
}

// findEIAFile finds the first file in the directory whose name (lowercase)
// contains the given keyword and ends with .xlsx.
func findEIAFile(dir, keyword string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", eris.Wrap(err, "read directory")
	}
	kw := strings.ToLower(keyword)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := strings.ToLower(e.Name())
		if strings.Contains(name, kw) && strings.HasSuffix(name, ".xlsx") {
			return filepath.Join(dir, e.Name()), nil
		}
	}
	return "", eris.Errorf("no %s .xlsx file found in %s", keyword, dir)
}

// parseEIAPlantXLSX parses the EIA-860 Plant XLSX file.
func parseEIAPlantXLSX(path string) ([]*eiaPlant, error) {
	wb, err := xlsx.OpenFile(path)
	if err != nil {
		return nil, eris.Wrap(err, "open plant XLSX")
	}

	// Find the sheet (typically first sheet or named "Plant").
	sheet := findSheet(wb, "plant")
	if sheet == nil {
		return nil, eris.New("no 'Plant' sheet found in XLSX")
	}

	// Find header row — look for "Plant Code" in column headers.
	headerIdx := -1
	for i := 0; i < sheet.MaxRow && i < 10; i++ {
		row := sheet.Row(i)
		if row == nil {
			continue
		}
		for j := 0; j < len(row.Cells); j++ {
			if strings.TrimSpace(row.Cells[j].Value) == "Plant Code" {
				headerIdx = i
				break
			}
		}
		if headerIdx >= 0 {
			break
		}
	}
	if headerIdx < 0 {
		return nil, eris.New("could not find header row with 'Plant Code'")
	}

	// Build column index.
	headerRow := sheet.Row(headerIdx)
	colIdx := make(map[string]int)
	for j := 0; j < len(headerRow.Cells); j++ {
		colIdx[strings.TrimSpace(headerRow.Cells[j].Value)] = j
	}

	// Parse data rows.
	var plants []*eiaPlant
	for i := headerIdx + 1; i < sheet.MaxRow; i++ {
		row := sheet.Row(i)
		if row == nil {
			continue
		}

		codeStr := xlsxVal(row, colIdx, "Plant Code")
		code, err := strconv.Atoi(codeStr)
		if err != nil || code == 0 {
			continue
		}

		lat, _ := strconv.ParseFloat(xlsxVal(row, colIdx, "Latitude"), 64)
		lon, _ := strconv.ParseFloat(xlsxVal(row, colIdx, "Longitude"), 64)

		extra := make(map[string]string)
		for col, idx := range colIdx {
			if !eiaPlantExclude[col] && idx < len(row.Cells) {
				v := strings.TrimSpace(row.Cells[idx].Value)
				if v != "" {
					extra[col] = v
				}
			}
		}

		plants = append(plants, &eiaPlant{
			code:   code,
			name:   xlsxVal(row, colIdx, "Plant Name"),
			state:  xlsxVal(row, colIdx, "State"),
			county: xlsxVal(row, colIdx, "County"),
			lat:    lat,
			lon:    lon,
			sector: xlsxVal(row, colIdx, "Sector Name"),
			extra:  extra,
		})
	}

	return plants, nil
}

// parseEIAGeneratorXLSX parses the EIA-860 Generator XLSX file and aggregates
// capacity and primary fuel type per plant code.
func parseEIAGeneratorXLSX(path string) (map[int]*eiaGeneratorAgg, error) {
	wb, err := xlsx.OpenFile(path)
	if err != nil {
		return nil, eris.Wrap(err, "open generator XLSX")
	}

	sheet := findSheet(wb, "operable")
	if sheet == nil {
		sheet = findSheet(wb, "generator")
	}
	if sheet == nil {
		return nil, eris.New("no generator sheet found")
	}

	// Find header row.
	headerIdx := -1
	for i := 0; i < sheet.MaxRow && i < 10; i++ {
		row := sheet.Row(i)
		if row == nil {
			continue
		}
		for j := 0; j < len(row.Cells); j++ {
			if strings.TrimSpace(row.Cells[j].Value) == "Plant Code" {
				headerIdx = i
				break
			}
		}
		if headerIdx >= 0 {
			break
		}
	}
	if headerIdx < 0 {
		return nil, eris.New("no header row found in generator sheet")
	}

	headerRow := sheet.Row(headerIdx)
	colIdx := make(map[string]int)
	for j := 0; j < len(headerRow.Cells); j++ {
		colIdx[strings.TrimSpace(headerRow.Cells[j].Value)] = j
	}

	result := make(map[int]*eiaGeneratorAgg)
	for i := headerIdx + 1; i < sheet.MaxRow; i++ {
		row := sheet.Row(i)
		if row == nil {
			continue
		}

		codeStr := xlsxVal(row, colIdx, "Plant Code")
		code, err := strconv.Atoi(codeStr)
		if err != nil || code == 0 {
			continue
		}

		mw, _ := strconv.ParseFloat(xlsxVal(row, colIdx, "Nameplate Capacity (MW)"), 64)
		fuel := xlsxVal(row, colIdx, "Energy Source 1")

		agg, ok := result[code]
		if !ok {
			agg = &eiaGeneratorAgg{}
			result[code] = agg
		}
		agg.totalCapacity += mw
		if mw > agg.maxCapFuel {
			agg.maxCapFuel = mw
			agg.primaryFuel = fuel
		}
	}

	return result, nil
}

// enrichPlantsWithGenerators adds capacity and fuel type from generator data.
func enrichPlantsWithGenerators(plants []*eiaPlant, genData map[int]*eiaGeneratorAgg) {
	for _, p := range plants {
		if agg, ok := genData[p.code]; ok {
			p.capacity = agg.totalCapacity
			p.fuelType = agg.primaryFuel
		}
	}
}

// findSheet finds a sheet whose name (lowercase) contains the keyword.
func findSheet(wb *xlsx.File, keyword string) *xlsx.Sheet {
	kw := strings.ToLower(keyword)
	for _, s := range wb.Sheets {
		if strings.Contains(strings.ToLower(s.Name), kw) {
			return s
		}
	}
	return nil
}

// xlsxVal extracts a trimmed string value from a row by column name.
func xlsxVal(row *xlsx.Row, colIdx map[string]int, col string) string {
	idx, ok := colIdx[col]
	if !ok || idx >= len(row.Cells) {
		return ""
	}
	return strings.TrimSpace(row.Cells[idx].Value)
}
