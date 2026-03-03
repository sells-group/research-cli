package dataset

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
)

const (
	censusGeoBaseURL = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer"
)

// censusGeoColumns defines the target DB columns in upsert order.
var censusGeoColumns = []string{
	"fips_state", "fips_county", "state_name", "county_name", "state_abbr",
	"ansi_code", "aland", "awater", "aland_sqmi", "awater_sqmi",
	"intptlat", "intptlong", "updated_at",
}

// CensusGeo implements the Census Bureau Gazetteer dataset for state and county
// geographic reference data (area, centroids). Populates fed_data.fips_codes.
type CensusGeo struct{}

// Name implements Dataset.
func (d *CensusGeo) Name() string { return "census_geo" }

// Table implements Dataset.
func (d *CensusGeo) Table() string { return "fed_data.fips_codes" }

// Phase implements Dataset.
func (d *CensusGeo) Phase() Phase { return Phase1 }

// Cadence implements Dataset.
func (d *CensusGeo) Cadence() Cadence { return Quarterly }

// ShouldRun implements Dataset.
func (d *CensusGeo) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return QuarterlyWithLag(now, lastSync, 1)
}

// Sync downloads Census Gazetteer state and county files, then upserts into fips_codes.
func (d *CensusGeo) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))

	year, err := d.detectYear(ctx, f)
	if err != nil {
		return nil, eris.Wrap(err, "census_geo: detect gazetteer year")
	}
	log.Info("using gazetteer year", zap.Int("year", year))

	// Stage 1: Download and parse states.
	stateRows, stateMap, err := d.syncStates(ctx, f, tempDir, year, log)
	if err != nil {
		return nil, err
	}

	// Stage 2: Download and parse counties.
	countyRows, err := d.syncCounties(ctx, f, tempDir, year, stateMap, log)
	if err != nil {
		return nil, err
	}

	// Stage 3: Upsert all rows.
	allRows := append(stateRows, countyRows...)
	n, err := db.BulkUpsert(ctx, pool, censusGeoUpsertCfg(), allRows)
	if err != nil {
		return nil, eris.Wrap(err, "census_geo: bulk upsert")
	}

	log.Info("census_geo sync complete",
		zap.Int("states", len(stateRows)),
		zap.Int("counties", len(countyRows)),
		zap.Int64("upserted", n),
	)

	return &SyncResult{
		RowsSynced: n,
		Metadata: map[string]any{
			"year":     year,
			"states":   len(stateRows),
			"counties": len(countyRows),
		},
	}, nil
}

// detectYear determines the gazetteer year by trying the current year first,
// falling back to previous year on HTTP 404.
func (d *CensusGeo) detectYear(ctx context.Context, f fetcher.Fetcher) (int, error) {
	year := time.Now().Year()
	url := stateGazURL(year)

	// Try HEAD request to check availability.
	body, err := f.Download(ctx, url)
	if err != nil {
		// Check if it's an HTTP error we can interpret as 404.
		if isNotFoundErr(err) {
			return year - 1, nil
		}
		// For other errors, try the fallback year.
		return year - 1, nil
	}
	_ = body.Close()
	return year, nil
}

// isNotFoundErr checks if an error contains a 404 indication.
func isNotFoundErr(err error) bool {
	return strings.Contains(err.Error(), "404") ||
		strings.Contains(err.Error(), "not found") ||
		strings.Contains(err.Error(), http.StatusText(http.StatusNotFound))
}

// syncStates downloads and parses the state gazetteer file.
// Returns parsed rows and a state FIPS → name lookup map.
func (d *CensusGeo) syncStates(ctx context.Context, f fetcher.Fetcher, tempDir string, year int, log *zap.Logger) ([][]any, map[string]string, error) {
	url := stateGazURL(year)
	zipPath := filepath.Join(tempDir, fmt.Sprintf("%d_Gaz_state_national.zip", year))

	log.Info("downloading state gazetteer", zap.String("url", url))
	if _, err := f.DownloadToFile(ctx, url, zipPath); err != nil {
		return nil, nil, eris.Wrap(err, "census_geo: download state gazetteer")
	}
	defer os.Remove(zipPath) //nolint:errcheck

	extracted, err := fetcher.ExtractZIP(zipPath, tempDir)
	if err != nil {
		return nil, nil, eris.Wrap(err, "census_geo: extract state ZIP")
	}
	if len(extracted) == 0 {
		return nil, nil, eris.New("census_geo: state ZIP contained no files")
	}
	defer func() {
		for _, p := range extracted {
			_ = os.Remove(p)
		}
	}()

	file, err := os.Open(extracted[0]) // #nosec G304 -- path from ExtractZIP in trusted temp dir
	if err != nil {
		return nil, nil, eris.Wrap(err, "census_geo: open state gazetteer")
	}
	defer file.Close() //nolint:errcheck

	rows, stateMap, err := d.parseStates(file)
	if err != nil {
		return nil, nil, eris.Wrap(err, "census_geo: parse state gazetteer")
	}

	log.Info("parsed state gazetteer", zap.Int("rows", len(rows)))
	return rows, stateMap, nil
}

// syncCounties downloads and parses the county gazetteer file.
func (d *CensusGeo) syncCounties(ctx context.Context, f fetcher.Fetcher, tempDir string, year int, stateMap map[string]string, log *zap.Logger) ([][]any, error) {
	url := countyGazURL(year)
	zipPath := filepath.Join(tempDir, fmt.Sprintf("%d_Gaz_counties_national.zip", year))

	log.Info("downloading county gazetteer", zap.String("url", url))
	if _, err := f.DownloadToFile(ctx, url, zipPath); err != nil {
		return nil, eris.Wrap(err, "census_geo: download county gazetteer")
	}
	defer os.Remove(zipPath) //nolint:errcheck

	extracted, err := fetcher.ExtractZIP(zipPath, tempDir)
	if err != nil {
		return nil, eris.Wrap(err, "census_geo: extract county ZIP")
	}
	if len(extracted) == 0 {
		return nil, eris.New("census_geo: county ZIP contained no files")
	}
	defer func() {
		for _, p := range extracted {
			_ = os.Remove(p)
		}
	}()

	file, err := os.Open(extracted[0]) // #nosec G304 -- path from ExtractZIP in trusted temp dir
	if err != nil {
		return nil, eris.Wrap(err, "census_geo: open county gazetteer")
	}
	defer file.Close() //nolint:errcheck

	rows, err := d.parseCounties(file, stateMap)
	if err != nil {
		return nil, eris.Wrap(err, "census_geo: parse county gazetteer")
	}

	log.Info("parsed county gazetteer", zap.Int("rows", len(rows)))
	return rows, nil
}

// parseStates reads a delimited state gazetteer and returns DB rows + state name map.
// Supports both pipe-delimited (2025+) and tab-delimited formats.
func (d *CensusGeo) parseStates(r io.Reader) ([][]any, map[string]string, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	// Read header.
	if !scanner.Scan() {
		return nil, nil, eris.New("census_geo: empty state file")
	}
	headerLine := scanner.Text()
	delim := detectDelimiter(headerLine)
	colIdx := mapColumnsNormalized(strings.Split(headerLine, delim))

	now := time.Now()
	var rows [][]any
	stateMap := make(map[string]string) // FIPS state code → state name

	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), delim)
		geoid := strings.TrimSpace(getColN(fields, colIdx, "GEOID"))
		if geoid == "" {
			continue
		}

		fipsState := fmt.Sprintf("%02s", geoid)
		if len(fipsState) > 2 {
			fipsState = fipsState[:2]
		}
		stateName := strings.TrimSpace(getColN(fields, colIdx, "NAME"))
		stateAbbr := strings.TrimSpace(getColN(fields, colIdx, "USPS"))

		stateMap[fipsState] = stateName

		rows = append(rows, []any{
			fipsState, // fips_state
			"000",     // fips_county (state sentinel)
			stateName, // state_name
			nil,       // county_name
			stateAbbr, // state_abbr
			nil,       // ansi_code (not in state file)
			parseInt64OrNil(getColN(fields, colIdx, "ALAND")),         // aland
			parseInt64OrNil(getColN(fields, colIdx, "AWATER")),        // awater
			parseFloat64OrNil(getColN(fields, colIdx, "ALAND_SQMI")),  // aland_sqmi
			parseFloat64OrNil(getColN(fields, colIdx, "AWATER_SQMI")), // awater_sqmi
			parseFloat64OrNil(getColN(fields, colIdx, "INTPTLAT")),    // intptlat
			parseFloat64OrNil(getColN(fields, colIdx, "INTPTLONG")),   // intptlong
			now, // updated_at
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, nil, eris.Wrap(err, "census_geo: scan state file")
	}

	return rows, stateMap, nil
}

// parseCounties reads a delimited county gazetteer and returns DB rows.
// Supports both pipe-delimited (2025+) and tab-delimited formats.
func (d *CensusGeo) parseCounties(r io.Reader, stateMap map[string]string) ([][]any, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	// Read header.
	if !scanner.Scan() {
		return nil, eris.New("census_geo: empty county file")
	}
	headerLine := scanner.Text()
	delim := detectDelimiter(headerLine)
	colIdx := mapColumnsNormalized(strings.Split(headerLine, delim))

	now := time.Now()
	var rows [][]any

	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), delim)
		geoid := strings.TrimSpace(getColN(fields, colIdx, "GEOID"))
		if len(geoid) < 5 {
			continue
		}

		fipsState := geoid[:2]
		fipsCounty := geoid[2:5]
		stateAbbr := strings.TrimSpace(getColN(fields, colIdx, "USPS"))
		countyName := strings.TrimSpace(getColN(fields, colIdx, "NAME"))
		ansiCode := strings.TrimSpace(getColN(fields, colIdx, "ANSICODE"))

		stateName := stateMap[fipsState]

		rows = append(rows, []any{
			fipsState,            // fips_state
			fipsCounty,           // fips_county
			stateName,            // state_name
			countyName,           // county_name
			stateAbbr,            // state_abbr
			nilIfEmpty(ansiCode), // ansi_code
			parseInt64OrNil(getColN(fields, colIdx, "ALAND")),         // aland
			parseInt64OrNil(getColN(fields, colIdx, "AWATER")),        // awater
			parseFloat64OrNil(getColN(fields, colIdx, "ALAND_SQMI")),  // aland_sqmi
			parseFloat64OrNil(getColN(fields, colIdx, "AWATER_SQMI")), // awater_sqmi
			parseFloat64OrNil(getColN(fields, colIdx, "INTPTLAT")),    // intptlat
			parseFloat64OrNil(getColN(fields, colIdx, "INTPTLONG")),   // intptlong
			now, // updated_at
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, eris.Wrap(err, "census_geo: scan county file")
	}

	return rows, nil
}

// censusGeoUpsertCfg returns the upsert configuration for fips_codes.
func censusGeoUpsertCfg() db.UpsertConfig {
	return db.UpsertConfig{
		Table:        "fed_data.fips_codes",
		Columns:      censusGeoColumns,
		ConflictKeys: []string{"fips_state", "fips_county"},
	}
}

// stateGazURL builds the URL for the state gazetteer ZIP.
func stateGazURL(year int) string {
	return fmt.Sprintf("%s/%d_Gazetteer/%d_Gaz_state_national.zip", censusGeoBaseURL, year, year)
}

// countyGazURL builds the URL for the county gazetteer ZIP.
func countyGazURL(year int) string {
	return fmt.Sprintf("%s/%d_Gazetteer/%d_Gaz_counties_national.zip", censusGeoBaseURL, year, year)
}

// parseInt64OrNil parses a trimmed string as int64, returning nil on failure.
func parseInt64OrNil(s string) any {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	v := parseInt64Or(s, 0)
	if v == 0 && s != "0" {
		return nil
	}
	return v
}

// parseFloat64OrNil parses a trimmed string as float64, returning nil on failure.
// Handles leading +/- signs from Census Gazetteer lat/long values.
func parseFloat64OrNil(s string) any {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	v := parseFloat64Or(s, 0)
	if v == 0 && s != "0" && s != "0.0" && s != "+0" && s != "-0" {
		return nil
	}
	return v
}

// nilIfEmpty returns nil if s is empty, otherwise s.
func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// detectDelimiter returns "|" if the header contains pipes, otherwise "\t".
// Census Gazetteer files switched from tab-delimited to pipe-delimited in 2025.
func detectDelimiter(header string) string {
	if strings.Contains(header, "|") {
		return "|"
	}
	return "\t"
}
