package dataset

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/db"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/fedsync/transform"
	"github.com/sells-group/research-cli/internal/fetcher"
)

const (
	econCensusBatchSize = 5000
	econCensusBaseURL   = "https://api.census.gov/data"
)

// econCensusYears are the Economic Census years available.
var econCensusYears = []int{2017, 2022}

// EconCensus implements the Economic Census dataset.
type EconCensus struct {
	cfg *config.Config
}

// Name implements Dataset.
func (d *EconCensus) Name() string { return "econ_census" }

// Table implements Dataset.
func (d *EconCensus) Table() string { return "fed_data.economic_census" }

// Phase implements Dataset.
func (d *EconCensus) Phase() Phase { return Phase1 }

// Cadence implements Dataset.
func (d *EconCensus) Cadence() Cadence { return Annual }

// ShouldRun implements Dataset.
func (d *EconCensus) ShouldRun(now time.Time, lastSync *time.Time) bool {
	// Only runs every 5 years when data is available (2017, 2022, 2027...)
	// Check if current year is a census release year (typically 2 years after census)
	if !AnnualAfter(now, lastSync, time.March) {
		return false
	}
	// If never synced, always run to backfill
	if lastSync == nil {
		return true
	}
	// Check if a new census year has become available since last sync
	for _, y := range econCensusYears {
		releaseYear := y + 2 // data releases ~2 years after
		if now.Year() >= releaseYear && lastSync.Year() < releaseYear {
			return true
		}
	}
	// Also check for future census years (2027 -> release 2029, etc.)
	nextCensus := 2027
	for nextCensus+2 <= now.Year() {
		if lastSync.Year() < nextCensus+2 {
			return true
		}
		nextCensus += 5
	}
	return false
}

// Sync fetches and loads Economic Census data.
func (d *EconCensus) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "econ_census"))

	apiKey := ""
	if d.cfg != nil {
		apiKey = d.cfg.Fedsync.CensusKey
	}
	if apiKey == "" {
		return nil, eris.New("econ_census: Census API key not configured (fedsync.census_api_key)")
	}

	var totalRows int64

	for _, year := range econCensusYears {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		log.Info("fetching Economic Census data", zap.Int("year", year))

		rows, err := d.fetchYear(ctx, f, apiKey, year)
		if err != nil {
			log.Warn("skipping Economic Census year", zap.Int("year", year), zap.Error(err))
			continue
		}

		if len(rows) == 0 {
			continue
		}

		n, err := d.upsertRows(ctx, pool, rows)
		if err != nil {
			return nil, eris.Wrapf(err, "econ_census: upsert year %d", year)
		}
		totalRows += n
		log.Info("processed Economic Census year", zap.Int("year", year), zap.Int64("rows", n))
	}

	return &SyncResult{
		RowsSynced: totalRows,
		Metadata:   map[string]any{"years": econCensusYears},
	}, nil
}

func (d *EconCensus) fetchYear(ctx context.Context, f fetcher.Fetcher, apiKey string, year int) ([][]any, error) {
	// Census API: get establishment count, receipts, payroll, employees by NAICS and geography
	// 2022+ uses NAICS2022 variable; earlier years use NAICS2017
	naicsVar := "NAICS2017"
	if year >= 2022 {
		naicsVar = "NAICS2022"
	}
	url := fmt.Sprintf(
		"%s/%d/ecnbasic?get=GEO_ID,%s,ESTAB,RCPTOT,PAYANN,EMP&for=state:*&key=%s",
		econCensusBaseURL,
		year,
		naicsVar,
		apiKey,
	)

	body, err := f.Download(ctx, url)
	if err != nil {
		return nil, eris.Wrapf(err, "econ_census: fetch year %d", year)
	}
	defer body.Close() //nolint:errcheck

	data, err := io.ReadAll(body)
	if err != nil {
		return nil, eris.Wrap(err, "econ_census: read response")
	}

	return d.parseResponse(data, year)
}

func (d *EconCensus) parseResponse(data []byte, year int) ([][]any, error) {
	// Census API returns JSON array of arrays: [[header], [row1], [row2], ...]
	var raw [][]string
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, eris.Wrap(err, "econ_census: unmarshal JSON")
	}

	if len(raw) < 2 {
		return nil, nil // no data rows
	}

	header := raw[0]
	colIdx := make(map[string]int, len(header))
	for i, col := range header {
		colIdx[col] = i
	}

	var rows [][]any
	seen := make(map[string]int) // conflict key → index in rows (dedup)
	for _, record := range raw[1:] {
		// 2022+ Census API returns NAICS2022; earlier years return NAICS2017
		naics := getColIdx(record, colIdx, "NAICS2017")
		if naics == "" {
			naics = getColIdx(record, colIdx, "NAICS2022")
		}
		if !transform.IsRelevantNAICS(naics) {
			continue
		}
		naics = transform.NormalizeNAICS(naics)
		if len(naics) > 6 {
			naics = naics[:6] // truncate to fit VARCHAR(6)
		}

		geoID := getColIdx(record, colIdx, "GEO_ID")

		row := []any{
			int16(year),
			geoID,
			naics,
			parseIntOr(getColIdx(record, colIdx, "ESTAB"), 0),
			parseInt64Or(getColIdx(record, colIdx, "RCPTOT"), 0),
			parseInt64Or(getColIdx(record, colIdx, "PAYANN"), 0),
			parseIntOr(getColIdx(record, colIdx, "EMP"), 0),
		}

		// Deduplicate by conflict key to avoid
		// "ON CONFLICT DO UPDATE cannot affect row a second time".
		key := fmt.Sprintf("%d|%s|%s", year, geoID, naics)
		if idx, exists := seen[key]; exists {
			rows[idx] = row // overwrite with latest
			continue
		}
		seen[key] = len(rows)
		rows = append(rows, row)
	}

	return rows, nil
}

func (d *EconCensus) upsertRows(ctx context.Context, pool db.Pool, rows [][]any) (int64, error) {
	columns := []string{"year", "geo_id", "naics", "estab", "rcptot", "payann", "emp"}
	conflictKeys := []string{"year", "geo_id", "naics"}

	var totalRows int64
	for i := 0; i < len(rows); i += econCensusBatchSize {
		end := i + econCensusBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        "fed_data.economic_census",
			Columns:      columns,
			ConflictKeys: conflictKeys,
		}, rows[i:end])
		if err != nil {
			return totalRows, eris.Wrap(err, "econ_census: bulk upsert")
		}
		totalRows += n
	}

	return totalRows, nil
}

// getColIdx gets a value from a string slice by column name index.
func getColIdx(record []string, colIdx map[string]int, name string) string {
	idx, ok := colIdx[name]
	if !ok || idx >= len(record) {
		return ""
	}
	return record[idx]
}
