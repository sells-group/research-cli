package dataset

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
)

var lodesCols = []string{
	"year", "state", "w_county_fips", "h_county_fips",
	"total_jobs", "jobs_age_29_or_younger", "jobs_age_30_to_54", "jobs_age_55_plus",
	"jobs_earn_1250_or_less", "jobs_earn_1251_to_3333", "jobs_earn_3334_or_more",
}

var lodesConflictKeys = []string{"year", "state", "w_county_fips", "h_county_fips"}

const lodesBatchSize = 5000

// lodesCountyAgg holds aggregated job counts for a county-to-county pair.
type lodesCountyAgg struct {
	S000 int
	SA01 int
	SA02 int
	SA03 int
	SE01 int
	SE02 int
	SE03 int
}

// lodesStates is the list of US state + DC 2-letter abbreviations (lowercase).
var lodesStates = []string{
	"al", "ak", "az", "ar", "ca", "co", "ct", "de", "dc", "fl",
	"ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me",
	"md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh",
	"nj", "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "ri",
	"sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy",
}

// LEHDLODES syncs Census LEHD/LODES origin-destination commuter flow data,
// aggregated from block to county level.
type LEHDLODES struct {
	baseURL string   // override for testing
	states  []string // override for testing; nil uses all states
}

// Name implements Dataset.
func (d *LEHDLODES) Name() string { return "lehd_lodes" }

// Table implements Dataset.
func (d *LEHDLODES) Table() string { return "fed_data.lehd_lodes" }

// Phase implements Dataset.
func (d *LEHDLODES) Phase() Phase { return Phase3 }

// Cadence implements Dataset.
func (d *LEHDLODES) Cadence() Cadence { return Annual }

// ShouldRun implements Dataset.
func (d *LEHDLODES) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return AnnualAfter(now, lastSync, time.June)
}

// Sync downloads LODES OD data for all states, aggregates to county level, and upserts.
func (d *LEHDLODES) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))
	log.Info("starting lehd_lodes sync")

	// LODES data lags 2-4 years. Probe first state to find the latest available year.
	year := time.Now().Year() - 2

	states := d.states
	if states == nil {
		states = lodesStates
	}

	if d.baseURL == "" {
		// Probe with first state to find latest available year.
		probeState := states[0]
		var found bool
		for offset := 2; offset <= 5; offset++ {
			tryYear := time.Now().Year() - offset
			probeURL := fmt.Sprintf("https://lehd.ces.census.gov/data/lodes/LODES8/%s/od/%s_od_main_JT00_%d.csv.gz",
				probeState, probeState, tryYear)
			probePath := filepath.Join(tempDir, "lodes_probe.csv.gz")
			if _, err := f.DownloadToFile(ctx, probeURL, probePath); err == nil {
				year = tryYear
				log.Info("found LODES data year", zap.Int("year", year), zap.String("probe_state", probeState))
				_ = os.Remove(probePath)
				found = true
				break
			}
		}
		if !found {
			log.Warn("lehd_lodes: could not find available year via probe, using default", zap.Int("year", year))
		}
	}

	var totalRows atomic.Int64

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(3)

	for _, st := range states {
		g.Go(func() error {
			n, err := d.syncState(gctx, pool, f, tempDir, st, year, log)
			if err != nil {
				return err
			}
			totalRows.Add(n)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	total := totalRows.Load()
	log.Info("lehd_lodes sync complete", zap.Int64("rows", total))
	return &SyncResult{RowsSynced: total}, nil
}

func (d *LEHDLODES) syncState(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir, state string, year int, log *zap.Logger) (int64, error) {
	url := d.baseURL
	if url == "" {
		url = fmt.Sprintf("https://lehd.ces.census.gov/data/lodes/LODES8/%s/od/%s_od_main_JT00_%d.csv.gz",
			state, state, year)
	}

	gzPath := filepath.Join(tempDir, fmt.Sprintf("lodes_%s.csv.gz", state))
	if _, err := f.DownloadToFile(ctx, url, gzPath); err != nil {
		// 404 is expected for some states/years — skip gracefully.
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "not found") {
			log.Warn("lehd_lodes: state not available, skipping", zap.String("state", state))
			return 0, nil
		}
		return 0, eris.Wrapf(err, "lehd_lodes: download %s", state)
	}

	gzFile, err := os.Open(gzPath) // #nosec G304 -- path from controlled temp dir
	if err != nil {
		return 0, eris.Wrapf(err, "lehd_lodes: open %s", state)
	}
	defer gzFile.Close() //nolint:errcheck

	gzReader, err := gzip.NewReader(gzFile)
	if err != nil {
		return 0, eris.Wrapf(err, "lehd_lodes: gzip %s", state)
	}
	defer gzReader.Close() //nolint:errcheck

	reader := csv.NewReader(gzReader)
	header, err := reader.Read()
	if err != nil {
		return 0, eris.Wrapf(err, "lehd_lodes: read %s header", state)
	}
	colIdx := mapColumns(header)

	// Aggregate block-level OD data to county level.
	agg := make(map[string]*lodesCountyAgg)

	for {
		row, rErr := reader.Read()
		if rErr == io.EOF {
			break
		}
		if rErr != nil {
			return 0, eris.Wrapf(rErr, "lehd_lodes: read %s row", state)
		}

		wGeo := getCol(row, colIdx, "w_geocode")
		hGeo := getCol(row, colIdx, "h_geocode")
		if len(wGeo) < 5 || len(hGeo) < 5 {
			continue
		}

		key := wGeo[:5] + "|" + hGeo[:5]
		entry := agg[key]
		if entry == nil {
			entry = &lodesCountyAgg{}
			agg[key] = entry
		}

		entry.S000 += parseIntOr(getCol(row, colIdx, "s000"), 0)
		entry.SA01 += parseIntOr(getCol(row, colIdx, "sa01"), 0)
		entry.SA02 += parseIntOr(getCol(row, colIdx, "sa02"), 0)
		entry.SA03 += parseIntOr(getCol(row, colIdx, "sa03"), 0)
		entry.SE01 += parseIntOr(getCol(row, colIdx, "se01"), 0)
		entry.SE02 += parseIntOr(getCol(row, colIdx, "se02"), 0)
		entry.SE03 += parseIntOr(getCol(row, colIdx, "se03"), 0)
	}

	// Flush aggregated county pairs.
	var batch [][]any
	var totalRows int64

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, uErr := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        d.Table(),
			Columns:      lodesCols,
			ConflictKeys: lodesConflictKeys,
		}, batch)
		if uErr != nil {
			return eris.Wrapf(uErr, "lehd_lodes: upsert %s", state)
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	for key, entry := range agg {
		parts := strings.SplitN(key, "|", 2)
		wCounty := parts[0]
		hCounty := parts[1]

		batch = append(batch, []any{
			year, state, wCounty, hCounty,
			entry.S000, entry.SA01, entry.SA02, entry.SA03,
			entry.SE01, entry.SE02, entry.SE03,
		})

		if len(batch) >= lodesBatchSize {
			if err := flush(); err != nil {
				return 0, err
			}
		}
	}

	if err := flush(); err != nil {
		return 0, err
	}

	log.Info("lehd_lodes state synced", zap.String("state", state), zap.Int64("rows", totalRows))
	return totalRows, nil
}
