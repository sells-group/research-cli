package tiger

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/db"
)

// LoadOptions configures the TIGER data load.
type LoadOptions struct {
	Year        int      // TIGER/Line data year (default 2024)
	States      []string // State abbreviations; empty = all 50 + DC
	Tables      []string // Product names; empty = all required
	TempDir     string   // Download directory
	Concurrency int      // Parallel state downloads (default 3)
	BatchSize   int      // COPY batch size (default 50,000)
	Incremental bool     // Skip already-loaded combos (default true)
	DryRun      bool     // Validate without loading
}

// StatusRow represents a row from tiger_data.load_status.
type StatusRow struct {
	StateFIPS  string
	StateAbbr  string
	TableName  string
	Year       int
	RowCount   int
	LoadedAt   time.Time
	DurationMs int
}

// Load downloads and loads TIGER/Line data for the given options.
func Load(ctx context.Context, pool db.Pool, opts LoadOptions) error {
	if opts.Year == 0 {
		opts.Year = 2024
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 3
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = defaultBatchSize
	}
	if opts.TempDir == "" {
		opts.TempDir = "/tmp/tiger"
	}

	log := zap.L().With(
		zap.String("component", "tiger.loader"),
		zap.Int("year", opts.Year),
	)

	// Determine which states to load.
	states := opts.States
	if len(states) == 0 {
		states = AllStateAbbrs()
	}

	// Determine which products to load.
	var products []Product
	if len(opts.Tables) > 0 {
		for _, name := range opts.Tables {
			p, ok := ProductByName(name)
			if !ok {
				return eris.Errorf("tiger: unknown product %q", name)
			}
			products = append(products, p)
		}
	} else {
		products = Products
	}

	// Split into national and per-state products.
	var national, perState []Product
	for _, p := range products {
		if p.National {
			national = append(national, p)
		} else {
			perState = append(perState, p)
		}
	}

	// Load national products first (sequentially).
	for _, p := range national {
		if _, err := LoadProduct(ctx, pool, p, "", "us", opts); err != nil {
			return eris.Wrapf(err, "tiger: load national product %s", p.Name)
		}
	}

	log.Info("national products loaded", zap.Int("count", len(national)))

	// Pre-validate all state abbreviations before starting any work.
	for _, stateAbbr := range states {
		if _, ok := FIPSCodes[stateAbbr]; !ok {
			return eris.Errorf("tiger: unknown state %q", stateAbbr)
		}
	}

	// Load per-state products in parallel.
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(opts.Concurrency)

	for _, stateAbbr := range states {
		fips := FIPSCodes[stateAbbr]

		// Create state-partitioned tables before loading data.
		if !opts.DryRun {
			if err := CreateStateTables(ctx, pool, stateAbbr, perState); err != nil {
				return eris.Wrapf(err, "tiger: create state tables for %s", stateAbbr)
			}
		}

		for _, p := range perState {
			stAbbr := stateAbbr
			stFIPS := fips
			g.Go(func() error {
				_, err := LoadProduct(gCtx, pool, p, stAbbr, stFIPS, opts)
				return err
			})
		}
	}

	if err := g.Wait(); err != nil {
		return err
	}

	log.Info("per-state products loaded",
		zap.Int("states", len(states)),
		zap.Int("products", len(perState)),
	)

	// Populate lookup tables.
	if !opts.DryRun {
		if err := PopulateLookups(ctx, pool); err != nil {
			return eris.Wrap(err, "tiger: populate lookups")
		}
	}

	log.Info("TIGER data load complete")
	return nil
}

// LoadProduct downloads, parses, and loads a single TIGER product.
// Exported for use by Temporal activities. Returns the number of rows loaded.
// Per-county products (EDGES, FACES, ADDR, FEATNAMES) are handled by downloading
// each county file separately and loading into the state's child table.
func LoadProduct(ctx context.Context, pool db.Pool, product Product, stateAbbr, stateFIPS string, opts LoadOptions) (int64, error) {
	log := zap.L().With(
		zap.String("component", "tiger.loader"),
		zap.String("product", product.Name),
		zap.String("state", stateAbbr),
	)

	tableName := product.Table
	if !product.National && stateAbbr != "" {
		tableName = fmt.Sprintf("%s_%s", strings.ToLower(stateAbbr), product.Table)
	}

	// Check if already loaded (incremental mode).
	if opts.Incremental {
		loaded, err := isLoaded(ctx, pool, stateFIPS, product.Table, opts.Year)
		if err != nil {
			return 0, err
		}
		if loaded {
			log.Debug("already loaded, skipping", zap.String("table", tableName))
			return 0, nil
		}
	}

	// Per-county products: download each county file and load into the state child table.
	if product.PerCounty && stateFIPS != "us" && stateFIPS != "" {
		return loadCountyProduct(ctx, pool, product, stateAbbr, stateFIPS, opts)
	}

	start := time.Now()

	// Download shapefile.
	url := DownloadURL(product, opts.Year, stateFIPS)
	destDir := fmt.Sprintf("%s/%s/%s", opts.TempDir, stateFIPS, strings.ToLower(product.Name))
	shpPath, err := Download(ctx, url, destDir)
	if err != nil {
		return 0, eris.Wrapf(err, "tiger: download %s for %s", product.Name, stateAbbr)
	}

	log.Info("shapefile downloaded", zap.String("path", shpPath))

	// Parse shapefile.
	rows, err := ParseShapefile(shpPath, product)
	if err != nil {
		return 0, eris.Wrapf(err, "tiger: parse %s for %s", product.Name, stateAbbr)
	}

	log.Info("shapefile parsed", zap.Int("rows", len(rows)))

	if opts.DryRun {
		log.Info("dry run — skipping load", zap.Int("rows", len(rows)))
		return 0, nil
	}

	// Truncate existing data before reload.
	if err := TruncateTable(ctx, pool, product, stateAbbr); err != nil {
		// Table may not have data yet, but warn so connection/permission errors are visible.
		log.Warn("truncate failed (table may be empty)", zap.Error(err))
	}

	// Bulk load via COPY.
	loaded, err := BulkLoad(ctx, pool, product, stateAbbr, rows, opts.BatchSize)
	if err != nil {
		return 0, err
	}

	// Clean up downloaded files to free disk space.
	if err := os.RemoveAll(destDir); err != nil {
		log.Warn("failed to clean up download dir", zap.String("dir", destDir), zap.Error(err))
	}

	duration := time.Since(start)

	// Record in load_status.
	if err := recordLoad(ctx, pool, stateFIPS, stateAbbr, product.Table, opts.Year, int(loaded), int(duration.Milliseconds())); err != nil {
		log.Warn("failed to record load status", zap.Error(err))
	}

	log.Info("product loaded",
		zap.String("table", tableName),
		zap.Int64("rows", loaded),
		zap.Duration("duration", duration),
	)

	return loaded, nil
}

// loadCountyProduct handles per-county TIGER products by downloading each county's
// shapefile separately and loading into the state's child table.
// Census publishes EDGES, FACES, ADDR, and FEATNAMES as per-county files using
// 5-digit county FIPS codes (e.g., tl_2024_01001_edges.zip for Autauga County, AL).
func loadCountyProduct(ctx context.Context, pool db.Pool, product Product, stateAbbr, stateFIPS string, opts LoadOptions) (int64, error) {
	log := zap.L().With(
		zap.String("component", "tiger.loader"),
		zap.String("product", product.Name),
		zap.String("state", stateAbbr),
	)

	// Query county FIPS codes from loaded county_all data.
	counties, err := CountyFIPSForState(ctx, pool, stateFIPS)
	if err != nil {
		return 0, eris.Wrapf(err, "tiger: get counties for %s", stateAbbr)
	}

	log.Info("loading per-county product",
		zap.Int("counties", len(counties)),
	)

	start := time.Now()

	// Truncate state child table once before loading all counties.
	if !opts.DryRun {
		if err := TruncateTable(ctx, pool, product, stateAbbr); err != nil {
			log.Warn("truncate failed (table may be empty)", zap.Error(err))
		}
	}

	var totalRows int64
	for i, countyFIPS := range counties {
		// Check for context cancellation between counties.
		if ctx.Err() != nil {
			return totalRows, ctx.Err()
		}

		url := DownloadURL(product, opts.Year, countyFIPS)
		destDir := fmt.Sprintf("%s/%s/%s/%s", opts.TempDir, stateFIPS, strings.ToLower(product.Name), countyFIPS)

		shpPath, err := Download(ctx, url, destDir)
		if err != nil {
			// Some counties may not have files for all products — skip on download error.
			log.Warn("county download failed, skipping",
				zap.String("county", countyFIPS),
				zap.Error(err),
			)
			continue
		}

		rows, err := ParseShapefile(shpPath, product)
		if err != nil {
			log.Warn("county parse failed, skipping",
				zap.String("county", countyFIPS),
				zap.Error(err),
			)
			continue
		}

		if opts.DryRun {
			totalRows += int64(len(rows))
			continue
		}

		loaded, err := BulkLoad(ctx, pool, product, stateAbbr, rows, opts.BatchSize)
		if err != nil {
			return totalRows, eris.Wrapf(err, "tiger: load %s county %s", product.Name, countyFIPS)
		}
		totalRows += loaded

		// Clean up county files to free disk space.
		_ = os.RemoveAll(destDir)

		if (i+1)%25 == 0 || i == len(counties)-1 {
			log.Info("county progress",
				zap.Int("loaded", i+1),
				zap.Int("total", len(counties)),
				zap.Int64("rows", totalRows),
			)
		}
	}

	duration := time.Since(start)

	// Record single load_status entry for the entire state+product.
	// Only record if rows were actually loaded — 0-row entries poison incremental checks.
	if !opts.DryRun && totalRows > 0 {
		if err := recordLoad(ctx, pool, stateFIPS, stateAbbr, product.Table, opts.Year, int(totalRows), int(duration.Milliseconds())); err != nil {
			log.Warn("failed to record load status", zap.Error(err))
		}
	}

	log.Info("per-county product loaded",
		zap.Int64("total_rows", totalRows),
		zap.Int("counties", len(counties)),
		zap.Duration("duration", duration),
	)

	return totalRows, nil
}

// CountyFIPSForState returns sorted 5-digit county FIPS codes for a state.
// Requires tiger_data.county_all to be populated (loaded as a national product).
func CountyFIPSForState(ctx context.Context, pool db.Pool, stateFIPS string) ([]string, error) {
	rows, err := pool.Query(ctx,
		"SELECT statefp || countyfp FROM tiger_data.county_all WHERE statefp = $1 ORDER BY countyfp",
		stateFIPS)
	if err != nil {
		return nil, eris.Wrap(err, "tiger: query county FIPS")
	}
	defer rows.Close()

	var codes []string
	for rows.Next() {
		var code string
		if err := rows.Scan(&code); err != nil {
			return nil, eris.Wrap(err, "tiger: scan county FIPS")
		}
		codes = append(codes, code)
	}
	return codes, rows.Err()
}

// isLoaded checks if a product has already been loaded for a given state/year.
func isLoaded(ctx context.Context, pool db.Pool, stateFIPS, tableName string, year int) (bool, error) {
	var count int
	row := pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM tiger_data.load_status WHERE state_fips = $1 AND table_name = $2 AND year = $3",
		stateFIPS, tableName, year,
	)
	if err := row.Scan(&count); err != nil {
		return false, eris.Wrap(err, "tiger: check load status")
	}
	return count > 0, nil
}

// recordLoad inserts or updates the load_status record for a completed load.
func recordLoad(ctx context.Context, pool db.Pool, stateFIPS, stateAbbr, tableName string, year, rowCount, durationMs int) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO tiger_data.load_status (state_fips, state_abbr, table_name, year, row_count, duration_ms)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (state_fips, table_name, year) DO UPDATE SET
			state_abbr = EXCLUDED.state_abbr,
			row_count = EXCLUDED.row_count,
			loaded_at = now(),
			duration_ms = EXCLUDED.duration_ms`,
		stateFIPS, stateAbbr, tableName, year, rowCount, durationMs,
	)
	if err != nil {
		return eris.Wrap(err, "tiger: record load status")
	}
	return nil
}

// LoadStatus returns current TIGER data load status from tiger_data.load_status.
func LoadStatus(ctx context.Context, pool db.Pool) ([]StatusRow, error) {
	rows, err := pool.Query(ctx, `
		SELECT state_fips, state_abbr, table_name, year, row_count, loaded_at, COALESCE(duration_ms, 0)
		FROM tiger_data.load_status
		ORDER BY state_fips, table_name`)
	if err != nil {
		return nil, eris.Wrap(err, "tiger: query load status")
	}
	defer rows.Close()

	var status []StatusRow
	for rows.Next() {
		var sr StatusRow
		if err := rows.Scan(&sr.StateFIPS, &sr.StateAbbr, &sr.TableName, &sr.Year, &sr.RowCount, &sr.LoadedAt, &sr.DurationMs); err != nil {
			return nil, eris.Wrap(err, "tiger: scan load status row")
		}
		status = append(status, sr)
	}
	return status, rows.Err()
}
