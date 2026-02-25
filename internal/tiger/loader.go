package tiger

import (
	"context"
	"fmt"
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
		if err := loadProduct(ctx, pool, p, "", "us", opts); err != nil {
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
				return loadProduct(gCtx, pool, p, stAbbr, stFIPS, opts)
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

// loadProduct downloads, parses, and loads a single product for a given state.
func loadProduct(ctx context.Context, pool db.Pool, product Product, stateAbbr, stateFIPS string, opts LoadOptions) error {
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
			return err
		}
		if loaded {
			log.Debug("already loaded, skipping", zap.String("table", tableName))
			return nil
		}
	}

	start := time.Now()

	// Download shapefile.
	url := DownloadURL(product, opts.Year, stateFIPS)
	destDir := fmt.Sprintf("%s/%s/%s", opts.TempDir, stateFIPS, strings.ToLower(product.Name))
	shpPath, err := Download(ctx, url, destDir)
	if err != nil {
		return eris.Wrapf(err, "tiger: download %s for %s", product.Name, stateAbbr)
	}

	log.Info("shapefile downloaded", zap.String("path", shpPath))

	// Parse shapefile.
	rows, err := ParseShapefile(shpPath, product)
	if err != nil {
		return eris.Wrapf(err, "tiger: parse %s for %s", product.Name, stateAbbr)
	}

	log.Info("shapefile parsed", zap.Int("rows", len(rows)))

	if opts.DryRun {
		log.Info("dry run — skipping load", zap.Int("rows", len(rows)))
		return nil
	}

	// Truncate existing data before reload.
	if err := TruncateTable(ctx, pool, product, stateAbbr); err != nil {
		// Table may not have data yet, but warn so connection/permission errors are visible.
		log.Warn("truncate failed (table may be empty)", zap.Error(err))
	}

	// Bulk load via COPY.
	loaded, err := BulkLoad(ctx, pool, product, stateAbbr, rows, opts.BatchSize)
	if err != nil {
		return err
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

	return nil
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
