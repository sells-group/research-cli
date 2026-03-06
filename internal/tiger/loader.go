package tiger

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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

	// Split products into three categories.
	var national, perState, perCounty []Product
	for _, p := range products {
		switch {
		case p.National:
			national = append(national, p)
		case p.PerCounty:
			perCounty = append(perCounty, p)
		default:
			perState = append(perState, p)
		}
	}

	// All per-state and per-county products need state child tables.
	allPerState := append(perState, perCounty...)

	// Prepare template tables and create parent tables before loading any data.
	if !opts.DryRun {
		if err := PrepareTemplates(ctx, pool); err != nil {
			return eris.Wrap(err, "tiger: prepare templates")
		}
		if err := CreateParentTables(ctx, pool, products); err != nil {
			return eris.Wrap(err, "tiger: create parent tables")
		}
	}

	// Cache table columns for filtering shapefile data to valid target columns.
	colCache := &columnCache{pool: pool}

	// Load national products first (sequentially).
	for _, p := range national {
		if err := loadProduct(ctx, pool, p, "", "us", opts, colCache); err != nil {
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

	// Load per-state products (PLACE, COUSUB) in parallel.
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(opts.Concurrency)

	for _, stateAbbr := range states {
		fips := FIPSCodes[stateAbbr]

		// Create state-partitioned tables before loading data.
		if !opts.DryRun {
			if err := CreateStateTables(ctx, pool, stateAbbr, allPerState); err != nil {
				return eris.Wrapf(err, "tiger: create state tables for %s", stateAbbr)
			}
		}

		for _, p := range perState {
			stAbbr := stateAbbr
			stFIPS := fips
			g.Go(func() error {
				return loadProduct(gCtx, pool, p, stAbbr, stFIPS, opts, colCache)
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

	// Load per-county products (EDGES, FACES, ADDR, FEATNAMES).
	// Each county file is downloaded and appended to the state child table.
	if len(perCounty) > 0 {
		for _, stateAbbr := range states {
			stateFIPS := FIPSCodes[stateAbbr]

			countyFIPS, err := CountyFIPSForState(ctx, pool, stateFIPS)
			if err != nil {
				return eris.Wrapf(err, "tiger: get county FIPS for %s", stateAbbr)
			}

			log.Info("loading per-county products",
				zap.String("state", stateAbbr),
				zap.Int("counties", len(countyFIPS)),
				zap.Int("products", len(perCounty)),
			)

			// Check incremental — skip entire state if already loaded.
			if opts.Incremental {
				allLoaded := true
				for _, p := range perCounty {
					loaded, chkErr := isLoaded(ctx, pool, stateFIPS, p.Table, opts.Year)
					if chkErr != nil {
						return chkErr
					}
					if !loaded {
						allLoaded = false
						break
					}
				}
				if allLoaded {
					log.Debug("per-county products already loaded, skipping", zap.String("state", stateAbbr))
					continue
				}
			}

			// Truncate state child tables before loading per-county data.
			if !opts.DryRun {
				for _, p := range perCounty {
					if err := TruncateTable(ctx, pool, p, stateAbbr); err != nil {
						log.Warn("truncate failed (table may be empty)", zap.Error(err))
					}
				}
			}

			// Download and load each county file, parallelized across counties.
			cg, cgCtx := errgroup.WithContext(ctx)
			cg.SetLimit(opts.Concurrency)

			for _, p := range perCounty {
				start := time.Now()
				var totalRows int64
				var failedCounties atomic.Int32

				for _, cFIPS := range countyFIPS {
					countyFIPSVal := cFIPS
					g2 := func() error {
						url := DownloadURL(p, opts.Year, countyFIPSVal)
						destDir := fmt.Sprintf("%s/%s/%s/%s", opts.TempDir, stateFIPS, strings.ToLower(p.Name), countyFIPSVal)
						shpPath, dlErr := Download(cgCtx, url, destDir)
						if dlErr != nil {
							log.Warn("skipping county: download failed",
								zap.String("product", p.Name), zap.String("county", countyFIPSVal), zap.Error(dlErr))
							failedCounties.Add(1)
							return nil
						}

						result, parseErr := ParseShapefile(shpPath, p)
						if parseErr != nil {
							log.Warn("skipping county: parse failed",
								zap.String("product", p.Name), zap.String("county", countyFIPSVal), zap.Error(parseErr))
							failedCounties.Add(1)
							return nil
						}

						if opts.DryRun || len(result.Rows) == 0 {
							return nil
						}

						// Rename, set statefp, and compute derived columns before filtering.
						result = RenameColumns(result, p.Table)
						result = SetStateFIPS(result, stateFIPS)
						result = ComputeDerivedColumns(result, p.Table)

						// Filter to columns that exist in the target table.
						validCols, colErr := colCache.get(cgCtx, p.Table)
						if colErr != nil {
							return colErr
						}
						filtered := FilterToTable(result, validCols)

						n, copyErr := BulkLoad(cgCtx, pool, p, stateAbbr, filtered.Columns, filtered.Rows, opts.BatchSize)
						if copyErr != nil {
							return copyErr
						}
						totalRows += n

						// Clean up county files to free disk space.
						_ = os.RemoveAll(destDir)
						return nil
					}
					cg.Go(g2)
				}

				if err := cg.Wait(); err != nil {
					return err
				}

				if fc := failedCounties.Load(); fc > 0 {
					log.Warn("some counties failed for product",
						zap.String("product", p.Name), zap.String("state", stateAbbr), zap.Int32("failed", fc))
				}

				duration := time.Since(start)
				// Only record if rows were loaded — 0-row entries poison incremental checks.
				if !opts.DryRun && totalRows > 0 {
					if recErr := recordLoad(ctx, pool, stateFIPS, stateAbbr, p.Table, opts.Year, int(totalRows), int(duration.Milliseconds())); recErr != nil {
						log.Warn("failed to record load status", zap.Error(recErr))
					}
				}

				log.Info("per-county product loaded",
					zap.String("product", p.Name),
					zap.String("state", stateAbbr),
					zap.Int64("rows", totalRows),
					zap.Int("counties", len(countyFIPS)),
					zap.Duration("duration", duration),
				)

				// Reset errgroup for next product.
				cg, cgCtx = errgroup.WithContext(ctx)
				cg.SetLimit(opts.Concurrency)
			}
		}
	}

	// Populate lookup tables.
	if !opts.DryRun {
		if err := PopulateLookups(ctx, pool); err != nil {
			return eris.Wrap(err, "tiger: populate lookups")
		}
	}

	log.Info("TIGER data load complete")
	return nil
}

// columnCache caches table column sets to avoid repeated DB queries.
type columnCache struct {
	pool  db.Pool
	mu    sync.Mutex
	cache map[string]map[string]bool
}

func (c *columnCache) get(ctx context.Context, tableName string) (map[string]bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cache == nil {
		c.cache = make(map[string]map[string]bool)
	}
	if cols, ok := c.cache[tableName]; ok {
		return cols, nil
	}

	cols, err := TableColumns(ctx, c.pool, tableName)
	if err != nil {
		return nil, err
	}
	c.cache[tableName] = cols
	return cols, nil
}

// loadProduct downloads, parses, and loads a single product for a given state.
// Used for national and per-state products; per-county products use inline logic in Load.
func loadProduct(ctx context.Context, pool db.Pool, product Product, stateAbbr, stateFIPS string, opts LoadOptions, colCache *columnCache) error {
	log := zap.L().With(
		zap.String("component", "tiger.loader"),
		zap.String("product", product.Name),
		zap.String("state", stateAbbr),
	)

	tableName := product.Table
	if (product.PerState || product.PerCounty) && stateAbbr != "" {
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

	// Parse shapefile — reads all columns from the file.
	result, err := ParseShapefile(shpPath, product)
	if err != nil {
		return eris.Wrapf(err, "tiger: parse %s for %s", product.Name, stateAbbr)
	}

	log.Info("shapefile parsed", zap.Int("rows", len(result.Rows)))

	if opts.DryRun {
		log.Info("dry run — skipping load", zap.Int("rows", len(result.Rows)))
		return nil
	}

	// Rename columns (e.g., zcta5ce20 → zcta5ce), set statefp for products
	// that omit it, and compute derived columns (e.g., cntyidfp) before filtering.
	result = RenameColumns(result, product.Table)
	result = SetStateFIPS(result, stateFIPS)
	result = ComputeDerivedColumns(result, product.Table)

	// Filter to columns that exist in the target table.
	validCols, colErr := colCache.get(ctx, product.Table)
	if colErr != nil {
		return colErr
	}
	filtered := FilterToTable(result, validCols)

	// Truncate existing data before reload.
	if err := TruncateTable(ctx, pool, product, stateAbbr); err != nil {
		// Table may not have data yet, but warn so connection/permission errors are visible.
		log.Warn("truncate failed (table may be empty)", zap.Error(err))
	}

	// Bulk load via COPY.
	loaded, err := BulkLoad(ctx, pool, product, stateAbbr, filtered.Columns, filtered.Rows, opts.BatchSize)
	if err != nil {
		return err
	}

	// Clean up downloaded files to free disk space.
	if rmErr := os.RemoveAll(destDir); rmErr != nil {
		log.Warn("failed to clean up download dir", zap.String("dir", destDir), zap.Error(rmErr))
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

// LoadProduct downloads, parses, and loads a single national or per-state TIGER product.
// Exported for use by Temporal activities. Returns the number of rows loaded.
func LoadProduct(ctx context.Context, pool db.Pool, product Product, stateAbbr, stateFIPS string, opts LoadOptions) (int64, error) {
	colCache := &columnCache{pool: pool}

	log := zap.L().With(
		zap.String("component", "tiger.loader"),
		zap.String("product", product.Name),
		zap.String("state", stateAbbr),
	)

	tableName := product.Table
	if (product.PerState || product.PerCounty) && stateAbbr != "" {
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

	start := time.Now()

	url := DownloadURL(product, opts.Year, stateFIPS)
	destDir := fmt.Sprintf("%s/%s/%s", opts.TempDir, stateFIPS, strings.ToLower(product.Name))
	shpPath, err := Download(ctx, url, destDir)
	if err != nil {
		return 0, eris.Wrapf(err, "tiger: download %s for %s", product.Name, stateAbbr)
	}

	result, err := ParseShapefile(shpPath, product)
	if err != nil {
		return 0, eris.Wrapf(err, "tiger: parse %s for %s", product.Name, stateAbbr)
	}

	result = RenameColumns(result, product.Table)
	result = SetStateFIPS(result, stateFIPS)
	result = ComputeDerivedColumns(result, product.Table)

	validCols, colErr := colCache.get(ctx, product.Table)
	if colErr != nil {
		return 0, colErr
	}
	filtered := FilterToTable(result, validCols)

	if err := TruncateTable(ctx, pool, product, stateAbbr); err != nil {
		log.Warn("truncate failed (table may be empty)", zap.Error(err))
	}

	loaded, err := BulkLoad(ctx, pool, product, stateAbbr, filtered.Columns, filtered.Rows, opts.BatchSize)
	if err != nil {
		return 0, err
	}

	// Clean up downloaded files to free disk space.
	_ = os.RemoveAll(destDir)

	duration := time.Since(start)
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

// LoadCountyProducts loads all per-county products for a state.
// Exported for use by Temporal activities. Returns total rows loaded.
func LoadCountyProducts(ctx context.Context, pool db.Pool, stateAbbr, stateFIPS string, products []Product, opts LoadOptions) (int64, error) {
	colCache := &columnCache{pool: pool}
	log := zap.L().With(
		zap.String("component", "tiger.loader"),
		zap.String("state", stateAbbr),
	)

	countyFIPS, err := CountyFIPSForState(ctx, pool, stateFIPS)
	if err != nil {
		return 0, eris.Wrapf(err, "tiger: get county FIPS for %s", stateAbbr)
	}

	var grandTotal int64

	for _, p := range products {
		if !p.PerCounty {
			continue
		}

		// Check incremental.
		if opts.Incremental {
			loaded, chkErr := isLoaded(ctx, pool, stateFIPS, p.Table, opts.Year)
			if chkErr != nil {
				return grandTotal, chkErr
			}
			if loaded {
				log.Debug("already loaded, skipping", zap.String("product", p.Name))
				continue
			}
		}

		// Truncate state child table before loading.
		if err := TruncateTable(ctx, pool, p, stateAbbr); err != nil {
			log.Warn("truncate failed (table may be empty)", zap.Error(err))
		}

		start := time.Now()
		var totalRows int64

		for i, cFIPS := range countyFIPS {
			if ctx.Err() != nil {
				return grandTotal, ctx.Err()
			}

			url := DownloadURL(p, opts.Year, cFIPS)
			destDir := fmt.Sprintf("%s/%s/%s/%s", opts.TempDir, stateFIPS, strings.ToLower(p.Name), cFIPS)

			shpPath, dlErr := Download(ctx, url, destDir)
			if dlErr != nil {
				log.Warn("county download failed, skipping",
					zap.String("product", p.Name), zap.String("county", cFIPS), zap.Error(dlErr))
				continue
			}

			result, parseErr := ParseShapefile(shpPath, p)
			if parseErr != nil {
				log.Warn("county parse failed, skipping",
					zap.String("product", p.Name), zap.String("county", cFIPS), zap.Error(parseErr))
				_ = os.RemoveAll(destDir)
				continue
			}

			if len(result.Rows) == 0 {
				_ = os.RemoveAll(destDir)
				continue
			}

			result = RenameColumns(result, p.Table)
			result = SetStateFIPS(result, stateFIPS)
			result = ComputeDerivedColumns(result, p.Table)

			validCols, colErr := colCache.get(ctx, p.Table)
			if colErr != nil {
				return grandTotal, colErr
			}
			filtered := FilterToTable(result, validCols)

			n, copyErr := BulkLoad(ctx, pool, p, stateAbbr, filtered.Columns, filtered.Rows, opts.BatchSize)
			if copyErr != nil {
				return grandTotal, eris.Wrapf(copyErr, "tiger: load %s county %s", p.Name, cFIPS)
			}
			totalRows += n

			// Clean up county files to free disk space.
			_ = os.RemoveAll(destDir)

			if (i+1)%25 == 0 || i == len(countyFIPS)-1 {
				log.Info("county progress",
					zap.String("product", p.Name),
					zap.Int("loaded", i+1),
					zap.Int("total", len(countyFIPS)),
					zap.Int64("rows", totalRows),
				)
			}
		}

		duration := time.Since(start)
		// Only record if rows were loaded — 0-row entries poison incremental checks.
		if totalRows > 0 {
			if recErr := recordLoad(ctx, pool, stateFIPS, stateAbbr, p.Table, opts.Year, int(totalRows), int(duration.Milliseconds())); recErr != nil {
				log.Warn("failed to record load status", zap.Error(recErr))
			}
		}

		grandTotal += totalRows
		log.Info("per-county product loaded",
			zap.String("product", p.Name),
			zap.Int64("rows", totalRows),
			zap.Int("counties", len(countyFIPS)),
			zap.Duration("duration", duration),
		)
	}

	return grandTotal, nil
}
