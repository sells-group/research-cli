package tiger

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
)

// PrepareTemplates relaxes NOT NULL constraints on tiger.* template columns
// that are derived (e.g., cntyidfp = statefp||countyfp) and not present in
// 2024+ TIGER shapefiles. This is required before child tables can inherit.
func PrepareTemplates(ctx context.Context, pool db.Pool) error {
	// These columns are NOT NULL in the template but don't exist in current shapefiles.
	// They are composite IDs computed by the standard loading scripts.
	alterations := []string{
		"ALTER TABLE tiger.county ALTER COLUMN cntyidfp DROP NOT NULL",
		"ALTER TABLE tiger.cousub ALTER COLUMN cosbidfp DROP NOT NULL",
		"ALTER TABLE tiger.place ALTER COLUMN plcidfp DROP NOT NULL",
		"ALTER TABLE tiger.zcta5 ALTER COLUMN statefp DROP NOT NULL",
		"ALTER TABLE tiger.zcta5 ALTER COLUMN zcta5ce DROP NOT NULL",
	}
	for _, sql := range alterations {
		if _, err := pool.Exec(ctx, sql); err != nil {
			// Ignore errors if column doesn't exist or constraint already dropped.
			zap.L().Debug("tiger: template alteration skipped", zap.String("sql", sql), zap.Error(err))
		}
	}
	return nil
}

// CreateParentTables creates parent tables in tiger_data for all products.
// Tables are created from the tiger.* template tables installed by postgis_tiger_geocoder,
// ensuring correct column types and schema compatibility with the geocoder functions.
// NOT NULL constraints (except on gid) are dropped since shapefiles may not populate all columns.
// Parent tables inherit from their tiger.* templates so the geocoder can find data via inheritance.
func CreateParentTables(ctx context.Context, pool db.Pool, products []Product) error {
	log := zap.L().With(zap.String("component", "tiger.schema"))

	for _, p := range products {
		tableName := pgx.Identifier{"tiger_data", p.Table}.Sanitize()
		templateName := pgx.Identifier{"tiger", p.Template()}.Sanitize()

		createSQL := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s (LIKE %s INCLUDING ALL)",
			tableName, templateName,
		)
		if _, err := pool.Exec(ctx, createSQL); err != nil {
			return eris.Wrapf(err, "tiger: create parent table tiger_data.%s from tiger.%s", p.Table, p.Template())
		}

		// zcta5: the 2020-vintage national shapefile lacks statefp (ZCTAs cross state
		// boundaries). Drop the PK and NOT NULL so the table can accept NULL statefp.
		// Skip inheritance since the inherited NOT NULL from tiger.zcta5 would block NULLs.
		if p.Template() == "zcta5" {
			relaxSQL := fmt.Sprintf(`DO $$ DECLARE r RECORD; BEGIN
				FOR r IN SELECT conname FROM pg_constraint WHERE conrelid = '%s.%s'::regclass AND contype = 'p' LOOP
					EXECUTE 'ALTER TABLE %s DROP CONSTRAINT ' || quote_ident(r.conname);
				END LOOP;
			END $$`, "tiger_data", p.Table, tableName)
			if _, err := pool.Exec(ctx, relaxSQL); err != nil {
				log.Warn("zcta5: failed to drop PK", zap.Error(err))
			}
			dropNullSQL := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN statefp DROP NOT NULL", tableName)
			if _, err := pool.Exec(ctx, dropNullSQL); err != nil {
				log.Warn("zcta5: failed to drop NOT NULL on statefp", zap.Error(err))
			}
			log.Debug("parent table ready (no inheritance)", zap.String("table", p.Table))
			continue
		}

		// Set up inheritance from tiger.{template} → tiger_data.{table}.
		// The geocoder queries tiger.* tables; inheritance makes our data visible.
		inheritSQL := fmt.Sprintf(
			"DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = '%s.%s'::regclass) THEN ALTER TABLE %s INHERIT %s; END IF; END $$",
			"tiger_data", p.Table, tableName, templateName,
		)
		if _, err := pool.Exec(ctx, inheritSQL); err != nil {
			return eris.Wrapf(err, "tiger: inherit tiger_data.%s → tiger.%s", p.Table, p.Template())
		}

		log.Debug("parent table ready", zap.String("table", p.Table), zap.String("template", p.Template()))
	}

	return nil
}

// CreateStateTables creates state-specific tables that inherit from parent tables.
// For each per-state product, creates tiger_data.{st}_{table} with proper inheritance
// and spatial indexes.
func CreateStateTables(ctx context.Context, pool db.Pool, stateAbbr string, products []Product) error {
	st := strings.ToLower(stateAbbr)
	log := zap.L().With(
		zap.String("component", "tiger.schema"),
		zap.String("state", stateAbbr),
	)

	for _, p := range products {
		if p.National {
			continue
		}

		childTable := fmt.Sprintf("%s_%s", st, p.Table)
		childQuoted := pgx.Identifier{"tiger_data", childTable}.Sanitize()
		parentQuoted := pgx.Identifier{"tiger_data", p.Table}.Sanitize()

		// Create child table with same structure as parent.
		createSQL := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s (LIKE %s INCLUDING ALL)",
			childQuoted, parentQuoted,
		)
		if _, err := pool.Exec(ctx, createSQL); err != nil {
			return eris.Wrapf(err, "tiger: create tiger_data.%s", childTable)
		}

		// Set up inheritance so the geocoder queries the parent and finds child rows.
		inheritSQL := fmt.Sprintf(
			"DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = '%s.%s'::regclass) THEN ALTER TABLE %s INHERIT %s; END IF; END $$",
			"tiger_data", childTable, childQuoted, parentQuoted,
		)
		if _, err := pool.Exec(ctx, inheritSQL); err != nil {
			return eris.Wrapf(err, "tiger: inherit tiger_data.%s → tiger_data.%s", childTable, p.Table)
		}

		// Create spatial index if product has geometry.
		if p.GeomType != "" {
			idxName := pgx.Identifier{fmt.Sprintf("idx_%s_the_geom", childTable)}.Sanitize()
			gistSQL := fmt.Sprintf(
				"CREATE INDEX IF NOT EXISTS %s ON %s USING GIST (the_geom)",
				idxName, childQuoted,
			)
			if _, err := pool.Exec(ctx, gistSQL); err != nil {
				return eris.Wrapf(err, "tiger: create GIST index on tiger_data.%s", childTable)
			}
		}

		// Product-specific indexes.
		switch p.Table {
		case "addr":
			idxName := pgx.Identifier{fmt.Sprintf("idx_%s_zip", childTable)}.Sanitize()
			zipSQL := fmt.Sprintf(
				"CREATE INDEX IF NOT EXISTS %s ON %s (zip)",
				idxName, childQuoted,
			)
			if _, err := pool.Exec(ctx, zipSQL); err != nil {
				return eris.Wrapf(err, "tiger: create zip index on tiger_data.%s", childTable)
			}
			// tlid index needed for zip_lookup_all join (addr.tlid = edges.tlid).
			tlidIdxName := pgx.Identifier{fmt.Sprintf("idx_%s_tlid", childTable)}.Sanitize()
			tlidSQL := fmt.Sprintf(
				"CREATE INDEX IF NOT EXISTS %s ON %s (tlid)",
				tlidIdxName, childQuoted,
			)
			if _, err := pool.Exec(ctx, tlidSQL); err != nil {
				return eris.Wrapf(err, "tiger: create tlid index on tiger_data.%s", childTable)
			}
		case "edges":
			idxName := pgx.Identifier{fmt.Sprintf("idx_%s_tlid", childTable)}.Sanitize()
			tlidSQL := fmt.Sprintf(
				"CREATE INDEX IF NOT EXISTS %s ON %s (tlid)",
				idxName, childQuoted,
			)
			if _, err := pool.Exec(ctx, tlidSQL); err != nil {
				return eris.Wrapf(err, "tiger: create tlid index on tiger_data.%s", childTable)
			}
		case "featnames":
			idxName := pgx.Identifier{fmt.Sprintf("idx_%s_tlid", childTable)}.Sanitize()
			tlidSQL := fmt.Sprintf(
				"CREATE INDEX IF NOT EXISTS %s ON %s (tlid)",
				idxName, childQuoted,
			)
			if _, err := pool.Exec(ctx, tlidSQL); err != nil {
				return eris.Wrapf(err, "tiger: create tlid index on tiger_data.%s", childTable)
			}
		}

		log.Debug("state table created", zap.String("table", childTable))
	}

	return nil
}

// TableColumns returns the set of column names for a tiger_data table, excluding "gid".
// Used to filter shapefile columns to only those accepted by the target table.
func TableColumns(ctx context.Context, pool db.Pool, tableName string) (map[string]bool, error) {
	rows, err := pool.Query(ctx,
		"SELECT column_name FROM information_schema.columns WHERE table_schema = 'tiger_data' AND table_name = $1 AND column_name != 'gid'",
		tableName,
	)
	if err != nil {
		return nil, eris.Wrapf(err, "tiger: query columns for tiger_data.%s", tableName)
	}
	defer rows.Close()

	cols := make(map[string]bool)
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, eris.Wrap(err, "tiger: scan column name")
		}
		cols[col] = true
	}
	return cols, rows.Err()
}

// FilterToTable filters a ParseResult to only include columns that exist in validCols.
// The geometry column ("the_geom") is preserved if present and valid.
func FilterToTable(result *ParseResult, validCols map[string]bool) *ParseResult {
	var keepIdx []int
	var keepCols []string

	for i, col := range result.Columns {
		if validCols[col] {
			keepIdx = append(keepIdx, i)
			keepCols = append(keepCols, col)
		}
	}

	filtered := &ParseResult{
		Columns: keepCols,
		Rows:    make([][]any, len(result.Rows)),
	}

	for i, row := range result.Rows {
		frow := make([]any, len(keepIdx))
		for j, idx := range keepIdx {
			frow[j] = row[idx]
		}
		filtered.Rows[i] = frow
	}

	return filtered
}

// derivedColumnDef specifies a column to compute by concatenating source columns.
type derivedColumnDef struct {
	Column  string   // target column name
	Sources []string // source columns to concatenate
}

// derivedColumnDefs maps product table names to computed column definitions.
// These columns exist in tiger.* templates as PRIMARY KEY but are not present
// in 2024+ TIGER shapefiles. They must be computed from component columns.
var derivedColumnDefs = map[string][]derivedColumnDef{
	"county_all": {{Column: "cntyidfp", Sources: []string{"statefp", "countyfp"}}},
	"cousub":     {{Column: "cosbidfp", Sources: []string{"statefp", "countyfp", "cousubfp"}}},
	"place":      {{Column: "plcidfp", Sources: []string{"statefp", "placefp"}}},
}

// ComputeDerivedColumns adds computed columns (e.g., cntyidfp = statefp||countyfp)
// to a ParseResult. These derived columns are required by tiger.* template PKs
// but are not present in 2024+ shapefiles.
func ComputeDerivedColumns(result *ParseResult, tableName string) *ParseResult {
	defs, ok := derivedColumnDefs[tableName]
	if !ok {
		return result
	}

	colIdx := make(map[string]int, len(result.Columns))
	for i, col := range result.Columns {
		colIdx[col] = i
	}

	type addDef struct {
		column     string
		sourceIdxs []int
	}
	var toAdd []addDef

	for _, def := range defs {
		if _, exists := colIdx[def.Column]; exists {
			continue
		}
		var srcIdxs []int
		allPresent := true
		for _, src := range def.Sources {
			idx, found := colIdx[src]
			if !found {
				allPresent = false
				break
			}
			srcIdxs = append(srcIdxs, idx)
		}
		if allPresent {
			toAdd = append(toAdd, addDef{column: def.Column, sourceIdxs: srcIdxs})
		}
	}

	if len(toAdd) == 0 {
		return result
	}

	newCols := make([]string, len(result.Columns), len(result.Columns)+len(toAdd))
	copy(newCols, result.Columns)
	for _, add := range toAdd {
		newCols = append(newCols, add.column)
	}

	newRows := make([][]any, len(result.Rows))
	for i, row := range result.Rows {
		newRow := make([]any, len(result.Columns), len(newCols))
		copy(newRow, row)
		for _, add := range toAdd {
			var val string
			allNonNil := true
			for _, idx := range add.sourceIdxs {
				if row[idx] == nil {
					allNonNil = false
					break
				}
				val += fmt.Sprintf("%v", row[idx])
			}
			if allNonNil {
				newRow = append(newRow, val)
			} else {
				newRow = append(newRow, nil)
			}
		}
		newRows[i] = newRow
	}

	return &ParseResult{Columns: newCols, Rows: newRows}
}

// columnRenames maps product table names to column rename mappings.
// Used for ZCTA520 where 2020-vintage column names (zcta5ce20) must be
// mapped to the tiger.zcta5 template column names (zcta5ce).
var columnRenames = map[string]map[string]string{
	"zcta5": {
		"zcta5ce20":  "zcta5ce",
		"geoid20":    "geoid",
		"classfp20":  "classfp",
		"mtfcc20":    "mtfcc",
		"funcstat20": "funcstat",
		"aland20":    "aland",
		"awater20":   "awater",
		"intptlat20": "intptlat",
		"intptlon20": "intptlon",
	},
}

// RenameColumns applies column name mappings to a ParseResult.
// Used for products where shapefile column names differ from template names.
func RenameColumns(result *ParseResult, tableName string) *ParseResult {
	renames, ok := columnRenames[tableName]
	if !ok {
		return result
	}
	newCols := make([]string, len(result.Columns))
	for i, col := range result.Columns {
		if newName, found := renames[col]; found {
			newCols[i] = newName
		} else {
			newCols[i] = col
		}
	}
	return &ParseResult{Columns: newCols, Rows: result.Rows}
}

// SetStateFIPS ensures a "statefp" column exists in the ParseResult with the given
// FIPS code. Per-county shapefiles (ADDR, FEATNAMES) omit statefp, but the geocoder
// requires it for state-level filtering. If the column already exists and has values,
// this is a no-op.
func SetStateFIPS(result *ParseResult, stateFIPS string) *ParseResult {
	if stateFIPS == "" || stateFIPS == "us" {
		return result
	}

	// Check if statefp already exists with data.
	for i, col := range result.Columns {
		if col == "statefp" {
			// Column exists — check if any row has a non-nil value.
			for _, row := range result.Rows {
				if row[i] != nil {
					return result // already populated
				}
			}
			// Column exists but all NULL — fill in the value.
			for _, row := range result.Rows {
				row[i] = stateFIPS
			}
			return result
		}
	}

	// Column doesn't exist — add it.
	newCols := make([]string, len(result.Columns)+1)
	copy(newCols, result.Columns)
	newCols[len(result.Columns)] = "statefp"

	newRows := make([][]any, len(result.Rows))
	for i, row := range result.Rows {
		newRow := make([]any, len(newCols))
		copy(newRow, row)
		newRow[len(result.Columns)] = stateFIPS
		newRows[i] = newRow
	}

	return &ParseResult{Columns: newCols, Rows: newRows}
}

// PopulateLookups populates tiger_data lookup tables from loaded data.
// These lookup tables are required by the PostGIS geocoder to resolve addresses.
func PopulateLookups(ctx context.Context, pool db.Pool) error {
	log := zap.L().With(zap.String("component", "tiger.schema"))

	lookupQueries := []struct {
		name string
		sql  string
	}{
		{
			name: "state_lookup",
			sql: `INSERT INTO tiger.state_lookup (st_code, abbrev, name, statefp)
				SELECT CAST(statefp AS integer), stusps, LEFT(name, 40), statefp
				FROM tiger_data.state_all
				ON CONFLICT (st_code) DO UPDATE SET
					abbrev = EXCLUDED.abbrev,
					name = EXCLUDED.name,
					statefp = EXCLUDED.statefp`,
		},
		{
			name: "county_lookup",
			sql: `INSERT INTO tiger.county_lookup (st_code, co_code, state, name)
				SELECT CAST(s.statefp AS integer),
					CAST(c.countyfp AS integer),
					s.stusps,
					c.name
				FROM tiger_data.county_all c
				JOIN tiger_data.state_all s ON c.statefp = s.statefp
				ON CONFLICT (st_code, co_code) DO UPDATE SET
					state = EXCLUDED.state,
					name = EXCLUDED.name`,
		},
		{
			name: "place_lookup",
			sql: `INSERT INTO tiger.place_lookup (st_code, pl_code, state, name)
				SELECT CAST(p.statefp AS integer),
					CAST(p.placefp AS integer),
					s.stusps,
					p.name
				FROM tiger_data.place p
				JOIN tiger_data.state_all s ON p.statefp = s.statefp
				ON CONFLICT (st_code, pl_code) DO UPDATE SET
					state = EXCLUDED.state,
					name = EXCLUDED.name`,
		},
		{
			name: "zip_lookup_all",
			sql: `INSERT INTO tiger.zip_lookup_all (zip, st_code, state, co_code, county, cnt)
				SELECT CAST(a.zip AS integer),
					CAST(a.statefp AS integer),
					s.stusps,
					0,
					'',
					1
				FROM tiger_data.addr a
				JOIN tiger_data.state_all s ON a.statefp = s.statefp
				WHERE a.zip IS NOT NULL AND a.zip != '' AND a.statefp IS NOT NULL
				GROUP BY a.zip, a.statefp, s.stusps
				ON CONFLICT DO NOTHING`,
		},
		{
			name: "countysub_lookup",
			sql: `INSERT INTO tiger.countysub_lookup (st_code, co_code, cs_code, state, name)
				SELECT CAST(c.statefp AS integer),
					CAST(c.countyfp AS integer),
					CAST(c.cousubfp AS integer),
					s.stusps,
					c.name
				FROM tiger_data.cousub c
				JOIN tiger_data.state_all s ON c.statefp = s.statefp
				ON CONFLICT DO NOTHING`,
		},
	}

	for _, q := range lookupQueries {
		log.Info("populating lookup table", zap.String("table", q.name))
		if _, err := pool.Exec(ctx, q.sql); err != nil {
			return eris.Wrapf(err, "tiger: populate %s", q.name)
		}
	}

	log.Info("lookup tables populated")
	return nil
}
