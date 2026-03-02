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
			sql: `INSERT INTO tiger.state_lookup (st_code, abbrev, name)
				SELECT CAST(statefp AS integer), stusps, name
				FROM tiger_data.state_all
				ON CONFLICT (st_code) DO UPDATE SET
					abbrev = EXCLUDED.abbrev,
					name = EXCLUDED.name`,
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
			sql: `INSERT INTO tiger.zip_lookup_all (zip, st_code, state, co_code, county, city, statefp, countyfp)
				SELECT DISTINCT ON (a.zip, s.statefp)
					a.zip,
					CAST(s.statefp AS integer),
					s.stusps,
					0,
					'',
					'',
					s.statefp,
					''
				FROM tiger_data.addr a
				JOIN tiger_data.state_all s ON a.statefp = s.statefp
				WHERE a.zip IS NOT NULL AND a.zip != ''
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
