package geospatial

import (
	"context"
	"embed"
	"io/fs"
	"sort"

	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/db"
	"go.uber.org/zap"
)

//go:embed migrations/*.sql
var geoMigrationFS embed.FS

// Migrate runs all pending SQL migrations in lexicographic order.
// It creates the geo schema and schema_migrations tracking table if needed,
// then applies any .sql files not yet recorded.
func Migrate(ctx context.Context, pool db.Pool) error {
	log := zap.L().With(zap.String("component", "geo.migrate"))

	// Advisory lock prevents concurrent migration runs (e.g. overlapping deploys).
	if _, err := pool.Exec(ctx, "SELECT pg_advisory_lock(8675310)"); err != nil {
		return eris.Wrap(err, "geo: acquire migration advisory lock")
	}
	defer func() {
		if _, err := pool.Exec(ctx, "SELECT pg_advisory_unlock(8675310)"); err != nil {
			log.Warn("geo: failed to release migration advisory lock", zap.Error(err))
		}
	}()

	// Ensure schema and tracking table exist.
	if err := ensureGeoMigrationTable(ctx, pool); err != nil {
		return err
	}

	// Read all migration files.
	entries, err := fs.ReadDir(geoMigrationFS, "migrations")
	if err != nil {
		return eris.Wrap(err, "geo: read migration dir")
	}

	// Sort by filename (lexicographic = numeric order with zero-padded names).
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	applied, err := appliedGeoMigrations(ctx, pool)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		name := entry.Name()
		if applied[name] {
			continue
		}

		data, err := geoMigrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return eris.Wrapf(err, "geo: read migration %s", name)
		}

		log.Info("applying migration", zap.String("file", name))

		if _, err := pool.Exec(ctx, string(data)); err != nil {
			return eris.Wrapf(err, "geo: apply migration %s", name)
		}

		if _, err := pool.Exec(ctx,
			"INSERT INTO geo.schema_migrations (filename, applied_at) VALUES ($1, now())",
			name,
		); err != nil {
			return eris.Wrapf(err, "geo: record migration %s", name)
		}

		log.Info("migration applied", zap.String("file", name))
	}

	return nil
}

// ensureGeoMigrationTable creates the geo schema and migration tracking table if they don't exist.
func ensureGeoMigrationTable(ctx context.Context, pool db.Pool) error {
	sql := `
		CREATE SCHEMA IF NOT EXISTS geo;
		CREATE TABLE IF NOT EXISTS geo.schema_migrations (
			id         SERIAL PRIMARY KEY,
			filename   TEXT NOT NULL UNIQUE,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`
	if _, err := pool.Exec(ctx, sql); err != nil {
		return eris.Wrap(err, "geo: ensure migration table")
	}
	return nil
}

// appliedGeoMigrations returns the set of already-applied migration filenames.
func appliedGeoMigrations(ctx context.Context, pool db.Pool) (map[string]bool, error) {
	rows, err := pool.Query(ctx, "SELECT filename FROM geo.schema_migrations")
	if err != nil {
		return nil, eris.Wrap(err, "geo: query applied migrations")
	}
	defer rows.Close()

	applied := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, eris.Wrap(err, "geo: scan migration row")
		}
		applied[name] = true
	}
	return applied, rows.Err()
}
