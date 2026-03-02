package analysis

import (
	"context"
	"embed"
	"io/fs"
	"sort"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
)

//go:embed migrations/*.sql
var analysisMigrationFS embed.FS

// Migrate runs all pending analysis SQL migrations in lexicographic order.
// It shares the geo.schema_migrations tracking table and advisory lock (8675310)
// with the geospatial package to ensure correct sequencing.
func Migrate(ctx context.Context, pool db.Pool) error {
	log := zap.L().With(zap.String("component", "analysis.migrate"))

	// Advisory lock prevents concurrent migration runs.
	if _, err := pool.Exec(ctx, "SELECT pg_advisory_lock(8675310)"); err != nil {
		return eris.Wrap(err, "analysis: acquire migration advisory lock")
	}
	defer func() {
		if _, err := pool.Exec(ctx, "SELECT pg_advisory_unlock(8675310)"); err != nil {
			log.Warn("analysis: failed to release migration advisory lock", zap.Error(err))
		}
	}()

	if err := ensureAnalysisMigrationTable(ctx, pool); err != nil {
		return err
	}

	entries, err := fs.ReadDir(analysisMigrationFS, "migrations")
	if err != nil {
		return eris.Wrap(err, "analysis: read migration dir")
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	applied, err := appliedAnalysisMigrations(ctx, pool)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		name := entry.Name()
		if applied[name] {
			continue
		}

		data, err := analysisMigrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return eris.Wrapf(err, "analysis: read migration %s", name)
		}

		log.Info("applying migration", zap.String("file", name))

		if _, err := pool.Exec(ctx, string(data)); err != nil {
			return eris.Wrapf(err, "analysis: apply migration %s", name)
		}

		if _, err := pool.Exec(ctx,
			"INSERT INTO geo.schema_migrations (filename, applied_at) VALUES ($1, now())",
			name,
		); err != nil {
			return eris.Wrapf(err, "analysis: record migration %s", name)
		}

		log.Info("migration applied", zap.String("file", name))
	}

	return nil
}

// ensureAnalysisMigrationTable creates the geo schema and migration tracking
// table if they don't exist.
func ensureAnalysisMigrationTable(ctx context.Context, pool db.Pool) error {
	sql := `
		CREATE SCHEMA IF NOT EXISTS geo;
		CREATE TABLE IF NOT EXISTS geo.schema_migrations (
			id         SERIAL PRIMARY KEY,
			filename   TEXT NOT NULL UNIQUE,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`
	if _, err := pool.Exec(ctx, sql); err != nil {
		return eris.Wrap(err, "analysis: ensure migration table")
	}
	return nil
}

// appliedAnalysisMigrations returns the set of already-applied migration filenames.
func appliedAnalysisMigrations(ctx context.Context, pool db.Pool) (map[string]bool, error) {
	rows, err := pool.Query(ctx, "SELECT filename FROM geo.schema_migrations")
	if err != nil {
		return nil, eris.Wrap(err, "analysis: query applied migrations")
	}
	defer rows.Close()

	applied := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, eris.Wrap(err, "analysis: scan migration row")
		}
		applied[name] = true
	}
	return applied, rows.Err()
}
