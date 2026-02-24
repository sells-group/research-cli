// Package fedsync provides incremental federal data synchronization into Postgres.
package fedsync

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
var migrationFS embed.FS

// Migrate runs all pending SQL migrations in lexicographic order.
// It creates the fed_data schema and schema_migrations tracking table if needed,
// then applies any .sql files not yet recorded.
func Migrate(ctx context.Context, pool db.Pool) error {
	log := zap.L().With(zap.String("component", "fedsync.migrate"))

	// Advisory lock prevents concurrent migration runs (e.g. overlapping deploys).
	if _, err := pool.Exec(ctx, "SELECT pg_advisory_lock(8675309)"); err != nil {
		return eris.Wrap(err, "fedsync: acquire migration advisory lock")
	}
	defer func() {
		if _, err := pool.Exec(ctx, "SELECT pg_advisory_unlock(8675309)"); err != nil {
			log.Warn("fedsync: failed to release migration advisory lock", zap.Error(err))
		}
	}()

	// Ensure schema and tracking table exist.
	if err := ensureMigrationTable(ctx, pool); err != nil {
		return err
	}

	// Read all migration files.
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return eris.Wrap(err, "fedsync: read migration dir")
	}

	// Sort by filename (lexicographic = numeric order with zero-padded names).
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	applied, err := appliedMigrations(ctx, pool)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		name := entry.Name()
		if applied[name] {
			continue
		}

		data, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return eris.Wrapf(err, "fedsync: read migration %s", name)
		}

		log.Info("applying migration", zap.String("file", name))

		if _, err := pool.Exec(ctx, string(data)); err != nil {
			return eris.Wrapf(err, "fedsync: apply migration %s", name)
		}

		if _, err := pool.Exec(ctx,
			"INSERT INTO fed_data.schema_migrations (filename, applied_at) VALUES ($1, now())",
			name,
		); err != nil {
			return eris.Wrapf(err, "fedsync: record migration %s", name)
		}

		log.Info("migration applied", zap.String("file", name))
	}

	return nil
}

// ensureMigrationTable creates the schema and migration tracking table if they don't exist.
func ensureMigrationTable(ctx context.Context, pool db.Pool) error {
	sql := `
		CREATE SCHEMA IF NOT EXISTS fed_data;
		CREATE TABLE IF NOT EXISTS fed_data.schema_migrations (
			id         SERIAL PRIMARY KEY,
			filename   TEXT NOT NULL UNIQUE,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`
	if _, err := pool.Exec(ctx, sql); err != nil {
		return eris.Wrap(err, "fedsync: ensure migration table")
	}
	return nil
}

// appliedMigrations returns the set of already-applied migration filenames.
func appliedMigrations(ctx context.Context, pool db.Pool) (map[string]bool, error) {
	rows, err := pool.Query(ctx, "SELECT filename FROM fed_data.schema_migrations")
	if err != nil {
		return nil, eris.Wrap(err, "fedsync: query applied migrations")
	}
	defer rows.Close()

	applied := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, eris.Wrap(err, "fedsync: scan migration row")
		}
		applied[name] = true
	}
	return applied, rows.Err()
}
