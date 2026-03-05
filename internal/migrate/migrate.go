// Package migrate provides sequential schema migrations via goose.
package migrate

import (
	"context"
	"database/sql"
	"embed"

	_ "github.com/jackc/pgx/v5/stdlib" // pgx driver for database/sql
	"github.com/pressly/goose/v3"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Options configures an Apply or Status call.
type Options struct {
	URL    string // Database connection URL (required).
	DryRun bool   // Preview changes without applying.
}

// ApplyResult contains the result of a schema apply.
type ApplyResult struct {
	Changes string // Summary of applied migrations.
	Applied int    // Number of migrations applied.
}

// Apply runs all pending goose migrations against the database.
func Apply(ctx context.Context, opts Options) (*ApplyResult, error) {
	if opts.URL == "" {
		return nil, eris.New("migrate: database URL is required")
	}

	db, err := sql.Open("pgx", opts.URL)
	if err != nil {
		return nil, eris.Wrap(err, "migrate: open database")
	}
	defer db.Close() //nolint:errcheck

	if err := db.PingContext(ctx); err != nil {
		return nil, eris.Wrap(err, "migrate: ping database")
	}

	goose.SetBaseFS(migrationsFS)
	if err := goose.SetDialect("postgres"); err != nil {
		return nil, eris.Wrap(err, "migrate: set dialect")
	}

	zap.L().Info("applying schema migrations",
		zap.Bool("dry_run", opts.DryRun),
	)

	if opts.DryRun {
		// In dry-run mode, get the current and target versions.
		current, err := goose.GetDBVersionContext(ctx, db)
		if err != nil {
			return nil, eris.Wrap(err, "migrate: get current version")
		}
		pending, err := goose.CollectMigrations(migrationsDir, current, maxVersion)
		if err != nil {
			return nil, eris.Wrap(err, "migrate: collect pending migrations")
		}
		result := &ApplyResult{Applied: 0}
		for _, m := range pending {
			result.Changes += m.Source + "\n"
			result.Applied++
		}
		return result, nil
	}

	// Count before.
	before, _ := goose.GetDBVersionContext(ctx, db)

	if err := goose.UpContext(ctx, db, migrationsDir); err != nil {
		return nil, eris.Wrap(err, "migrate: apply migrations")
	}

	// Count after.
	after, _ := goose.GetDBVersionContext(ctx, db)
	applied := int(after - before)

	zap.L().Info("schema migrations complete",
		zap.Int("migrations_applied", applied),
	)

	return &ApplyResult{Applied: applied}, nil
}

// Status prints the migration status to the logger.
func Status(ctx context.Context, opts Options) error {
	if opts.URL == "" {
		return eris.New("migrate: database URL is required")
	}

	db, err := sql.Open("pgx", opts.URL)
	if err != nil {
		return eris.Wrap(err, "migrate: open database")
	}
	defer db.Close() //nolint:errcheck

	goose.SetBaseFS(migrationsFS)
	if err := goose.SetDialect("postgres"); err != nil {
		return eris.Wrap(err, "migrate: set dialect")
	}

	return goose.StatusContext(ctx, db, migrationsDir)
}

const (
	migrationsDir = "migrations"
	maxVersion    = int64(1<<63 - 1)
)
