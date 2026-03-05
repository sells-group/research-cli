// Package migrate provides versioned schema migrations via Goose.
package migrate

import (
	"context"
	"database/sql"
	"embed"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib" // register pgx driver for database/sql
	"github.com/pressly/goose/v3"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Apply runs all pending Goose migrations against the database.
// It auto-baselines existing databases: if goose_db_version does not
// exist but public.companies does, version 1 is recorded without
// executing the baseline SQL.
func Apply(ctx context.Context, dbURL string) error {
	if dbURL == "" {
		return eris.New("migrate: database URL is required")
	}

	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		return eris.Wrap(err, "migrate: open database")
	}
	defer db.Close() //nolint:errcheck

	if err := db.PingContext(ctx); err != nil {
		return eris.Wrap(err, "migrate: ping database")
	}

	goose.SetBaseFS(migrationsFS)

	if err := goose.SetDialect("postgres"); err != nil {
		return eris.Wrap(err, "migrate: set dialect")
	}

	if baseline, err := needsBaseline(ctx, db); err != nil {
		return eris.Wrap(err, "migrate: check baseline")
	} else if baseline {
		zap.L().Info("existing database detected, recording baseline")
		if err := recordBaseline(ctx, db); err != nil {
			return eris.Wrap(err, "migrate: record baseline")
		}
	}

	zap.L().Info("applying migrations")
	if err := goose.UpContext(ctx, db, "migrations"); err != nil {
		return eris.Wrap(err, "migrate: apply migrations")
	}

	zap.L().Info("migrations complete")
	return nil
}

// Status prints the current migration status to stdout.
func Status(ctx context.Context, dbURL string) error {
	if dbURL == "" {
		return eris.New("migrate: database URL is required")
	}

	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		return eris.Wrap(err, "migrate: open database")
	}
	defer db.Close() //nolint:errcheck

	goose.SetBaseFS(migrationsFS)

	if err := goose.SetDialect("postgres"); err != nil {
		return eris.Wrap(err, "migrate: set dialect")
	}

	if err := goose.StatusContext(ctx, db, "migrations"); err != nil {
		return eris.Wrap(err, "migrate: status")
	}

	return nil
}

// Baseline explicitly marks version 1 as applied without running its SQL.
// Use this for manually baselining an existing database.
func Baseline(ctx context.Context, dbURL string) error {
	if dbURL == "" {
		return eris.New("migrate: database URL is required")
	}

	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		return eris.Wrap(err, "migrate: open database")
	}
	defer db.Close() //nolint:errcheck

	goose.SetBaseFS(migrationsFS)

	if err := goose.SetDialect("postgres"); err != nil {
		return eris.Wrap(err, "migrate: set dialect")
	}

	if err := recordBaseline(ctx, db); err != nil {
		return eris.Wrap(err, "migrate: record baseline")
	}

	fmt.Println("Baseline version 1 recorded.")
	return nil
}

// needsBaseline returns true when the database has existing tables
// (public.companies) but no goose_db_version table.
func needsBaseline(ctx context.Context, db *sql.DB) (bool, error) {
	// Check if goose version table exists.
	var exists bool
	err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = 'goose_db_version'
		)
	`).Scan(&exists)
	if err != nil {
		return false, eris.Wrap(err, "check goose_db_version")
	}
	if exists {
		return false, nil
	}

	// Check if this is an existing database with tables.
	err = db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = 'companies'
		)
	`).Scan(&exists)
	if err != nil {
		return false, eris.Wrap(err, "check public.companies")
	}

	return exists, nil
}

// recordBaseline creates the goose version table and inserts version 1
// without executing any SQL.
func recordBaseline(ctx context.Context, db *sql.DB) error {
	// Ensure the version table exists.
	if _, err := goose.EnsureDBVersionContext(ctx, db); err != nil {
		return eris.Wrap(err, "ensure version table")
	}

	// Check if version 1 is already recorded.
	current, err := goose.GetDBVersionContext(ctx, db)
	if err != nil {
		return eris.Wrap(err, "get current version")
	}
	if current >= 1 {
		zap.L().Info("baseline already recorded", zap.Int64("version", current))
		return nil
	}

	// Insert version 1 as applied.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return eris.Wrap(err, "begin tx")
	}
	defer tx.Rollback() //nolint:errcheck

	if _, err := tx.ExecContext(ctx,
		"INSERT INTO goose_db_version (version_id, is_applied) VALUES ($1, true)", 1,
	); err != nil {
		return eris.Wrap(err, "insert baseline version")
	}

	if err := tx.Commit(); err != nil {
		return eris.Wrap(err, "commit baseline")
	}

	zap.L().Info("baseline recorded", zap.Int64("version", 1))
	return nil
}
