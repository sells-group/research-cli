package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
)

var fedsyncCmd = &cobra.Command{
	Use:   "fedsync",
	Short: "Federal data sync pipeline",
	Long:  "Incrementally syncs 26 federal datasets (Census, BLS, SEC, FINRA, OSHA, EPA, FRED) into fed_data.* Postgres tables.",
}

func init() {
	rootCmd.AddCommand(fedsyncCmd)
}

// fedsyncPool creates a pgxpool.Pool for the fedsync subsystem.
// Uses cfg.Fedsync.DatabaseURL, falling back to cfg.Store.DatabaseURL.
func fedsyncPool(ctx context.Context) (*pgxpool.Pool, error) {
	dsn := cfg.Fedsync.DatabaseURL
	if dsn == "" {
		dsn = cfg.Store.DatabaseURL
	}
	if dsn == "" {
		return nil, eris.New("fedsync: no database_url configured (set fedsync.database_url or store.database_url)")
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, eris.Wrap(err, "fedsync: create connection pool")
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, eris.Wrap(err, "fedsync: ping database")
	}

	fmt.Println("Connected to database")
	return pool, nil
}
