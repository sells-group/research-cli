package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"
)

// discoveryPool creates a pgxpool.Pool for the discovery subsystem.
// Uses cfg.Store.DatabaseURL.
func discoveryPool(ctx context.Context) (*pgxpool.Pool, error) {
	dsn := cfg.Store.DatabaseURL
	if dsn == "" {
		return nil, eris.New("discovery: no database_url configured (set store.database_url)")
	}

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, eris.Wrap(err, "discovery: parse connection string")
	}
	poolCfg.MaxConns = 15
	poolCfg.MinConns = 2
	poolCfg.MaxConnLifetime = 30 * time.Minute
	poolCfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, eris.Wrap(err, "discovery: create connection pool")
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, eris.Wrap(err, "discovery: ping database")
	}

	fmt.Println("Connected to database")
	return pool, nil
}
