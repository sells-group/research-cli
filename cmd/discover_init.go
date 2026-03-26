package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// discoveryPool creates a pgxpool.Pool for the discovery subsystem.
// Uses cfg.Store.DatabaseURL.
func discoveryPool(ctx context.Context) (*pgxpool.Pool, error) {
	return openStorePostgresPool(ctx)
}
