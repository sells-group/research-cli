package main

import (
	"context"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/migrate"
)

// ensureSchema applies versioned migrations via Goose.
// It resolves the database URL from fedsync or store config.
func ensureSchema(ctx context.Context) error {
	dbURL := cfg.Fedsync.DatabaseURL
	if dbURL == "" {
		dbURL = cfg.Store.DatabaseURL
	}
	if dbURL == "" {
		return eris.New("ensureSchema: database URL is required (set store.database_url or fedsync.database_url)")
	}

	return migrate.Apply(ctx, dbURL)
}
