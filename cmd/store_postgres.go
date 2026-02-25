//go:build integration

package main

import (
	"context"
	"os"
	"time"

	"github.com/k-capehart/go-salesforce/v3"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/store"
	sfpkg "github.com/sells-group/research-cli/pkg/salesforce"
)

func initStore(ctx context.Context) (store.Store, error) {
	switch cfg.Store.Driver {
	case "sqlite":
		dsn := cfg.Store.DatabaseURL
		if dsn == "" {
			dsn = "research.db"
		}
		return store.NewSQLite(dsn)
	case "postgres":
		return store.NewPostgres(ctx, cfg.Store.DatabaseURL, &store.PoolConfig{
			MaxConns: cfg.Store.MaxConns,
			MinConns: cfg.Store.MinConns,
		})
	default:
		return nil, eris.Errorf("unsupported store driver: %s", cfg.Store.Driver)
	}
}

func initSalesforce() (sfpkg.Client, error) {
	if cfg.Salesforce.ClientID == "" {
		zap.L().Warn("salesforce not configured, SF writes will be skipped")
		return nil, nil
	}

	pemData, err := os.ReadFile(cfg.Salesforce.KeyPath)
	if err != nil {
		return nil, eris.Wrap(err, "read salesforce JWT private key")
	}

	sf, err := salesforce.Init(salesforce.Creds{
		Domain:         cfg.Salesforce.LoginURL,
		Username:       cfg.Salesforce.Username,
		ConsumerKey:    cfg.Salesforce.ClientID,
		ConsumerRSAPem: string(pemData),
	})
	if err != nil {
		return nil, eris.Wrap(err, "init salesforce")
	}

	client := sfpkg.NewClient(sf, sfpkg.WithRateLimit(cfg.Salesforce.RateLimit))

	// Health check: verify credentials work before running the pipeline.
	healthCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := client.DescribeSObject(healthCtx, "Account"); err != nil {
		return nil, eris.Wrap(err, "salesforce health check failed — verify credentials")
	}
	zap.L().Debug("salesforce health check passed")

	return client, nil
}
