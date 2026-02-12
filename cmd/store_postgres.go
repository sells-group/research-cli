//go:build integration

package main

import (
	"context"
	"os"

	"github.com/k-capehart/go-salesforce/v3"
	"github.com/rotisserie/eris"

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
		return store.NewPostgres(ctx, cfg.Store.DatabaseURL)
	default:
		return nil, eris.Errorf("unsupported store driver: %s", cfg.Store.Driver)
	}
}

func initSalesforce() (sfpkg.Client, error) {
	if cfg.Salesforce.ClientID == "" {
		return nil, eris.New("salesforce client ID is required (RESEARCH_SF_CLIENT_ID)")
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

	return sfpkg.NewClient(sf), nil
}
