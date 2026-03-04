// Package temporal provides shared Temporal.io client and worker utilities.
package temporal

import (
	"github.com/rotisserie/eris"
	"go.temporal.io/sdk/client"

	"github.com/sells-group/research-cli/internal/config"
)

// TaskQueue constants for the three worker types.
const (
	FedsyncTaskQueue    = "fedsync"
	EnrichmentTaskQueue = "enrichment"
	GeoTaskQueue        = "geo"
)

// NewClient creates a Temporal client from the application config.
func NewClient(cfg config.TemporalConfig) (client.Client, error) {
	c, err := client.Dial(client.Options{
		HostPort:  cfg.HostPort,
		Namespace: cfg.Namespace,
	})
	if err != nil {
		return nil, eris.Wrap(err, "temporal: dial")
	}
	return c, nil
}
