// Package temporal provides Temporal workflow client and worker factories.
package temporal

import (
	"github.com/rotisserie/eris"
	"go.temporal.io/sdk/client"

	"github.com/sells-group/research-cli/internal/config"
)

// TaskQueue is the Temporal task queue name for ADV document workflows.
const TaskQueue = "adv-documents"

// NewClient creates a Temporal client from fedsync config.
func NewClient(cfg config.FedsyncConfig) (client.Client, error) {
	if cfg.TemporalHostPort == "" {
		return nil, eris.New("temporal: host_port not configured")
	}
	opts := client.Options{
		HostPort:  cfg.TemporalHostPort,
		Namespace: cfg.TemporalNamespace,
	}
	if opts.Namespace == "" {
		opts.Namespace = "default"
	}
	c, err := client.Dial(opts)
	if err != nil {
		return nil, eris.Wrap(err, "temporal: dial")
	}
	return c, nil
}
