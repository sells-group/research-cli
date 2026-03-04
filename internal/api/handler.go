package api

import (
	"context"
	"sync"

	"go.temporal.io/sdk/client"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/monitoring"
	"github.com/sells-group/research-cli/internal/store"
)

// WebhookSemSize limits concurrent webhook pipeline executions.
const WebhookSemSize = 20

// Runner abstracts the pipeline execution so handlers are testable.
// *pipeline.Pipeline satisfies this interface.
type Runner interface {
	Run(ctx context.Context, company model.Company) (*model.EnrichmentResult, error)
}

// Handlers holds dependencies for all HTTP handlers.
type Handlers struct {
	store          store.Store
	runner         Runner
	collector      *monitoring.Collector
	cfg            *config.Config
	sem            chan struct{}
	wg             sync.WaitGroup
	temporalClient client.Client // optional — when set, webhook starts Temporal workflows
}

// NewHandlers creates a Handlers with the given dependencies.
func NewHandlers(cfg *config.Config, st store.Store, runner Runner, collector *monitoring.Collector) *Handlers {
	return &Handlers{
		store:     st,
		runner:    runner,
		collector: collector,
		cfg:       cfg,
		sem:       make(chan struct{}, WebhookSemSize),
	}
}

// SetTemporalClient injects an optional Temporal client.
// When set, webhook enrich requests start Temporal workflows instead of goroutines.
func (h *Handlers) SetTemporalClient(c client.Client) {
	h.temporalClient = c
}

// Drain blocks until all in-flight webhook enrichment jobs complete.
func (h *Handlers) Drain() {
	h.wg.Wait()
}
