package api

import (
	"context"
	"net/http"
	"sync"

	"go.temporal.io/sdk/client"

	"github.com/sells-group/research-cli/internal/apicache"
	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/enrichmentstart"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/monitoring"
	"github.com/sells-group/research-cli/internal/readmodel"
	"github.com/sells-group/research-cli/internal/store"
)

// WebhookSemSize limits concurrent webhook pipeline executions.
const WebhookSemSize = 20

// Runner abstracts the pipeline execution so handlers are testable.
// *pipeline.Pipeline satisfies this interface.
type Runner interface {
	Run(ctx context.Context, company model.Company) (*model.EnrichmentResult, error)
}

type tileService interface {
	ServeHTTP(http.ResponseWriter, *http.Request)
	StatsHandler(http.ResponseWriter, *http.Request)
}

type enrichmentStarter interface {
	StartWebhook(ctx context.Context, company model.Company, requestID string) (*enrichmentstart.StartResult, error)
	StartRetry(ctx context.Context, originalRunID string, company model.Company, requestID string) (*enrichmentstart.StartResult, error)
}

// Handlers holds dependencies for all HTTP handlers.
type Handlers struct {
	store          store.Store
	runner         Runner
	collector      *monitoring.Collector
	cfg            *config.Config
	readModel      *readmodel.Service
	cache          apicache.Cache
	tileHandler    tileService
	sem            chan struct{}
	wg             sync.WaitGroup
	temporalClient client.Client // optional — when set, webhook starts Temporal workflows
	starter        enrichmentStarter
}

// NewHandlers creates a Handlers with the given dependencies.
func NewHandlers(cfg *config.Config, st store.Store, runner Runner, collector *monitoring.Collector, readSvc *readmodel.Service) *Handlers {
	return &Handlers{
		store:     st,
		runner:    runner,
		collector: collector,
		cfg:       cfg,
		readModel: readSvc,
		cache:     apicache.NewMemory(),
		sem:       make(chan struct{}, WebhookSemSize),
	}
}

// SetTemporalClient injects an optional Temporal client.
// When set, webhook enrich requests start Temporal workflows instead of goroutines.
func (h *Handlers) SetTemporalClient(c client.Client) {
	h.temporalClient = c
}

// SetEnrichmentStarter injects an optional API workflow starter.
func (h *Handlers) SetEnrichmentStarter(starter enrichmentStarter) {
	h.starter = starter
}

// SetReadModel injects the read-side query service used by read-model APIs.
func (h *Handlers) SetReadModel(readSvc *readmodel.Service) {
	h.readModel = readSvc
}

// SetTileHandler injects the HTTP tile service used by tile endpoints.
func (h *Handlers) SetTileHandler(handler tileService) {
	h.tileHandler = handler
}

// SetCache injects the shared API cache implementation.
func (h *Handlers) SetCache(cache apicache.Cache) {
	h.cache = cache
}

// Drain blocks until all in-flight webhook enrichment jobs complete.
func (h *Handlers) Drain() {
	h.wg.Wait()
}

func (h *Handlers) invalidateRunsCache() {
	if h.cache == nil {
		return
	}
	_ = h.cache.DeleteDomains(apicache.DomainRuns)
}

func (h *Handlers) requireCompanies(w http.ResponseWriter, r *http.Request) bool {
	if h.readModel == nil || h.readModel.Companies == nil {
		WriteError(w, r, http.StatusServiceUnavailable, "not_configured", "company read model not configured")
		return false
	}
	return true
}

func (h *Handlers) requireData(w http.ResponseWriter, r *http.Request) bool {
	if h.readModel == nil || h.readModel.Data == nil {
		WriteError(w, r, http.StatusServiceUnavailable, "not_configured", "data read model not configured")
		return false
	}
	return true
}

func (h *Handlers) requireAnalytics(w http.ResponseWriter, r *http.Request) bool {
	if h.readModel == nil || h.readModel.Analytics == nil {
		WriteError(w, r, http.StatusServiceUnavailable, "not_configured", "analytics read model not configured")
		return false
	}
	return true
}

func (h *Handlers) requireFedsync(w http.ResponseWriter, r *http.Request) bool {
	if h.readModel == nil || h.readModel.Fedsync == nil {
		WriteError(w, r, http.StatusServiceUnavailable, "not_configured", "fedsync read model not configured")
		return false
	}
	return true
}

func (h *Handlers) requireStore(w http.ResponseWriter, r *http.Request) bool {
	if h.store == nil {
		WriteError(w, r, http.StatusServiceUnavailable, "not_configured", "store not configured")
		return false
	}
	return true
}
