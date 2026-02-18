package store

import (
	"context"
	"time"

	"github.com/sells-group/research-cli/internal/model"
)

// RunFilter specifies criteria for listing runs.
type RunFilter struct {
	Status     model.RunStatus `json:"status,omitempty"`
	CompanyURL string          `json:"company_url,omitempty"`
	Limit      int             `json:"limit,omitempty"`
	Offset     int             `json:"offset,omitempty"`
}

// Store defines the persistence interface for the enrichment pipeline.
type Store interface {
	// Runs
	CreateRun(ctx context.Context, company model.Company) (*model.Run, error)
	UpdateRunStatus(ctx context.Context, runID string, status model.RunStatus) error
	UpdateRunResult(ctx context.Context, runID string, result *model.RunResult) error
	GetRun(ctx context.Context, runID string) (*model.Run, error)
	ListRuns(ctx context.Context, filter RunFilter) ([]model.Run, error)

	// Phases
	CreatePhase(ctx context.Context, runID string, name string) (*model.RunPhase, error)
	CompletePhase(ctx context.Context, phaseID string, result *model.PhaseResult) error

	// Crawl cache
	GetCachedCrawl(ctx context.Context, companyURL string) (*model.CrawlCache, error)
	SetCachedCrawl(ctx context.Context, companyURL string, pages []model.CrawledPage, ttl time.Duration) error
	DeleteExpiredCrawls(ctx context.Context) (int, error)

	// LinkedIn cache
	GetCachedLinkedIn(ctx context.Context, domain string) ([]byte, error)
	SetCachedLinkedIn(ctx context.Context, domain string, data []byte, ttl time.Duration) error

	// Lifecycle
	Migrate(ctx context.Context) error
	Close() error
}
