package store

import (
	"context"
	"time"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/resilience"
)

// RunFilter specifies criteria for listing runs.
type RunFilter struct {
	Status       model.RunStatus `json:"status,omitempty"`
	CompanyURL   string          `json:"company_url,omitempty"`
	CreatedAfter time.Time       `json:"created_after,omitempty"`
	Limit        int             `json:"limit,omitempty"`
	Offset       int             `json:"offset,omitempty"`
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

	// Scrape cache (per-URL Firecrawl result caching)
	GetCachedScrape(ctx context.Context, urlHash string) ([]byte, error)
	SetCachedScrape(ctx context.Context, urlHash string, content []byte, ttl time.Duration) error

	// High-confidence answer lookup (skip re-extraction)
	GetHighConfidenceAnswers(ctx context.Context, companyURL string, minConfidence float64) ([]model.ExtractionAnswer, error)

	// Checkpoint/resume
	SaveCheckpoint(ctx context.Context, companyID string, phase string, data []byte) error
	LoadCheckpoint(ctx context.Context, companyID string) (*model.Checkpoint, error)
	DeleteCheckpoint(ctx context.Context, companyID string) error

	// Cache cleanup
	DeleteExpiredLinkedIn(ctx context.Context) (int, error)
	DeleteExpiredScrapes(ctx context.Context) (int, error)

	// Dead letter queue
	EnqueueDLQ(ctx context.Context, entry resilience.DLQEntry) error
	DequeueDLQ(ctx context.Context, filter resilience.DLQFilter) ([]resilience.DLQEntry, error)
	IncrementDLQRetry(ctx context.Context, id string, nextRetryAt time.Time, lastErr string) error
	RemoveDLQ(ctx context.Context, id string) error
	CountDLQ(ctx context.Context) (int, error)

	// Lifecycle
	Ping(ctx context.Context) error
	Migrate(ctx context.Context) error
	Close() error
}
