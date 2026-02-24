package dataset

import (
	"context"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync/advextract"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/pkg/anthropic"
)

// ADVExtract runs the tiered M&A intelligence extraction pipeline on ADV filings.
// When invoked via the fedsync engine (e.g., --datasets adv_extract), it runs
// T1-only extraction on un-extracted advisors with a default limit.
type ADVExtract struct {
	cfg *config.Config
}

// Name implements Dataset.
func (d *ADVExtract) Name() string { return "adv_extract" }

// Table implements Dataset.
func (d *ADVExtract) Table() string { return "fed_data.adv_advisor_answers" }

// Phase implements Dataset.
func (d *ADVExtract) Phase() Phase { return Phase3 }

// Cadence implements Dataset.
func (d *ADVExtract) Cadence() Cadence { return Monthly }

// ShouldRun implements Dataset.
func (d *ADVExtract) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return MonthlySchedule(now, lastSync)
}

// Sync fetches and loads ADV tiered extraction data.
func (d *ADVExtract) Sync(ctx context.Context, pool db.Pool, _ fetcher.Fetcher, _ string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))

	if d.cfg == nil || d.cfg.Anthropic.Key == "" {
		return nil, eris.New("adv_extract: anthropic API key is required")
	}

	client := anthropic.NewClient(d.cfg.Anthropic.Key)
	extractor := advextract.NewExtractor(pool, client, advextract.ExtractorOpts{
		MaxTier: 1, // engine mode defaults to T1 only for cost safety
	})

	store := advextract.NewStore(pool)
	crds, err := store.ListAdvisors(ctx, advextract.ListOpts{
		Limit:            100, // default batch limit for engine mode
		IncludeExtracted: false,
	})
	if err != nil {
		return nil, eris.Wrap(err, "adv_extract: list advisors")
	}

	if len(crds) == 0 {
		log.Info("no un-extracted advisors found")
		return &SyncResult{RowsSynced: 0}, nil
	}

	log.Info("starting ADV extraction via engine",
		zap.Int("advisors", len(crds)),
		zap.Int("max_tier", 1))

	if err := extractor.RunBatch(ctx, crds); err != nil {
		return nil, eris.Wrap(err, "adv_extract: run batch")
	}

	return &SyncResult{
		RowsSynced: int64(len(crds)),
		Metadata: map[string]any{
			"advisors_extracted": len(crds),
			"max_tier":           1,
		},
	}, nil
}
