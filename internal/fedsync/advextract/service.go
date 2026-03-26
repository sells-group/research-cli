package advextract

import (
	"context"
	"strings"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/pkg/anthropic"
)

// Service orchestrates ADV extraction use cases for CLI and workers.
type Service struct {
	pool   db.Pool
	client anthropic.Client
	store  *Store
}

// ServiceOptions configures an ADV extraction run.
type ServiceOptions struct {
	CRD          int     `json:"crd"`
	Limit        int     `json:"limit"`
	MaxTier      int     `json:"max_tier"`
	MaxCost      float64 `json:"max_cost"`
	FilterState  string  `json:"filter_state"`
	FilterAUMMin int64   `json:"filter_aum_min"`
	DryRun       bool    `json:"dry_run"`
	Force        bool    `json:"force"`
	FundsOnly    bool    `json:"funds_only"`
}

// ServiceResult summarizes the outcome of a service run.
type ServiceResult struct {
	SingleCRD    int      `json:"single_crd,omitempty"`
	AdvisorCount int      `json:"advisor_count"`
	CRDs         []int    `json:"crds,omitempty"`
	CostEstimate string   `json:"cost_estimate,omitempty"`
	Mode         string   `json:"mode"`
	Filters      ListOpts `json:"filters,omitempty"`
}

// NewService creates a new ADV extraction service.
func NewService(pool db.Pool, client anthropic.Client) *Service {
	return &Service{
		pool:   pool,
		client: client,
		store:  NewStore(pool),
	}
}

// Run executes either a single-advisor or batch extraction workflow.
func (s *Service) Run(ctx context.Context, opts ServiceOptions) (*ServiceResult, error) {
	if opts.MaxTier < 1 || opts.MaxTier > 2 {
		return nil, eris.Errorf("advextract: --tier must be 1 or 2 (got %d)", opts.MaxTier)
	}

	extractor := NewExtractor(s.pool, s.client, ExtractorOpts{
		MaxTier:   opts.MaxTier,
		MaxCost:   opts.MaxCost,
		DryRun:    opts.DryRun,
		FundsOnly: opts.FundsOnly,
		Force:     opts.Force,
	})

	if opts.CRD > 0 {
		if err := extractor.RunAdvisor(ctx, opts.CRD); err != nil {
			return nil, eris.Wrapf(err, "advextract: CRD %d", opts.CRD)
		}
		return &ServiceResult{
			SingleCRD:    opts.CRD,
			AdvisorCount: 1,
			CRDs:         []int{opts.CRD},
			CostEstimate: EstimateBatchCost(1, opts.MaxTier),
			Mode:         "single",
		}, nil
	}

	listOpts := ListOpts{
		Limit:            opts.Limit,
		State:            strings.ToUpper(opts.FilterState),
		MinAUM:           opts.FilterAUMMin,
		IncludeExtracted: opts.Force,
	}
	crds, err := s.store.ListAdvisors(ctx, listOpts)
	if err != nil {
		return nil, eris.Wrap(err, "advextract: list advisors")
	}

	result := &ServiceResult{
		AdvisorCount: len(crds),
		CRDs:         crds,
		CostEstimate: EstimateBatchCost(len(crds), opts.MaxTier),
		Mode:         "batch",
		Filters:      listOpts,
	}
	if len(crds) == 0 || opts.DryRun {
		return result, nil
	}

	if err := extractor.RunBatch(ctx, crds); err != nil {
		return nil, eris.Wrap(err, "advextract: batch")
	}
	return result, nil
}
