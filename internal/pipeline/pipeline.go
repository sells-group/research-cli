package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/cost"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/estimate"
	"github.com/sells-group/research-cli/internal/geo"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/resilience"
	"github.com/sells-group/research-cli/internal/scrape"
	"github.com/sells-group/research-cli/internal/store"
	"github.com/sells-group/research-cli/internal/waterfall"
	"github.com/sells-group/research-cli/pkg/anthropic"
	"github.com/sells-group/research-cli/pkg/firecrawl"
	"github.com/sells-group/research-cli/pkg/geocode"
	"github.com/sells-group/research-cli/pkg/google"
	"github.com/sells-group/research-cli/pkg/jina"
	"github.com/sells-group/research-cli/pkg/notion"
	"github.com/sells-group/research-cli/pkg/perplexity"
	"github.com/sells-group/research-cli/pkg/ppp"
	"github.com/sells-group/research-cli/pkg/salesforce"
)

// Pipeline orchestrates phases 1-9 of the enrichment pipeline.
type Pipeline struct {
	cfg           *config.Config
	store         store.Store
	chain         *scrape.Chain
	jina          jina.Client
	firecrawl     firecrawl.Client
	perplexity    perplexity.Client
	anthropic     anthropic.Client
	salesforce    salesforce.Client
	notion        notion.Client
	google        google.Client
	ppp           ppp.Querier
	costCalc      *cost.Calculator
	estimator     *estimate.RevenueEstimator
	waterfallExec *waterfall.Executor
	questions     []model.Question
	fields        *model.FieldRegistry
	breakers      *resilience.ServiceBreakers
	retryCfg      resilience.RetryConfig
	fedsyncPool   db.Pool // optional: enables ADV pre-fill when set

	// Geocoding (Phase 7D) — set via SetGeocoder / SetGeoAssociator.
	geocoder geocode.Client
	geoAssoc *geo.Associator

	// Deferred SF write mode: when set, Phase 9 builds write intents
	// via PrepareGate instead of executing SF writes via QualityGate.
	// The callback is invoked with each intent for external collection.
	deferSFWrites  bool
	onWriteIntent  func(*SFWriteIntent)
	forceReExtract bool
}

// New creates a new Pipeline with all dependencies.
func New(
	cfg *config.Config,
	st store.Store,
	chain *scrape.Chain,
	jinaClient jina.Client,
	fcClient firecrawl.Client,
	pplxClient perplexity.Client,
	aiClient anthropic.Client,
	sfClient salesforce.Client,
	notionClient notion.Client,
	googleClient google.Client,
	pppClient ppp.Querier,
	estimator *estimate.RevenueEstimator,
	waterfallExec *waterfall.Executor,
	questions []model.Question,
	fields *model.FieldRegistry,
) *Pipeline {
	rates := cost.RatesFromConfig(cost.PricingConfig{
		Anthropic:  convertAnthropicPricing(cfg.Pricing.Anthropic),
		Jina:       cost.JinaPricing{PerMTok: cfg.Pricing.Jina.PerMTok},
		Perplexity: cost.PerplexityPricing{PerQuery: cfg.Pricing.Perplexity.PerQuery},
		Firecrawl:  cost.FirecrawlPricing{PlanMonthly: cfg.Pricing.Firecrawl.PlanMonthly, CreditsIncluded: cfg.Pricing.Firecrawl.CreditsIncluded},
	})

	cbCfg := resilience.FromCircuitConfig(cfg.Circuit.FailureThreshold, cfg.Circuit.ResetTimeoutSecs)
	cbCfg.OnStateChange = func(from, to resilience.CircuitState) {
		zap.L().Warn("circuit breaker state change",
			zap.String("from", from.String()),
			zap.String("to", to.String()),
		)
	}
	cbCfg.ShouldTrip = resilience.IsTransient

	retryCfg := resilience.FromRetryConfig(
		cfg.Retry.MaxAttempts,
		cfg.Retry.InitialBackoffMs,
		cfg.Retry.MaxBackoffMs,
		cfg.Retry.Multiplier,
		cfg.Retry.JitterFraction,
	)

	return &Pipeline{
		cfg:           cfg,
		store:         st,
		chain:         chain,
		jina:          jinaClient,
		firecrawl:     fcClient,
		perplexity:    pplxClient,
		anthropic:     aiClient,
		salesforce:    sfClient,
		notion:        notionClient,
		google:        googleClient,
		ppp:           pppClient,
		costCalc:      cost.NewCalculator(rates),
		estimator:     estimator,
		waterfallExec: waterfallExec,
		questions:     questions,
		fields:        fields,
		breakers:      resilience.NewServiceBreakers(cbCfg),
		retryCfg:      retryCfg,
	}
}

// SetFedsyncPool sets an optional fed_data database pool for ADV pre-fill.
func (p *Pipeline) SetFedsyncPool(pool db.Pool) {
	p.fedsyncPool = pool
}

// SetDeferredWrites enables deferred SF write mode for batch aggregation.
// When set, Phase 9 calls PrepareGate (building intents) instead of QualityGate
// (executing writes). The callback fn is invoked with each write intent.
// Call FlushDeferredWrites after all pipeline runs complete.
func (p *Pipeline) SetDeferredWrites(fn func(*SFWriteIntent)) {
	p.deferSFWrites = true
	p.onWriteIntent = fn
}

// SetForceReExtract disables answer reuse so all fields are re-extracted.
func (p *Pipeline) SetForceReExtract(force bool) {
	p.forceReExtract = force
}

// FlushDeferredWrites executes collected SF write intents in bulk using the
// Pipeline's own SF and Notion clients.
func (p *Pipeline) FlushDeferredWrites(ctx context.Context, intents []*SFWriteIntent) (*FlushSummary, error) {
	return FlushSFWrites(ctx, p.salesforce, p.notion, intents)
}

// convertAnthropicPricing maps config pricing to cost pricing types.
func convertAnthropicPricing(src map[string]config.ModelPricing) map[string]cost.ModelPricing {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]cost.ModelPricing, len(src))
	for k, v := range src {
		dst[k] = cost.ModelPricing{
			Input:         v.Input,
			Output:        v.Output,
			BatchDiscount: v.BatchDiscount,
			CacheWriteMul: v.CacheWriteMul,
			CacheReadMul:  v.CacheReadMul,
		}
	}
	return dst
}

// Run executes the full enrichment pipeline for a single company.
func (p *Pipeline) Run(ctx context.Context, company model.Company) (*model.EnrichmentResult, error) {
	log := zap.L().With(zap.String("company", company.Name), zap.String("url", company.URL))

	mode := p.cfg.Pipeline.Mode
	if mode == "" {
		mode = "full"
	}
	isSourcing := mode == "sourcing"
	log.Info("pipeline: starting", zap.String("mode", mode))

	result := &model.EnrichmentResult{
		Company: company,
	}

	// Create run record.
	run, err := p.store.CreateRun(ctx, company)
	if err != nil {
		return nil, eris.Wrap(err, "pipeline: create run")
	}
	result.RunID = run.ID

	// Update status helper.
	setStatus := func(status model.RunStatus) {
		if statusErr := p.store.UpdateRunStatus(ctx, run.ID, status); statusErr != nil {
			log.Warn("pipeline: failed to update status", zap.Error(statusErr))
		}
	}

	// Fail run helper: persists structured error with category + phase snapshot.
	failRun := func(failErr error, phaseName string) {
		category := model.ErrorCategoryPermanent
		if resilience.IsTransient(failErr) {
			category = model.ErrorCategoryTransient
		}
		runErr := &model.RunError{
			Message:     failErr.Error(),
			Category:    category,
			FailedPhase: phaseName,
			Phases:      append([]model.PhaseResult(nil), result.Phases...),
		}
		if storeErr := p.store.FailRun(ctx, run.ID, runErr); storeErr != nil {
			log.Warn("pipeline: failed to persist run error", zap.Error(storeErr))
		}
	}

	// Phase tracking helper with mutex for concurrent access.
	var phasesMu sync.Mutex
	trackPhase := func(name string, fn func() (*model.PhaseResult, error)) *model.PhaseResult {
		return p.executePhase(ctx, run.ID, name, fn, log, &phasesMu, result)
	}

	// trackPhaseWithRetry wraps a phase with retry + circuit breaker logic.
	// The service parameter identifies which circuit breaker to use.
	trackPhaseWithRetry := func(name, service string, fn func() (*model.PhaseResult, error)) *model.PhaseResult {
		cb := p.breakers.Get(service)
		return p.executePhase(ctx, run.ID, name, func() (*model.PhaseResult, error) {
			var lastResult *model.PhaseResult
			retryCfg := p.retryCfg
			retryCfg.OnRetry = func(attempt int, retryErr error) {
				log.Warn("pipeline: retrying phase",
					zap.String("phase", name),
					zap.String("service", service),
					zap.Int("attempt", attempt),
					zap.Error(retryErr),
				)
			}
			err := cb.Execute(ctx, func(cbCtx context.Context) error {
				return resilience.Do(cbCtx, retryCfg, func(_ context.Context) error {
					pr, fnErr := fn()
					lastResult = pr
					if fnErr != nil && resilience.IsTransient(fnErr) {
						return resilience.NewTransientError(fnErr, 0)
					}
					return fnErr
				})
			})
			return lastResult, err
		}, log, &phasesMu, result)
	}

	// Suppress unused variable warning — trackPhaseWithRetry is used in extraction phases.
	_ = trackPhaseWithRetry

	// ===== Phase 0: Derive Company Info (URL-only mode) =====
	var probeResult *model.ProbeResult

	// Detect and log input mode.
	company.InputMode = DetectInputMode(company)
	log.Info("pipeline: input mode", zap.String("input_mode", string(company.InputMode)))

	if company.Name == "" {
		trackPhase("0_derive", func() (*model.PhaseResult, error) {
			lc := NewLocalCrawlerWithMatcher(p.chain.PathMatcher)
			probe, probeErr := lc.Probe(ctx, company.URL)
			if probeErr != nil {
				return nil, probeErr
			}
			probeResult = probe

			if probe.Reachable && !probe.Blocked && len(probe.Body) > 0 {
				name, city, state := DeriveCompanyInfo(probe.Body, probe.FinalURL)
				if name != "" {
					company.Name = name
				}
				if company.City == "" {
					company.City = city
				}
				if company.State == "" {
					company.State = state
				}
				if company.Location == "" && city != "" && state != "" {
					company.Location = city + ", " + state
				}
			}

			meta := map[string]any{
				"derived_name": company.Name,
				"reachable":    probe.Reachable,
			}
			if probe.Blocked {
				meta["blocked"] = true
				meta["block_type"] = probe.BlockType
			}
			return &model.PhaseResult{Metadata: meta}, nil
		})
		result.Company = company
	}

	// ===== Phase 1: Data Collection (1A, 1B, 1C, 1D conditionally in parallel) =====
	setStatus(model.RunStatusCrawling)
	hasName := company.Name != ""

	var crawlResult *model.CrawlResult
	var externalPages []model.CrawledPage
	var linkedInData *LinkedInData
	var pppMatches []ppp.LoanMatch
	var totalUsage model.TokenUsage

	g, gCtx := errgroup.WithContext(ctx)

	// Track Phase 1 sub-phase outcomes for error categorization.
	var phase1Mu sync.Mutex
	phase1Results := make(map[string]bool) // phase name → succeeded

	// Phase 1A: Crawl — always runs. Pass probe to skip re-probe when available.
	g.Go(func() error {
		pr := trackPhase("1a_crawl", func() (*model.PhaseResult, error) {
			cr, crawlErr := CrawlPhase(gCtx, company, p.cfg.Crawl, p.store, p.chain, p.firecrawl, probeResult)
			if crawlErr != nil {
				return nil, crawlErr
			}
			crawlResult = cr
			return &model.PhaseResult{
				Metadata: map[string]any{
					"source":      cr.Source,
					"pages_count": cr.PagesCount,
					"from_cache":  cr.FromCache,
				},
			}, nil
		})
		phase1Mu.Lock()
		phase1Results["1a_crawl"] = pr.Status == model.PhaseStatusComplete
		phase1Mu.Unlock()
		return nil
	})

	// Phase 1B: External Scrape — needs Name. Skipped in sourcing mode.
	if isSourcing {
		trackPhase("1b_scrape", func() (*model.PhaseResult, error) {
			return &model.PhaseResult{
				Status:   model.PhaseStatusSkipped,
				Metadata: map[string]any{"reason": "sourcing_mode"},
			}, nil
		})
	} else if hasName {
		g.Go(func() error {
			pr := trackPhase("1b_scrape", func() (*model.PhaseResult, error) {
				ep, addrMatches, sourceResults := ScrapePhase(gCtx, company, p.jina, p.chain, p.perplexity, p.google, p.cfg.Scrape)
				externalPages = ep
				metadata := map[string]any{
					"external_pages": len(ep),
					"source_results": sourceResults,
				}
				if len(addrMatches) > 0 {
					metadata["address_matches"] = addrMatches
				}
				return &model.PhaseResult{
					Metadata: metadata,
				}, nil
			})
			phase1Mu.Lock()
			phase1Results["1b_scrape"] = pr.Status == model.PhaseStatusComplete
			phase1Mu.Unlock()
			return nil
		})
	} else {
		trackPhase("1b_scrape", func() (*model.PhaseResult, error) {
			return &model.PhaseResult{
				Status:   model.PhaseStatusSkipped,
				Metadata: map[string]any{"reason": "no_company_name"},
			}, nil
		})
	}

	// Phase 1C: LinkedIn — needs Name.
	if hasName {
		g.Go(func() error {
			pr := trackPhase("1c_linkedin", func() (*model.PhaseResult, error) {
				ld, usage, liErr := LinkedInPhase(gCtx, company, p.chain, p.perplexity, p.anthropic, p.cfg.Anthropic, p.store)
				if liErr != nil {
					return nil, liErr
				}
				linkedInData = ld
				if usage != nil {
					totalUsage.Add(*usage)
				}
				return &model.PhaseResult{
					TokenUsage: *usage,
				}, nil
			})
			phase1Mu.Lock()
			phase1Results["1c_linkedin"] = pr.Status == model.PhaseStatusComplete
			phase1Mu.Unlock()
			return nil
		})
	} else {
		trackPhase("1c_linkedin", func() (*model.PhaseResult, error) {
			return &model.PhaseResult{
				Status:   model.PhaseStatusSkipped,
				Metadata: map[string]any{"reason": "no_company_name"},
			}, nil
		})
	}

	// Phase 1D: PPP Loan Lookup — needs Name + Location.
	if hasName && company.Location != "" {
		g.Go(func() error {
			pr := trackPhase("1d_ppp", func() (*model.PhaseResult, error) {
				matches, pppErr := PPPPhase(gCtx, company, p.ppp)
				if pppErr != nil {
					return nil, pppErr
				}
				pppMatches = matches
				return &model.PhaseResult{
					Metadata: map[string]any{
						"matches": len(matches),
					},
				}, nil
			})
			phase1Mu.Lock()
			phase1Results["1d_ppp"] = pr.Status == model.PhaseStatusComplete
			phase1Mu.Unlock()
			return nil
		})
	} else {
		trackPhase("1d_ppp", func() (*model.PhaseResult, error) {
			return &model.PhaseResult{
				Status:   model.PhaseStatusSkipped,
				Metadata: map[string]any{"reason": "no_name_or_location"},
			}, nil
		})
	}

	_ = g.Wait()

	// Post-Phase-1 name recovery: if Phase 0 failed (or was skipped) but crawl succeeded.
	if company.Name == "" && crawlResult != nil {
		company.Name = deriveNameFromPages(crawlResult.Pages, company.URL)
		result.Company = company
		if company.Name != "" {
			log.Info("pipeline: derived name from crawl pages", zap.String("derived_name", company.Name))
		}
	}

	// Categorize Phase 1 errors: count data-producing phases that succeeded.
	dataPhases := []string{"1a_crawl", "1b_scrape", "1c_linkedin"}
	var succeeded, failed int
	var failedNames []string
	for _, name := range dataPhases {
		if phase1Results[name] {
			succeeded++
		} else {
			failed++
			failedNames = append(failedNames, name)
		}
	}

	if succeeded == 0 && failed == len(dataPhases) {
		allFailedErr := eris.Errorf("pipeline: all Phase 1 data sources failed (%s)", strings.Join(failedNames, ", "))
		failRun(allFailedErr, "1_data_collection")
		return result, allFailedErr
	}
	if failed > 0 {
		log.Warn("pipeline: some Phase 1 sources failed, continuing with partial data",
			zap.Strings("failed_phases", failedNames),
			zap.Int("succeeded", succeeded),
			zap.Int("failed", failed),
		)
	}

	// Store PPP matches.
	result.PPPMatches = pppMatches

	// Combine pages.
	var allPages []model.CrawledPage
	if crawlResult != nil {
		allPages = append(allPages, crawlResult.Pages...)
	}
	allPages = append(allPages, externalPages...)

	// Add LinkedIn data as a synthetic page if available.
	if linkedInData != nil {
		allPages = append(allPages, linkedInToPage(linkedInData, company))
	}

	if len(allPages) == 0 {
		noPagesErr := eris.New("pipeline: no pages collected")
		failRun(noPagesErr, "1_data_collection")
		return result, noPagesErr
	}

	// Post-Phase-1: extract structured address from BBB/SoS pages if missing.
	if company.Street == "" || company.City == "" {
		for _, page := range externalPages {
			street, city, state, zip, extracted := ExtractStructuredAddress(page.Markdown, page.Title)
			if extracted {
				if company.Street == "" {
					company.Street = street
				}
				if company.City == "" {
					company.City = city
				}
				if company.State == "" {
					company.State = state
				}
				if company.ZipCode == "" {
					company.ZipCode = zip
				}
				if company.Location == "" && city != "" && state != "" {
					company.Location = city + ", " + state
				}
				result.Company = company
				log.Info("pipeline: extracted address from external page",
					zap.String("source", page.Title),
					zap.String("street", street),
					zap.String("city", city),
					zap.String("state", state),
					zap.String("zip", zip),
				)
				break
			}
		}
	}

	// ===== Phase 2: Classification =====
	setStatus(model.RunStatusClassifying)
	var pageIndex model.PageIndex

	trackPhaseWithRetry("2_classify", "anthropic", func() (*model.PhaseResult, error) {
		idx, usage, classifyErr := ClassifyPhase(ctx, allPages, p.anthropic, p.cfg.Anthropic)
		if classifyErr != nil {
			return nil, classifyErr
		}
		pageIndex = idx
		if usage != nil {
			totalUsage.Add(*usage)
		}
		return &model.PhaseResult{
			TokenUsage: *usage,
			Metadata: map[string]any{
				"page_types": len(idx),
			},
		}, nil
	})

	if pageIndex == nil {
		pageIndex = make(model.PageIndex)
	}

	// ===== Phase 3: Routing =====
	// In sourcing mode, filter to P0+P1 questions only.
	questionsForRouting := p.questions
	if isSourcing {
		questionsForRouting = model.FilterByMaxPriority(p.questions, "P1")
		log.Info("pipeline: sourcing mode question filter",
			zap.Int("total_questions", len(p.questions)),
			zap.Int("after_filter", len(questionsForRouting)),
		)
	}

	var batches *model.RoutedBatches
	trackPhase("3_route", func() (*model.PhaseResult, error) {
		batches = RouteQuestions(questionsForRouting, pageIndex)
		return &model.PhaseResult{
			Metadata: map[string]any{
				"tier1_count":   len(batches.Tier1),
				"tier2_count":   len(batches.Tier2),
				"tier3_count":   len(batches.Tier3),
				"skipped_count": len(batches.Skipped),
			},
		}, nil
	})

	// --- Optimization: Existing-answer lookup ---
	// Skip questions that already have high-confidence answers from prior runs.
	// Disabled when forceReExtract is set (--force flag).
	var existingAnswers []model.ExtractionAnswer
	var skippedByExisting int
	if !p.forceReExtract {
		skipThreshold := p.cfg.Pipeline.SkipConfidenceThreshold
		if skipThreshold <= 0 {
			skipThreshold = 0.8
		}
		var reuseTTL time.Duration
		if p.cfg.Pipeline.AnswerReuseTTLDays > 0 {
			reuseTTL = time.Duration(p.cfg.Pipeline.AnswerReuseTTLDays) * 24 * time.Hour
		}
		var existErr error
		existingAnswers, existErr = p.store.GetHighConfidenceAnswers(ctx, company.URL, skipThreshold, reuseTTL)
		if existErr != nil {
			log.Warn("pipeline: failed to load existing answers", zap.Error(existErr))
		}
		if len(existingAnswers) > 0 {
			existingKeys := make(map[string]bool, len(existingAnswers))
			for _, a := range existingAnswers {
				existingKeys[a.FieldKey] = true
			}
			batches.Tier1 = filterRoutedQuestions(batches.Tier1, existingKeys, &skippedByExisting)
			batches.Tier2 = filterRoutedQuestions(batches.Tier2, existingKeys, &skippedByExisting)
			batches.Tier3 = filterRoutedQuestions(batches.Tier3, existingKeys, &skippedByExisting)
			if skippedByExisting > 0 {
				log.Info("pipeline: skipped questions with existing high-confidence answers",
					zap.Int("skipped", skippedByExisting),
					zap.Int("existing_answers", len(existingAnswers)),
				)
			}
		}
	}

	// --- Optimization: ADV pre-fill ---
	// If a CRD number is available (e.g., from pre-seeded data) and a fedsync
	// pool is connected, pre-fill answers from ADV filing data.
	var advPrefilled []model.ExtractionAnswer
	if p.fedsyncPool != nil {
		var crdNumber int
		if v, ok := company.PreSeeded["crd_number"]; ok {
			switch n := v.(type) {
			case int:
				crdNumber = n
			case float64:
				crdNumber = int(n)
			}
		}
		if crdNumber > 0 {
			prefilled, prefillErr := prefillFromADV(ctx, p.fedsyncPool, crdNumber, questionsForRouting)
			if prefillErr != nil {
				log.Warn("pipeline: ADV pre-fill failed", zap.Error(prefillErr))
			} else if len(prefilled) > 0 {
				advPrefilled = prefilled
				// Filter pre-filled questions from extraction batches.
				pfKeys := prefilledKeySet(prefilled)
				var pfSkipped int
				batches.Tier1, pfSkipped = filterPrefilledQuestions(batches.Tier1, pfKeys)
				t2Skip := 0
				batches.Tier2, t2Skip = filterPrefilledQuestions(batches.Tier2, pfKeys)
				pfSkipped += t2Skip
				t3Skip := 0
				batches.Tier3, t3Skip = filterPrefilledQuestions(batches.Tier3, pfKeys)
				pfSkipped += t3Skip
				log.Info("pipeline: ADV pre-fill complete",
					zap.Int("prefilled_answers", len(prefilled)),
					zap.Int("skipped_questions", pfSkipped),
				)
			}
		}
	}

	// --- Optimization: Checkpoint/resume ---
	// Check for existing T1 checkpoint from a prior failed run.
	var checkpointT1 []model.ExtractionAnswer
	checkpoint, cpErr := p.store.LoadCheckpoint(ctx, company.URL)
	if cpErr != nil {
		log.Warn("pipeline: failed to load checkpoint", zap.Error(cpErr))
	}
	if checkpoint != nil && checkpoint.Phase == "t1_complete" {
		if err := json.Unmarshal(checkpoint.Data, &checkpointT1); err != nil {
			log.Warn("pipeline: failed to parse checkpoint data", zap.Error(err))
		} else {
			log.Info("pipeline: resuming from T1 checkpoint",
				zap.Int("checkpoint_answers", len(checkpointT1)),
			)
		}
	}

	// --- Optimization: Per-company cost budget ---
	maxCost := p.cfg.Pipeline.MaxCostPerCompanyUSD
	if maxCost <= 0 {
		maxCost = 10.0
	}
	var cumulativeCost float64

	// ===== Phases 4+5: T1, T2-native, and T2-escalated with max overlap =====
	// T1 and T2-native start in parallel. Once T1 completes, T2-escalated
	// starts immediately (overlapping with the still-running T2-native).
	setStatus(model.RunStatusExtracting)
	var t1Answers []model.ExtractionAnswer
	var t2NativeAnswers []model.ExtractionAnswer
	var t2NativeUsage model.TokenUsage
	var escalatedAnswers []model.ExtractionAnswer
	var escalatedUsage model.TokenUsage

	// Channel signals T1 completion so T2-escalated can start immediately.
	t1Done := make(chan struct{})

	g2, g2Ctx := errgroup.WithContext(ctx)

	// Phase 4: T1 extraction (concurrent with T2-native).
	// If we have a checkpoint, skip T1 extraction and use cached answers.
	g2.Go(func() error {
		defer close(t1Done) // Signal T1 is complete, unblocking T2-escalated.

		if len(checkpointT1) > 0 {
			trackPhase("4_extract_t1", func() (*model.PhaseResult, error) {
				t1Answers = checkpointT1
				return &model.PhaseResult{
					Status: model.PhaseStatusComplete,
					Metadata: map[string]any{
						"answers":         len(checkpointT1),
						"from_checkpoint": true,
					},
				}, nil
			})
			return nil
		}

		trackPhaseWithRetry("4_extract_t1", "anthropic", func() (*model.PhaseResult, error) {
			t1Result, t1Err := ExtractTier1(g2Ctx, batches.Tier1, company, pppMatches, p.anthropic, p.cfg.Anthropic)
			if t1Err != nil {
				return nil, t1Err
			}
			t1Answers = t1Result.Answers
			totalUsage.Add(t1Result.TokenUsage)

			// Save T1 checkpoint for resume on failure.
			if cpData, marshalErr := json.Marshal(t1Result.Answers); marshalErr == nil {
				if saveErr := p.store.SaveCheckpoint(ctx, company.URL, "t1_complete", cpData); saveErr != nil {
					log.Warn("pipeline: failed to save T1 checkpoint", zap.Error(saveErr))
				}
			}

			return &model.PhaseResult{
				TokenUsage: t1Result.TokenUsage,
				Metadata: map[string]any{
					"answers":     len(t1Result.Answers),
					"duration_ms": t1Result.Duration,
				},
			}, nil
		})
		return nil
	})

	// T2-native: questions routed directly to T2. Waits for T1 so it can
	// receive T1 answers as supplementary context for better synthesis.
	// Skipped in sourcing mode (lean T1 only).
	if len(batches.Tier2) > 0 && !isSourcing {
		g2.Go(func() error {
			// Wait for T1 to finish so we can pass its answers as context.
			select {
			case <-t1Done:
			case <-g2Ctx.Done():
				return nil
			}

			t2Result, t2Err := ExtractTier2(g2Ctx, batches.Tier2, t1Answers, company, pppMatches, p.anthropic, p.cfg.Anthropic)
			if t2Err != nil {
				zap.L().Warn("pipeline: t2-native extraction failed", zap.Error(t2Err))
				return nil
			}
			t2NativeAnswers = t2Result.Answers
			t2NativeUsage = t2Result.TokenUsage
			return nil
		})
	}

	// T2-escalated: starts as soon as T1 completes, overlapping with T2-native.
	// Skipped in sourcing mode (no confidence-based re-queuing).
	if !isSourcing {
		g2.Go(func() error {
			select {
			case <-t1Done:
			case <-g2Ctx.Done():
				return nil
			}

			esc := EscalateQuestions(t1Answers, p.questions, pageIndex, p.cfg.Pipeline.ConfidenceEscalationThreshold, p.cfg.Pipeline.EscalationFailRateThreshold)
			if len(esc) == 0 {
				return nil
			}

			t2Result, t2Err := ExtractTier2(g2Ctx, esc, t1Answers, company, pppMatches, p.anthropic, p.cfg.Anthropic)
			if t2Err != nil {
				zap.L().Warn("pipeline: t2-escalated extraction failed", zap.Error(t2Err))
				return nil
			}
			escalatedAnswers = t2Result.Answers
			escalatedUsage = t2Result.TokenUsage
			return nil
		})
	}

	_ = g2.Wait()

	// Escalation count for reporting (re-derive from answers).
	var escalated []model.RoutedQuestion
	if !isSourcing {
		escalated = EscalateQuestions(t1Answers, p.questions, pageIndex, p.cfg.Pipeline.ConfidenceEscalationThreshold, p.cfg.Pipeline.EscalationFailRateThreshold)
	}

	// ===== Phase 5: Combine T2 results =====
	var t2Answers []model.ExtractionAnswer

	trackPhase("5_extract_t2", func() (*model.PhaseResult, error) {
		// Merge T2-native + T2-escalated.
		t2Answers = append(t2NativeAnswers, escalatedAnswers...)

		// Combine usage for reporting.
		combinedUsage := t2NativeUsage
		combinedUsage.Add(escalatedUsage)
		totalUsage.Add(combinedUsage)

		return &model.PhaseResult{
			TokenUsage: combinedUsage,
			Metadata: map[string]any{
				"answers":   len(t2Answers),
				"escalated": len(escalated),
				"native":    len(t2NativeAnswers),
			},
		}, nil
	})

	// ===== Phase 6: Tier 3 Extraction =====
	var t3Answers []model.ExtractionAnswer

	// Update cumulative cost from phases so far.
	for _, ph := range result.Phases {
		cumulativeCost += ph.TokenUsage.Cost
	}

	// Determine if T3 should run. Sourcing mode always skips T3.
	shouldRunT3 := len(batches.Tier3) > 0
	var t3SkipReason string
	if isSourcing {
		shouldRunT3 = false
		t3SkipReason = "sourcing_mode"
	}
	switch p.cfg.Pipeline.Tier3Gate {
	case "always":
		// Run T3 unconditionally (if there are T3 questions).
	case "ambiguity_only":
		// Only run T3 if there are ambiguous answers.
		allCurrent := MergeAnswers(t1Answers, t2Answers, nil)
		hasAmbiguity := false
		for _, a := range allCurrent {
			if a.Confidence < 0.6 {
				hasAmbiguity = true
				break
			}
		}
		shouldRunT3 = shouldRunT3 && hasAmbiguity
	default: // "off" or unrecognized — skip T3 entirely.
		shouldRunT3 = false
	}

	// Cost budget gate: skip T3 if cumulative cost exceeds budget.
	if shouldRunT3 && cumulativeCost >= maxCost {
		shouldRunT3 = false
		t3SkipReason = "cost_budget_exceeded"
		log.Warn("pipeline: skipping T3 due to cost budget",
			zap.Float64("cumulative_cost", cumulativeCost),
			zap.Float64("max_cost", maxCost),
		)
	}

	if shouldRunT3 {
		trackPhaseWithRetry("6_extract_t3", "anthropic", func() (*model.PhaseResult, error) {
			t3Result, t3Err := ExtractTier3(ctx, batches.Tier3, MergeAnswers(t1Answers, t2Answers, nil), allPages, company, pppMatches, p.anthropic, p.cfg.Anthropic)
			if t3Err != nil {
				return nil, t3Err
			}
			t3Answers = t3Result.Answers
			totalUsage.Add(t3Result.TokenUsage)
			return &model.PhaseResult{
				TokenUsage: t3Result.TokenUsage,
				Metadata: map[string]any{
					"answers": len(t3Result.Answers),
				},
			}, nil
		})
	} else {
		if t3SkipReason == "" {
			t3SkipReason = "not needed"
			if p.cfg.Pipeline.Tier3Gate == "off" || p.cfg.Pipeline.Tier3Gate == "" {
				t3SkipReason = "tier3_gate=off (use --with-t3 to enable)"
			}
		}
		trackPhase("6_extract_t3", func() (*model.PhaseResult, error) {
			return &model.PhaseResult{
				Status: model.PhaseStatusSkipped,
				Metadata: map[string]any{
					"reason": t3SkipReason,
				},
			}, nil
		})
	}

	// ===== Phase 7: Aggregate =====
	setStatus(model.RunStatusAggregating)

	var allAnswers []model.ExtractionAnswer
	var fieldValues map[string]model.FieldValue

	trackPhase("7_aggregate", func() (*model.PhaseResult, error) {
		allAnswers = MergeAnswers(t1Answers, t2Answers, t3Answers)
		// Merge in ADV pre-filled answers (Tier 0, high confidence).
		if len(advPrefilled) > 0 {
			allAnswers = MergeAnswers(advPrefilled, allAnswers, nil)
		}
		// Merge in existing high-confidence answers for fields we skipped.
		if len(existingAnswers) > 0 {
			allAnswers = MergeAnswers(existingAnswers, allAnswers, nil)
		}
		// Inject LinkedIn executive contacts as a "contacts" answer.
		if linkedInData != nil && len(linkedInData.ExecContacts) > 0 {
			contacts := make([]map[string]string, 0, len(linkedInData.ExecContacts))
			for _, c := range linkedInData.ExecContacts {
				contacts = append(contacts, map[string]string{
					"first_name":   c.FirstName,
					"last_name":    c.LastName,
					"title":        c.Title,
					"email":        c.Email,
					"phone":        c.Phone,
					"linkedin_url": c.LinkedInURL,
				})
			}
			allAnswers = appendOrUpgrade(allAnswers, "contacts",
				contacts, 0.75, "linkedin")
		}
		// Merge contacts from multiple sources (LinkedIn + web extraction).
		allAnswers = MergeContacts(allAnswers)
		// Parse phone numbers from homepage/contact pages (deterministic).
		parsePhoneFromPages(allPages, pageIndex)
		// Inject review metadata directly from scraped pages (bypasses LLM).
		allAnswers = InjectPageMetadata(allAnswers, allPages, company.PreSeeded)
		// Cross-validate employee count against LinkedIn range.
		allAnswers = CrossValidateEmployeeCount(allAnswers, linkedInData)
		// Validate NAICS codes against reference data and cross-reference with SoS filings.
		allAnswers = ValidateAndCrossReferenceNAICS(allAnswers, allPages)
		// Normalize business model to canonical taxonomy.
		allAnswers = NormalizeBusinessModelAnswer(allAnswers)
		// Enrich with CBP-based revenue estimate if available.
		allAnswers = EnrichWithRevenueEstimate(ctx, allAnswers, company, p.estimator)
		// Enrich with PPP loan data (revenue + employees from database).
		allAnswers = EnrichFromPPP(allAnswers, pppMatches)
		fieldValues = BuildFieldValues(allAnswers, p.fields, company)
		populateOwnerFromContacts(fieldValues, p.fields)
		return &model.PhaseResult{
			Metadata: map[string]any{
				"total_answers":        len(allAnswers),
				"field_values":         len(fieldValues),
				"reused_from_existing": len(existingAnswers),
				"skipped_by_existing":  skippedByExisting,
			},
		}, nil
	})

	result.Answers = allAnswers
	result.FieldValues = fieldValues

	// ===== Phase 7B: Waterfall Cascade =====
	var waterfallRes *waterfall.WaterfallResult
	if p.waterfallExec != nil {
		trackPhase("7b_waterfall", func() (*model.PhaseResult, error) {
			wr, wfErr := p.waterfallExec.Run(ctx, company, fieldValues)
			if wfErr != nil {
				return nil, wfErr
			}
			waterfallRes = wr
			// Apply waterfall results back into field values.
			fieldValues = waterfall.ApplyToFieldValues(fieldValues, wr)
			result.FieldValues = fieldValues
			return &model.PhaseResult{
				TokenUsage: model.TokenUsage{
					Cost: wr.TotalPremiumUSD,
				},
				Metadata: map[string]any{
					"fields_resolved":  wr.FieldsResolved,
					"fields_total":     wr.FieldsTotal,
					"premium_cost_usd": wr.TotalPremiumUSD,
				},
			}, nil
		})
	}

	// ===== Phase 7C: Field Provenance =====
	trackPhase("7c_provenance", func() (*model.PhaseResult, error) {
		// Load previous provenance for override detection (non-fatal).
		prevProvenance, prevErr := p.store.GetLatestProvenance(ctx, company.URL)
		if prevErr != nil {
			log.Warn("pipeline: failed to load previous provenance", zap.Error(prevErr))
		}

		provenanceRecords := BuildProvenance(
			run.ID, company.URL, fieldValues, allAnswers,
			waterfallRes, prevProvenance, p.fields,
		)

		if saveErr := p.store.SaveProvenance(ctx, provenanceRecords); saveErr != nil {
			log.Warn("pipeline: failed to save provenance", zap.Error(saveErr))
		}

		return &model.PhaseResult{
			Metadata: map[string]any{
				"fields_tracked": len(provenanceRecords),
				"values_changed": CountChanged(provenanceRecords),
			},
		}, nil
	})

	// ===== Phase 7D: Geocode =====
	if p.cfg.Geo.Enabled && p.geocoder != nil {
		trackPhase("7d_geocode", func() (*model.PhaseResult, error) {
			phaseRes, phaseErr := p.Phase7DGeocode(ctx, company, run.ID)
			if phaseErr == nil && phaseRes != nil {
				result.GeoData = p.collectGeoData(ctx, company)
			}
			return phaseRes, phaseErr
		})
	}

	// ===== Phase 8: Report =====
	// Set totalUsage.Cost from per-phase costs so the report shows the correct total.
	var reportCost float64
	for _, ph := range result.Phases {
		reportCost += ph.TokenUsage.Cost
	}
	totalUsage.Cost = reportCost

	trackPhase("8_report", func() (*model.PhaseResult, error) {
		report := FormatReport(company, allAnswers, fieldValues, result.Phases, totalUsage)
		result.Report = report
		return &model.PhaseResult{}, nil
	})

	// ===== Phase 9: Quality Gate =====
	setStatus(model.RunStatusWritingSF)

	if p.deferSFWrites && p.onWriteIntent != nil {
		// Deferred mode: build write intent without executing SF writes.
		trackPhaseWithRetry("9_gate", "salesforce", func() (*model.PhaseResult, error) {
			gate, intent, gateErr := PrepareGate(ctx, result, p.fields, p.questions, p.salesforce, p.notion, p.cfg)
			if gateErr != nil {
				return nil, gateErr
			}
			if intent != nil {
				p.onWriteIntent(intent)
			}
			return &model.PhaseResult{
				Metadata: map[string]any{
					"score":           gate.Score,
					"score_breakdown": gate.ScoreBreakdown,
					"passed":          gate.Passed,
					"dedup_match":     gate.DedupMatch,
					"manual_review":   gate.ManualReview,
					"deferred":        true,
				},
			}, nil
		})
	} else {
		// Immediate mode: execute SF writes inline (single-company run).
		trackPhaseWithRetry("9_gate", "salesforce", func() (*model.PhaseResult, error) {
			gate, gateErr := QualityGate(ctx, result, p.fields, p.questions, p.salesforce, p.notion, p.cfg)
			if gateErr != nil {
				return nil, gateErr
			}
			return &model.PhaseResult{
				Metadata: map[string]any{
					"score":           gate.Score,
					"score_breakdown": gate.ScoreBreakdown,
					"passed":          gate.Passed,
					"sf_updated":      gate.SFUpdated,
					"manual_review":   gate.ManualReview,
				},
			}, nil
		})
	}

	// Finalize: sum per-phase costs for accurate per-model pricing.
	result.TotalTokens = totalUsage.InputTokens + totalUsage.OutputTokens
	var totalCost float64
	for _, ph := range result.Phases {
		totalCost += ph.TokenUsage.Cost
	}
	result.TotalCost = totalCost

	setStatus(model.RunStatusComplete)

	// Save final result.
	runResult := &model.RunResult{
		Score:          result.Score,
		FieldsFound:    len(fieldValues),
		FieldsTotal:    len(p.fields.Fields),
		TotalTokens:    result.TotalTokens,
		TotalCost:      result.TotalCost,
		Phases:         result.Phases,
		Answers:        allAnswers,
		Report:         result.Report,
		SalesforceSync: true,
	}
	if saveErr := p.store.UpdateRunResult(ctx, run.ID, runResult); saveErr != nil {
		log.Warn("pipeline: failed to save run result", zap.Error(saveErr))
	}

	// Clean up checkpoint on successful completion.
	if delErr := p.store.DeleteCheckpoint(ctx, company.URL); delErr != nil {
		log.Warn("pipeline: failed to delete checkpoint", zap.Error(delErr))
	}

	log.Info("pipeline: enrichment complete",
		zap.String("run_id", run.ID),
		zap.Float64("score", result.Score),
		zap.Int("fields", len(fieldValues)),
		zap.Int("tokens", result.TotalTokens),
	)

	return result, nil
}

// executePhase runs a phase function with tracking, logging, cost computation, and persistence.
func (p *Pipeline) executePhase(ctx context.Context, runID, name string, fn func() (*model.PhaseResult, error), log *zap.Logger, phasesMu *sync.Mutex, result *model.EnrichmentResult) *model.PhaseResult {
	phase, phaseErr := p.store.CreatePhase(ctx, runID, name)
	if phaseErr != nil {
		log.Warn("pipeline: failed to create phase", zap.String("phase", name), zap.Error(phaseErr))
	}

	start := time.Now()
	phaseResult, fnErr := fn()
	duration := time.Since(start).Milliseconds()

	if phaseResult == nil {
		phaseResult = &model.PhaseResult{Name: name}
	}
	phaseResult.Name = name
	phaseResult.Duration = duration

	if fnErr != nil {
		phaseResult.Status = model.PhaseStatusFailed
		phaseResult.Error = fnErr.Error()
		log.Error("pipeline: phase failed",
			zap.String("phase", name),
			zap.Int64("duration_ms", duration),
			zap.Error(fnErr),
		)
	} else {
		phaseResult.Status = model.PhaseStatusComplete
		log.Info("pipeline: phase complete",
			zap.String("phase", name),
			zap.Int64("duration_ms", duration),
		)
	}

	// Compute per-phase cost based on model used.
	if phaseResult.Status != model.PhaseStatusSkipped {
		phaseResult.TokenUsage.Cost = p.computePhaseCost(name, phaseResult.TokenUsage)
	}

	if phase != nil {
		if cpErr := p.store.CompletePhase(ctx, phase.ID, phaseResult); cpErr != nil {
			log.Warn("pipeline: failed to persist phase result",
				zap.String("phase", name),
				zap.String("phase_id", phase.ID),
				zap.Error(cpErr),
			)
		}
	}
	phasesMu.Lock()
	result.Phases = append(result.Phases, *phaseResult)
	phasesMu.Unlock()
	return phaseResult
}

// computePhaseCost maps a phase name to the correct model and computes cost.
func (p *Pipeline) computePhaseCost(phase string, usage model.TokenUsage) float64 {
	var modelName string
	isBatch := !p.cfg.Anthropic.NoBatch

	switch phase {
	case "1c_linkedin", "2_classify", "4_extract_t1":
		modelName = p.cfg.Anthropic.HaikuModel
	case "5_extract_t2":
		modelName = p.cfg.Anthropic.SonnetModel
	case "6_extract_t3":
		modelName = p.cfg.Anthropic.OpusModel
	default:
		return usage.Cost // preserve any cost already set (e.g., waterfall premium)
	}

	// Warn if model has no pricing entry — cost will report as $0.
	if _, ok := p.cfg.Pricing.Anthropic[modelName]; !ok {
		zap.L().Warn("pipeline: no pricing entry for model, cost will be $0",
			zap.String("model", modelName),
			zap.String("phase", phase),
		)
	}

	return p.costCalc.Claude(modelName, isBatch,
		usage.InputTokens, usage.OutputTokens,
		usage.CacheCreationTokens, usage.CacheReadTokens,
	)
}

// filterRoutedQuestions removes questions whose field keys already have
// high-confidence answers, returning only questions that still need extraction.
func filterRoutedQuestions(routed []model.RoutedQuestion, existingKeys map[string]bool, skipped *int) []model.RoutedQuestion {
	var filtered []model.RoutedQuestion
	for _, rq := range routed {
		if existingKeys[rq.Question.FieldKey] {
			*skipped++
			continue
		}
		filtered = append(filtered, rq)
	}
	return filtered
}

// parsePhoneFromPages extracts phone numbers from homepage/contact pages
// and attaches them as metadata for deterministic injection.
func parsePhoneFromPages(pages []model.CrawledPage, pageIndex model.PageIndex) {
	// Build a set of URLs classified as homepage, contact, about, or services.
	// Phone numbers commonly appear on all of these page types.
	targetURLs := make(map[string]bool)
	for _, pt := range []model.PageType{
		model.PageTypeHomepage,
		model.PageTypeContact,
		model.PageTypeAbout,
		model.PageTypeServices,
	} {
		for _, cp := range pageIndex[pt] {
			targetURLs[cp.URL] = true
		}
	}

	for i := range pages {
		if !targetURLs[pages[i].URL] {
			continue
		}
		phone := ParsePhoneFromMarkdown(pages[i].Markdown)
		if phone == "" {
			continue
		}
		if pages[i].Metadata == nil {
			pages[i].Metadata = &model.PageMetadata{}
		}
		pages[i].Metadata.Phone = phone
		return // Use first phone found
	}
}

// linkedInToPage converts LinkedIn data into a synthetic CrawledPage.
func linkedInToPage(data *LinkedInData, company model.Company) model.CrawledPage {
	var content strings.Builder
	content.WriteString("# LinkedIn Company Profile\n\n")
	if data.CompanyName != "" {
		content.WriteString("**Company Name:** " + data.CompanyName + "\n")
	}
	if data.Description != "" {
		content.WriteString("**Description:** " + data.Description + "\n")
	}
	if data.Industry != "" {
		content.WriteString("**Industry:** " + data.Industry + "\n")
	}
	if data.EmployeeCount != "" {
		content.WriteString("**Employee Count:** " + data.EmployeeCount + "\n")
	}
	if data.Headquarters != "" {
		content.WriteString("**Headquarters:** " + data.Headquarters + "\n")
	}
	if data.Founded != "" {
		content.WriteString("**Founded:** " + data.Founded + "\n")
	}
	if data.Specialties != "" {
		content.WriteString("**Specialties:** " + data.Specialties + "\n")
	}
	if data.Website != "" {
		content.WriteString("**Website:** " + data.Website + "\n")
	}
	if data.CompanyType != "" {
		content.WriteString("**Company Type:** " + data.CompanyType + "\n")
	}
	// Render executive contacts.
	if len(data.ExecContacts) > 0 {
		for i, c := range data.ExecContacts {
			fmt.Fprintf(&content, "**Executive %d:** %s %s, %s\n", i+1, c.FirstName, c.LastName, c.Title)
			if c.Email != "" {
				content.WriteString("  Email: " + c.Email + "\n")
			}
			if c.LinkedInURL != "" {
				content.WriteString("  LinkedIn: " + c.LinkedInURL + "\n")
			}
		}
	} else {
		// Fallback to flat exec fields for backward compat.
		if data.ExecFirstName != "" {
			content.WriteString("**CEO/Owner First Name:** " + data.ExecFirstName + "\n")
		}
		if data.ExecLastName != "" {
			content.WriteString("**CEO/Owner Last Name:** " + data.ExecLastName + "\n")
		}
		if data.ExecTitle != "" {
			content.WriteString("**CEO/Owner Title:** " + data.ExecTitle + "\n")
		}
	}

	return model.CrawledPage{
		URL:        data.LinkedInURL,
		Title:      "[linkedin] " + company.Name,
		Markdown:   content.String(),
		StatusCode: 200,
	}
}
