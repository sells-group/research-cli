package pipeline

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/cost"
	"github.com/sells-group/research-cli/internal/estimate"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/scrape"
	"github.com/sells-group/research-cli/internal/store"
	"github.com/sells-group/research-cli/internal/waterfall"
	"github.com/sells-group/research-cli/pkg/anthropic"
	"github.com/sells-group/research-cli/pkg/firecrawl"
	"github.com/sells-group/research-cli/pkg/jina"
	"github.com/sells-group/research-cli/pkg/notion"
	"github.com/sells-group/research-cli/pkg/perplexity"
	"github.com/sells-group/research-cli/pkg/ppp"
	"github.com/sells-group/research-cli/pkg/salesforce"
)

// Pipeline orchestrates phases 1-9 of the enrichment pipeline.
type Pipeline struct {
	cfg        *config.Config
	store      store.Store
	chain      *scrape.Chain
	jina       jina.Client
	firecrawl  firecrawl.Client
	perplexity perplexity.Client
	anthropic  anthropic.Client
	salesforce salesforce.Client
	notion     notion.Client
	ppp        ppp.Querier
	costCalc       *cost.Calculator
	estimator      *estimate.RevenueEstimator
	waterfallExec  *waterfall.Executor
	questions      []model.Question
	fields         *model.FieldRegistry
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
	return &Pipeline{
		cfg:        cfg,
		store:      st,
		chain:      chain,
		jina:       jinaClient,
		firecrawl:  fcClient,
		perplexity: pplxClient,
		anthropic:  aiClient,
		salesforce: sfClient,
		notion:     notionClient,
		ppp:        pppClient,
		costCalc:      cost.NewCalculator(rates),
		estimator:     estimator,
		waterfallExec: waterfallExec,
		questions:     questions,
		fields:        fields,
	}
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
	log.Info("pipeline: starting enrichment")

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

	// Phase tracking helper with mutex for concurrent access.
	var phasesMu sync.Mutex
	trackPhase := func(name string, fn func() (*model.PhaseResult, error)) *model.PhaseResult {
		phase, phaseErr := p.store.CreatePhase(ctx, run.ID, name)
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

	// ===== Phase 1: Data Collection (1A, 1B, 1C, 1D in parallel) =====
	setStatus(model.RunStatusCrawling)

	var crawlResult *model.CrawlResult
	var externalPages []model.CrawledPage
	var linkedInData *LinkedInData
	var pppMatches []ppp.LoanMatch
	var totalUsage model.TokenUsage

	g, gCtx := errgroup.WithContext(ctx)

	// Track Phase 1 sub-phase outcomes for error categorization.
	var phase1Mu sync.Mutex
	phase1Results := make(map[string]bool) // phase name → succeeded

	// Phase 1A: Crawl
	g.Go(func() error {
		pr := trackPhase("1a_crawl", func() (*model.PhaseResult, error) {
			cr, crawlErr := CrawlPhase(gCtx, company, p.cfg.Crawl, p.store, p.chain, p.firecrawl)
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

	// Phase 1B: External Scrape (search-then-scrape)
	g.Go(func() error {
		pr := trackPhase("1b_scrape", func() (*model.PhaseResult, error) {
			ep, addrMatches, sourceResults := ScrapePhase(gCtx, company, p.jina, p.chain, p.cfg.Scrape)
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

	// Phase 1C: LinkedIn
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

	// Phase 1D: PPP Loan Lookup
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

	_ = g.Wait()

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
		setStatus(model.RunStatusFailed)
		return result, eris.Errorf("pipeline: all Phase 1 data sources failed (%s)", strings.Join(failedNames, ", "))
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
		setStatus(model.RunStatusFailed)
		return result, eris.New("pipeline: no pages collected")
	}

	// ===== Phase 2: Classification =====
	setStatus(model.RunStatusClassifying)
	var pageIndex model.PageIndex

	trackPhase("2_classify", func() (*model.PhaseResult, error) {
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
	var batches *model.RoutedBatches
	trackPhase("3_route", func() (*model.PhaseResult, error) {
		batches = RouteQuestions(p.questions, pageIndex)
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
	skipThreshold := p.cfg.Pipeline.SkipConfidenceThreshold
	if skipThreshold <= 0 {
		skipThreshold = 0.8
	}
	existingAnswers, existErr := p.store.GetHighConfidenceAnswers(ctx, company.URL, skipThreshold)
	if existErr != nil {
		log.Warn("pipeline: failed to load existing answers", zap.Error(existErr))
	}
	var skippedByExisting int
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
						"answers":        len(checkpointT1),
						"from_checkpoint": true,
					},
				}, nil
			})
			return nil
		}

		trackPhase("4_extract_t1", func() (*model.PhaseResult, error) {
			t1Result, t1Err := ExtractTier1(g2Ctx, batches.Tier1, p.anthropic, p.cfg.Anthropic)
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

	// T2-native: questions routed directly to T2 (no T1 dependency).
	if len(batches.Tier2) > 0 {
		g2.Go(func() error {
			t2Result, t2Err := ExtractTier2(g2Ctx, batches.Tier2, nil, p.anthropic, p.cfg.Anthropic)
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
	g2.Go(func() error {
		select {
		case <-t1Done:
		case <-g2Ctx.Done():
			return nil
		}

		esc := EscalateQuestions(t1Answers, p.questions, pageIndex, p.cfg.Pipeline.ConfidenceEscalationThreshold)
		if len(esc) == 0 {
			return nil
		}

		t2Result, t2Err := ExtractTier2(g2Ctx, esc, t1Answers, p.anthropic, p.cfg.Anthropic)
		if t2Err != nil {
			zap.L().Warn("pipeline: t2-escalated extraction failed", zap.Error(t2Err))
			return nil
		}
		escalatedAnswers = t2Result.Answers
		escalatedUsage = t2Result.TokenUsage
		return nil
	})

	_ = g2.Wait()

	// Escalation count for reporting (re-derive from answers).
	escalated := EscalateQuestions(t1Answers, p.questions, pageIndex, p.cfg.Pipeline.ConfidenceEscalationThreshold)

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

	// Determine if T3 should run.
	shouldRunT3 := len(batches.Tier3) > 0
	var t3SkipReason string
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
		trackPhase("6_extract_t3", func() (*model.PhaseResult, error) {
			t3Result, t3Err := ExtractTier3(ctx, batches.Tier3, MergeAnswers(t1Answers, t2Answers, nil), allPages, p.anthropic, p.cfg.Anthropic)
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
		// Merge in existing high-confidence answers for fields we skipped.
		if len(existingAnswers) > 0 {
			allAnswers = MergeAnswers(existingAnswers, allAnswers, nil)
		}
		// Enrich with CBP-based revenue estimate if available.
		allAnswers = EnrichWithRevenueEstimate(ctx, allAnswers, company, p.estimator)
		fieldValues = BuildFieldValues(allAnswers, p.fields)
		return &model.PhaseResult{
			Metadata: map[string]any{
				"total_answers":         len(allAnswers),
				"field_values":          len(fieldValues),
				"reused_from_existing":  len(existingAnswers),
				"skipped_by_existing":   skippedByExisting,
			},
		}, nil
	})

	result.Answers = allAnswers
	result.FieldValues = fieldValues

	// ===== Phase 7B: Waterfall Cascade =====
	if p.waterfallExec != nil {
		trackPhase("7b_waterfall", func() (*model.PhaseResult, error) {
			wr, wfErr := p.waterfallExec.Run(ctx, company, fieldValues)
			if wfErr != nil {
				return nil, wfErr
			}
			// Apply waterfall results back into field values.
			fieldValues = waterfall.ApplyToFieldValues(fieldValues, wr)
			result.FieldValues = fieldValues
			return &model.PhaseResult{
				Metadata: map[string]any{
					"fields_resolved":   wr.FieldsResolved,
					"fields_total":      wr.FieldsTotal,
					"premium_cost_usd":  wr.TotalPremiumUSD,
				},
			}, nil
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

	trackPhase("9_gate", func() (*model.PhaseResult, error) {
		gate, gateErr := QualityGate(ctx, result, p.fields, p.salesforce, p.notion, p.cfg)
		if gateErr != nil {
			return nil, gateErr
		}
		return &model.PhaseResult{
			Metadata: map[string]any{
				"score":         gate.Score,
				"passed":        gate.Passed,
				"sf_updated":    gate.SFUpdated,
				"manual_review": gate.ManualReview,
			},
		}, nil
	})

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
		return 0
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

	return model.CrawledPage{
		URL:        data.LinkedInURL,
		Title:      "[linkedin] " + company.Name,
		Markdown:   content.String(),
		StatusCode: 200,
	}
}
