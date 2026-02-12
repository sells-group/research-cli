package pipeline

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/cost"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/store"
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
	jina       jina.Client
	firecrawl  firecrawl.Client
	perplexity perplexity.Client
	anthropic  anthropic.Client
	salesforce salesforce.Client
	notion     notion.Client
	ppp        ppp.Querier
	costCalc   *cost.Calculator
	questions  []model.Question
	fields     *model.FieldRegistry
}

// New creates a new Pipeline with all dependencies.
func New(
	cfg *config.Config,
	st store.Store,
	jinaClient jina.Client,
	fcClient firecrawl.Client,
	pplxClient perplexity.Client,
	aiClient anthropic.Client,
	sfClient salesforce.Client,
	notionClient notion.Client,
	pppClient ppp.Querier,
	questions []model.Question,
	fields *model.FieldRegistry,
) *Pipeline {
	return &Pipeline{
		cfg:        cfg,
		store:      st,
		jina:       jinaClient,
		firecrawl:  fcClient,
		perplexity: pplxClient,
		anthropic:  aiClient,
		salesforce: sfClient,
		notion:     notionClient,
		ppp:        pppClient,
		costCalc:   cost.NewCalculator(cost.DefaultRates()),
		questions:  questions,
		fields:     fields,
	}
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

		if phase != nil {
			_ = p.store.CompletePhase(ctx, phase.ID, phaseResult)
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

	// Phase 1A: Crawl
	g.Go(func() error {
		pr := trackPhase("1a_crawl", func() (*model.PhaseResult, error) {
			cr, crawlErr := CrawlPhase(gCtx, company, p.cfg.Crawl, p.store, p.jina, p.firecrawl)
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
		_ = pr
		return nil
	})

	// Phase 1B: External Scrape
	g.Go(func() error {
		pr := trackPhase("1b_scrape", func() (*model.PhaseResult, error) {
			ep := ScrapePhase(gCtx, company, p.jina, p.firecrawl)
			externalPages = ep
			return &model.PhaseResult{
				Metadata: map[string]any{
					"external_pages": len(ep),
				},
			}, nil
		})
		_ = pr
		return nil
	})

	// Phase 1C: LinkedIn
	g.Go(func() error {
		pr := trackPhase("1c_linkedin", func() (*model.PhaseResult, error) {
			ld, usage, liErr := LinkedInPhase(gCtx, company, p.jina, p.perplexity, p.anthropic, p.cfg.Anthropic)
			if liErr != nil {
				return nil, liErr
			}
			linkedInData = ld
			if usage != nil {
				totalUsage.Add(*usage)
			}
			return &model.PhaseResult{
				TokenUsage: model.TokenUsage{
					InputTokens:  usage.InputTokens,
					OutputTokens: usage.OutputTokens,
				},
			}, nil
		})
		_ = pr
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
		_ = pr
		return nil
	})

	// Wait for all Phase 1 tasks (errors are tracked per-phase, don't fail the pipeline).
	_ = g.Wait()

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
			TokenUsage: model.TokenUsage{
				InputTokens:  usage.InputTokens,
				OutputTokens: usage.OutputTokens,
			},
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

	// ===== Phase 4: Tier 1 Extraction =====
	setStatus(model.RunStatusExtracting)
	var t1Answers []model.ExtractionAnswer

	trackPhase("4_extract_t1", func() (*model.PhaseResult, error) {
		t1Result, t1Err := ExtractTier1(ctx, batches.Tier1, p.anthropic, p.cfg.Anthropic)
		if t1Err != nil {
			return nil, t1Err
		}
		t1Answers = t1Result.Answers
		totalUsage.Add(t1Result.TokenUsage)
		return &model.PhaseResult{
			TokenUsage: model.TokenUsage{
				InputTokens:  t1Result.TokenUsage.InputTokens,
				OutputTokens: t1Result.TokenUsage.OutputTokens,
			},
			Metadata: map[string]any{
				"answers":     len(t1Result.Answers),
				"duration_ms": t1Result.Duration,
			},
		}, nil
	})

	// Escalate low-confidence T1 answers to T2.
	escalated := EscalateQuestions(t1Answers, p.questions, pageIndex, p.cfg.Pipeline.ConfidenceEscalationThreshold)
	t2Questions := append(batches.Tier2, escalated...)

	// ===== Phase 5: Tier 2 Extraction =====
	var t2Answers []model.ExtractionAnswer

	trackPhase("5_extract_t2", func() (*model.PhaseResult, error) {
		t2Result, t2Err := ExtractTier2(ctx, t2Questions, t1Answers, p.anthropic, p.cfg.Anthropic)
		if t2Err != nil {
			return nil, t2Err
		}
		t2Answers = t2Result.Answers
		totalUsage.Add(t2Result.TokenUsage)
		return &model.PhaseResult{
			TokenUsage: model.TokenUsage{
				InputTokens:  t2Result.TokenUsage.InputTokens,
				OutputTokens: t2Result.TokenUsage.OutputTokens,
			},
			Metadata: map[string]any{
				"answers":   len(t2Result.Answers),
				"escalated": len(escalated),
			},
		}, nil
	})

	// ===== Phase 6: Tier 3 Extraction =====
	var t3Answers []model.ExtractionAnswer

	// Determine if T3 should run.
	shouldRunT3 := len(batches.Tier3) > 0
	if p.cfg.Pipeline.Tier3Gate == "ambiguity_only" {
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
				TokenUsage: model.TokenUsage{
					InputTokens:  t3Result.TokenUsage.InputTokens,
					OutputTokens: t3Result.TokenUsage.OutputTokens,
				},
				Metadata: map[string]any{
					"answers": len(t3Result.Answers),
				},
			}, nil
		})
	} else {
		trackPhase("6_extract_t3", func() (*model.PhaseResult, error) {
			return &model.PhaseResult{
				Status: model.PhaseStatusSkipped,
				Metadata: map[string]any{
					"reason": "not needed",
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
		fieldValues = BuildFieldValues(allAnswers, p.fields)
		return &model.PhaseResult{
			Metadata: map[string]any{
				"total_answers": len(allAnswers),
				"field_values":  len(fieldValues),
			},
		}, nil
	})

	result.Answers = allAnswers
	result.FieldValues = fieldValues

	// ===== Phase 8: Report =====
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

	// Finalize.
	result.TotalTokens = totalUsage.InputTokens + totalUsage.OutputTokens
	result.TotalCost = p.costCalc.Claude(
		p.cfg.Anthropic.HaikuModel, false,
		totalUsage.InputTokens, totalUsage.OutputTokens,
		totalUsage.CacheCreationTokens, totalUsage.CacheReadTokens,
	)

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

	log.Info("pipeline: enrichment complete",
		zap.String("run_id", run.ID),
		zap.Float64("score", result.Score),
		zap.Int("fields", len(fieldValues)),
		zap.Int("tokens", result.TotalTokens),
	)

	return result, nil
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
