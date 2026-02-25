package peextract

import (
	"context"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/scrape"
	"github.com/sells-group/research-cli/pkg/anthropic"
)

const (
	batchConcurrency = 3 // lower than advextract's 5 — crawling is I/O-heavy
	crawlCacheTTL    = 7 * 24 * time.Hour
)

// Extractor orchestrates PE firm intelligence extraction.
type Extractor struct {
	store       *Store
	client      anthropic.Client
	chain       *scrape.Chain
	costTracker *CostTracker
	maxTier     int
	dryRun      bool
	force       bool
}

// ExtractorOpts configures the extractor.
type ExtractorOpts struct {
	MaxTier int
	MaxCost float64 // per-firm budget (0=unlimited)
	DryRun  bool
	Force   bool
}

// NewExtractor creates a new PE extractor.
func NewExtractor(pool db.Pool, client anthropic.Client, chain *scrape.Chain, opts ExtractorOpts) *Extractor {
	maxTier := opts.MaxTier
	if maxTier < 1 {
		maxTier = 1
	}

	return &Extractor{
		store:       NewStore(pool),
		client:      client,
		chain:       chain,
		costTracker: NewCostTracker(opts.MaxCost),
		maxTier:     maxTier,
		dryRun:      opts.DryRun,
		force:       opts.Force,
	}
}

// RunFirm extracts intelligence for a single PE firm.
func (e *Extractor) RunFirm(ctx context.Context, firmID int64) error {
	log := zap.L().With(zap.Int64("pe_firm_id", firmID))
	start := time.Now()

	if e.dryRun {
		log.Info("dry run — estimating cost",
			zap.String("estimate", EstimateBatchCost(1, e.maxTier)))
		return nil
	}

	// Load PE firm.
	firm, err := e.store.LoadFirm(ctx, firmID)
	if err != nil {
		return eris.Wrapf(err, "peextract: run firm %d", firmID)
	}

	log = log.With(zap.String("firm", firm.FirmName))

	// Check crawl cache.
	var docs *PEFirmDocs
	var pagesCrawled int

	fresh, _ := e.store.IsCrawlCacheFresh(ctx, firmID, crawlCacheTTL)
	if fresh && !e.force {
		// Reuse cached pages.
		cache, cacheErr := e.store.LoadCrawlCache(ctx, firmID)
		if cacheErr != nil {
			return eris.Wrapf(cacheErr, "peextract: load crawl cache for firm %d", firmID)
		}
		docs = AssembleDocsFromCache(firmID, firm.FirmName, cache)
		pagesCrawled = len(cache)
		log.Info("using cached crawl", zap.Int("pages", pagesCrawled))
	} else if firm.WebsiteURL != nil && *firm.WebsiteURL != "" {
		// Crawl the website.
		crawlResult, crawlErr := CrawlPEFirm(ctx, *firm.WebsiteURL, e.chain)
		if crawlErr != nil {
			log.Warn("crawl failed, proceeding without website data", zap.Error(crawlErr))
			docs = AssembleDocs(firmID, firm.FirmName, nil)
		} else {
			// Cache the crawl results.
			if cacheErr := e.store.WriteCrawlCache(ctx, firmID, crawlResult.Pages); cacheErr != nil {
				log.Warn("failed to cache crawl results", zap.Error(cacheErr))
			}
			docs = AssembleDocs(firmID, firm.FirmName, crawlResult.Pages)
			pagesCrawled = len(crawlResult.Pages)
			log.Info("crawl complete",
				zap.Int("pages", pagesCrawled),
				zap.String("source", crawlResult.Source))
		}
	} else {
		// No website available.
		log.Info("no website URL, limited extraction")
		docs = AssembleDocs(firmID, firm.FirmName, nil)
	}

	// Check if we have any content to extract from.
	if !HasPages(docs) {
		log.Info("no pages available for extraction")
		_ = e.store.SkipRun(ctx, firmID, "no_pages")
		return nil
	}

	// Create extraction run.
	runID, err := e.store.CreateRun(ctx, firmID)
	if err != nil {
		return eris.Wrapf(err, "peextract: create run for firm %d", firmID)
	}

	var allAnswers []Answer
	var totalInput, totalOutput int64

	// Phase 1: T1 (Haiku) extraction.
	if e.maxTier >= 1 {
		t1Qs := QuestionsByTier(1)
		if len(t1Qs) > 0 {
			systemText := T1SystemPrompt(docs)
			items := buildBatchItems(t1Qs, docs, systemText, 1)

			if len(items) > 0 {
				// Fire primer async.
				primerDone := make(chan struct{})
				var primerIn, primerOut int64
				go func() {
					primerIn, primerOut = firePrimer(ctx, e.client, items)
					close(primerDone)
				}()

				answers, inputTok, outputTok, extractErr := executeBatch(ctx, items, 1, e.client)
				<-primerDone
				totalInput += primerIn + inputTok
				totalOutput += primerOut + outputTok

				if extractErr != nil {
					log.Warn("T1 extraction failed", zap.Error(extractErr))
				} else {
					for i := range answers {
						answers[i].PEFirmID = firmID
						answers[i].RunID = runID
					}
					allAnswers = append(allAnswers, answers...)
					e.costTracker.RecordUsage(firmID, 1, inputTok, outputTok, 0, 0)
				}

				log.Info("T1 complete", zap.Int("answers", len(answers)))

				if e.costTracker.CheckBudget(firmID) {
					log.Warn("budget exceeded after T1")
					goto writeAnswers
				}
			}
		}
	}

	// Phase 2: T2 (Sonnet) synthesis + confidence escalation.
	if e.maxTier >= 2 {
		t2Qs := QuestionsByTier(2)

		// Escalate low-confidence T1 answers to T2.
		var escalateQs []Question
		qMap := QuestionMap()
		for _, a := range allAnswers {
			if a.Confidence < 0.4 && a.Tier == 1 {
				if q, ok := qMap[a.QuestionKey]; ok {
					escalateQs = append(escalateQs, q)
				}
			}
		}

		if len(escalateQs) > 0 {
			log.Info("escalating low-confidence T1 answers to T2",
				zap.Int("escalated", len(escalateQs)))
		}

		t2Qs = append(t2Qs, escalateQs...)

		// Separate blog intelligence questions (Cat G) — they use a specialized prompt.
		var synthesisQs, blogQs []Question
		for _, q := range t2Qs {
			if q.Category == CatBlogIntel {
				blogQs = append(blogQs, q)
			} else {
				synthesisQs = append(synthesisQs, q)
			}
		}

		// Run synthesis questions with T2 system prompt.
		if len(synthesisQs) > 0 {
			systemText := T2SystemPrompt(docs, allAnswers)
			items := buildBatchItems(synthesisQs, docs, systemText, 2)

			if len(items) > 0 {
				answers, inputTok, outputTok, extractErr := executeBatch(ctx, items, 2, e.client)
				totalInput += inputTok
				totalOutput += outputTok

				if extractErr != nil {
					log.Warn("T2 synthesis extraction failed", zap.Error(extractErr))
				} else {
					for i := range answers {
						answers[i].PEFirmID = firmID
						answers[i].RunID = runID
					}
					allAnswers = mergeAnswers(allAnswers, answers)
					e.costTracker.RecordUsage(firmID, 2, inputTok, outputTok, 0, 0)
				}

				log.Info("T2 synthesis complete", zap.Int("answers", len(answers)))
			}
		}

		// Run blog intelligence questions with blog-specific system prompt.
		if len(blogQs) > 0 {
			blogSystem := BlogSystemPrompt(docs, allAnswers)
			items := buildBatchItems(blogQs, docs, blogSystem, 2)

			if len(items) > 0 {
				answers, inputTok, outputTok, extractErr := executeBatch(ctx, items, 2, e.client)
				totalInput += inputTok
				totalOutput += outputTok

				if extractErr != nil {
					log.Warn("T2 blog extraction failed", zap.Error(extractErr))
				} else {
					for i := range answers {
						answers[i].PEFirmID = firmID
						answers[i].RunID = runID
					}
					allAnswers = mergeAnswers(allAnswers, answers)
					e.costTracker.RecordUsage(firmID, 2, inputTok, outputTok, 0, 0)
				}

				log.Info("T2 blog intel complete", zap.Int("answers", len(answers)))
			}
		}
	}

writeAnswers:
	// Write all answers.
	if err := e.store.WriteAnswers(ctx, allAnswers); err != nil {
		_ = e.store.FailRun(ctx, runID, err.Error())
		return eris.Wrapf(err, "peextract: write answers for firm %d", firmID)
	}

	// Complete run.
	costInfo := e.costTracker.FirmTotal(firmID)
	stats := RunStats{
		TierCompleted:  e.maxTier,
		TotalQuestions: len(AllQuestions()),
		Answered:       len(allAnswers),
		PagesCrawled:   pagesCrawled,
		InputTokens:    int(totalInput),
		OutputTokens:   int(totalOutput),
		CostUSD:        costInfo.CostUSD,
	}

	if err := e.store.CompleteRun(ctx, runID, stats); err != nil {
		log.Error("failed to complete run", zap.Error(err))
	}

	elapsed := time.Since(start)
	log.Info("PE firm extraction complete",
		zap.Int("answers", len(allAnswers)),
		zap.Float64("cost_usd", costInfo.CostUSD),
		zap.Duration("elapsed", elapsed),
	)

	return nil
}

// RunBatch extracts intelligence for multiple PE firms with concurrency control.
func (e *Extractor) RunBatch(ctx context.Context, firmIDs []int64) error {
	if len(firmIDs) == 0 {
		return nil
	}

	log := zap.L().With(zap.Int("total_firms", len(firmIDs)))

	if e.dryRun {
		log.Info("dry run — estimated costs",
			zap.String("estimate", EstimateBatchCost(len(firmIDs), e.maxTier)))
		return nil
	}

	log.Info("starting batch extraction")
	start := time.Now()

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(batchConcurrency)

	var completed, failed int64
	for _, id := range firmIDs {
		g.Go(func() error {
			if err := e.RunFirm(gctx, id); err != nil {
				zap.L().Error("PE firm extraction failed",
					zap.Int64("pe_firm_id", id),
					zap.Error(err))
				failed++
				return nil // don't abort other firms
			}
			completed++
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return eris.Wrap(err, "peextract: batch run")
	}

	// Refresh materialized view after batch.
	if refreshErr := e.store.RefreshMaterializedView(ctx); refreshErr != nil {
		log.Warn("failed to refresh materialized view", zap.Error(refreshErr))
	}

	elapsed := time.Since(start)
	log.Info("batch extraction complete",
		zap.Int64("completed", completed),
		zap.Int64("failed", failed),
		zap.Float64("total_cost_usd", e.costTracker.TotalCost()),
		zap.Duration("elapsed", elapsed),
	)

	return nil
}

// mergeAnswers merges new answers into existing, preferring higher-tier answers
// for the same question key.
func mergeAnswers(existing, incoming []Answer) []Answer {
	byKey := make(map[string]int)
	result := make([]Answer, len(existing))
	copy(result, existing)

	for i, a := range result {
		byKey[a.QuestionKey] = i
	}

	for _, a := range incoming {
		if idx, ok := byKey[a.QuestionKey]; ok {
			if a.Tier > result[idx].Tier || (a.Tier == result[idx].Tier && a.Confidence > result[idx].Confidence) {
				result[idx] = a
			}
		} else {
			byKey[a.QuestionKey] = len(result)
			result = append(result, a)
		}
	}

	return result
}
