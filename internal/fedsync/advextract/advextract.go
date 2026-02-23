package advextract

import (
	"context"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/pkg/anthropic"
)

const batchConcurrency = 5

// Extractor orchestrates ADV intelligence extraction.
type Extractor struct {
	store       *Store
	client      anthropic.Client
	costTracker *CostTracker
	maxTier     int  // 1, 2, or 3 — limits extraction depth
	dryRun      bool // if true, estimate cost only
	fundsOnly   bool // if true, skip advisor-level extraction
	force       bool // if true, re-extract even if already done
}

// ExtractorOpts configures the extractor.
type ExtractorOpts struct {
	MaxTier    int
	MaxCost    float64 // per-advisor budget (0=unlimited)
	DryRun     bool
	FundsOnly  bool
	Force      bool
}

// NewExtractor creates a new ADV extractor.
func NewExtractor(pool db.Pool, client anthropic.Client, opts ExtractorOpts) *Extractor {
	maxTier := opts.MaxTier
	if maxTier < 1 {
		maxTier = 3
	}

	return &Extractor{
		store:       NewStore(pool),
		client:      client,
		costTracker: NewCostTracker(opts.MaxCost),
		maxTier:     maxTier,
		dryRun:      opts.DryRun,
		fundsOnly:   opts.FundsOnly,
		force:       opts.Force,
	}
}

// RunAdvisor extracts intelligence for a single advisor.
func (e *Extractor) RunAdvisor(ctx context.Context, crd int) error {
	log := zap.L().With(zap.Int("crd", crd))
	start := time.Now()

	// Dry run: just estimate cost.
	if e.dryRun {
		log.Info("dry run — estimating cost",
			zap.String("estimate", EstimateBatchCost(1, e.maxTier)))
		return nil
	}

	// Load all documents.
	advisor, err := e.store.LoadAdvisor(ctx, crd)
	if err != nil {
		return eris.Wrapf(err, "advextract: run advisor %d", crd)
	}

	brochures, err := e.store.LoadBrochures(ctx, crd)
	if err != nil {
		return eris.Wrapf(err, "advextract: load brochures %d", crd)
	}

	crs, err := e.store.LoadCRS(ctx, crd)
	if err != nil {
		return eris.Wrapf(err, "advextract: load CRS %d", crd)
	}

	owners, err := e.store.LoadOwners(ctx, crd)
	if err != nil {
		return eris.Wrapf(err, "advextract: load owners %d", crd)
	}

	funds, err := e.store.LoadFunds(ctx, crd)
	if err != nil {
		return eris.Wrapf(err, "advextract: load funds %d", crd)
	}

	// Assemble documents.
	docs := AssembleDocs(advisor, brochures, crs, owners, funds)

	log.Info("documents assembled",
		zap.Bool("has_brochure", len(brochures) > 0),
		zap.Bool("has_crs", len(crs) > 0),
		zap.Int("owners", len(owners)),
		zap.Int("funds", len(funds)),
	)

	// Write section index for document coverage tracking.
	sectionEntries := buildSectionIndex(docs, brochures, crs)
	if err := e.store.WriteSectionIndex(ctx, sectionEntries); err != nil {
		log.Warn("failed to write section index", zap.Error(err))
	}

	// Create extraction run.
	scope := ScopeAdvisor
	if e.fundsOnly {
		scope = ScopeFund
	}
	runID, err := e.store.CreateRun(ctx, crd, scope, "")
	if err != nil {
		return eris.Wrapf(err, "advextract: create run %d", crd)
	}

	// Archive existing answers before re-extraction (for audit trail).
	if e.force {
		if archiveErr := e.store.ArchiveExistingAnswers(ctx, crd, runID); archiveErr != nil {
			log.Warn("failed to archive existing answers", zap.Error(archiveErr))
		}
	}

	var allAnswers []Answer
	var totalInput, totalOutput int64

	// Advisor-level extraction.
	if !e.fundsOnly {
		advisorAnswers, input, output, extractErr := e.extractAdvisor(ctx, docs, runID)
		if extractErr != nil {
			_ = e.store.FailRun(ctx, runID, extractErr.Error())
			return eris.Wrapf(extractErr, "advextract: extract advisor %d", crd)
		}
		allAnswers = append(allAnswers, advisorAnswers...)
		totalInput += input
		totalOutput += output
	}

	// Fund-level extraction.
	if len(docs.Funds) > 0 {
		fundAnswers, fundErr := ExtractFunds(ctx, docs, e.client, e.store, runID, e.maxTier, e.costTracker)
		if fundErr != nil {
			log.Warn("fund extraction had errors", zap.Error(fundErr))
		}
		allAnswers = append(allAnswers, fundAnswers...)
	}

	// Populate normalized relationship tables from extracted answers.
	if err := PopulateRelationships(ctx, e.store.pool, crd, allAnswers); err != nil {
		log.Warn("failed to populate relationships", zap.Error(err))
	}

	// Compute derived metrics.
	metrics, metricsErr := ComputeAllMetrics(ctx, e.store.pool, crd, advisor, allAnswers)
	if metricsErr != nil {
		log.Warn("failed to compute metrics", zap.Error(metricsErr))
	} else if metrics != nil {
		if writeErr := e.store.WriteComputedMetrics(ctx, metrics); writeErr != nil {
			log.Warn("failed to write computed metrics", zap.Error(writeErr))
		}
	}

	// Complete run.
	costInfo := e.costTracker.AdvisorTotal(crd)
	stats := RunStats{
		TierCompleted:  e.maxTier,
		TotalQuestions: len(AllQuestions()),
		Answered:       len(allAnswers),
		InputTokens:    int(totalInput),
		OutputTokens:   int(totalOutput),
		CostUSD:        costInfo.CostUSD,
	}

	if err := e.store.CompleteRun(ctx, runID, stats); err != nil {
		log.Error("failed to complete run", zap.Error(err))
	}

	elapsed := time.Since(start)
	log.Info("advisor extraction complete",
		zap.Int("answers", len(allAnswers)),
		zap.Float64("cost_usd", costInfo.CostUSD),
		zap.Duration("elapsed", elapsed),
	)

	return nil
}

// extractAdvisor runs tiered extraction for advisor-level questions.
func (e *Extractor) extractAdvisor(ctx context.Context, docs *AdvisorDocs, runID int64) ([]Answer, int64, int64, error) {
	log := zap.L().With(zap.Int("crd", docs.CRDNumber))

	advisorQuestions := QuestionsByScope(ScopeAdvisor)
	var allAnswers []Answer
	var totalInput, totalOutput int64

	// Phase 0: Structured bypass.
	for _, q := range advisorQuestions {
		if q.StructuredBypass {
			a := StructuredBypassAnswer(q, docs.Advisor, nil, docs.Funds)
			if a != nil {
				a.CRDNumber = docs.CRDNumber
				a.RunID = runID
				allAnswers = append(allAnswers, *a)
			}
		}
	}

	log.Info("structured bypass complete", zap.Int("answers", len(allAnswers)))

	// Filter to LLM-needed questions.
	bypassKeys := make(map[string]bool)
	for _, a := range allAnswers {
		bypassKeys[a.QuestionKey] = true
	}
	var llmQuestions []Question
	for _, q := range advisorQuestions {
		if !q.StructuredBypass && !bypassKeys[q.Key] {
			llmQuestions = append(llmQuestions, q)
		}
	}

	// Phase 1: T1 (Haiku) extraction.
	if e.maxTier >= 1 {
		t1Qs := filterByTier(llmQuestions, 1)
		if len(t1Qs) > 0 {
			systemText := T1SystemPrompt(docs)
			items := buildBatchItems(t1Qs, docs, systemText, 1)

			// Fire primer async.
			primerDone := make(chan struct{})
			var primerIn, primerOut int64
			go func() {
				primerIn, primerOut = firePrimer(ctx, e.client, items)
				close(primerDone)
			}()

			answers, inputTok, outputTok, err := executeBatch(ctx, items, 1, e.client)
			<-primerDone
			totalInput += primerIn + inputTok
			totalOutput += primerOut + outputTok

			if err != nil {
				log.Warn("T1 extraction failed", zap.Error(err))
			} else {
				for i := range answers {
					answers[i].CRDNumber = docs.CRDNumber
					answers[i].RunID = runID
				}
				allAnswers = append(allAnswers, answers...)
				e.costTracker.RecordUsage(docs.CRDNumber, 1, inputTok, outputTok, 0, 0)
			}

			log.Info("T1 complete", zap.Int("answers", len(answers)))

			// Check budget.
			if e.costTracker.CheckBudget(docs.CRDNumber) {
				log.Warn("budget exceeded after T1")
				goto writeAnswers
			}
		}
	}

	// Phase 2: T2 (Sonnet) extraction + confidence escalation.
	if e.maxTier >= 2 {
		t2Qs := filterByTier(llmQuestions, 2)
		t2Qs = filterEscalationQuestions(allAnswers, t2Qs)

		if len(t2Qs) > 0 {
			systemText := T2SystemPrompt(docs, allAnswers)
			items := buildBatchItems(t2Qs, docs, systemText, 2)

			// Fire primer async.
			primerDone := make(chan struct{})
			var primerIn, primerOut int64
			go func() {
				primerIn, primerOut = firePrimer(ctx, e.client, items)
				close(primerDone)
			}()

			answers, inputTok, outputTok, err := executeBatch(ctx, items, 2, e.client)
			<-primerDone
			totalInput += primerIn + inputTok
			totalOutput += primerOut + outputTok

			if err != nil {
				log.Warn("T2 extraction failed", zap.Error(err))
			} else {
				for i := range answers {
					answers[i].CRDNumber = docs.CRDNumber
					answers[i].RunID = runID
				}
				// Merge: T2 answers supersede T1 for same question key.
				allAnswers = mergeAnswers(allAnswers, answers)
				e.costTracker.RecordUsage(docs.CRDNumber, 2, inputTok, outputTok, 0, 0)
			}

			log.Info("T2 complete", zap.Int("answers", len(answers)))

			if e.costTracker.CheckBudget(docs.CRDNumber) {
				log.Warn("budget exceeded after T2")
				goto writeAnswers
			}
		}
	}

	// Phase 3: T3 (Opus) extraction.
	if e.maxTier >= 3 {
		t3Qs := filterByTier(llmQuestions, 3)
		if len(t3Qs) > 0 {
			systemText := T3SystemPrompt(docs, allAnswers)
			items := buildBatchItems(t3Qs, docs, systemText, 3)

			// Fire primer async for large batches.
			primerDone := make(chan struct{})
			var primerIn, primerOut int64
			if len(items) >= 3 {
				go func() {
					primerIn, primerOut = firePrimer(ctx, e.client, items)
					close(primerDone)
				}()
			} else {
				close(primerDone)
			}

			answers, inputTok, outputTok, err := executeBatch(ctx, items, 3, e.client)
			<-primerDone
			totalInput += primerIn + inputTok
			totalOutput += primerOut + outputTok

			if err != nil {
				log.Warn("T3 extraction failed", zap.Error(err))
			} else {
				for i := range answers {
					answers[i].CRDNumber = docs.CRDNumber
					answers[i].RunID = runID
				}
				allAnswers = mergeAnswers(allAnswers, answers)
				e.costTracker.RecordUsage(docs.CRDNumber, 3, inputTok, outputTok, 0, 0)
			}

			log.Info("T3 complete", zap.Int("answers", len(answers)))
		}
	}

writeAnswers:
	// Write all advisor answers.
	if err := e.store.WriteAdvisorAnswers(ctx, allAnswers); err != nil {
		return allAnswers, totalInput, totalOutput, eris.Wrap(err, "advextract: write answers")
	}

	return allAnswers, totalInput, totalOutput, nil
}

// RunBatch extracts intelligence for multiple advisors with concurrency control.
func (e *Extractor) RunBatch(ctx context.Context, crds []int) error {
	if len(crds) == 0 {
		return nil
	}

	log := zap.L().With(zap.Int("total_advisors", len(crds)))

	if e.dryRun {
		log.Info("dry run — estimated costs",
			zap.String("estimate", EstimateBatchCost(len(crds), e.maxTier)))
		return nil
	}

	log.Info("starting batch extraction")
	start := time.Now()

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(batchConcurrency)

	var completed, failed int64
	for _, crd := range crds {
		crd := crd
		g.Go(func() error {
			if err := e.RunAdvisor(gctx, crd); err != nil {
				zap.L().Error("advisor extraction failed",
					zap.Int("crd", crd),
					zap.Error(err))
				failed++
				return nil // don't abort other advisors
			}
			completed++
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return eris.Wrap(err, "advextract: batch run")
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

// buildSectionIndex creates section index entries from assembled documents.
func buildSectionIndex(docs *AdvisorDocs, brochures []BrochureRow, crs []CRSRow) []SectionIndexEntry {
	var entries []SectionIndexEntry

	// Index brochure sections.
	if len(brochures) > 0 {
		brochureID := brochures[0].BrochureID
		for key, text := range docs.BrochureSections {
			if key == SectionFull {
				continue
			}
			title := ""
			if h, ok := itemHeaders[key]; ok {
				title = h
			}
			entries = append(entries, SectionIndexEntry{
				CRDNumber:     docs.CRDNumber,
				DocType:       "part2",
				DocID:         brochureID,
				SectionKey:    key,
				SectionTitle:  title,
				CharLength:    len(text),
				TokenEstimate: len(text) / 4, // rough estimate
			})
		}
	}

	// Index CRS as single section.
	if len(crs) > 0 && docs.CRSText != "" {
		entries = append(entries, SectionIndexEntry{
			CRDNumber:     docs.CRDNumber,
			DocType:       "part3",
			DocID:         crs[0].CRSID,
			SectionKey:    "full",
			SectionTitle:  "Client Relationship Summary",
			CharLength:    len(docs.CRSText),
			TokenEstimate: len(docs.CRSText) / 4,
		})
	}

	return entries
}

// mergeAnswers merges new answers into existing, preferring higher-tier answers
// for the same question key.
func mergeAnswers(existing, new []Answer) []Answer {
	byKey := make(map[string]int) // key → index in result
	result := make([]Answer, len(existing))
	copy(result, existing)

	for i, a := range result {
		byKey[a.QuestionKey] = i
	}

	for _, a := range new {
		if idx, ok := byKey[a.QuestionKey]; ok {
			// Higher tier or higher confidence supersedes.
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
