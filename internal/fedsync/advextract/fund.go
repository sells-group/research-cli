package advextract

import (
	"context"
	"fmt"
	"sync"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/pkg/anthropic"
)

const maxFundConcurrency = 5

// ExtractFunds runs fund-level extraction for all funds of an advisor.
func ExtractFunds(ctx context.Context, docs *AdvisorDocs, client anthropic.Client, store *Store, runID int64, maxTier int, costTracker *CostTracker) ([]Answer, error) {
	if len(docs.Funds) == 0 {
		return nil, nil
	}

	fundQuestions := QuestionsByScope(ScopeFund)
	if len(fundQuestions) == 0 {
		return nil, nil
	}

	log := zap.L().With(
		zap.Int("crd", docs.CRDNumber),
		zap.Int("funds", len(docs.Funds)),
		zap.Int("fund_questions", len(fundQuestions)),
	)
	log.Info("starting fund-level extraction")

	var (
		allAnswers []Answer
		mu         = &lockableAnswers{}
	)

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxFundConcurrency)

	for _, fund := range docs.Funds {
		fund := fund
		g.Go(func() error {
			answers, err := extractSingleFund(gctx, docs, fund, fundQuestions, client, maxTier, costTracker)
			if err != nil {
				log.Warn("fund extraction failed",
					zap.String("fund_id", fund.FundID),
					zap.Error(err))
				return nil // don't fail other funds
			}

			// Set common fields.
			for i := range answers {
				answers[i].CRDNumber = docs.CRDNumber
				answers[i].FundID = fund.FundID
				answers[i].RunID = runID
			}

			mu.append(answers)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, eris.Wrap(err, "advextract: extract funds")
	}

	allAnswers = mu.get()

	// Write fund answers.
	if len(allAnswers) > 0 {
		if err := store.WriteFundAnswers(ctx, allAnswers); err != nil {
			return allAnswers, eris.Wrap(err, "advextract: write fund answers")
		}
	}

	log.Info("fund extraction complete",
		zap.Int("total_answers", len(allAnswers)))

	return allAnswers, nil
}

// extractSingleFund extracts answers for one fund.
func extractSingleFund(ctx context.Context, docs *AdvisorDocs, fund FundRow, questions []Question, client anthropic.Client, maxTier int, costTracker *CostTracker) ([]Answer, error) {
	log := zap.L().With(
		zap.Int("crd", docs.CRDNumber),
		zap.String("fund_id", fund.FundID),
		zap.String("fund_name", fund.FundName),
	)

	var allAnswers []Answer
	fundCtx := FundContext(docs, fund)

	// Structured bypass first.
	for _, q := range questions {
		if q.StructuredBypass {
			a := StructuredBypassAnswer(q, docs.Advisor, &fund)
			if a != nil {
				allAnswers = append(allAnswers, *a)
			}
		}
	}

	// Filter to LLM-needed questions.
	var llmQuestions []Question
	bypassKeys := make(map[string]bool)
	for _, a := range allAnswers {
		bypassKeys[a.QuestionKey] = true
	}
	for _, q := range questions {
		if !q.StructuredBypass && !bypassKeys[q.Key] {
			llmQuestions = append(llmQuestions, q)
		}
	}

	// T1 fund questions.
	if maxTier >= 1 {
		t1Qs := filterByTier(llmQuestions, 1)
		if len(t1Qs) > 0 {
			systemText := T1SystemPrompt(docs) + "\n\n" + fundCtx
			items := buildBatchItems(t1Qs, docs, systemText, 1)
			answers, inputTok, outputTok, err := executeBatch(ctx, items, 1, client)
			if err != nil {
				log.Warn("fund T1 extraction failed", zap.Error(err))
			} else {
				allAnswers = append(allAnswers, answers...)
				if costTracker != nil {
					costTracker.RecordUsage(docs.CRDNumber, 1, inputTok, outputTok, 0, 0)
				}
			}
		}
	}

	// T2 fund questions.
	if maxTier >= 2 {
		t2Qs := filterByTier(llmQuestions, 2)
		if len(t2Qs) > 0 {
			systemText := T2SystemPrompt(docs, allAnswers) + "\n\n" + fundCtx
			items := buildBatchItems(t2Qs, docs, systemText, 2)
			answers, inputTok, outputTok, err := executeBatch(ctx, items, 2, client)
			if err != nil {
				log.Warn("fund T2 extraction failed", zap.Error(err))
			} else {
				allAnswers = append(allAnswers, answers...)
				if costTracker != nil {
					costTracker.RecordUsage(docs.CRDNumber, 2, inputTok, outputTok, 0, 0)
				}
			}
		}
	}

	// T3 fund questions.
	if maxTier >= 3 {
		t3Qs := filterByTier(llmQuestions, 3)
		if len(t3Qs) > 0 {
			systemText := T3SystemPrompt(docs, allAnswers) + "\n\n" + fundCtx
			items := buildBatchItems(t3Qs, docs, systemText, 3)
			answers, inputTok, outputTok, err := executeBatch(ctx, items, 3, client)
			if err != nil {
				log.Warn("fund T3 extraction failed", zap.Error(err))
			} else {
				allAnswers = append(allAnswers, answers...)
				if costTracker != nil {
					costTracker.RecordUsage(docs.CRDNumber, 3, inputTok, outputTok, 0, 0)
				}
			}
		}
	}

	log.Debug("fund extraction done",
		zap.Int("answers", len(allAnswers)),
		zap.String("fund", fmt.Sprintf("%s (%s)", fund.FundName, fund.FundID)))

	return allAnswers, nil
}

func filterByTier(questions []Question, tier int) []Question {
	var out []Question
	for _, q := range questions {
		if q.Tier == tier {
			out = append(out, q)
		}
	}
	return out
}

// lockableAnswers provides thread-safe answer collection.
type lockableAnswers struct {
	mu      sync.Mutex
	answers []Answer
}

func (l *lockableAnswers) append(answers []Answer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.answers = append(l.answers, answers...)
}

func (l *lockableAnswers) get() []Answer {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Answer, len(l.answers))
	copy(out, l.answers)
	return out
}
