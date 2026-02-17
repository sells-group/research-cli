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
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/anthropic"
)

// maxDirectConcurrency limits concurrent CreateMessage calls in no-batch mode.
const maxDirectConcurrency = 10

const tier1Prompt = `You are a research analyst extracting specific data from a web page.

Question: %s
%s
Expected output format: %s

Page URL: %s
Page content:
%s

Extract the answer from this page. Return a valid JSON object:
{"value": <extracted value>, "confidence": <0.0-1.0>, "reasoning": "<brief explanation>", "source_url": "%s"}`

const tier2Prompt = `You are a senior research analyst synthesizing data from multiple sources.

Question: %s
%s
Expected output format: %s

Previous findings (Tier 1):
%s

Source pages:
%s

Synthesize the best answer from all available sources. Return a valid JSON object:
{"value": <synthesized value>, "confidence": <0.0-1.0>, "reasoning": "<brief explanation>", "source_url": "<most relevant source URL>"}`

const tier3Prompt = `You are an expert research analyst providing definitive answers to complex questions.

Question: %s
%s
Expected output format: %s

All available context:
%s

Provide a thorough, well-reasoned answer. Return a valid JSON object:
{"value": <definitive value>, "confidence": <0.0-1.0>, "reasoning": "<detailed explanation>", "source_url": "<most relevant source URL>"}`

// ExtractTier1 runs Tier 1 extraction: single-page fact extraction using Haiku.
func ExtractTier1(ctx context.Context, routed []model.RoutedQuestion, aiClient anthropic.Client, aiCfg config.AnthropicConfig) (*model.TierResult, error) {
	start := time.Now()
	result := &model.TierResult{Tier: 1}

	if len(routed) == 0 {
		return result, nil
	}

	// Use cache for the system block (primer strategy), matching T2/T3.
	systemBlocks := anthropic.BuildCachedSystemBlocks(
		"You are a research analyst extracting specific data from a web page. Return a valid JSON object with value, confidence, reasoning, and source_url.",
	)

	// Build batch items: one per question, using the first matched page.
	var batchItems []anthropic.BatchRequestItem
	for i, rq := range routed {
		page := rq.Pages[0] // Tier 1: single page
		instructions := ""
		if rq.Question.Instructions != "" {
			instructions = fmt.Sprintf("Instructions: %s", rq.Question.Instructions)
		}

		content := page.Markdown
		if len(content) > 8000 {
			content = content[:8000]
		}

		// Append compact external source snippets so T1 can see BBB/Google Maps data.
		if len(rq.Pages) > 1 {
			externalCtx := buildExternalSnippets(rq.Pages[1:], 2000)
			if externalCtx != "" {
				content += "\n\n--- Additional Sources ---\n" + externalCtx
			}
		}

		prompt := fmt.Sprintf(tier1Prompt,
			rq.Question.Text,
			instructions,
			rq.Question.OutputFormat,
			page.URL,
			content,
			page.URL,
		)

		batchItems = append(batchItems, anthropic.BatchRequestItem{
			CustomID: fmt.Sprintf("t1-%d-%s", i, rq.Question.ID),
			Params: anthropic.MessageRequest{
				Model:     aiCfg.HaikuModel,
				MaxTokens: 512,
				System:    systemBlocks,
				Messages: []anthropic.Message{
					{Role: "user", Content: prompt},
				},
			},
		})
	}

	// Fire primer asynchronously to warm cache; don't block batch submission.
	var primerUsage model.TokenUsage
	var primerWg sync.WaitGroup
	if !aiCfg.NoBatch && len(batchItems) > 1 {
		primerWg.Add(1)
		go func() {
			defer primerWg.Done()
			primerResp, primerErr := anthropic.PrimerRequest(ctx, aiClient, batchItems[0].Params)
			if primerErr != nil {
				zap.L().Warn("extract: tier 1 primer failed", zap.Error(primerErr))
			} else if primerResp != nil {
				primerUsage.InputTokens = int(primerResp.Usage.InputTokens)
				primerUsage.OutputTokens = int(primerResp.Usage.OutputTokens)
				primerUsage.CacheCreationTokens = int(primerResp.Usage.CacheCreationInputTokens)
				primerUsage.CacheReadTokens = int(primerResp.Usage.CacheReadInputTokens)
			}
		}()
	}

	answers, usage, err := executeBatch(ctx, batchItems, routed, 1, aiClient, aiCfg)
	primerWg.Wait() // ensure primer goroutine completes before reading usage
	if err != nil {
		return nil, eris.Wrap(err, "extract: tier 1")
	}

	result.Answers = answers
	result.TokenUsage.Add(primerUsage)
	result.TokenUsage.Add(*usage)
	result.Duration = time.Since(start).Milliseconds()
	return result, nil
}

// ExtractTier2 runs Tier 2 extraction: multi-page synthesis using Sonnet.
// Includes T1 answers as context.
func ExtractTier2(ctx context.Context, routed []model.RoutedQuestion, t1Answers []model.ExtractionAnswer, aiClient anthropic.Client, aiCfg config.AnthropicConfig) (*model.TierResult, error) {
	start := time.Now()
	result := &model.TierResult{Tier: 2}

	if len(routed) == 0 {
		return result, nil
	}

	// Build context from T1 answers.
	t1Context := buildT1Context(t1Answers)

	// Build page context per question.
	var batchItems []anthropic.BatchRequestItem
	for i, rq := range routed {
		instructions := ""
		if rq.Question.Instructions != "" {
			instructions = fmt.Sprintf("Instructions: %s", rq.Question.Instructions)
		}

		pagesContext := buildPagesContext(rq.Pages, 4000)

		prompt := fmt.Sprintf(tier2Prompt,
			rq.Question.Text,
			instructions,
			rq.Question.OutputFormat,
			t1Context,
			pagesContext,
		)

		// Use cache for the system block (primer strategy).
		systemBlocks := anthropic.BuildCachedSystemBlocks(
			"You are a senior research analyst. Synthesize data from multiple sources to provide accurate answers.",
		)

		batchItems = append(batchItems, anthropic.BatchRequestItem{
			CustomID: fmt.Sprintf("t2-%d-%s", i, rq.Question.ID),
			Params: anthropic.MessageRequest{
				Model:     aiCfg.SonnetModel,
				MaxTokens: 1024,
				System:    systemBlocks,
				Messages: []anthropic.Message{
					{Role: "user", Content: prompt},
				},
			},
		})
	}

	// Fire primer asynchronously to warm cache; don't block batch submission.
	var primerUsage model.TokenUsage
	var primerWg sync.WaitGroup
	if !aiCfg.NoBatch && len(batchItems) > 1 {
		primerWg.Add(1)
		go func() {
			defer primerWg.Done()
			primerResp, primerErr := anthropic.PrimerRequest(ctx, aiClient, batchItems[0].Params)
			if primerErr != nil {
				zap.L().Warn("extract: tier 2 primer failed", zap.Error(primerErr))
			} else if primerResp != nil {
				primerUsage.InputTokens = int(primerResp.Usage.InputTokens)
				primerUsage.OutputTokens = int(primerResp.Usage.OutputTokens)
				primerUsage.CacheCreationTokens = int(primerResp.Usage.CacheCreationInputTokens)
				primerUsage.CacheReadTokens = int(primerResp.Usage.CacheReadInputTokens)
			}
		}()
	}

	answers, usage, err := executeBatch(ctx, batchItems, routed, 2, aiClient, aiCfg)
	primerWg.Wait() // ensure primer goroutine completes before reading usage
	if err != nil {
		return nil, eris.Wrap(err, "extract: tier 2")
	}

	result.Answers = answers
	result.TokenUsage.Add(primerUsage)
	result.TokenUsage.Add(*usage)
	result.Duration = time.Since(start).Milliseconds()
	return result, nil
}

// ExtractTier3 runs Tier 3 extraction: expert analysis using Opus with
// prepared context (Haiku summarization).
func ExtractTier3(ctx context.Context, routed []model.RoutedQuestion, allAnswers []model.ExtractionAnswer, pages []model.CrawledPage, aiClient anthropic.Client, aiCfg config.AnthropicConfig) (*model.TierResult, error) {
	start := time.Now()
	result := &model.TierResult{Tier: 3}

	if len(routed) == 0 {
		return result, nil
	}

	// Prepare context: summarize pages with Haiku first (keep under ~25K tokens).
	summaryCtx, summaryUsage, err := prepareTier3Context(ctx, pages, allAnswers, aiClient, aiCfg)
	if err != nil {
		return nil, eris.Wrap(err, "extract: tier 3 context preparation")
	}

	var totalUsage model.TokenUsage
	totalUsage.Add(*summaryUsage)

	// Build requests for each T3 question.
	var batchItems []anthropic.BatchRequestItem
	for i, rq := range routed {
		instructions := ""
		if rq.Question.Instructions != "" {
			instructions = fmt.Sprintf("Instructions: %s", rq.Question.Instructions)
		}

		prompt := fmt.Sprintf(tier3Prompt,
			rq.Question.Text,
			instructions,
			rq.Question.OutputFormat,
			summaryCtx,
		)

		systemBlocks := anthropic.BuildCachedSystemBlocks(
			"You are an expert research analyst providing definitive, well-reasoned answers.",
		)

		batchItems = append(batchItems, anthropic.BatchRequestItem{
			CustomID: fmt.Sprintf("t3-%d-%s", i, rq.Question.ID),
			Params: anthropic.MessageRequest{
				Model:     aiCfg.OpusModel,
				MaxTokens: 2048,
				System:    systemBlocks,
				Messages: []anthropic.Message{
					{Role: "user", Content: prompt},
				},
			},
		})
	}

	// Fire primer asynchronously to warm cache; don't block batch submission.
	var primerUsage model.TokenUsage
	var primerWg sync.WaitGroup
	if !aiCfg.NoBatch && len(batchItems) > 1 {
		primerWg.Add(1)
		go func() {
			defer primerWg.Done()
			primerResp, primerErr := anthropic.PrimerRequest(ctx, aiClient, batchItems[0].Params)
			if primerErr != nil {
				zap.L().Warn("extract: tier 3 primer failed", zap.Error(primerErr))
			} else if primerResp != nil {
				primerUsage.InputTokens = int(primerResp.Usage.InputTokens)
				primerUsage.OutputTokens = int(primerResp.Usage.OutputTokens)
				primerUsage.CacheCreationTokens = int(primerResp.Usage.CacheCreationInputTokens)
				primerUsage.CacheReadTokens = int(primerResp.Usage.CacheReadInputTokens)
			}
		}()
	}

	answers, batchUsage, err := executeBatch(ctx, batchItems, routed, 3, aiClient, aiCfg)
	primerWg.Wait() // ensure primer goroutine completes before reading usage
	if err != nil {
		return nil, eris.Wrap(err, "extract: tier 3")
	}

	totalUsage.Add(primerUsage)
	totalUsage.Add(*batchUsage)
	result.Answers = answers
	result.TokenUsage = totalUsage
	result.Duration = time.Since(start).Milliseconds()
	return result, nil
}

// prepareTier3Context uses Haiku to summarize pages into a compact context (~25K tokens).
func prepareTier3Context(ctx context.Context, pages []model.CrawledPage, answers []model.ExtractionAnswer, aiClient anthropic.Client, aiCfg config.AnthropicConfig) (string, *model.TokenUsage, error) {
	usage := &model.TokenUsage{}

	// Build a compact representation of existing answers.
	answersJSON, _ := json.Marshal(answers)

	// Summarize pages, prioritizing external sources (BBB, Google Maps, etc.)
	// so they don't get truncated by the char budget.
	var externalPages, otherPages []model.CrawledPage
	for _, p := range pages {
		if isExternalPage(p.Title) {
			externalPages = append(externalPages, p)
		} else {
			otherPages = append(otherPages, p)
		}
	}
	orderedPages := append(externalPages, otherPages...)

	var pageTexts []string
	totalLen := 0
	for _, p := range orderedPages {
		content := p.Markdown
		if len(content) > 3000 {
			content = content[:3000]
		}
		pageTexts = append(pageTexts, fmt.Sprintf("--- %s (%s) ---\n%s", p.Title, p.URL, content))
		totalLen += len(content)
		if totalLen > 50000 {
			break
		}
	}

	summarizePrompt := fmt.Sprintf(`Summarize the following company research data into a concise but comprehensive briefing.
Preserve all factual data points (numbers, names, dates, locations, certifications).
Keep the summary under 25000 characters.

Previous research findings:
%s

Source pages:
%s`, string(answersJSON), strings.Join(pageTexts, "\n\n"))

	resp, err := aiClient.CreateMessage(ctx, anthropic.MessageRequest{
		Model:     aiCfg.HaikuModel,
		MaxTokens: 8192,
		Messages: []anthropic.Message{
			{Role: "user", Content: summarizePrompt},
		},
	})
	if err != nil {
		return "", usage, eris.Wrap(err, "prepare t3 context: summarize")
	}

	usage.InputTokens = int(resp.Usage.InputTokens)
	usage.OutputTokens = int(resp.Usage.OutputTokens)
	usage.CacheCreationTokens = int(resp.Usage.CacheCreationInputTokens)
	usage.CacheReadTokens = int(resp.Usage.CacheReadInputTokens)

	return extractText(resp), usage, nil
}

// executeBatch sends items via batch API (or direct for small counts) and
// parses the extraction answers. Uses SmallBatchThreshold from config to
// determine when to skip the Batch API and use direct calls instead.
func executeBatch(ctx context.Context, items []anthropic.BatchRequestItem, routed []model.RoutedQuestion, tier int, aiClient anthropic.Client, aiCfg config.AnthropicConfig) ([]model.ExtractionAnswer, *model.TokenUsage, error) {
	usage := &model.TokenUsage{}
	var answers []model.ExtractionAnswer

	threshold := aiCfg.SmallBatchThreshold
	if threshold <= 0 {
		threshold = 3 // fallback default
	}
	if aiCfg.NoBatch || len(items) <= threshold {
		// Concurrent direct execution.
		type indexedAnswer struct {
			index  int
			answer model.ExtractionAnswer
			usage  anthropic.TokenUsage
		}

		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(maxDirectConcurrency)

		var mu sync.Mutex
		var results []indexedAnswer

		for i, item := range items {
			g.Go(func() error {
				resp, err := aiClient.CreateMessage(gCtx, item.Params)
				if err != nil {
					zap.L().Warn("extract: direct message failed",
						zap.Int("tier", tier),
						zap.String("question", routed[i].Question.ID),
						zap.Error(err),
					)
					return nil // Don't fail the group on individual errors.
				}

				answer := parseExtractionAnswer(extractText(resp), routed[i].Question, tier)

				mu.Lock()
				results = append(results, indexedAnswer{
					index:  i,
					answer: answer,
					usage:  resp.Usage,
				})
				mu.Unlock()
				return nil
			})
		}

		_ = g.Wait()

		// Aggregate usage and answers in original order.
		for _, r := range results {
			usage.InputTokens += int(r.usage.InputTokens)
			usage.OutputTokens += int(r.usage.OutputTokens)
			usage.CacheCreationTokens += int(r.usage.CacheCreationInputTokens)
			usage.CacheReadTokens += int(r.usage.CacheReadInputTokens)
			answers = append(answers, r.answer)
		}
		return answers, usage, nil
	}

	// Batch execution.
	batch, err := aiClient.CreateBatch(ctx, anthropic.BatchRequest{Requests: items})
	if err != nil {
		return nil, usage, eris.Wrap(err, "execute batch: create")
	}

	// Use tighter poll cap for small batches that complete quickly.
	var pollOpts []anthropic.PollOption
	if len(items) < 20 {
		pollOpts = append(pollOpts, anthropic.WithPollCap(10*time.Second))
	}
	batch, err = anthropic.PollBatch(ctx, aiClient, batch.ID, pollOpts...)
	if err != nil {
		return nil, usage, eris.Wrap(err, "execute batch: poll")
	}

	iter, err := aiClient.GetBatchResults(ctx, batch.ID)
	if err != nil {
		return nil, usage, eris.Wrap(err, "execute batch: get results")
	}

	results, err := anthropic.CollectBatchResults(iter)
	if err != nil {
		return nil, usage, eris.Wrap(err, "execute batch: collect results")
	}

	for i, rq := range routed {
		var prefix string
		switch tier {
		case 1:
			prefix = "t1"
		case 2:
			prefix = "t2"
		case 3:
			prefix = "t3"
		}
		customID := fmt.Sprintf("%s-%d-%s", prefix, i, rq.Question.ID)
		resp, ok := results[customID]
		if !ok || resp == nil {
			zap.L().Warn("extract: batch item missing from results",
				zap.String("custom_id", customID),
				zap.String("question", rq.Question.ID),
				zap.Int("tier", tier),
			)
			continue
		}

		usage.InputTokens += int(resp.Usage.InputTokens)
		usage.OutputTokens += int(resp.Usage.OutputTokens)
		usage.CacheCreationTokens += int(resp.Usage.CacheCreationInputTokens)
		usage.CacheReadTokens += int(resp.Usage.CacheReadInputTokens)

		answer := parseExtractionAnswer(extractText(resp), rq.Question, tier)
		answers = append(answers, answer)
	}

	return answers, usage, nil
}

func parseExtractionAnswer(text string, q model.Question, tier int) model.ExtractionAnswer {
	answer := model.ExtractionAnswer{
		QuestionID: q.ID,
		FieldKey:   q.FieldKey,
		Tier:       tier,
		Confidence: 0.0,
	}

	cleaned := cleanJSON(text)
	var raw struct {
		Value      any     `json:"value"`
		Confidence float64 `json:"confidence"`
		Reasoning  string  `json:"reasoning"`
		SourceURL  string  `json:"source_url"`
	}

	if err := json.Unmarshal([]byte(cleaned), &raw); err != nil {
		// If JSON parsing fails, use raw text as value.
		answer.Value = text
		return answer
	}

	answer.Value = raw.Value
	answer.Confidence = raw.Confidence
	answer.Reasoning = raw.Reasoning
	answer.SourceURL = raw.SourceURL

	return answer
}

func buildT1Context(answers []model.ExtractionAnswer) string {
	if len(answers) == 0 {
		return "No previous findings."
	}

	var parts []string
	for _, a := range answers {
		parts = append(parts, fmt.Sprintf("- %s: %v (confidence: %.2f)", a.FieldKey, a.Value, a.Confidence))
	}
	return strings.Join(parts, "\n")
}

// isExternalPage checks if a page title has an external source prefix.
func isExternalPage(title string) bool {
	lower := strings.ToLower(title)
	return strings.HasPrefix(lower, "[bbb] ") ||
		strings.HasPrefix(lower, "[google_maps] ") ||
		strings.HasPrefix(lower, "[sos] ") ||
		strings.HasPrefix(lower, "[linkedin] ")
}

// buildExternalSnippets extracts compact snippets from external source pages
// (BBB, Google Maps, SoS, LinkedIn) within a character budget.
func buildExternalSnippets(pages []model.ClassifiedPage, budget int) string {
	var parts []string
	totalLen := 0

	for _, p := range pages {
		if !isExternalPage(p.Title) {
			continue
		}
		content := p.Markdown
		remaining := budget - totalLen
		if remaining <= 0 {
			break
		}
		if len(content) > remaining {
			content = content[:remaining]
		}
		parts = append(parts, fmt.Sprintf("[%s] %s:\n%s", p.Title, p.URL, content))
		totalLen += len(content)
	}

	return strings.Join(parts, "\n\n")
}

func buildPagesContext(pages []model.ClassifiedPage, maxCharsPerPage int) string {
	var parts []string
	for _, p := range pages {
		content := p.Markdown
		if len(content) > maxCharsPerPage {
			content = content[:maxCharsPerPage]
		}
		parts = append(parts, fmt.Sprintf("--- %s (%s) ---\n%s", p.Title, p.URL, content))
	}
	return strings.Join(parts, "\n\n")
}
