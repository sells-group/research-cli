package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/anthropic"
)

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
				Messages: []anthropic.Message{
					{Role: "user", Content: prompt},
				},
			},
		})
	}

	answers, usage, err := executeBatch(ctx, batchItems, routed, 1, aiClient)
	if err != nil {
		return nil, eris.Wrap(err, "extract: tier 1")
	}

	result.Answers = answers
	result.TokenUsage = *usage
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

	// Send primer request to warm cache if there are enough items.
	if len(batchItems) > 1 {
		_, err := anthropic.PrimerRequest(ctx, aiClient, batchItems[0].Params)
		if err != nil {
			zap.L().Warn("extract: tier 2 primer failed", zap.Error(err))
		}
	}

	answers, usage, err := executeBatch(ctx, batchItems, routed, 2, aiClient)
	if err != nil {
		return nil, eris.Wrap(err, "extract: tier 2")
	}

	result.Answers = answers
	result.TokenUsage = *usage
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

	// Primer for cache warming.
	if len(batchItems) > 1 {
		_, err := anthropic.PrimerRequest(ctx, aiClient, batchItems[0].Params)
		if err != nil {
			zap.L().Warn("extract: tier 3 primer failed", zap.Error(err))
		}
	}

	answers, batchUsage, err := executeBatch(ctx, batchItems, routed, 3, aiClient)
	if err != nil {
		return nil, eris.Wrap(err, "extract: tier 3")
	}

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

	// Summarize pages.
	var pageTexts []string
	totalLen := 0
	for _, p := range pages {
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

	return extractText(resp), usage, nil
}

// executeBatch sends items via batch API (or direct for small counts) and
// parses the extraction answers.
func executeBatch(ctx context.Context, items []anthropic.BatchRequestItem, routed []model.RoutedQuestion, tier int, aiClient anthropic.Client) ([]model.ExtractionAnswer, *model.TokenUsage, error) {
	usage := &model.TokenUsage{}
	var answers []model.ExtractionAnswer

	if len(items) <= 3 {
		// Direct execution for small batches.
		for i, item := range items {
			resp, err := aiClient.CreateMessage(ctx, item.Params)
			if err != nil {
				zap.L().Warn("extract: direct message failed",
					zap.Int("tier", tier),
					zap.String("question", routed[i].Question.ID),
					zap.Error(err),
				)
				continue
			}

			usage.InputTokens += int(resp.Usage.InputTokens)
			usage.OutputTokens += int(resp.Usage.OutputTokens)

			answer := parseExtractionAnswer(extractText(resp), routed[i].Question, tier)
			answers = append(answers, answer)
		}
		return answers, usage, nil
	}

	// Batch execution.
	batch, err := aiClient.CreateBatch(ctx, anthropic.BatchRequest{Requests: items})
	if err != nil {
		return nil, usage, eris.Wrap(err, "execute batch: create")
	}

	batch, err = anthropic.PollBatch(ctx, aiClient, batch.ID)
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
			continue
		}

		usage.InputTokens += int(resp.Usage.InputTokens)
		usage.OutputTokens += int(resp.Usage.OutputTokens)

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
