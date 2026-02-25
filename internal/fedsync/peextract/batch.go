// Package peextract implements PE firm website extraction via tiered Claude models.
package peextract

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

	"github.com/sells-group/research-cli/pkg/anthropic"
)

const (
	// batchThresholdT1 is the minimum items to use Batch API for T1.
	batchThresholdT1 = 15
	// batchThresholdT2 is the minimum items to use Batch API for T2.
	batchThresholdT2 = 4

	// maxDirectConcurrency limits parallel direct API calls.
	maxDirectConcurrency = 10

	// maxRetries for individual direct calls.
	maxRetries = 3
)

// batchItem represents a single LLM request in a batch.
type batchItem struct {
	CustomID string
	Question Question
	Request  anthropic.MessageRequest
}

// executeBatch runs a batch of extraction requests either via Batch API or direct calls.
func executeBatch(ctx context.Context, items []batchItem, tier int, client anthropic.Client) ([]Answer, int64, int64, error) {
	if len(items) == 0 {
		return nil, 0, 0, nil
	}

	threshold := batchThresholdT1
	if tier == 2 {
		threshold = batchThresholdT2
	}

	if len(items) <= threshold {
		return executeDirectConcurrent(ctx, items, tier, client)
	}
	return executeBatchAPI(ctx, items, tier, client)
}

// executeDirectConcurrent runs items as concurrent direct API calls.
func executeDirectConcurrent(ctx context.Context, items []batchItem, tier int, client anthropic.Client) ([]Answer, int64, int64, error) {
	log := zap.L().With(zap.Int("tier", tier), zap.String("mode", "direct"), zap.Int("items", len(items)))
	log.Debug("executing direct concurrent calls")

	var (
		mu          sync.Mutex
		answers     []Answer
		totalInput  int64
		totalOutput int64
	)

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxDirectConcurrency)

	for _, item := range items {
		g.Go(func() error {
			var resp *anthropic.MessageResponse
			var err error

			for attempt := 0; attempt < maxRetries; attempt++ {
				resp, err = client.CreateMessage(gctx, item.Request)
				if err == nil {
					break
				}
				if gctx.Err() != nil {
					return nil //nolint:nilerr // context cancelled; abort retries without failing the errgroup
				}
				backoff := time.Duration(1<<uint(attempt)) * 500 * time.Millisecond
				time.Sleep(backoff)
			}

			if err != nil {
				log.Warn("direct call failed after retries",
					zap.String("question", item.Question.Key),
					zap.Error(err))
				return nil
			}

			answer := parseAnswerFromResponse(resp, item.Question, tier)

			mu.Lock()
			answers = append(answers, answer...)
			totalInput += resp.Usage.InputTokens
			totalOutput += resp.Usage.OutputTokens
			mu.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return answers, totalInput, totalOutput, eris.Wrap(err, "peextract: direct batch")
	}

	return answers, totalInput, totalOutput, nil
}

// executeBatchAPI runs items via the Anthropic Batch API.
func executeBatchAPI(ctx context.Context, items []batchItem, tier int, client anthropic.Client) ([]Answer, int64, int64, error) {
	log := zap.L().With(zap.Int("tier", tier), zap.String("mode", "batch"), zap.Int("items", len(items)))
	log.Info("submitting batch API request")

	batchReqs := make([]anthropic.BatchRequestItem, len(items))
	for i, item := range items {
		batchReqs[i] = anthropic.BatchRequestItem{
			CustomID: item.CustomID,
			Params:   item.Request,
		}
	}

	batchResp, err := client.CreateBatch(ctx, anthropic.BatchRequest{
		Requests: batchReqs,
	})
	if err != nil {
		return nil, 0, 0, eris.Wrap(err, "peextract: create batch")
	}

	log.Info("batch submitted", zap.String("batch_id", batchResp.ID))

	batchResp, err = anthropic.PollBatch(ctx, client, batchResp.ID,
		anthropic.WithPollInterval(2*time.Second),
		anthropic.WithPollCap(15*time.Second),
		anthropic.WithPollTimeout(30*time.Minute),
	)
	if err != nil {
		return nil, 0, 0, eris.Wrap(err, "peextract: poll batch")
	}

	log.Info("batch completed",
		zap.Int64("succeeded", batchResp.RequestCounts.Succeeded),
		zap.Int64("errored", batchResp.RequestCounts.Errored))

	iter, err := client.GetBatchResults(ctx, batchResp.ID)
	if err != nil {
		return nil, 0, 0, eris.Wrap(err, "peextract: get batch results")
	}
	defer func() { _ = iter.Close() }()

	results, err := anthropic.CollectBatchResults(iter)
	if err != nil {
		return nil, 0, 0, eris.Wrap(err, "peextract: collect batch results")
	}

	itemMap := make(map[string]batchItem, len(items))
	for _, item := range items {
		itemMap[item.CustomID] = item
	}

	var answers []Answer
	var totalInput, totalOutput int64

	for customID, resp := range results {
		item, ok := itemMap[customID]
		if !ok {
			log.Warn("unknown custom_id in batch results", zap.String("custom_id", customID))
			continue
		}

		parsed := parseAnswerFromResponse(resp, item.Question, tier)
		answers = append(answers, parsed...)
		totalInput += resp.Usage.InputTokens
		totalOutput += resp.Usage.OutputTokens
	}

	return answers, totalInput, totalOutput, nil
}

// buildBatchItems constructs batch items for a set of questions.
func buildBatchItems(questions []Question, docs *PEFirmDocs, systemText string, tier int) []batchItem {
	model := ModelForTier(tier)
	maxTokens := MaxTokensForTier(tier)
	system := anthropic.BuildCachedSystemBlocks(systemText)

	var items []batchItem
	for i, q := range questions {
		docCtx := DocumentForQuestion(docs, q)
		if docCtx == "" {
			continue
		}

		userMsg := BuildUserMessage(q, docCtx)

		items = append(items, batchItem{
			CustomID: fmt.Sprintf("t%d-%d-%s", tier, i, q.Key),
			Question: q,
			Request: anthropic.MessageRequest{
				Model:     model,
				MaxTokens: maxTokens,
				System:    system,
				Messages: []anthropic.Message{
					{Role: "user", Content: userMsg},
				},
			},
		})
	}

	return items
}

// firePrimer sends a primer request to warm the cache.
func firePrimer(ctx context.Context, client anthropic.Client, items []batchItem) (int64, int64) {
	if len(items) == 0 {
		return 0, 0
	}

	resp, err := anthropic.PrimerRequest(ctx, client, items[0].Request)
	if err != nil {
		zap.L().Debug("primer request failed", zap.Error(err))
		return 0, 0
	}

	return resp.Usage.InputTokens, resp.Usage.OutputTokens
}

// parseAnswerFromResponse extracts Answer(s) from an LLM response.
func parseAnswerFromResponse(resp *anthropic.MessageResponse, q Question, tier int) []Answer {
	if resp == nil || len(resp.Content) == 0 {
		return nil
	}

	text := extractText(resp)
	cleaned := cleanJSON(text)

	var raw struct {
		Value      any     `json:"value"`
		Confidence float64 `json:"confidence"`
		Reasoning  string  `json:"reasoning"`
	}

	if err := json.Unmarshal([]byte(cleaned), &raw); err != nil {
		zap.L().Debug("failed to parse extraction JSON",
			zap.String("question", q.Key),
			zap.Error(err))
		return nil
	}

	sourcePageType := ""
	if len(q.PageTypes) > 0 {
		sourcePageType = q.PageTypes[0]
	}

	return []Answer{{
		QuestionKey:    q.Key,
		Value:          raw.Value,
		Confidence:     raw.Confidence,
		Tier:           tier,
		Reasoning:      raw.Reasoning,
		SourcePageType: sourcePageType,
		Model:          resp.Model,
		InputTokens:    int(resp.Usage.InputTokens),
		OutputTokens:   int(resp.Usage.OutputTokens),
	}}
}

// extractText concatenates all text content blocks.
func extractText(resp *anthropic.MessageResponse) string {
	if resp == nil {
		return ""
	}
	var sb strings.Builder
	for _, b := range resp.Content {
		if b.Type == "" || b.Type == "text" {
			sb.WriteString(b.Text)
		}
	}
	return sb.String()
}

// cleanJSON strips markdown fences, extracts JSON object, and repairs truncation.
func cleanJSON(text string) string {
	text = strings.TrimSpace(text)

	if strings.HasPrefix(text, "```json") {
		text = strings.TrimPrefix(text, "```json")
		if idx := strings.LastIndex(text, "```"); idx >= 0 {
			text = text[:idx]
		}
	} else if strings.HasPrefix(text, "```") {
		text = strings.TrimPrefix(text, "```")
		if idx := strings.LastIndex(text, "```"); idx >= 0 {
			text = text[:idx]
		}
	}

	start := strings.Index(text, "{")
	end := strings.LastIndex(text, "}")
	if start >= 0 && end > start {
		text = text[start : end+1]
	}

	text = strings.TrimSpace(text)

	// Attempt to repair truncated JSON (unclosed brackets/braces).
	text = repairTruncatedJSON(text)

	return text
}

// repairTruncatedJSON closes any unclosed brackets or braces in truncated JSON.
func repairTruncatedJSON(text string) string {
	if len(text) == 0 {
		return text
	}

	// Track open delimiters in order.
	var stack []byte
	inString := false
	escape := false

	for i := 0; i < len(text); i++ {
		c := text[i]

		if escape {
			escape = false
			continue
		}

		if c == '\\' && inString {
			escape = true
			continue
		}

		if c == '"' {
			inString = !inString
			continue
		}

		if inString {
			continue
		}

		switch c {
		case '{':
			stack = append(stack, '}')
		case '[':
			stack = append(stack, ']')
		case '}', ']':
			if len(stack) > 0 && stack[len(stack)-1] == c {
				stack = stack[:len(stack)-1]
			}
		}
	}

	// Close unclosed delimiters in reverse order.
	for i := len(stack) - 1; i >= 0; i-- {
		// Trim trailing comma before closing (common in truncated arrays).
		text = strings.TrimRight(text, " \t\n\r,")
		text += string(stack[i])
	}

	return text
}
