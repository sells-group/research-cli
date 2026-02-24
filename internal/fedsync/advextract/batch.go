package advextract

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
	batchThresholdT2 = 8
	// batchThresholdT3 is the minimum items to use Batch API for T3.
	batchThresholdT3 = 4

	// maxDirectConcurrency limits parallel direct API calls.
	maxDirectConcurrency = 10

	// maxRetries for individual direct calls.
	maxRetries = 3

	// confidenceEscalationThreshold: T1 answers below this go to T2.
	confidenceEscalationThreshold = 0.4
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
	switch tier {
	case 2:
		threshold = batchThresholdT2
	case 3:
		threshold = batchThresholdT3
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
					return gctx.Err()
				}
				backoff := time.Duration(1<<uint(attempt)) * 500 * time.Millisecond
				time.Sleep(backoff)
			}

			if err != nil {
				log.Warn("direct call failed after retries",
					zap.String("question", item.Question.Key),
					zap.Error(err))
				return nil // don't fail the group
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
		return answers, totalInput, totalOutput, eris.Wrap(err, "advextract: direct batch")
	}

	return answers, totalInput, totalOutput, nil
}

// executeBatchAPI runs items via the Anthropic Batch API.
func executeBatchAPI(ctx context.Context, items []batchItem, tier int, client anthropic.Client) ([]Answer, int64, int64, error) {
	log := zap.L().With(zap.Int("tier", tier), zap.String("mode", "batch"), zap.Int("items", len(items)))
	log.Info("submitting batch API request")

	// Build batch request.
	batchReqs := make([]anthropic.BatchRequestItem, len(items))
	for i, item := range items {
		batchReqs[i] = anthropic.BatchRequestItem{
			CustomID: item.CustomID,
			Params:   item.Request,
		}
	}

	// Submit batch.
	batchResp, err := client.CreateBatch(ctx, anthropic.BatchRequest{
		Requests: batchReqs,
	})
	if err != nil {
		return nil, 0, 0, eris.Wrap(err, "advextract: create batch")
	}

	log.Info("batch submitted", zap.String("batch_id", batchResp.ID))

	// Poll for completion.
	batchResp, err = anthropic.PollBatch(ctx, client, batchResp.ID,
		anthropic.WithPollInterval(2*time.Second),
		anthropic.WithPollCap(15*time.Second),
		anthropic.WithPollTimeout(30*time.Minute),
	)
	if err != nil {
		return nil, 0, 0, eris.Wrap(err, "advextract: poll batch")
	}

	log.Info("batch completed",
		zap.Int64("succeeded", batchResp.RequestCounts.Succeeded),
		zap.Int64("errored", batchResp.RequestCounts.Errored))

	// Collect results.
	iter, err := client.GetBatchResults(ctx, batchResp.ID)
	if err != nil {
		return nil, 0, 0, eris.Wrap(err, "advextract: get batch results")
	}
	defer iter.Close() //nolint:errcheck

	results, err := anthropic.CollectBatchResults(iter)
	if err != nil {
		return nil, 0, 0, eris.Wrap(err, "advextract: collect batch results")
	}

	// Build lookup from customID → item.
	itemMap := make(map[string]batchItem, len(items))
	for _, item := range items {
		itemMap[item.CustomID] = item
	}

	// Parse results.
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
func buildBatchItems(questions []Question, docs *AdvisorDocs, systemText string, tier int) []batchItem {
	model := ModelForTier(tier)
	maxTokens := MaxTokensForTier(tier)
	system := anthropic.BuildCachedSystemBlocks(systemText)

	var items []batchItem
	for i, q := range questions {
		docCtx := DocumentForQuestion(docs, q)
		if docCtx == "" {
			continue // no documents available for this question
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

// firePrimer sends a primer request to warm the cache, returning usage.
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

	sourceDoc := "part2"
	if len(q.SourceDocs) > 0 {
		sourceDoc = q.SourceDocs[0]
	}
	sourceSection := ""
	if len(q.SourceSections) > 0 {
		sourceSection = q.SourceSections[0]
	}

	return []Answer{{
		QuestionKey:   q.Key,
		Value:         raw.Value,
		Confidence:    raw.Confidence,
		Tier:          tier,
		Reasoning:     raw.Reasoning,
		SourceDoc:     sourceDoc,
		SourceSection: sourceSection,
		Model:         resp.Model,
		InputTokens:   int(resp.Usage.InputTokens),
		OutputTokens:  int(resp.Usage.OutputTokens),
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

// cleanJSON strips markdown fences and extracts JSON object.
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

	return strings.TrimSpace(text)
}

// filterEscalationQuestions returns T1 questions whose answers had low confidence.
func filterEscalationQuestions(t1Answers []Answer, t2Questions []Question) []Question {
	// Build set of T1 question keys with low confidence.
	lowConf := make(map[string]bool)
	for _, a := range t1Answers {
		if a.Confidence < confidenceEscalationThreshold && a.Value != nil {
			lowConf[a.QuestionKey] = true
		}
	}

	// Find corresponding T1 questions that should escalate to T2.
	// These are questions originally assigned to T1 that had low confidence.
	escalated := make(map[string]bool)
	for _, q := range AllQuestions() {
		if q.Tier == 1 && lowConf[q.Key] {
			escalated[q.Key] = true
		}
	}

	// Return the existing T2 questions plus any escalated T1 questions.
	var result []Question
	added := make(map[string]bool)

	// Add original T2 questions first.
	for _, q := range t2Questions {
		result = append(result, q)
		added[q.Key] = true
	}

	// Add escalated T1 questions (re-assigned to T2).
	for _, q := range AllQuestions() {
		if escalated[q.Key] && !added[q.Key] {
			q2 := q
			q2.Tier = 2 // escalate
			result = append(result, q2)
			added[q.Key] = true
		}
	}

	return result
}
