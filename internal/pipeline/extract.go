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
	"github.com/sells-group/research-cli/pkg/ppp"
)

// maxDirectConcurrency limits concurrent CreateMessage calls in no-batch mode.
const maxDirectConcurrency = 10

// Per-tier small batch thresholds: below these, use direct calls instead of
// the Batch API. T1/Classify process many items so higher thresholds reduce
// overhead; T3 has few items so a lower threshold is appropriate.
const (
	smallBatchThresholdT1       = 20
	smallBatchThresholdClassify = 20
	smallBatchThresholdT2       = 10
	smallBatchThresholdT3       = 5
)

// directRetryAttempts is the max number of retries for direct API calls.
const directRetryAttempts = 3

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

// Rich prompt templates for multi-field questions with Instructions.
const tier1PromptRich = `%s

Output JSON schema:
%s

Page URL: %s
Page content:
%s

Extract the requested data from this page. Return valid JSON matching the schema above.`

const tier2PromptRich = `%s

Output JSON schema:
%s

Previous findings (Tier 1):
%s

Source pages:
%s

Synthesize the best answer from all available sources. Return valid JSON matching the schema above.`

const tier3PromptRich = `%s

Output JSON schema:
%s

All available context:
%s

Provide a thorough, well-reasoned answer. Return valid JSON matching the schema above.`

// richSystemText is the system prompt for multi-field (rich) extractions.
const richSystemText = "You are a research analyst extracting structured data from web pages. Return valid JSON matching the requested schema. Use null for fields not found."

// FormatPageMetadata formats structured metadata from external pages
// (Google Maps reviews, BBB rating) into a context block for injection
// into extraction prompts. Returns "" if no metadata found.
func FormatPageMetadata(pages []model.ClassifiedPage) string {
	var b strings.Builder
	for _, p := range pages {
		if p.Metadata == nil {
			continue
		}
		meta := p.Metadata
		if meta.ReviewCount > 0 || meta.Rating > 0 || meta.BBBRating != "" {
			b.WriteString("--- Structured Metadata: " + p.Title + " ---\n")
			if meta.Rating > 0 {
				fmt.Fprintf(&b, "Google Rating: %.1f stars\n", meta.Rating)
			}
			if meta.ReviewCount > 0 {
				fmt.Fprintf(&b, "Google Review Count: %d\n", meta.ReviewCount)
			}
			if meta.BBBRating != "" {
				b.WriteString("BBB Rating: " + meta.BBBRating + "\n")
			}
			b.WriteString("\n")
		}
	}
	return b.String()
}

// FormatPPPContext formats the best PPP loan match into a concise context block
// for injection into extraction prompts. Returns "" if matches is empty.
func FormatPPPContext(matches []ppp.LoanMatch) string {
	if len(matches) == 0 {
		return ""
	}

	best := matches[0]
	var b strings.Builder
	b.WriteString("--- PPP Loan Record (Federal Database) ---\n")
	if best.BorrowerName != "" {
		b.WriteString("Borrower: " + best.BorrowerName + "\n")
	}
	if best.CurrentApproval > 0 {
		fmt.Fprintf(&b, "Loan Amount: $%.0f\n", best.CurrentApproval)
	}
	if best.JobsReported > 0 {
		fmt.Fprintf(&b, "Jobs Reported: %d\n", best.JobsReported)
	}
	if best.NAICSCode != "" {
		b.WriteString("NAICS: " + best.NAICSCode + "\n")
	}
	if best.BusinessType != "" {
		b.WriteString("Business Type: " + best.BusinessType + "\n")
	}
	if best.BusinessAge != "" {
		b.WriteString("Business Age: " + best.BusinessAge + "\n")
	}
	if !best.DateApproved.IsZero() {
		b.WriteString("Approved: " + best.DateApproved.Format("2006-01-02") + "\n")
	}
	if best.LoanStatus != "" {
		b.WriteString("Status: " + best.LoanStatus + "\n")
	}
	return b.String()
}

// FormatPreSeededContext formats pre-seeded CSV data (Grata employee count,
// NAICS, description, etc.) into a context block for injection into extraction
// prompts. The LLM can verify/correct these against website evidence.
// Returns "" if no pre-seeded data is available.
func FormatPreSeededContext(preSeeded map[string]any) string {
	if len(preSeeded) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("--- Industry Data (verify against website) ---\n")
	if v, ok := preSeeded["employees"]; ok {
		fmt.Fprintf(&b, "Employee Count (industry estimate): %v — use this as baseline unless website explicitly states a different current headcount\n", v)
	}
	if v, ok := preSeeded["naics_code"]; ok {
		fmt.Fprintf(&b, "NAICS Code: %v\n", v)
	}
	if v, ok := preSeeded["description"]; ok {
		fmt.Fprintf(&b, "Business Description: %v\n", v)
	}
	if v, ok := preSeeded["revenue_range"]; ok {
		fmt.Fprintf(&b, "Revenue Estimate: %v\n", v)
	}
	if v, ok := preSeeded["year_established"]; ok {
		fmt.Fprintf(&b, "Year Founded: %v — use this unless website explicitly states a different founding year\n", v)
	}
	if v, ok := preSeeded["email"]; ok {
		fmt.Fprintf(&b, "Contact Email: %v\n", v)
	}
	if v, ok := preSeeded["exec_first_name"]; ok {
		fmt.Fprintf(&b, "Executive First Name: %v\n", v)
	}
	if v, ok := preSeeded["exec_last_name"]; ok {
		fmt.Fprintf(&b, "Executive Last Name: %v\n", v)
	}
	if v, ok := preSeeded["exec_title"]; ok {
		fmt.Fprintf(&b, "Executive Title: %v\n", v)
	}
	return b.String()
}

// ExtractTier1 runs Tier 1 extraction: single-page fact extraction using Haiku.
func ExtractTier1(ctx context.Context, routed []model.RoutedQuestion, company model.Company, pppMatches []ppp.LoanMatch, aiClient anthropic.Client, aiCfg config.AnthropicConfig) (*model.TierResult, error) {
	start := time.Now()
	result := &model.TierResult{Tier: 1}

	if len(routed) == 0 {
		return result, nil
	}

	const t1SystemText = "You are a research analyst extracting specific data from a web page. Return a valid JSON object with value, confidence, reasoning, and source_url. If the requested information is not found on the page, return {\"value\": null, \"confidence\": 0.0, \"reasoning\": \"Information not found on page\", \"source_url\": \"<page URL>\"}."

	// Both primer and batch items use cached system blocks so batch items
	// signal a cache read and benefit from the primer's warm cache.
	systemBlocks := anthropic.BuildCachedSystemBlocks(t1SystemText)

	// Pre-compute external snippets per routed question (dedup: one call per
	// unique page set instead of per-question inside the loop).
	externalSnippetCache := make(map[int]string, len(routed))
	for i, rq := range routed {
		if len(rq.Pages) > 1 {
			externalSnippetCache[i] = buildExternalSnippets(rq.Pages[1:], 2000)
		}
	}

	// Build batch items: one per question, using the first matched page.
	// Multi-field questions (with Instructions) use a rich prompt template
	// and a dedicated system prompt.
	richSystemBlocks := anthropic.BuildCachedSystemBlocks(richSystemText)

	var batchItems []anthropic.BatchRequestItem
	for i, rq := range routed {
		if len(rq.Pages) == 0 {
			continue
		}
		page := rq.Pages[0] // Tier 1: single page

		content := truncateByRelevance(page.Markdown, rq.Question.Text, 8000)

		// Append pre-computed external source snippets.
		if externalCtx, ok := externalSnippetCache[i]; ok && externalCtx != "" {
			content += "\n\n--- Additional Sources ---\n" + externalCtx
		}

		// Append PPP loan context if available.
		if pppCtx := FormatPPPContext(pppMatches); pppCtx != "" {
			content += "\n\n" + pppCtx
		}

		// Append structured metadata from external pages (reviews, BBB rating).
		if metaCtx := FormatPageMetadata(rq.Pages); metaCtx != "" {
			content += "\n\n" + metaCtx
		}

		// Append pre-seeded CSV data (employee count, NAICS, etc.) as context.
		if preCtx := FormatPreSeededContext(company.PreSeeded); preCtx != "" {
			content += "\n\n" + preCtx
		}

		// Choose prompt template and system blocks based on whether the
		// question has structured Instructions (multi-field grouped prompt).
		var prompt string
		sysBlocks := systemBlocks
		if rq.Question.Instructions != "" && len(splitFieldKeys(rq.Question.FieldKey)) > 1 {
			prompt = fmt.Sprintf(tier1PromptRich,
				rq.Question.Instructions,
				rq.Question.OutputFormat,
				page.URL,
				content,
			)
			sysBlocks = richSystemBlocks
		} else {
			instructions := ""
			if rq.Question.Instructions != "" {
				instructions = fmt.Sprintf("Instructions: %s", rq.Question.Instructions)
			}
			prompt = fmt.Sprintf(tier1Prompt,
				rq.Question.Text,
				instructions,
				rq.Question.OutputFormat,
				page.URL,
				content,
				page.URL,
			)
		}

		batchItems = append(batchItems, anthropic.BatchRequestItem{
			CustomID: fmt.Sprintf("t1-%d-%s", i, rq.Question.ID),
			Params: anthropic.MessageRequest{
				Model:     aiCfg.HaikuModel,
				MaxTokens: maxTokensForQuestion(rq.Question),
				System:    sysBlocks,
				Messages: []anthropic.Message{
					{Role: "user", Content: prompt},
				},
			},
		})
	}

	// Fire primer asynchronously to warm cache; it overlaps with batch
	// submission + early polling instead of blocking before submission.
	var primerUsage model.TokenUsage
	var primerWg sync.WaitGroup
	if !aiCfg.NoBatch && len(batchItems) > 1 {
		primerWg.Add(1)
		go func() {
			defer primerWg.Done()
			primerReq := batchItems[0].Params
			primerResp, primerErr := anthropic.PrimerRequest(ctx, aiClient, primerReq)
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
// Includes T1 answers as context (only low-confidence ones to reduce prompt size).
func ExtractTier2(ctx context.Context, routed []model.RoutedQuestion, t1Answers []model.ExtractionAnswer, company model.Company, pppMatches []ppp.LoanMatch, aiClient anthropic.Client, aiCfg config.AnthropicConfig) (*model.TierResult, error) {
	start := time.Now()
	result := &model.TierResult{Tier: 2}

	if len(routed) == 0 {
		return result, nil
	}

	const t2SystemText = "You are a senior research analyst. Synthesize data from multiple sources to provide accurate answers. If the requested information is not found on the page, return {\"value\": null, \"confidence\": 0.0, \"reasoning\": \"Information not found on page\", \"source_url\": \"<page URL>\"}."

	// Both primer and batch items use cached system blocks so batch items
	// signal a cache read and benefit from the primer's warm cache.
	systemBlocks := anthropic.BuildCachedSystemBlocks(t2SystemText)

	// Filter T1 answers to only include low-confidence ones for T2 context.
	// High-confidence answers are already reliable and just add noise/cost.
	const t2ConfidenceThreshold = 0.4
	var lowConfT1 []model.ExtractionAnswer
	for _, a := range t1Answers {
		if a.Confidence < t2ConfidenceThreshold {
			lowConfT1 = append(lowConfT1, a)
		}
	}

	// Build context from low-confidence T1 answers.
	t1Context := buildT1Context(lowConfT1)

	// Build page context per question.
	richSystemBlocks := anthropic.BuildCachedSystemBlocks(richSystemText)

	var batchItems []anthropic.BatchRequestItem
	for i, rq := range routed {
		pagesContext := buildPagesContext(rq.Pages, 4000)

		// Append PPP loan context if available.
		if pppCtx := FormatPPPContext(pppMatches); pppCtx != "" {
			pagesContext += "\n\n" + pppCtx
		}

		// Append structured metadata from external pages (reviews, BBB rating).
		if metaCtx := FormatPageMetadata(rq.Pages); metaCtx != "" {
			pagesContext += "\n\n" + metaCtx
		}

		// Append pre-seeded CSV data (employee count, NAICS, etc.) as context.
		if preCtx := FormatPreSeededContext(company.PreSeeded); preCtx != "" {
			pagesContext += "\n\n" + preCtx
		}

		var prompt string
		sysBlocks := systemBlocks
		if rq.Question.Instructions != "" && len(splitFieldKeys(rq.Question.FieldKey)) > 1 {
			prompt = fmt.Sprintf(tier2PromptRich,
				rq.Question.Instructions,
				rq.Question.OutputFormat,
				t1Context,
				pagesContext,
			)
			sysBlocks = richSystemBlocks
		} else {
			instructions := ""
			if rq.Question.Instructions != "" {
				instructions = fmt.Sprintf("Instructions: %s", rq.Question.Instructions)
			}
			prompt = fmt.Sprintf(tier2Prompt,
				rq.Question.Text,
				instructions,
				rq.Question.OutputFormat,
				t1Context,
				pagesContext,
			)
		}

		batchItems = append(batchItems, anthropic.BatchRequestItem{
			CustomID: fmt.Sprintf("t2-%d-%s", i, rq.Question.ID),
			Params: anthropic.MessageRequest{
				Model:     aiCfg.SonnetModel,
				MaxTokens: maxTokensForQuestion(rq.Question),
				System:    sysBlocks,
				Messages: []anthropic.Message{
					{Role: "user", Content: prompt},
				},
			},
		})
	}

	// Fire primer asynchronously to warm cache; it overlaps with batch
	// submission + early polling instead of blocking before submission.
	var primerUsage model.TokenUsage
	var primerWg sync.WaitGroup
	if !aiCfg.NoBatch && len(batchItems) > 1 {
		primerWg.Add(1)
		go func() {
			defer primerWg.Done()
			primerReq := batchItems[0].Params
			primerResp, primerErr := anthropic.PrimerRequest(ctx, aiClient, primerReq)
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
func ExtractTier3(ctx context.Context, routed []model.RoutedQuestion, allAnswers []model.ExtractionAnswer, pages []model.CrawledPage, company model.Company, pppMatches []ppp.LoanMatch, aiClient anthropic.Client, aiCfg config.AnthropicConfig) (*model.TierResult, error) {
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

	// Inject PPP context into the summary.
	if pppCtx := FormatPPPContext(pppMatches); pppCtx != "" {
		summaryCtx += "\n\n" + pppCtx
	}

	// Inject page metadata into the summary context.
	var allClassified []model.ClassifiedPage
	for _, p := range pages {
		allClassified = append(allClassified, model.ClassifiedPage{CrawledPage: p})
	}
	if metaCtx := FormatPageMetadata(allClassified); metaCtx != "" {
		summaryCtx += "\n\n" + metaCtx
	}

	// Inject pre-seeded CSV data into T3 context.
	if preCtx := FormatPreSeededContext(company.PreSeeded); preCtx != "" {
		summaryCtx += "\n\n" + preCtx
	}

	var totalUsage model.TokenUsage
	totalUsage.Add(*summaryUsage)

	const t3SystemText = "You are an expert research analyst providing definitive, well-reasoned answers. If the requested information is not found on the page, return {\"value\": null, \"confidence\": 0.0, \"reasoning\": \"Information not found on page\", \"source_url\": \"<page URL>\"}."

	// Both primer and batch items use cached system blocks so batch items
	// signal a cache read and benefit from the primer's warm cache.
	systemBlocks := anthropic.BuildCachedSystemBlocks(t3SystemText)

	// Build requests for each T3 question.
	richSystemBlocks := anthropic.BuildCachedSystemBlocks(richSystemText)

	var batchItems []anthropic.BatchRequestItem
	for i, rq := range routed {
		var prompt string
		sysBlocks := systemBlocks
		if rq.Question.Instructions != "" && len(splitFieldKeys(rq.Question.FieldKey)) > 1 {
			prompt = fmt.Sprintf(tier3PromptRich,
				rq.Question.Instructions,
				rq.Question.OutputFormat,
				summaryCtx,
			)
			sysBlocks = richSystemBlocks
		} else {
			instructions := ""
			if rq.Question.Instructions != "" {
				instructions = fmt.Sprintf("Instructions: %s", rq.Question.Instructions)
			}
			prompt = fmt.Sprintf(tier3Prompt,
				rq.Question.Text,
				instructions,
				rq.Question.OutputFormat,
				summaryCtx,
			)
		}

		batchItems = append(batchItems, anthropic.BatchRequestItem{
			CustomID: fmt.Sprintf("t3-%d-%s", i, rq.Question.ID),
			Params: anthropic.MessageRequest{
				Model:     aiCfg.OpusModel,
				MaxTokens: maxTokensForQuestion(rq.Question),
				System:    sysBlocks,
				Messages: []anthropic.Message{
					{Role: "user", Content: prompt},
				},
			},
		})
	}

	// Fire primer asynchronously to warm cache. Skip for small batches (< 3
	// items) where primer overhead exceeds the cache benefit.
	var primerUsage model.TokenUsage
	var primerWg sync.WaitGroup
	if !aiCfg.NoBatch && len(batchItems) >= 3 {
		primerWg.Add(1)
		go func() {
			defer primerWg.Done()
			primerReq := batchItems[0].Params
			primerResp, primerErr := anthropic.PrimerRequest(ctx, aiClient, primerReq)
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

// prepareTier3Context uses Haiku to summarize pages into a compact context
// (~25K tokens). When there are multiple page chunks, it batches the
// summarization calls in parallel for 40-60% faster T3 prep.
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

	// Chunk pages into groups of ~15K chars each for parallel summarization.
	const chunkCharLimit = 15000
	var chunks []string
	var currentChunk []string
	currentLen := 0

	for _, p := range orderedPages {
		content := p.Markdown
		if len(content) > 3000 {
			content = content[:3000]
		}
		pageText := fmt.Sprintf("--- %s (%s) ---\n%s", p.Title, p.URL, content)

		if currentLen+len(content) > chunkCharLimit && len(currentChunk) > 0 {
			chunks = append(chunks, strings.Join(currentChunk, "\n\n"))
			currentChunk = nil
			currentLen = 0
		}
		currentChunk = append(currentChunk, pageText)
		currentLen += len(content)

		// Stop if we've accumulated too much total content.
		if len(chunks)*chunkCharLimit+currentLen > 50000 {
			break
		}
	}
	if len(currentChunk) > 0 {
		chunks = append(chunks, strings.Join(currentChunk, "\n\n"))
	}

	// Single chunk: use a single sequential call (same as before).
	if len(chunks) <= 1 {
		allPages := ""
		if len(chunks) == 1 {
			allPages = chunks[0]
		}
		summarizePrompt := fmt.Sprintf(`Summarize the following company research data into a concise but comprehensive briefing.
Preserve all factual data points (numbers, names, dates, locations, certifications).
Keep the summary under 25000 characters.

Previous research findings:
%s

Source pages:
%s`, string(answersJSON), allPages)

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

	// Multiple chunks: summarize in parallel, then merge.
	summaries := make([]string, len(chunks))
	usages := make([]model.TokenUsage, len(chunks))

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(maxDirectConcurrency)

	for idx, chunk := range chunks {
		g.Go(func() error {
			prompt := fmt.Sprintf(`Summarize the following company research pages into a concise briefing.
Preserve all factual data points (numbers, names, dates, locations, certifications).

Previous research findings:
%s

Source pages:
%s`, string(answersJSON), chunk)

			resp, err := aiClient.CreateMessage(gCtx, anthropic.MessageRequest{
				Model:     aiCfg.HaikuModel,
				MaxTokens: 4096,
				Messages: []anthropic.Message{
					{Role: "user", Content: prompt},
				},
			})
			if err != nil {
				zap.L().Warn("prepare t3 context: chunk summarize failed",
					zap.Int("chunk", idx),
					zap.Error(err),
				)
				return nil // Don't fail the group on individual errors.
			}

			summaries[idx] = extractText(resp)
			usages[idx] = model.TokenUsage{
				InputTokens:        int(resp.Usage.InputTokens),
				OutputTokens:       int(resp.Usage.OutputTokens),
				CacheCreationTokens: int(resp.Usage.CacheCreationInputTokens),
				CacheReadTokens:    int(resp.Usage.CacheReadInputTokens),
			}
			return nil
		})
	}

	_ = g.Wait()

	// Aggregate chunk usages.
	for _, u := range usages {
		usage.Add(u)
	}

	// Collect non-empty summaries.
	var validSummaries []string
	for _, s := range summaries {
		if s != "" {
			validSummaries = append(validSummaries, s)
		}
	}

	// If we only got one summary back, return it directly.
	if len(validSummaries) <= 1 {
		result := ""
		if len(validSummaries) == 1 {
			result = validSummaries[0]
		}
		return result, usage, nil
	}

	// Merge summaries into a single briefing.
	mergePrompt := fmt.Sprintf(`Merge the following partial summaries into a single cohesive company research briefing.
Preserve all factual data points. Remove duplicates. Keep the output under 25000 characters.

%s`, strings.Join(validSummaries, "\n\n---\n\n"))

	mergeResp, err := aiClient.CreateMessage(ctx, anthropic.MessageRequest{
		Model:     aiCfg.HaikuModel,
		MaxTokens: 8192,
		Messages: []anthropic.Message{
			{Role: "user", Content: mergePrompt},
		},
	})
	if err != nil {
		// Fall back to concatenation if merge fails.
		return strings.Join(validSummaries, "\n\n"), usage, nil
	}

	usage.InputTokens += int(mergeResp.Usage.InputTokens)
	usage.OutputTokens += int(mergeResp.Usage.OutputTokens)
	usage.CacheCreationTokens += int(mergeResp.Usage.CacheCreationInputTokens)
	usage.CacheReadTokens += int(mergeResp.Usage.CacheReadInputTokens)

	return extractText(mergeResp), usage, nil
}

// tierThreshold returns the per-tier small batch threshold, falling back to
// the config value or the tier-specific default constant.
func tierThreshold(tier int, cfgThreshold int) int {
	if cfgThreshold > 0 {
		return cfgThreshold
	}
	switch tier {
	case 1:
		return smallBatchThresholdT1
	case 2:
		return smallBatchThresholdT2
	case 3:
		return smallBatchThresholdT3
	default:
		return smallBatchThresholdT1
	}
}

// executeBatch sends items via batch API (or direct for small counts) and
// parses the extraction answers. Uses per-tier thresholds to determine when
// to skip the Batch API and use direct calls instead.
func executeBatch(ctx context.Context, items []anthropic.BatchRequestItem, routed []model.RoutedQuestion, tier int, aiClient anthropic.Client, aiCfg config.AnthropicConfig) ([]model.ExtractionAnswer, *model.TokenUsage, error) {
	usage := &model.TokenUsage{}
	var answers []model.ExtractionAnswer

	threshold := tierThreshold(tier, aiCfg.SmallBatchThreshold)
	if aiCfg.NoBatch || len(items) <= threshold {
		// Concurrent direct execution with retry + exponential backoff.
		type indexedAnswer struct {
			index   int
			answers []model.ExtractionAnswer
			usage   anthropic.TokenUsage
		}

		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(maxDirectConcurrency)

		var mu sync.Mutex
		var results []indexedAnswer

		for i, item := range items {
			g.Go(func() error {
				var resp *anthropic.MessageResponse
				var lastErr error
				backoff := 500 * time.Millisecond

				for attempt := 0; attempt < directRetryAttempts; attempt++ {
					resp, lastErr = aiClient.CreateMessage(gCtx, item.Params)
					if lastErr == nil {
						break
					}
					if attempt < directRetryAttempts-1 {
						zap.L().Warn("extract: direct message failed, retrying",
							zap.Int("tier", tier),
							zap.String("question", routed[i].Question.ID),
							zap.Int("attempt", attempt+1),
							zap.Error(lastErr),
						)
						timer := time.NewTimer(backoff)
						select {
						case <-gCtx.Done():
							timer.Stop()
							return nil
						case <-timer.C:
						}
						backoff *= 2
					}
				}
				if lastErr != nil {
					zap.L().Warn("extract: direct message failed after retries",
						zap.Int("tier", tier),
						zap.String("question", routed[i].Question.ID),
						zap.Error(lastErr),
					)
					return nil // Don't fail the group on individual errors.
				}

				parsed := parseExtractionAnswer(extractText(resp), routed[i].Question, tier)

				mu.Lock()
				results = append(results, indexedAnswer{
					index:   i,
					answers: parsed,
					usage:   resp.Usage,
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
			answers = append(answers, r.answers...)
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

		parsed := parseExtractionAnswer(extractText(resp), rq.Question, tier)
		answers = append(answers, parsed...)
	}

	return answers, usage, nil
}

// maxTokensForQuestion returns an appropriate MaxTokens value based on the
// number of target fields. Multi-field questions need more output tokens.
func maxTokensForQuestion(q model.Question) int64 {
	fieldKeys := splitFieldKeys(q.FieldKey)
	if len(fieldKeys) <= 1 {
		return 512
	}
	tokens := len(fieldKeys) * 100
	if tokens < 512 {
		tokens = 512
	}
	if tokens > 4096 {
		tokens = 4096
	}
	return int64(tokens)
}

// parseExtractionAnswer parses the LLM response text for a question.
// For multi-field questions (FieldKey contains commas), it splits the JSON
// response into one ExtractionAnswer per target field. For single-field
// questions using the legacy {"value":..., "confidence":...} format, it returns
// a single-element slice.
func parseExtractionAnswer(text string, q model.Question, tier int) []model.ExtractionAnswer {
	cleaned := cleanJSON(text)

	// Try to parse as a generic JSON object.
	var rawMap map[string]any
	if err := json.Unmarshal([]byte(cleaned), &rawMap); err != nil {
		zap.L().Warn("extract: failed to parse answer JSON",
			zap.String("question", q.ID),
			zap.Error(err),
		)
		return []model.ExtractionAnswer{{
			QuestionID: q.ID,
			FieldKey:   q.FieldKey,
			Tier:       tier,
			Confidence: 0.0,
		}}
	}

	fieldKeys := splitFieldKeys(q.FieldKey)

	// Single-field with legacy {"value":..., "confidence":...} format.
	if len(fieldKeys) == 1 {
		if _, hasValue := rawMap["value"]; hasValue {
			conf, _ := toFloat64(rawMap["confidence"])
			reasoning, _ := rawMap["reasoning"].(string)
			sourceURL, _ := rawMap["source_url"].(string)
			return []model.ExtractionAnswer{{
				QuestionID: q.ID,
				FieldKey:   fieldKeys[0],
				Value:      rawMap["value"],
				Confidence: conf,
				Reasoning:  reasoning,
				SourceURL:  sourceURL,
				Tier:       tier,
			}}
		}
	}

	// Multi-field: extract global metadata, then one answer per field key.
	globalConf, _ := toFloat64(rawMap["confidence"])
	globalReasoning, _ := rawMap["reasoning"].(string)
	globalSourceURL, _ := rawMap["source_url"].(string)

	var answers []model.ExtractionAnswer
	for _, fk := range fieldKeys {
		fk = strings.TrimSpace(fk)
		val, found := rawMap[fk]
		if !found {
			// Emit null answer instead of skipping — marks the field as
			// attempted but not found. Downstream escalation can decide
			// whether to re-try at a higher tier.
			answers = append(answers, model.ExtractionAnswer{
				QuestionID: q.ID,
				FieldKey:   fk,
				Value:      nil,
				Confidence: globalConf * 0.5, // Halve confidence for missing fields.
				Tier:       tier,
			})
			continue
		}
		answers = append(answers, model.ExtractionAnswer{
			QuestionID: q.ID,
			FieldKey:   fk,
			Value:      val,
			Confidence: globalConf,
			Reasoning:  globalReasoning,
			SourceURL:  globalSourceURL,
			Tier:       tier,
		})
	}

	return answers
}

// splitFieldKeys splits a comma-separated field key string, trimming whitespace.
func splitFieldKeys(fieldKey string) []string {
	parts := strings.Split(fieldKey, ",")
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

// toFloat64 attempts to convert an any value to float64.
func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
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

// truncateByRelevance performs keyword-aware content truncation. Instead of
// blindly cutting at a character limit, it splits content into sections (by
// headers or double-newlines), scores each section by keyword overlap with the
// question text, and keeps the highest-scoring sections within the char limit.
// Falls back to a hard truncation if the content has no meaningful sections.
func truncateByRelevance(content, questionText string, limit int) string {
	if len(content) <= limit {
		return content
	}

	// Extract keywords from the question (words of 3+ chars, lowercased).
	keywords := extractKeywords(questionText)
	if len(keywords) == 0 {
		return content[:limit]
	}

	// Split content into sections by markdown headers or double-newlines.
	sections := splitSections(content)
	if len(sections) <= 1 {
		return content[:limit]
	}

	// Score each section by keyword overlap.
	type scoredSection struct {
		idx   int
		text  string
		score int
	}
	scored := make([]scoredSection, len(sections))
	for i, sec := range sections {
		lower := strings.ToLower(sec)
		score := 0
		for _, kw := range keywords {
			score += strings.Count(lower, kw)
		}
		scored[i] = scoredSection{idx: i, text: sec, score: score}
	}

	// Sort by score descending (insertion sort; section count is small).
	for i := 1; i < len(scored); i++ {
		for j := i; j > 0 && scored[j].score > scored[j-1].score; j-- {
			scored[j], scored[j-1] = scored[j-1], scored[j]
		}
	}

	// Greedily pick highest-scoring sections within the budget.
	selected := make(map[int]bool)
	totalLen := 0
	for _, s := range scored {
		if totalLen+len(s.text) > limit {
			continue
		}
		selected[s.idx] = true
		totalLen += len(s.text)
	}

	// If nothing was selected (all sections too large), fall back.
	if len(selected) == 0 {
		return content[:limit]
	}

	// Reassemble selected sections in their original order.
	var result strings.Builder
	for i, sec := range sections {
		if selected[i] {
			if result.Len() > 0 {
				result.WriteString("\n\n")
			}
			result.WriteString(sec)
		}
	}
	return result.String()
}

// extractKeywords returns lowercase words of 3+ characters from text,
// excluding common stop words.
func extractKeywords(text string) []string {
	stopWords := map[string]bool{
		"the": true, "and": true, "for": true, "are": true, "was": true,
		"were": true, "been": true, "have": true, "has": true, "had": true,
		"this": true, "that": true, "with": true, "from": true, "what": true,
		"how": true, "does": true, "which": true, "where": true, "when": true,
		"who": true, "why": true, "can": true, "will": true, "not": true,
	}

	words := strings.Fields(strings.ToLower(text))
	var keywords []string
	seen := make(map[string]bool)
	for _, w := range words {
		// Strip punctuation.
		w = strings.Trim(w, "?.,!;:'\"()[]{}") //nolint:gocritic
		if len(w) < 3 || stopWords[w] || seen[w] {
			continue
		}
		seen[w] = true
		keywords = append(keywords, w)
	}
	return keywords
}

// splitSections splits markdown content into sections by headers (lines
// starting with #) or double-newline paragraph breaks.
func splitSections(content string) []string {
	var sections []string
	var current strings.Builder

	lines := strings.Split(content, "\n")
	for _, line := range lines {
		// Header line or paragraph break starts a new section.
		if strings.HasPrefix(line, "#") || (line == "" && current.Len() > 0) {
			if current.Len() > 0 {
				sections = append(sections, strings.TrimSpace(current.String()))
				current.Reset()
			}
		}
		current.WriteString(line)
		current.WriteString("\n")
	}
	if current.Len() > 0 {
		if s := strings.TrimSpace(current.String()); s != "" {
			sections = append(sections, s)
		}
	}

	// Filter out empty sections that arise from consecutive paragraph breaks.
	filtered := sections[:0]
	for _, s := range sections {
		if s != "" {
			filtered = append(filtered, s)
		}
	}
	return filtered
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
