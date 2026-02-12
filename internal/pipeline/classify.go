package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/anthropic"
)

const classifyPrompt = `Classify the following web page into exactly one of these categories:
homepage, about, services, products, pricing, careers, contact, team, blog, news, faq, testimonials, case_studies, partners, legal, investors, other

URL: %s
Title: %s

Page content (first 2000 chars):
%s

Respond with a valid JSON object:
{"page_type": "<category>", "confidence": <0.0-1.0>}`

// ClassifyPhase implements Phase 2: classify crawled pages using Haiku.
func ClassifyPhase(ctx context.Context, pages []model.CrawledPage, aiClient anthropic.Client, aiCfg config.AnthropicConfig) (model.PageIndex, *model.TokenUsage, error) {
	index := make(model.PageIndex)
	totalUsage := &model.TokenUsage{}

	if len(pages) == 0 {
		return index, totalUsage, nil
	}

	// Build batch request items for classification.
	var batchItems []anthropic.BatchRequestItem
	for i, page := range pages {
		content := page.Markdown
		if len(content) > 2000 {
			content = content[:2000]
		}

		prompt := fmt.Sprintf(classifyPrompt, page.URL, page.Title, content)
		batchItems = append(batchItems, anthropic.BatchRequestItem{
			CustomID: fmt.Sprintf("classify-%d", i),
			Params: anthropic.MessageRequest{
				Model:     aiCfg.HaikuModel,
				MaxTokens: 128,
				Messages: []anthropic.Message{
					{Role: "user", Content: prompt},
				},
			},
		})
	}

	// If only a few pages, use direct messages instead of batch.
	if len(batchItems) <= 3 {
		return classifyDirect(ctx, pages, batchItems, aiClient, totalUsage)
	}

	// Use batch API for larger sets.
	return classifyBatch(ctx, pages, batchItems, aiClient, totalUsage)
}

func classifyDirect(ctx context.Context, pages []model.CrawledPage, items []anthropic.BatchRequestItem, aiClient anthropic.Client, usage *model.TokenUsage) (model.PageIndex, *model.TokenUsage, error) {
	index := make(model.PageIndex)

	for i, item := range items {
		resp, err := aiClient.CreateMessage(ctx, item.Params)
		if err != nil {
			zap.L().Warn("classify: failed to classify page",
				zap.String("url", pages[i].URL),
				zap.Error(err),
			)
			// Default to "other" on error.
			cp := model.ClassifiedPage{
				CrawledPage: pages[i],
				Classification: model.PageClassification{
					PageType:   model.PageTypeOther,
					Confidence: 0.0,
				},
			}
			index[model.PageTypeOther] = append(index[model.PageTypeOther], cp)
			continue
		}

		usage.InputTokens += int(resp.Usage.InputTokens)
		usage.OutputTokens += int(resp.Usage.OutputTokens)

		classification := parseClassification(extractText(resp))
		cp := model.ClassifiedPage{
			CrawledPage:    pages[i],
			Classification: classification,
		}
		index[classification.PageType] = append(index[classification.PageType], cp)
	}

	return index, usage, nil
}

func classifyBatch(ctx context.Context, pages []model.CrawledPage, items []anthropic.BatchRequestItem, aiClient anthropic.Client, usage *model.TokenUsage) (model.PageIndex, *model.TokenUsage, error) {
	index := make(model.PageIndex)

	batch, err := aiClient.CreateBatch(ctx, anthropic.BatchRequest{
		Requests: items,
	})
	if err != nil {
		return nil, usage, eris.Wrap(err, "classify: create batch")
	}

	// Poll until done.
	batch, err = anthropic.PollBatch(ctx, aiClient, batch.ID)
	if err != nil {
		return nil, usage, eris.Wrap(err, "classify: poll batch")
	}

	// Collect results.
	iter, err := aiClient.GetBatchResults(ctx, batch.ID)
	if err != nil {
		return nil, usage, eris.Wrap(err, "classify: get batch results")
	}

	results, err := anthropic.CollectBatchResults(iter)
	if err != nil {
		return nil, usage, eris.Wrap(err, "classify: collect batch results")
	}

	// Map results back to pages.
	for i, page := range pages {
		customID := fmt.Sprintf("classify-%d", i)
		resp, ok := results[customID]

		var classification model.PageClassification
		if ok && resp != nil {
			usage.InputTokens += int(resp.Usage.InputTokens)
			usage.OutputTokens += int(resp.Usage.OutputTokens)
			classification = parseClassification(extractText(resp))
		} else {
			classification = model.PageClassification{
				PageType:   model.PageTypeOther,
				Confidence: 0.0,
			}
		}

		cp := model.ClassifiedPage{
			CrawledPage:    page,
			Classification: classification,
		}
		index[classification.PageType] = append(index[classification.PageType], cp)
	}

	return index, usage, nil
}

func parseClassification(text string) model.PageClassification {
	text = cleanJSON(text)

	var result struct {
		PageType   string  `json:"page_type"`
		Confidence float64 `json:"confidence"`
	}

	if err := json.Unmarshal([]byte(text), &result); err != nil {
		return model.PageClassification{
			PageType:   model.PageTypeOther,
			Confidence: 0.0,
		}
	}

	pt := model.PageType(strings.ToLower(result.PageType))

	// Validate page type.
	valid := false
	for _, t := range model.AllPageTypes() {
		if t == pt {
			valid = true
			break
		}
	}
	if !valid {
		pt = model.PageTypeOther
	}

	return model.PageClassification{
		PageType:   pt,
		Confidence: result.Confidence,
	}
}
