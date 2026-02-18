package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/anthropic"
)

const classifySystemPrompt = `Classify web pages into exactly one of these categories: homepage, about, services, products, pricing, careers, contact, team, blog, news, faq, testimonials, case_studies, partners, legal, investors, other. Respond with a valid JSON object: {"page_type": "<category>", "confidence": <0.0-1.0>}`

const classifyUserPrompt = `URL: %s
Title: %s

Page content (first 2000 chars):
%s`

// externalPrefixToPageType maps title prefixes from scrape.go to page types.
// Pages with these prefixes are auto-classified without an LLM call.
var externalPrefixToPageType = map[string]model.PageType{
	"[bbb] ":         model.PageTypeBBB,
	"[google_maps] ": model.PageTypeGoogleMaps,
	"[sos] ":         model.PageTypeSoS,
	"[linkedin] ":    model.PageTypeLinkedIn,
}

// classifyByPrefix checks if a page has a known external source prefix
// and returns the corresponding page type. Returns ("", false) if no match.
func classifyByPrefix(title string) (model.PageType, bool) {
	lower := strings.ToLower(title)
	for prefix, pt := range externalPrefixToPageType {
		if strings.HasPrefix(lower, prefix) {
			return pt, true
		}
	}
	return "", false
}

// urlPathPatterns maps URL path segments to page types. The path is cleaned
// (lowercase, stripped of leading/trailing slashes) before matching.
var urlPathPatterns = map[string]model.PageType{
	"about":              model.PageTypeAbout,
	"about-us":           model.PageTypeAbout,
	"about_us":           model.PageTypeAbout,
	"aboutus":            model.PageTypeAbout,
	"who-we-are":         model.PageTypeAbout,
	"our-story":          model.PageTypeAbout,
	"contact":            model.PageTypeContact,
	"contact-us":         model.PageTypeContact,
	"contact_us":         model.PageTypeContact,
	"contactus":          model.PageTypeContact,
	"services":           model.PageTypeServices,
	"our-services":       model.PageTypeServices,
	"what-we-do":         model.PageTypeServices,
	"products":           model.PageTypeProducts,
	"pricing":            model.PageTypePricing,
	"careers":            model.PageTypeCareers,
	"jobs":               model.PageTypeCareers,
	"team":               model.PageTypeTeam,
	"our-team":           model.PageTypeTeam,
	"leadership":         model.PageTypeTeam,
	"staff":              model.PageTypeTeam,
	"faq":                model.PageTypeFAQ,
	"faqs":               model.PageTypeFAQ,
	"blog":               model.PageTypeBlog,
	"news":               model.PageTypeNews,
	"testimonials":       model.PageTypeTestimonials,
	"reviews":            model.PageTypeTestimonials,
	"case-studies":       model.PageTypeCaseStudies,
	"case_studies":       model.PageTypeCaseStudies,
	"partners":           model.PageTypePartners,
	"investors":          model.PageTypeInvestors,
	"investor-relations": model.PageTypeInvestors,
	"legal":              model.PageTypeLegal,
	"privacy":            model.PageTypeLegal,
	"privacy-policy":     model.PageTypeLegal,
	"terms":              model.PageTypeLegal,
	"terms-of-service":   model.PageTypeLegal,
}

// classifyByURL checks if a page URL path matches a known page type pattern.
// Returns ("", false) if no match. Only matches the first path segment to avoid
// false positives on deep paths like /blog/about-our-team.
func classifyByURL(rawURL string) (model.PageType, bool) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", false
	}
	path := strings.Trim(u.Path, "/")
	if path == "" {
		return model.PageTypeHomepage, true
	}
	// Use the first path segment only.
	if idx := strings.Index(path, "/"); idx > 0 {
		path = path[:idx]
	}
	path = strings.ToLower(path)
	if pt, ok := urlPathPatterns[path]; ok {
		return pt, true
	}
	return "", false
}

// ClassifyPhase implements Phase 2: classify crawled pages using Haiku.
// External pages (BBB, Google Maps, SoS, LinkedIn) are auto-classified
// by title prefix without an LLM call.
func ClassifyPhase(ctx context.Context, pages []model.CrawledPage, aiClient anthropic.Client, aiCfg config.AnthropicConfig) (model.PageIndex, *model.TokenUsage, error) {
	index := make(model.PageIndex)
	totalUsage := &model.TokenUsage{}

	if len(pages) == 0 {
		return index, totalUsage, nil
	}

	// Separate pages that can be auto-classified from those needing LLM.
	var llmPages []model.CrawledPage
	for _, page := range pages {
		// 1. External source prefix (BBB, Google Maps, SoS, LinkedIn).
		if pt, ok := classifyByPrefix(page.Title); ok {
			cp := model.ClassifiedPage{
				CrawledPage: page,
				Classification: model.PageClassification{
					PageType:   pt,
					Confidence: 1.0,
				},
			}
			index[pt] = append(index[pt], cp)
			zap.L().Debug("classify: auto-classified external page",
				zap.String("url", page.URL),
				zap.String("page_type", string(pt)),
			)
			continue
		}

		// 2. URL path pattern (e.g., /about → about, /contact → contact).
		if pt, ok := classifyByURL(page.URL); ok {
			cp := model.ClassifiedPage{
				CrawledPage: page,
				Classification: model.PageClassification{
					PageType:   pt,
					Confidence: 0.9,
				},
			}
			index[pt] = append(index[pt], cp)
			zap.L().Debug("classify: auto-classified by URL pattern",
				zap.String("url", page.URL),
				zap.String("page_type", string(pt)),
			)
			continue
		}

		// 3. Tiny page filter: pages with <100 chars content (error pages,
		// redirects, empty stubs) waste classification tokens and add noise.
		if len(strings.TrimSpace(page.Markdown)) < 100 {
			cp := model.ClassifiedPage{
				CrawledPage: page,
				Classification: model.PageClassification{
					PageType:   model.PageTypeOther,
					Confidence: 1.0,
				},
			}
			index[model.PageTypeOther] = append(index[model.PageTypeOther], cp)
			zap.L().Debug("classify: auto-classified tiny page as other",
				zap.String("url", page.URL),
				zap.Int("content_len", len(page.Markdown)),
			)
			continue
		}

		llmPages = append(llmPages, page)
	}

	if len(llmPages) == 0 {
		return index, totalUsage, nil
	}

	// Deduplicate pages with identical content (different URLs, same body).
	// Keeps the first URL encountered; duplicates inherit the winner's classification.
	llmPages, dupes := deduplicatePages(llmPages)

	// Build batch request items for LLM classification.
	systemBlocks := anthropic.BuildCachedSystemBlocks(classifySystemPrompt)
	var batchItems []anthropic.BatchRequestItem
	for i, page := range llmPages {
		content := page.Markdown
		if len(content) > 2000 {
			content = content[:2000]
		}

		prompt := fmt.Sprintf(classifyUserPrompt, page.URL, page.Title, content)
		batchItems = append(batchItems, anthropic.BatchRequestItem{
			CustomID: fmt.Sprintf("classify-%d", i),
			Params: anthropic.MessageRequest{
				Model:     aiCfg.HaikuModel,
				MaxTokens: 128,
				System:    systemBlocks,
				Messages: []anthropic.Message{
					{Role: "user", Content: prompt},
				},
			},
		})
	}

	// If no-batch mode or only a few pages, use direct messages instead of batch.
	var llmIndex model.PageIndex
	var llmUsage *model.TokenUsage
	var err error
	threshold := aiCfg.SmallBatchThreshold
	if threshold <= 0 {
		threshold = 8
	}
	if aiCfg.NoBatch || len(batchItems) <= threshold {
		llmIndex, llmUsage, err = classifyDirect(ctx, llmPages, batchItems, aiClient, totalUsage)
	} else {
		llmIndex, llmUsage, err = classifyBatch(ctx, llmPages, batchItems, aiClient, totalUsage)
	}
	if err != nil {
		return nil, totalUsage, err
	}

	// Merge LLM-classified pages into the index.
	for pt, pages := range llmIndex {
		index[pt] = append(index[pt], pages...)
	}
	if llmUsage != nil {
		totalUsage = llmUsage
	}

	// Re-attach deduplicated pages: give them the same classification as
	// their content twin (looked up by URL in the merged index).
	if len(dupes) > 0 {
		urlToClassification := make(map[string]model.PageClassification)
		for _, classified := range llmIndex {
			for _, cp := range classified {
				urlToClassification[cp.URL] = cp.Classification
			}
		}
		for originalURL, dupPages := range dupes {
			if cls, ok := urlToClassification[originalURL]; ok {
				for _, dp := range dupPages {
					cp := model.ClassifiedPage{
						CrawledPage:    dp,
						Classification: cls,
					}
					index[cls.PageType] = append(index[cls.PageType], cp)
				}
			}
		}
	}

	return index, totalUsage, nil
}

func classifyDirect(ctx context.Context, pages []model.CrawledPage, items []anthropic.BatchRequestItem, aiClient anthropic.Client, usage *model.TokenUsage) (model.PageIndex, *model.TokenUsage, error) {
	index := make(model.PageIndex)

	type classifyResult struct {
		page           model.CrawledPage
		classification model.PageClassification
		usage          anthropic.TokenUsage
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(maxDirectConcurrency)

	var mu sync.Mutex
	var results []classifyResult

	for i, item := range items {
		g.Go(func() error {
			resp, err := aiClient.CreateMessage(gCtx, item.Params)
			if err != nil {
				zap.L().Warn("classify: failed to classify page",
					zap.String("url", pages[i].URL),
					zap.Error(err),
				)
				mu.Lock()
				results = append(results, classifyResult{
					page: pages[i],
					classification: model.PageClassification{
						PageType:   model.PageTypeOther,
						Confidence: 0.0,
					},
				})
				mu.Unlock()
				return nil
			}

			classification := parseClassification(extractText(resp))

			mu.Lock()
			results = append(results, classifyResult{
				page:           pages[i],
				classification: classification,
				usage:          resp.Usage,
			})
			mu.Unlock()
			return nil
		})
	}

	_ = g.Wait()

	for _, r := range results {
		usage.InputTokens += int(r.usage.InputTokens)
		usage.OutputTokens += int(r.usage.OutputTokens)
		usage.CacheCreationTokens += int(r.usage.CacheCreationInputTokens)
		usage.CacheReadTokens += int(r.usage.CacheReadInputTokens)

		cp := model.ClassifiedPage{
			CrawledPage:    r.page,
			Classification: r.classification,
		}
		index[r.classification.PageType] = append(index[r.classification.PageType], cp)
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
			usage.CacheCreationTokens += int(resp.Usage.CacheCreationInputTokens)
			usage.CacheReadTokens += int(resp.Usage.CacheReadInputTokens)
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

// deduplicatePages removes pages with identical markdown content (by hash),
// keeping the first occurrence. Returns the unique pages and a map from the
// kept page's URL to the list of duplicate CrawledPages (which should inherit
// the same classification later).
func deduplicatePages(pages []model.CrawledPage) ([]model.CrawledPage, map[string][]model.CrawledPage) {
	seen := make(map[string]string) // hash → first URL
	dupes := make(map[string][]model.CrawledPage)
	var unique []model.CrawledPage

	for _, p := range pages {
		h := contentHash(p.Markdown)
		if firstURL, ok := seen[h]; ok {
			dupes[firstURL] = append(dupes[firstURL], p)
			zap.L().Debug("classify: deduplicated page",
				zap.String("url", p.URL),
				zap.String("duplicate_of", firstURL),
			)
			continue
		}
		seen[h] = p.URL
		unique = append(unique, p)
	}

	if removed := len(pages) - len(unique); removed > 0 {
		zap.L().Info("classify: deduplicated pages before classification",
			zap.Int("original", len(pages)),
			zap.Int("unique", len(unique)),
			zap.Int("duplicates_removed", removed),
		)
	}

	return unique, dupes
}
