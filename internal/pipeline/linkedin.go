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
	jinapkg "github.com/sells-group/research-cli/pkg/jina"
	"github.com/sells-group/research-cli/pkg/perplexity"
)

// LinkedInData holds structured LinkedIn profile information.
type LinkedInData struct {
	CompanyName   string `json:"company_name"`
	Description   string `json:"description"`
	Industry      string `json:"industry"`
	EmployeeCount string `json:"employee_count"`
	Headquarters  string `json:"headquarters"`
	Founded       string `json:"founded"`
	Specialties   string `json:"specialties"`
	Website       string `json:"website"`
	LinkedInURL   string `json:"linkedin_url"`
	CompanyType   string `json:"company_type"`
}

const perplexityPrompt = `Find the LinkedIn company profile for "%s" (%s).
Return all available company information including: company name, description, industry,
employee count, headquarters location, founded year, specialties, website, LinkedIn URL,
and company type. Return the raw information as text.`

const haikuLinkedInPrompt = `Extract structured company information from the following LinkedIn research data.
Return a valid JSON object with these fields:
- company_name: string
- description: string
- industry: string
- employee_count: string (e.g. "51-200" or "1000+")
- headquarters: string
- founded: string (year)
- specialties: string (comma-separated)
- website: string
- linkedin_url: string
- company_type: string (e.g. "Privately Held", "Public Company")

If a field cannot be determined, use an empty string.

Research data:
%s`

// LinkedInPhase implements Phase 1C: Jina-first LinkedIn lookup with Perplexity fallback.
func LinkedInPhase(ctx context.Context, company model.Company, jinaClient jinapkg.Client, pplxClient perplexity.Client, aiClient anthropic.Client, aiCfg config.AnthropicConfig) (*LinkedInData, *model.TokenUsage, error) {
	log := zap.L().With(zap.String("company", company.Name), zap.String("phase", "1c_linkedin"))
	usage := &model.TokenUsage{}

	// Step 1: Try Jina Reader for LinkedIn page.
	linkedInURL := buildLinkedInURL(company.Name)
	var rawData string

	if jinaClient != nil {
		page, jinaErr := fetchViaJina(ctx, linkedInURL, jinaClient)
		if jinaErr != nil {
			log.Debug("linkedin: jina failed, falling back to perplexity", zap.Error(jinaErr))
		} else if page != nil {
			rawData = page.Markdown
		}
	}

	// Check if Jina response is a login wall.
	if rawData != "" && isLinkedInLoginWall(rawData) {
		log.Debug("linkedin: jina returned login wall, falling back to perplexity")
		rawData = ""
	}

	// Step 2: Fallback to Perplexity if Jina failed or returned empty/login wall.
	if rawData == "" {
		pplxResp, err := pplxClient.ChatCompletion(ctx, perplexity.ChatCompletionRequest{
			Messages: []perplexity.Message{
				{Role: "user", Content: fmt.Sprintf(perplexityPrompt, company.Name, company.URL)},
			},
		})
		if err != nil {
			return nil, usage, eris.Wrap(err, "linkedin: perplexity search")
		}

		if len(pplxResp.Choices) == 0 {
			return nil, usage, eris.New("linkedin: no perplexity results")
		}

		rawData = pplxResp.Choices[0].Message.Content
		usage.InputTokens += pplxResp.Usage.PromptTokens
		usage.OutputTokens += pplxResp.Usage.CompletionTokens
	}

	if strings.TrimSpace(rawData) == "" {
		return nil, usage, eris.New("linkedin: empty response from both jina and perplexity")
	}

	// Step 3: Haiku JSON extraction.
	aiResp, err := aiClient.CreateMessage(ctx, anthropic.MessageRequest{
		Model:     aiCfg.HaikuModel,
		MaxTokens: 1024,
		Messages: []anthropic.Message{
			{Role: "user", Content: fmt.Sprintf(haikuLinkedInPrompt, rawData)},
		},
	})
	if err != nil {
		return nil, usage, eris.Wrap(err, "linkedin: haiku extraction")
	}

	usage.InputTokens += int(aiResp.Usage.InputTokens)
	usage.OutputTokens += int(aiResp.Usage.OutputTokens)

	// Parse JSON from Haiku response.
	text := extractText(aiResp)
	var data LinkedInData
	if err := json.Unmarshal([]byte(cleanJSON(text)), &data); err != nil {
		log.Warn("linkedin: failed to parse haiku json", zap.Error(err))
		return nil, usage, eris.Wrap(err, "linkedin: parse haiku json")
	}

	// Fill in LinkedIn URL if not extracted.
	if data.LinkedInURL == "" {
		data.LinkedInURL = linkedInURL
	}

	return &data, usage, nil
}

// buildLinkedInURL constructs a LinkedIn company page URL from the company name.
func buildLinkedInURL(name string) string {
	slug := strings.ToLower(strings.TrimSpace(name))
	slug = strings.ReplaceAll(slug, " ", "-")
	slug = strings.ReplaceAll(slug, "&", "and")
	// Remove common entity suffixes for cleaner slug.
	for _, suffix := range []string{"-llc", "-inc", "-corp", "-ltd", "-co"} {
		slug = strings.TrimSuffix(slug, suffix)
	}
	slug = strings.TrimRight(slug, "-")
	return fmt.Sprintf("https://www.linkedin.com/company/%s", slug)
}

// isLinkedInLoginWall detects if Jina returned a LinkedIn login wall instead of content.
func isLinkedInLoginWall(content string) bool {
	if len(content) < 100 {
		return true
	}
	lower := strings.ToLower(content)
	loginIndicators := []string{
		"sign in",
		"join now",
		"authwall",
		"login_required",
		"please log in",
		"sign up to view",
	}
	for _, indicator := range loginIndicators {
		if strings.Contains(lower, indicator) {
			return true
		}
	}
	return false
}

// extractText concatenates all text content blocks from a message response.
func extractText(resp *anthropic.MessageResponse) string {
	if resp == nil {
		return ""
	}
	var parts []string
	for _, block := range resp.Content {
		if block.Text != "" {
			parts = append(parts, block.Text)
		}
	}
	return strings.Join(parts, "\n")
}

// cleanJSON attempts to extract a JSON object from text that may contain
// markdown code fences or other wrapping.
func cleanJSON(text string) string {
	text = strings.TrimSpace(text)

	// Strip markdown code fences.
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

	// Find first { and last }.
	start := strings.Index(text, "{")
	end := strings.LastIndex(text, "}")
	if start >= 0 && end > start {
		text = text[start : end+1]
	}

	return strings.TrimSpace(text)
}
