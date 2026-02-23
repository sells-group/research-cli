package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/scrape"
	"github.com/sells-group/research-cli/internal/store"
	"github.com/sells-group/research-cli/pkg/anthropic"
	"github.com/sells-group/research-cli/pkg/perplexity"
)

// LinkedInContact holds structured contact information for a key executive.
type LinkedInContact struct {
	FirstName   string `json:"first_name"`
	LastName    string `json:"last_name"`
	Title       string `json:"title"`
	Email       string `json:"email,omitempty"`
	Phone       string `json:"phone,omitempty"`
	LinkedInURL string `json:"linkedin_url,omitempty"`
}

// LinkedInData holds structured LinkedIn profile information.
type LinkedInData struct {
	CompanyName   string            `json:"company_name"`
	Description   string            `json:"description"`
	Industry      string            `json:"industry"`
	EmployeeCount string            `json:"employee_count"`
	Headquarters  string            `json:"headquarters"`
	Founded       string            `json:"founded"`
	Specialties   string            `json:"specialties"`
	Website       string            `json:"website"`
	LinkedInURL   string            `json:"linkedin_url"`
	CompanyType   string            `json:"company_type"`
	ExecFirstName string            `json:"exec_first_name"`
	ExecLastName  string            `json:"exec_last_name"`
	ExecTitle     string            `json:"exec_title"`
	ExecContacts  []LinkedInContact `json:"exec_contacts,omitempty"`
}

const perplexityPrompt = `Find the LinkedIn company profile for "%s" (%s).
Return all available company information including: company name, description, industry,
employee count, headquarters location, founded year, specialties, website, LinkedIn URL,
company type, and the names and titles of up to 3 key executives (CEO, president, owner,
VP, partner, founder, etc.) with first name, last name, and title for each.
Return the raw information as text.`

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
- exec_first_name: string (first name of primary CEO/owner/president)
- exec_last_name: string (last name of primary CEO/owner/president)
- exec_title: string (title of primary CEO/owner/president, e.g. "President", "CEO & Founder")
- exec_contacts: array of up to 3 objects, each with: first_name, last_name, title, linkedin_url (if found). Include CEO/owner/president as the first entry, then other key executives (VP, partner, director, etc.)

If a field cannot be determined, use an empty string. If no executives are found, use an empty array for exec_contacts.

Research data:
%s`

// linkedInCacheTTL is the default TTL for cached LinkedIn data.
const linkedInCacheTTL = 7 * 24 * time.Hour // 7 days

// extractDomain returns the bare domain from a company URL for cache keying.
func extractDomain(companyURL string) string {
	u, err := url.Parse(companyURL)
	if err != nil || u.Host == "" {
		return companyURL
	}
	return strings.TrimPrefix(u.Host, "www.")
}

// LinkedInPhase implements Phase 1C: chain-first LinkedIn lookup with Perplexity fallback.
// Results are cached by domain with a 7-day TTL to avoid redundant API calls on re-runs.
func LinkedInPhase(ctx context.Context, company model.Company, chain *scrape.Chain, pplxClient perplexity.Client, aiClient anthropic.Client, aiCfg config.AnthropicConfig, st store.Store) (*LinkedInData, *model.TokenUsage, error) {
	if company.Name == "" {
		return nil, &model.TokenUsage{}, nil
	}

	log := zap.L().With(zap.String("company", company.Name), zap.String("phase", "1c_linkedin"))
	usage := &model.TokenUsage{}

	// Check cache first.
	domain := extractDomain(company.URL)
	if st != nil {
		cached, cacheErr := st.GetCachedLinkedIn(ctx, domain)
		if cacheErr != nil {
			log.Debug("linkedin: cache lookup failed", zap.Error(cacheErr))
		}
		if cached != nil {
			var data LinkedInData
			if err := json.Unmarshal(cached, &data); err == nil {
				log.Info("linkedin: using cached result", zap.String("domain", domain))
				return &data, usage, nil
			}
		}
	}

	// Step 1: Try scrape chain for LinkedIn page.
	linkedInURL := buildLinkedInURL(company.Name)
	var rawData string

	if chain != nil {
		result, chainErr := chain.Scrape(ctx, linkedInURL)
		if chainErr != nil {
			log.Debug("linkedin: chain scrape failed, falling back to perplexity", zap.Error(chainErr))
		} else if result != nil {
			rawData = result.Page.Markdown
		}
	}

	// Check if response is a login wall.
	if rawData != "" && isLinkedInLoginWall(rawData) {
		log.Debug("linkedin: scrape returned login wall, falling back to perplexity")
		rawData = ""
	}

	// Step 2: Fallback to Perplexity if Jina failed or returned empty/login wall.
	if rawData == "" {
		pplxTemp := 0.2
		pplxResp, err := pplxClient.ChatCompletion(ctx, perplexity.ChatCompletionRequest{
			Messages: []perplexity.Message{
				{Role: "user", Content: fmt.Sprintf(perplexityPrompt, company.Name, company.URL)},
			},
			Temperature: &pplxTemp,
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
	usage.CacheCreationTokens += int(aiResp.Usage.CacheCreationInputTokens)
	usage.CacheReadTokens += int(aiResp.Usage.CacheReadInputTokens)

	// Parse JSON from Haiku response.
	text := extractText(aiResp)
	var data LinkedInData
	if err := json.Unmarshal([]byte(cleanJSON(text)), &data); err != nil {
		log.Warn("linkedin: failed to parse haiku json", zap.Error(err))
		return nil, usage, eris.Wrap(err, "linkedin: parse haiku json")
	}

	// Auto-populate ExecFirstName/ExecLastName/ExecTitle from ExecContacts[0]
	// for backward compatibility when the old flat fields are empty.
	if len(data.ExecContacts) > 0 {
		if data.ExecFirstName == "" {
			data.ExecFirstName = data.ExecContacts[0].FirstName
		}
		if data.ExecLastName == "" {
			data.ExecLastName = data.ExecContacts[0].LastName
		}
		if data.ExecTitle == "" {
			data.ExecTitle = data.ExecContacts[0].Title
		}
	}

	// Fill in LinkedIn URL if not extracted.
	if data.LinkedInURL == "" {
		data.LinkedInURL = linkedInURL
	}

	// Cache the result.
	if st != nil {
		if cacheData, marshalErr := json.Marshal(&data); marshalErr == nil {
			if cacheErr := st.SetCachedLinkedIn(ctx, domain, cacheData, linkedInCacheTTL); cacheErr != nil {
				log.Debug("linkedin: failed to cache result", zap.Error(cacheErr))
			}
		}
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
