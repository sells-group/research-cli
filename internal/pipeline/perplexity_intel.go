package pipeline

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/perplexity"
)

const perplexityIntelPrompt = `Provide factual business information about "%s" (%s). Include:
- Year founded
- Approximate number of employees
- Estimated annual revenue (or revenue range)
- Headquarters city and state
- Number of office/branch locations
- Industries and end markets served
- Core services or products offered
- Key differentiators or specializations
- Business model (services, manufacturing, distribution, etc.)

Provide only verified facts. If a data point is unavailable, omit it rather than guessing.`

// PerplexityIntelPhase runs a targeted Perplexity query to gather key company
// facts. Acts as a safety net for Cloudflare-blocked sites, thin websites, and
// companies with minimal web presence. Returns a synthetic page that flows
// through the existing classify -> route -> extract pipeline.
func PerplexityIntelPhase(ctx context.Context, company model.Company, pplxClient perplexity.Client) (*model.CrawledPage, *model.TokenUsage, error) {
	log := zap.L().With(zap.String("company", company.Name), zap.String("phase", "1e_pplx_intel"))
	usage := &model.TokenUsage{}

	if pplxClient == nil {
		log.Debug("perplexity_intel: skipped, no client")
		return nil, usage, nil
	}

	prompt := fmt.Sprintf(perplexityIntelPrompt, company.Name, company.URL)
	temp := 0.2
	resp, err := pplxClient.ChatCompletion(ctx, perplexity.ChatCompletionRequest{
		Messages:    []perplexity.Message{{Role: "user", Content: prompt}},
		Temperature: &temp,
	})
	if err != nil {
		return nil, usage, eris.Wrap(err, "perplexity_intel: query failed")
	}

	if len(resp.Choices) == 0 || strings.TrimSpace(resp.Choices[0].Message.Content) == "" {
		log.Debug("perplexity_intel: empty response")
		return nil, usage, nil
	}

	usage.InputTokens += resp.Usage.PromptTokens
	usage.OutputTokens += resp.Usage.CompletionTokens

	domain := extractIntelDomain(company.URL)

	page := &model.CrawledPage{
		URL:        fmt.Sprintf("perplexity://intel/%s", domain),
		Title:      "[perplexity_intel] " + company.Name,
		Markdown:   resp.Choices[0].Message.Content,
		StatusCode: 200,
	}

	log.Info("perplexity_intel: page created",
		zap.Int("input_tokens", resp.Usage.PromptTokens),
		zap.Int("output_tokens", resp.Usage.CompletionTokens),
		zap.Int("markdown_len", len(page.Markdown)),
	)

	return page, usage, nil
}

// yearPattern matches 4-digit years preceded by founding-related keywords.
var yearPattern = regexp.MustCompile(`(?i)(?:founded|established|started|incorporated|formed|opened)\s+(?:in\s+)?(\d{4})`)

// empPattern matches employee count phrases in natural language.
var empPattern = regexp.MustCompile(`(?i)(?:approximately|about|around|roughly|estimated|has|with|employs|employ)\s+(\d[\d,]*)\s+(?:employees|workers|staff|people|team members)`)

// empRangePattern matches employee range phrases like "50-100 employees".
var empRangePattern = regexp.MustCompile(`(?i)(\d[\d,]*)\s*[-–to]+\s*(\d[\d,]*)\s+(?:employees|workers|staff|people)`)

// ParsePerplexityIntel extracts year_founded and employee_count from the
// Perplexity intel page markdown using regex patterns. Returns extraction
// answers for fields that were found. Only injects values for fields that
// are not already populated in existingAnswers.
func ParsePerplexityIntel(markdown string, existingAnswers []model.ExtractionAnswer) []model.ExtractionAnswer {
	if markdown == "" {
		return nil
	}

	// Check which fields already have values.
	hasYear := false
	hasEmployees := false
	for _, a := range existingAnswers {
		switch a.FieldKey {
		case "year_founded", "year_established":
			if a.Value != nil {
				if n, ok := toNumber(a.Value); ok && n > 0 {
					hasYear = true
				}
				if s, ok := a.Value.(string); ok && s != "" {
					hasYear = true
				}
			}
		case "employee_count", "employees", "employee_estimate":
			if a.Value != nil {
				if n, ok := toNumber(a.Value); ok && n > 0 {
					hasEmployees = true
				}
			}
		}
	}

	var results []model.ExtractionAnswer

	// Extract year_founded.
	if !hasYear {
		if m := yearPattern.FindStringSubmatch(markdown); len(m) >= 2 {
			if year, err := strconv.Atoi(m[1]); err == nil && year >= 1800 && year <= 2030 {
				results = append(results, model.ExtractionAnswer{
					FieldKey:   "year_founded",
					Value:      year,
					Confidence: 0.60,
					Source:     "perplexity_intel",
					Tier:       0,
					Reasoning:  fmt.Sprintf("Perplexity intel: %s", m[0]),
				})
			}
		}
	}

	// Extract employee_count.
	if !hasEmployees {
		// Try range pattern first (e.g. "50-100 employees").
		if m := empRangePattern.FindStringSubmatch(markdown); len(m) >= 3 {
			lo := parseCommaInt(m[1])
			hi := parseCommaInt(m[2])
			if lo > 0 && hi > 0 {
				midpoint := (lo + hi) / 2
				results = append(results, model.ExtractionAnswer{
					FieldKey:   "employee_count",
					Value:      midpoint,
					Confidence: 0.55,
					Source:     "perplexity_intel",
					Tier:       0,
					Reasoning:  fmt.Sprintf("Perplexity intel: %s → midpoint %d", m[0], midpoint),
				})
			}
		} else if m := empPattern.FindStringSubmatch(markdown); len(m) >= 2 {
			if count := parseCommaInt(m[1]); count > 0 {
				results = append(results, model.ExtractionAnswer{
					FieldKey:   "employee_count",
					Value:      count,
					Confidence: 0.55,
					Source:     "perplexity_intel",
					Tier:       0,
					Reasoning:  fmt.Sprintf("Perplexity intel: %s", m[0]),
				})
			}
		}
	}

	if len(results) > 0 {
		zap.L().Info("perplexity_intel: parsed fields",
			zap.Int("fields_found", len(results)),
		)
	}

	return results
}

// parseCommaInt parses an integer that may contain commas (e.g. "1,500").
func parseCommaInt(s string) int {
	s = strings.ReplaceAll(s, ",", "")
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}

// extractIntelDomain returns the bare domain from a URL for synthetic page keying.
func extractIntelDomain(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return rawURL
	}
	return strings.TrimPrefix(u.Host, "www.")
}
