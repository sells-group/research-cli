package peextract

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Model constants.
const (
	ModelHaiku  = "claude-haiku-4-5-20251001"
	ModelSonnet = "claude-sonnet-4-5-20250929"
)

// ModelForTier returns the Claude model for a given tier.
func ModelForTier(tier int) string {
	switch tier {
	case 1:
		return ModelHaiku
	case 2:
		return ModelSonnet
	default:
		return ModelHaiku
	}
}

// MaxTokensForTier returns the max output tokens for a tier.
func MaxTokensForTier(tier int) int64 {
	switch tier {
	case 1:
		return 1024 // JSON array answers (portfolio, team, funds) need room to complete
	case 2:
		return 1024
	default:
		return 1024
	}
}

// systemPrompt is the shared system instruction for PE extraction.
const systemPrompt = `You are an expert M&A analyst specializing in private equity firms, aggregators, and holding companies that acquire investment advisory firms (RIAs).

You are analyzing a PE firm's website to extract structured intelligence about their operations, team, portfolio, and investment strategy.

Rules:
- Answer ONLY based on information present in the provided website pages
- Return valid JSON for every response
- Use null for the value if the information is not found in the pages
- Confidence should be 0.0-1.0 based on how directly the pages address the question
- Be precise and factual — this data will be used for M&A competitive intelligence
- For numerical values, use raw numbers without formatting (e.g., 1000000 not "1,000,000")
- For lists, return JSON arrays
- For yes/no questions, return true/false`

// T1SystemPrompt returns the Tier 1 (Haiku) system prompt with firm context.
func T1SystemPrompt(docs *PEFirmDocs) string {
	return fmt.Sprintf(`%s

You are performing Tier 1 extraction: single-page fact extraction from the firm's website.
Focus on finding explicit, directly stated facts. Do not infer or synthesize across pages.

Firm: %s`, systemPrompt, docs.FirmName)
}

// T2SystemPrompt returns the Tier 2 (Sonnet) system prompt with cross-page context.
func T2SystemPrompt(docs *PEFirmDocs, t1Answers []Answer) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, `%s

You are performing Tier 2 extraction: cross-page synthesis and strategic analysis.
You may synthesize information across multiple pages of the firm's website.
Consider the full context of the firm's business, portfolio, and strategy when answering.

Firm: %s`, systemPrompt, docs.FirmName)

	// Include T1 context.
	if len(t1Answers) > 0 {
		sb.WriteString("\n\n--- Previously Extracted Facts (Tier 1) ---\n")
		for _, a := range t1Answers {
			if a.Value != nil {
				valJSON, _ := json.Marshal(a.Value)
				fmt.Fprintf(&sb, "- %s: %s (confidence: %.2f)\n", a.QuestionKey, string(valJSON), a.Confidence)
			}
		}
	}

	return sb.String()
}

// BlogSystemPrompt returns the Tier 2 system prompt specialized for blog/content intelligence.
func BlogSystemPrompt(docs *PEFirmDocs, t1Answers []Answer) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, `You are an expert M&A analyst specializing in private equity firms, aggregators, and holding companies that acquire investment advisory firms (RIAs).

You are analyzing blog posts, press releases, and insights content from a PE firm to extract intelligence signals about their strategy, deal activity, and market positioning.

Rules:
- Answer ONLY based on information present in the provided blog/press content
- Return valid JSON for every response
- Use null for the value if the information is not found in the content
- Confidence should be 0.0-1.0 based on how directly the content addresses the question
- Recency matters: prioritize recent content over older posts
- Distinguish between firm announcements (factual) and thought leadership (opinion/analysis)
- Extract specific dates, names, amounts, and deal details when present
- For numerical values, use raw numbers without formatting
- For lists, return JSON arrays
- For yes/no questions, return true/false

Firm: %s`, docs.FirmName)

	// Include T1 context to ground the analysis.
	if len(t1Answers) > 0 {
		sb.WriteString("\n\n--- Firm Context (Previously Extracted Facts) ---\n")
		for _, a := range t1Answers {
			if a.Value != nil {
				valJSON, _ := json.Marshal(a.Value)
				fmt.Fprintf(&sb, "- %s: %s\n", a.QuestionKey, string(valJSON))
			}
		}
	}

	return sb.String()
}

// BuildUserMessage constructs the user message for a question with document context.
func BuildUserMessage(q Question, docContext string) string {
	formatHint := ""
	if q.OutputFormat == "json" {
		formatHint = "\nIMPORTANT: The value field must be valid JSON. If returning an array, ensure all brackets are closed. Keep arrays concise — include the top 10 items maximum."
	}

	return fmt.Sprintf(`Question: %s
%s
Document Context:
%s

Respond with ONLY valid JSON in this format:
{
  "value": <answer in the format specified by the question>,
  "confidence": <0.0-1.0>,
  "reasoning": "<brief explanation of where this information was found>"
}`, q.Text, formatHint, docContext)
}
