package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/pkg/anthropic"
)

const (
	// maxHomepageBytes limits the amount of HTML downloaded for scoring.
	maxHomepageBytes = 512 * 1024 // 512 KB

	// maxMarkdownChars is the truncation limit for the markdown sent to Claude.
	maxMarkdownChars = 16000 // ~4K tokens
)

// t1Prompt is the system prompt for T1 Haiku scoring.
const t1Prompt = `You are evaluating a business for lead qualification. Score this business on a scale of 0.0 to 1.0 based on:
- Legitimacy: Is this a real operating business with identifiable services?
- Revenue potential: Does this appear to be a mid-market business ($1M+ revenue)?
- B2B service fit: Would this business benefit from professional B2B services?

Respond with ONLY valid JSON, no other text:
{"score": 0.0, "reasoning": "brief explanation"}`

// t2Prompt is the system prompt for T2 Sonnet scoring.
const t2Prompt = `You are performing a deeper evaluation of a business for lead qualification. Analyze the provided web content and score this business on a scale of 0.0 to 1.0 based on:
- Legitimacy and operational maturity (established team, real office, client testimonials)
- Revenue indicators (employee count, office locations, client list quality)
- Service sophistication (specialized offerings, industry focus, professional certifications)
- B2B service fit (complexity of operations suggests need for professional services)

Respond with ONLY valid JSON, no other text:
{"score": 0.0, "reasoning": "detailed explanation"}`

type scoreResponse struct {
	Score     float64 `json:"score"`
	Reasoning string  `json:"reasoning"`
}

// RunT1 scores non-disqualified candidates using Haiku.
// Returns the number of candidates scored.
func RunT1(ctx context.Context, store Store, ai anthropic.Client, cfg *config.DiscoveryConfig, model string, runID string, limit int) (int, error) {
	log := zap.L().With(zap.String("phase", "t1"), zap.String("run_id", runID))

	notDisqualified := false
	candidates, err := store.ListCandidates(ctx, runID, ListOpts{
		Disqualified: &notDisqualified,
		Limit:        limit,
	})
	if err != nil {
		return 0, eris.Wrap(err, "t1: list candidates")
	}

	// Filter to candidates with T0 score but no T1 score.
	var toScore []Candidate
	for _, c := range candidates {
		if c.ScoreT0 != nil && c.ScoreT1 == nil {
			toScore = append(toScore, c)
		}
	}

	log.Info("running T1 scoring", zap.Int("candidates", len(toScore)))

	scored := 0
	for _, c := range toScore {
		if ctx.Err() != nil {
			return scored, ctx.Err()
		}

		content, fetchErr := fetchHomepage(ctx, c.Website)
		if fetchErr != nil {
			log.Debug("fetch homepage failed", zap.String("website", c.Website), zap.Error(fetchErr))
			continue
		}

		if len(content) > maxMarkdownChars {
			content = content[:maxMarkdownChars]
		}

		score, scoreErr := scoreByClaude(ctx, ai, model, t1Prompt, c.Name, content)
		if scoreErr != nil {
			log.Debug("claude scoring failed", zap.String("name", c.Name), zap.Error(scoreErr))
			continue
		}

		if updateErr := store.UpdateCandidateScore(ctx, c.ID, "t1", score); updateErr != nil {
			log.Warn("update T1 score failed", zap.Int64("id", c.ID), zap.Error(updateErr))
			continue
		}

		// Disqualify if below threshold.
		if score < cfg.T1ScoreThreshold {
			if dqErr := store.DisqualifyCandidate(ctx, c.ID, "t1_low_score"); dqErr != nil {
				log.Warn("disqualify failed", zap.Int64("id", c.ID), zap.Error(dqErr))
			}
		}

		scored++
	}

	log.Info("T1 complete", zap.Int("scored", scored))
	return scored, nil
}

// RunT2 scores candidates in the T1-T2 threshold range using Sonnet.
// Returns the number of candidates scored.
func RunT2(ctx context.Context, store Store, ai anthropic.Client, cfg *config.DiscoveryConfig, model string, runID string, limit int) (int, error) {
	log := zap.L().With(zap.String("phase", "t2"), zap.String("run_id", runID))

	notDisqualified := false
	candidates, err := store.ListCandidates(ctx, runID, ListOpts{
		Disqualified: &notDisqualified,
		Limit:        limit,
	})
	if err != nil {
		return 0, eris.Wrap(err, "t2: list candidates")
	}

	// Filter to candidates with T1 score in the T2 band but no T2 score.
	var toScore []Candidate
	for _, c := range candidates {
		if c.ScoreT1 != nil && c.ScoreT2 == nil {
			s := *c.ScoreT1
			if s >= cfg.T2ScoreThreshold && s < cfg.T1ScoreThreshold {
				toScore = append(toScore, c)
			}
		}
	}

	log.Info("running T2 scoring", zap.Int("candidates", len(toScore)))

	scored := 0
	for _, c := range toScore {
		if ctx.Err() != nil {
			return scored, ctx.Err()
		}

		content, fetchErr := fetchHomepage(ctx, c.Website)
		if fetchErr != nil {
			log.Debug("fetch homepage failed", zap.String("website", c.Website), zap.Error(fetchErr))
			continue
		}

		// T2 gets more content.
		contentLimit := maxMarkdownChars * 2
		if len(content) > contentLimit {
			content = content[:contentLimit]
		}

		score, scoreErr := scoreByClaude(ctx, ai, model, t2Prompt, c.Name, content)
		if scoreErr != nil {
			log.Debug("claude scoring failed", zap.String("name", c.Name), zap.Error(scoreErr))
			continue
		}

		if updateErr := store.UpdateCandidateScore(ctx, c.ID, "t2", score); updateErr != nil {
			log.Warn("update T2 score failed", zap.Int64("id", c.ID), zap.Error(updateErr))
			continue
		}

		// Disqualify if below T2 threshold.
		if score < cfg.T2ScoreThreshold {
			if dqErr := store.DisqualifyCandidate(ctx, c.ID, "t2_low_score"); dqErr != nil {
				log.Warn("disqualify failed", zap.Int64("id", c.ID), zap.Error(dqErr))
			}
		}

		scored++
	}

	log.Info("T2 complete", zap.Int("scored", scored))
	return scored, nil
}

// scoreByClaude sends homepage content to Claude and parses the score.
func scoreByClaude(ctx context.Context, ai anthropic.Client, model, systemPrompt, name, content string) (float64, error) {
	userMsg := fmt.Sprintf("Business name: %s\n\nWebsite content:\n%s", name, content)

	resp, err := ai.CreateMessage(ctx, anthropic.MessageRequest{
		Model:     model,
		MaxTokens: 256,
		System:    []anthropic.SystemBlock{{Text: systemPrompt}},
		Messages:  []anthropic.Message{{Role: "user", Content: userMsg}},
	})
	if err != nil {
		return 0, eris.Wrap(err, "score: claude request")
	}

	// Extract text from response.
	var text string
	for _, block := range resp.Content {
		if block.Type == "text" {
			text = block.Text
			break
		}
	}

	if text == "" {
		return 0, eris.New("score: empty claude response")
	}

	// Parse JSON response.
	// Find JSON in the response (it may have surrounding text).
	jsonStart := strings.Index(text, "{")
	jsonEnd := strings.LastIndex(text, "}")
	if jsonStart < 0 || jsonEnd < 0 || jsonEnd <= jsonStart {
		return 0, eris.Errorf("score: no JSON in response: %s", text)
	}

	var result scoreResponse
	if err := json.Unmarshal([]byte(text[jsonStart:jsonEnd+1]), &result); err != nil {
		return 0, eris.Wrap(err, "score: parse response JSON")
	}

	// Clamp score to [0, 1].
	if result.Score < 0 {
		result.Score = 0
	}
	if result.Score > 1 {
		result.Score = 1
	}

	return result.Score, nil
}

// fetchHomepage downloads the homepage and returns the raw HTML text (stripped of tags).
func fetchHomepage(ctx context.Context, website string) (string, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, website, nil)
	if err != nil {
		return "", eris.Wrap(err, "score: create request")
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; research-cli/1.0)")

	resp, err := client.Do(req)
	if err != nil {
		return "", eris.Wrap(err, "score: fetch homepage")
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode >= 400 {
		return "", eris.Errorf("score: homepage returned %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxHomepageBytes))
	if err != nil {
		return "", eris.Wrap(err, "score: read homepage")
	}

	// Strip HTML tags for a rough text extraction.
	text := stripHTMLTags(string(body))
	return text, nil
}

// stripHTMLTags removes HTML tags from a string, producing plain text.
func stripHTMLTags(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inTag := false
	for _, r := range s {
		switch {
		case r == '<':
			inTag = true
		case r == '>':
			inTag = false
			b.WriteRune(' ')
		case !inTag:
			b.WriteRune(r)
		}
	}
	return b.String()
}
