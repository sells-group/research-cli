package model

import "time"

// Question represents a question from the Question Registry.
type Question struct {
	ID           string     `json:"id"`
	Text         string     `json:"text"`
	Tier         int        `json:"tier"`
	FieldKey     string     `json:"field_key"`
	PageTypes    []PageType `json:"page_types"`
	Instructions string     `json:"instructions"`
	OutputFormat string     `json:"output_format"`
	Status       string     `json:"status"`
}

// RoutedQuestion is a question matched to specific pages.
type RoutedQuestion struct {
	Question Question         `json:"question"`
	Pages    []ClassifiedPage `json:"pages"`
}

// SkippedQuestion is a question that could not be routed.
type SkippedQuestion struct {
	Question Question `json:"question"`
	Reason   string   `json:"reason"`
}

// RoutedBatches groups routed questions by tier.
type RoutedBatches struct {
	Tier1   []RoutedQuestion  `json:"tier1"`
	Tier2   []RoutedQuestion  `json:"tier2"`
	Tier3   []RoutedQuestion  `json:"tier3"`
	Skipped []SkippedQuestion `json:"skipped"`
}

// ExtractionAnswer holds the result of extracting an answer for a question.
type ExtractionAnswer struct {
	QuestionID    string         `json:"question_id"`
	FieldKey      string         `json:"field_key"`
	Value         any            `json:"value"`
	Confidence    float64        `json:"confidence"`
	Source        string         `json:"source"`
	SourceURL     string         `json:"source_url"`
	Tier          int            `json:"tier"`
	Reasoning     string         `json:"reasoning"`
	DataAsOf      *time.Time     `json:"data_as_of,omitempty"`
	Contradiction *Contradiction `json:"contradiction,omitempty"`
}

// Contradiction flags when two tiers disagree on a field value
// with moderate+ confidence on both sides.
type Contradiction struct {
	OtherTier       int     `json:"other_tier"`
	OtherValue      any     `json:"other_value"`
	OtherConfidence float64 `json:"other_confidence"`
}

// TierResult holds the outcome of a single tier extraction.
type TierResult struct {
	Tier       int                `json:"tier"`
	Answers    []ExtractionAnswer `json:"answers"`
	TokenUsage TokenUsage         `json:"token_usage"`
	Duration   int64              `json:"duration_ms"`
}

// TokenUsage tracks token consumption.
type TokenUsage struct {
	InputTokens         int     `json:"input_tokens"`
	OutputTokens        int     `json:"output_tokens"`
	CacheCreationTokens int     `json:"cache_creation_tokens"`
	CacheReadTokens     int     `json:"cache_read_tokens"`
	Cost                float64 `json:"cost"`
}

// Add merges token usage from another instance.
func (t *TokenUsage) Add(other TokenUsage) {
	t.InputTokens += other.InputTokens
	t.OutputTokens += other.OutputTokens
	t.CacheCreationTokens += other.CacheCreationTokens
	t.CacheReadTokens += other.CacheReadTokens
	t.Cost += other.Cost
}
