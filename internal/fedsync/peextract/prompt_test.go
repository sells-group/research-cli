package peextract

import (
	"strings"
	"testing"
)

func TestT1SystemPrompt_ContainsFirmName(t *testing.T) {
	docs := &PEFirmDocs{
		PEFirmID:    1,
		FirmName:    "Test Capital Partners",
		PagesByType: make(map[PEPageType][]ClassifiedPage),
	}

	prompt := T1SystemPrompt(docs)

	if !strings.Contains(prompt, "Test Capital Partners") {
		t.Error("T1 prompt should contain firm name")
	}
	if !strings.Contains(prompt, "Tier 1 extraction") {
		t.Error("T1 prompt should reference Tier 1")
	}
	if !strings.Contains(prompt, "M&A analyst") {
		t.Error("T1 prompt should reference M&A analyst role")
	}
}

func TestT2SystemPrompt_IncludesT1Context(t *testing.T) {
	docs := &PEFirmDocs{
		PEFirmID:    1,
		FirmName:    "Test Capital Partners",
		PagesByType: make(map[PEPageType][]ClassifiedPage),
	}

	t1Answers := []Answer{
		{QuestionKey: "pe_firm_type", Value: "private_equity", Confidence: 0.9},
		{QuestionKey: "pe_portfolio_count", Value: 15, Confidence: 0.8},
	}

	prompt := T2SystemPrompt(docs, t1Answers)

	if !strings.Contains(prompt, "Tier 2 extraction") {
		t.Error("T2 prompt should reference Tier 2")
	}
	if !strings.Contains(prompt, "Previously Extracted Facts") {
		t.Error("T2 prompt should include T1 context")
	}
	if !strings.Contains(prompt, "pe_firm_type") {
		t.Error("T2 prompt should include T1 answer keys")
	}
}

func TestT2SystemPrompt_NoT1Context(t *testing.T) {
	docs := &PEFirmDocs{
		PEFirmID:    1,
		FirmName:    "Test Capital Partners",
		PagesByType: make(map[PEPageType][]ClassifiedPage),
	}

	prompt := T2SystemPrompt(docs, nil)

	if strings.Contains(prompt, "Previously Extracted Facts") {
		t.Error("T2 prompt without answers should not include T1 context")
	}
}

func TestBlogSystemPrompt_ContainsFirmName(t *testing.T) {
	docs := &PEFirmDocs{
		PEFirmID:    1,
		FirmName:    "Test Capital Partners",
		PagesByType: make(map[PEPageType][]ClassifiedPage),
	}

	prompt := BlogSystemPrompt(docs, nil)

	if !strings.Contains(prompt, "Test Capital Partners") {
		t.Error("blog prompt should contain firm name")
	}
	if !strings.Contains(prompt, "blog posts") {
		t.Error("blog prompt should reference blog posts")
	}
	if !strings.Contains(prompt, "press releases") {
		t.Error("blog prompt should reference press releases")
	}
	if !strings.Contains(prompt, "Recency matters") {
		t.Error("blog prompt should emphasize recency")
	}
}

func TestBlogSystemPrompt_IncludesT1Context(t *testing.T) {
	docs := &PEFirmDocs{
		PEFirmID:    1,
		FirmName:    "Test Capital Partners",
		PagesByType: make(map[PEPageType][]ClassifiedPage),
	}

	t1Answers := []Answer{
		{QuestionKey: "pe_firm_type", Value: "private_equity", Confidence: 0.9},
		{QuestionKey: "pe_portfolio_count", Value: 15, Confidence: 0.8},
	}

	prompt := BlogSystemPrompt(docs, t1Answers)

	if !strings.Contains(prompt, "Firm Context") {
		t.Error("blog prompt should include firm context section")
	}
	if !strings.Contains(prompt, "pe_firm_type") {
		t.Error("blog prompt should include T1 answer keys")
	}
}

func TestBlogSystemPrompt_NoT1Context(t *testing.T) {
	docs := &PEFirmDocs{
		PEFirmID:    1,
		FirmName:    "Test Capital Partners",
		PagesByType: make(map[PEPageType][]ClassifiedPage),
	}

	prompt := BlogSystemPrompt(docs, nil)

	if strings.Contains(prompt, "Firm Context") {
		t.Error("blog prompt without answers should not include firm context")
	}
}

func TestBuildUserMessage(t *testing.T) {
	q := Question{
		Key:  "pe_year_founded",
		Text: "What year was this firm founded?",
	}

	msg := BuildUserMessage(q, "The firm was established in 2005.")

	if !strings.Contains(msg, "What year was this firm founded?") {
		t.Error("user message should contain question text")
	}
	if !strings.Contains(msg, "established in 2005") {
		t.Error("user message should contain document context")
	}
	if !strings.Contains(msg, `"value"`) {
		t.Error("user message should contain JSON format instructions")
	}
	if !strings.Contains(msg, `"confidence"`) {
		t.Error("user message should contain confidence field")
	}
}

func TestModelForTier(t *testing.T) {
	tests := []struct {
		tier int
		want string
	}{
		{1, ModelHaiku},
		{2, ModelSonnet},
		{0, ModelHaiku},
		{3, ModelHaiku},
	}

	for _, tt := range tests {
		got := ModelForTier(tt.tier)
		if got != tt.want {
			t.Errorf("ModelForTier(%d) = %q, want %q", tt.tier, got, tt.want)
		}
	}
}

func TestMaxTokensForTier(t *testing.T) {
	if MaxTokensForTier(1) != 1024 {
		t.Errorf("T1 max tokens should be 1024, got %d", MaxTokensForTier(1))
	}
	if MaxTokensForTier(2) != 1024 {
		t.Errorf("T2 max tokens should be 1024, got %d", MaxTokensForTier(2))
	}
}

func TestBuildUserMessage_JSONFormatHint(t *testing.T) {
	q := Question{
		Key:          "pe_portfolio_companies",
		Text:         "What are the current portfolio companies?",
		OutputFormat: "json",
	}

	msg := BuildUserMessage(q, "Portfolio includes Acme Corp.")

	if !strings.Contains(msg, "IMPORTANT: The value field must be valid JSON") {
		t.Error("JSON format hint should be included for json OutputFormat")
	}
	if !strings.Contains(msg, "top 10 items maximum") {
		t.Error("JSON format hint should include array size guidance")
	}
}

func TestBuildUserMessage_NoFormatHintForString(t *testing.T) {
	q := Question{
		Key:          "pe_firm_description",
		Text:         "Describe the firm.",
		OutputFormat: "string",
	}

	msg := BuildUserMessage(q, "A firm that does things.")

	if strings.Contains(msg, "IMPORTANT: The value field must be valid JSON") {
		t.Error("JSON format hint should not appear for string OutputFormat")
	}
}
