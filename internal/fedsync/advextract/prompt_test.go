package advextract

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/sells-group/research-cli/pkg/anthropic"
)

func TestStructuredBypass_AUMCurrent(t *testing.T) {
	total := int64(1_000_000_000)
	disc := int64(800_000_000)
	nonDisc := int64(200_000_000)

	advisor := &AdvisorRow{
		CRDNumber:           12345,
		AUMTotal:            &total,
		AUMDiscretionary:    &disc,
		AUMNonDiscretionary: &nonDisc,
		Filing:              map[string]any{},
	}

	q := Question{Key: "aum_current", StructuredBypass: true}
	a := StructuredBypassAnswer(q, advisor, nil, nil)
	if a == nil {
		t.Fatal("expected non-nil answer for aum_current")
	}
	if a.Confidence != 1.0 {
		t.Errorf("expected confidence 1.0, got %f", a.Confidence)
	}
	if a.Model != "structured_bypass" {
		t.Errorf("expected model 'structured_bypass', got %s", a.Model)
	}
	if a.SourceDoc != "part1" {
		t.Errorf("expected source_doc 'part1', got %s", a.SourceDoc)
	}

	// Check value structure.
	valMap, ok := a.Value.(map[string]any)
	if !ok {
		t.Fatalf("expected map value, got %T", a.Value)
	}
	if valMap["total"] != &total {
		// Pointer comparison expected here.
		if *(valMap["total"].(*int64)) != total {
			t.Errorf("unexpected total AUM value")
		}
	}
}

func TestStructuredBypass_AvgAccountSize(t *testing.T) {
	total := int64(500_000_000)
	accounts := 500

	advisor := &AdvisorRow{
		CRDNumber:   12345,
		AUMTotal:    &total,
		NumAccounts: &accounts,
		Filing:      map[string]any{},
	}

	q := Question{Key: "avg_account_size", StructuredBypass: true}
	a := StructuredBypassAnswer(q, advisor, nil, nil)
	if a == nil {
		t.Fatal("expected non-nil answer")
	}

	val, ok := a.Value.(int64)
	if !ok {
		t.Fatalf("expected int64, got %T", a.Value)
	}
	if val != 1_000_000 {
		t.Errorf("expected avg account size 1000000, got %d", val)
	}
}

func TestStructuredBypass_CompensationTypes(t *testing.T) {
	advisor := &AdvisorRow{
		CRDNumber: 12345,
		Filing: map[string]any{
			"comp_pct_aum":      true,
			"comp_hourly":       true,
			"comp_fixed":        false,
			"comp_commissions":  false,
			"comp_performance":  true,
			"comp_subscription": false,
			"comp_other":        false,
		},
	}

	q := Question{Key: "compensation_types", StructuredBypass: true}
	a := StructuredBypassAnswer(q, advisor, nil, nil)
	if a == nil {
		t.Fatal("expected non-nil answer")
	}

	types, ok := a.Value.([]string)
	if !ok {
		t.Fatalf("expected []string, got %T", a.Value)
	}
	if len(types) != 3 {
		t.Errorf("expected 3 compensation types, got %d: %v", len(types), types)
	}
}

func TestStructuredBypass_DisciplinaryHistory(t *testing.T) {
	advisor := &AdvisorRow{
		CRDNumber: 12345,
		Filing: map[string]any{
			"has_any_drp":         true,
			"drp_regulatory_firm": true,
			"drp_criminal_firm":   false,
		},
	}

	q := Question{Key: "disciplinary_history", StructuredBypass: true}
	a := StructuredBypassAnswer(q, advisor, nil, nil)
	if a == nil {
		t.Fatal("expected non-nil answer")
	}

	valMap, ok := a.Value.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", a.Value)
	}
	if valMap["has_disciplinary_history"] != true {
		t.Error("expected has_disciplinary_history=true")
	}
}

func TestStructuredBypass_FundAUM(t *testing.T) {
	advisor := &AdvisorRow{CRDNumber: 12345, Filing: map[string]any{}}
	gav := int64(100_000_000)
	nav := int64(90_000_000)
	fund := &FundRow{
		CRDNumber:       12345,
		FundID:          "F001",
		FundName:        "Acme Growth Fund",
		GrossAssetValue: &gav,
		NetAssetValue:   &nav,
	}

	q := Question{Key: "fund_aum", StructuredBypass: true}
	a := StructuredBypassAnswer(q, advisor, fund, nil)
	if a == nil {
		t.Fatal("expected non-nil answer for fund_aum")
	}
	if a.FundID != "F001" {
		t.Errorf("expected fund_id F001, got %s", a.FundID)
	}
}

func TestStructuredBypass_NoData(t *testing.T) {
	advisor := &AdvisorRow{CRDNumber: 12345, Filing: map[string]any{}}

	q := Question{Key: "aum_discretionary_split", StructuredBypass: true}
	a := StructuredBypassAnswer(q, advisor, nil, nil)
	if a == nil {
		t.Fatal("expected non-nil answer")
	}
	if a.Confidence != 0 {
		t.Errorf("expected confidence 0 when no data, got %f", a.Confidence)
	}
}

func TestT1SystemPrompt(t *testing.T) {
	docs := &AdvisorDocs{CRDNumber: 12345, FirmName: "Acme Advisors"}
	prompt := T1SystemPrompt(docs)

	if !strings.Contains(prompt, "Tier 1 extraction") {
		t.Error("T1 prompt should mention Tier 1")
	}
	if !strings.Contains(prompt, "Acme Advisors") {
		t.Error("T1 prompt should include firm name")
	}
	if !strings.Contains(prompt, "12345") {
		t.Error("T1 prompt should include CRD number")
	}
}

func TestT2SystemPrompt_WithT1Answers(t *testing.T) {
	docs := &AdvisorDocs{CRDNumber: 12345, FirmName: "Acme Advisors"}
	t1Answers := []Answer{
		{QuestionKey: "investment_philosophy", Value: "Value investing", Confidence: 0.85},
		{QuestionKey: "fee_schedule_complete", Value: "1% AUM", Confidence: 0.9},
	}

	prompt := T2SystemPrompt(docs, t1Answers)

	if !strings.Contains(prompt, "Tier 2 extraction") {
		t.Error("T2 prompt should mention Tier 2")
	}
	if !strings.Contains(prompt, "Previously Extracted Facts") {
		t.Error("T2 prompt should include T1 context")
	}
	if !strings.Contains(prompt, "investment_philosophy") {
		t.Error("T2 prompt should reference T1 answers")
	}
}

func TestBuildUserMessage(t *testing.T) {
	q := Question{
		Key:  "fee_schedule_complete",
		Text: "What is the complete fee schedule?",
	}

	msg := BuildUserMessage(q, "Item 5 content about fees...")

	if !strings.Contains(msg, "What is the complete fee schedule?") {
		t.Error("user message should include question text")
	}
	if !strings.Contains(msg, "Item 5 content about fees") {
		t.Error("user message should include document context")
	}
	if !strings.Contains(msg, `"value"`) {
		t.Error("user message should include JSON format instructions")
	}
}

func TestFormatPart1Structured(t *testing.T) {
	total := int64(500_000_000)
	employees := 25
	fd := time.Date(2025, 3, 15, 0, 0, 0, 0, time.UTC)

	advisor := &AdvisorRow{
		CRDNumber:      12345,
		FirmName:       "Test Advisors",
		City:           "San Francisco",
		State:          "CA",
		AUMTotal:       &total,
		TotalEmployees: &employees,
		FilingDate:     &fd,
		Filing: map[string]any{
			"comp_pct_aum":        true,
			"sec_registered":      true,
			"has_any_drp":         false,
			"custody_client_cash": true,
		},
	}

	result := FormatPart1Structured(advisor)

	if !strings.Contains(result, "Test Advisors") {
		t.Error("should include firm name")
	}
	if !strings.Contains(result, "500,000,000") {
		t.Error("should include formatted AUM")
	}
	if !strings.Contains(result, "San Francisco") {
		t.Error("should include city")
	}
}

func TestCleanJSON(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			input:    `{"value": "test", "confidence": 0.9}`,
			expected: `{"value": "test", "confidence": 0.9}`,
		},
		{
			input:    "```json\n{\"value\": \"test\"}\n```",
			expected: `{"value": "test"}`,
		},
		{
			input:    "Here is the result:\n{\"value\": 42}\nDone.",
			expected: `{"value": 42}`,
		},
	}

	for _, tt := range tests {
		result := cleanJSON(tt.input)
		if result != tt.expected {
			t.Errorf("cleanJSON(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestParseAnswerFromResponse(t *testing.T) {
	q := Question{
		Key:            "investment_philosophy",
		SourceDocs:     []string{"part2"},
		SourceSections: []string{"item_8"},
	}

	resp := &anthropic.MessageResponse{
		Model: "claude-haiku-4-5-20251001",
		Content: []anthropic.ContentBlock{
			{Type: "text", Text: `{"value": "Value-oriented growth investing", "confidence": 0.85, "reasoning": "Found in Item 8"}`},
		},
		Usage: anthropic.TokenUsage{InputTokens: 1000, OutputTokens: 50},
	}

	answers := parseAnswerFromResponse(resp, q, 1)
	if len(answers) != 1 {
		t.Fatalf("expected 1 answer, got %d", len(answers))
	}

	a := answers[0]
	if a.QuestionKey != "investment_philosophy" {
		t.Errorf("expected key investment_philosophy, got %s", a.QuestionKey)
	}
	if a.Value != "Value-oriented growth investing" {
		t.Errorf("unexpected value: %v", a.Value)
	}
	if a.Confidence != 0.85 {
		t.Errorf("expected confidence 0.85, got %f", a.Confidence)
	}
	if a.Tier != 1 {
		t.Errorf("expected tier 1, got %d", a.Tier)
	}
}

func TestJsonValue(t *testing.T) {
	tests := []struct {
		input    any
		expected string
	}{
		{nil, "null"},
		{"test", `"test"`},
		{42, "42"},
		{true, "true"},
		{[]string{"a", "b"}, `["a","b"]`},
	}

	for _, tt := range tests {
		result := jsonValue(tt.input)
		if string(result) != tt.expected {
			t.Errorf("jsonValue(%v) = %s, want %s", tt.input, result, tt.expected)
		}
	}
}

// Need to import anthropic for the test
func init() {
	// Ensure json is used (for the test compilation)
	_ = json.Marshal
}
