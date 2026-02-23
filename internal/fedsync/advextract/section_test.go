package advextract

import (
	"testing"
)

func TestSectionBrochure_StandardItems(t *testing.T) {
	text := `Some intro text before items.

Item 1 – Cover Page
This is the cover page content for Acme Advisors LLC.

Item 4 – Advisory Business
Acme provides investment advisory services to high-net-worth individuals.
We manage approximately $500M in assets.

Item 5: Fees and Compensation
We charge 1% on the first $1M, 0.75% thereafter.
Fees are billed quarterly in advance.

Item 8 - Methods of Analysis, Investment Strategies and Risk of Loss
Our investment approach focuses on diversified equity portfolios.
We employ fundamental analysis and modern portfolio theory.

Item 12 – Brokerage Practices
We primarily use Schwab and Fidelity as custodians.

Item 18 – Financial Information
There are no financial conditions that would impair our ability to meet obligations.`

	sections := SectionBrochure(text)

	// Should have full text.
	if _, ok := sections[SectionFull]; !ok {
		t.Fatal("expected 'full' section")
	}

	// Check detected items.
	expectedKeys := []string{"item_1", "item_4", "item_5", "item_8", "item_12", "item_18"}
	for _, key := range expectedKeys {
		if _, ok := sections[key]; !ok {
			t.Errorf("expected section %q not found", key)
		}
	}

	// Check content.
	if s, ok := sections["item_4"]; ok {
		if len(s) == 0 {
			t.Error("item_4 should have content")
		}
		if !contains(s, "investment advisory services") {
			t.Error("item_4 should contain 'investment advisory services'")
		}
	}

	if s, ok := sections["item_5"]; ok {
		if !contains(s, "1%") {
			t.Error("item_5 should contain fee information")
		}
	}

	// Check item_1 doesn't contain item_4 content.
	if s, ok := sections["item_1"]; ok {
		if contains(s, "investment advisory services") {
			t.Error("item_1 should not contain item_4 content")
		}
	}
}

func TestSectionBrochure_NoItems(t *testing.T) {
	text := "This is a brochure with no standard item headers."
	sections := SectionBrochure(text)

	if _, ok := sections[SectionFull]; !ok {
		t.Fatal("expected 'full' section")
	}

	// Should only have "full" key.
	if len(sections) != 1 {
		t.Errorf("expected 1 section (full only), got %d", len(sections))
	}
}

func TestSectionBrochure_Empty(t *testing.T) {
	sections := SectionBrochure("")
	if _, ok := sections[SectionFull]; !ok {
		t.Fatal("expected 'full' section even for empty input")
	}
}

func TestSectionBrochure_AllCapsHeaders(t *testing.T) {
	text := `ITEM 4 ADVISORY BUSINESS
We provide portfolio management.

ITEM 5 FEES AND COMPENSATION
We charge asset-based fees.`

	sections := SectionBrochure(text)

	if _, ok := sections["item_4"]; !ok {
		t.Error("expected item_4 from all-caps header")
	}
	if _, ok := sections["item_5"]; !ok {
		t.Error("expected item_5 from all-caps header")
	}
}

func TestSectionsForItems(t *testing.T) {
	sections := map[string]string{
		"item_4":  "Advisory business content",
		"item_5":  "Fee content",
		"item_8":  "Investment strategy content",
		SectionFull: "Full brochure text",
	}

	result := SectionsForItems(sections, "item_4", "item_5")
	if !contains(result, "Advisory business content") {
		t.Error("should include item_4 content")
	}
	if !contains(result, "Fee content") {
		t.Error("should include item_5 content")
	}
	if contains(result, "Investment strategy") {
		t.Error("should not include item_8")
	}

	// Fallback to full.
	result2 := SectionsForItems(sections, "item_99")
	if !contains(result2, "Full brochure text") {
		t.Error("should fall back to full text for missing items")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && containsSubstr(s, substr)
}

func containsSubstr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
