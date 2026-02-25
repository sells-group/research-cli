package peextract

import (
	"testing"
)

func TestAllQuestions_Count(t *testing.T) {
	qs := AllQuestions()
	if len(qs) != 44 {
		t.Errorf("expected 44 questions, got %d", len(qs))
	}
}

func TestAllQuestions_TierCounts(t *testing.T) {
	t1 := QuestionsByTier(1)
	t2 := QuestionsByTier(2)

	if len(t1) != 30 {
		t.Errorf("expected 30 T1 questions, got %d", len(t1))
	}
	if len(t2) != 14 {
		t.Errorf("expected 14 T2 questions, got %d", len(t2))
	}
}

func TestAllQuestions_UniqueKeys(t *testing.T) {
	seen := make(map[string]bool)
	for _, q := range AllQuestions() {
		if seen[q.Key] {
			t.Errorf("duplicate question key: %s", q.Key)
		}
		seen[q.Key] = true
	}
}

func TestAllQuestions_HavePEPrefix(t *testing.T) {
	for _, q := range AllQuestions() {
		if len(q.Key) < 3 || q.Key[:3] != "pe_" {
			t.Errorf("question key %q does not have pe_ prefix", q.Key)
		}
	}
}

func TestAllQuestions_HavePageTypes(t *testing.T) {
	for _, q := range AllQuestions() {
		if len(q.PageTypes) == 0 {
			t.Errorf("question %q has no page types", q.Key)
		}
	}
}

func TestAllQuestions_ValidTiers(t *testing.T) {
	for _, q := range AllQuestions() {
		if q.Tier < 1 || q.Tier > 2 {
			t.Errorf("question %q has invalid tier %d", q.Key, q.Tier)
		}
	}
}

func TestAllQuestions_HaveText(t *testing.T) {
	for _, q := range AllQuestions() {
		if q.Text == "" {
			t.Errorf("question %q has empty text", q.Key)
		}
	}
}

func TestQuestionMap(t *testing.T) {
	m := QuestionMap()
	if len(m) != 44 {
		t.Errorf("expected 44 entries in question map, got %d", len(m))
	}

	// Spot check a few keys.
	if _, ok := m["pe_hq_address"]; !ok {
		t.Error("pe_hq_address not found in question map")
	}
	if _, ok := m["pe_strategic_assessment"]; !ok {
		t.Error("pe_strategic_assessment not found in question map")
	}
	if _, ok := m["pe_investment_themes"]; !ok {
		t.Error("pe_investment_themes not found in question map")
	}
	if _, ok := m["pe_content_recency"]; !ok {
		t.Error("pe_content_recency not found in question map")
	}
}

func TestFilterByTier(t *testing.T) {
	qs := AllQuestions()
	t1 := filterByTier(qs, 1)
	t2 := filterByTier(qs, 2)
	t3 := filterByTier(qs, 3)

	if len(t1) != 30 {
		t.Errorf("expected 30 T1, got %d", len(t1))
	}
	if len(t2) != 14 {
		t.Errorf("expected 14 T2, got %d", len(t2))
	}
	if len(t3) != 0 {
		t.Errorf("expected 0 T3, got %d", len(t3))
	}
}

func TestBlogQuestions_AllTier2(t *testing.T) {
	for _, q := range AllQuestions() {
		if q.Category == CatBlogIntel && q.Tier != 2 {
			t.Errorf("blog question %q should be T2, got T%d", q.Key, q.Tier)
		}
	}
}

func TestBlogQuestions_HaveBlogPageType(t *testing.T) {
	for _, q := range AllQuestions() {
		if q.Category != CatBlogIntel {
			continue
		}
		hasBlog := false
		for _, pt := range q.PageTypes {
			if pt == "blog" || pt == "news" {
				hasBlog = true
				break
			}
		}
		if !hasBlog {
			t.Errorf("blog question %q should route to blog or news page type", q.Key)
		}
	}
}
