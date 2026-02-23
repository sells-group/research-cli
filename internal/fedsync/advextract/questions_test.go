package advextract

import (
	"testing"
)

func TestAllQuestions_Count(t *testing.T) {
	qs := AllQuestions()
	if len(qs) != 95 {
		t.Errorf("expected 95 questions, got %d", len(qs))
	}
}

func TestAllQuestions_UniqueKeys(t *testing.T) {
	qs := AllQuestions()
	seen := make(map[string]bool)
	for _, q := range qs {
		if seen[q.Key] {
			t.Errorf("duplicate question key: %s", q.Key)
		}
		seen[q.Key] = true
	}
}

func TestAllQuestions_ValidTiers(t *testing.T) {
	for _, q := range AllQuestions() {
		if q.Tier < 1 || q.Tier > 3 {
			t.Errorf("question %s has invalid tier %d", q.Key, q.Tier)
		}
	}
}

func TestAllQuestions_ValidScopes(t *testing.T) {
	for _, q := range AllQuestions() {
		if q.Scope != ScopeAdvisor && q.Scope != ScopeFund {
			t.Errorf("question %s has invalid scope %q", q.Key, q.Scope)
		}
	}
}

func TestAllQuestions_ValidCategories(t *testing.T) {
	validCats := map[string]bool{
		CatFirmIdentity: true, CatAUMGrowth: true, CatInvestment: true,
		CatFees: true, CatClients: true, CatCompliance: true,
		CatOperations: true, CatPersonnel: true, CatFundDetail: true,
		CatConflicts: true, CatGrowth: true,
	}
	for _, q := range AllQuestions() {
		if !validCats[q.Category] {
			t.Errorf("question %s has invalid category %q", q.Key, q.Category)
		}
	}
}

func TestQuestionsByTier_Distribution(t *testing.T) {
	t1 := QuestionsByTier(1)
	t2 := QuestionsByTier(2)
	t3 := QuestionsByTier(3)

	total := len(t1) + len(t2) + len(t3)
	if total != 95 {
		t.Errorf("tier distribution should sum to 95, got %d (T1=%d, T2=%d, T3=%d)",
			total, len(t1), len(t2), len(t3))
	}
}

func TestQuestionsByScope_Distribution(t *testing.T) {
	advisor := QuestionsByScope(ScopeAdvisor)
	fund := QuestionsByScope(ScopeFund)

	total := len(advisor) + len(fund)
	if total != 95 {
		t.Errorf("scope distribution should sum to 95, got %d (advisor=%d, fund=%d)",
			total, len(advisor), len(fund))
	}

	if len(fund) != 15 {
		t.Errorf("expected 15 fund-level questions, got %d", len(fund))
	}
}

func TestStructuredBypassQuestions(t *testing.T) {
	bypass := StructuredBypassQuestions()
	if len(bypass) != 17 {
		t.Errorf("expected 17 structured bypass questions, got %d", len(bypass))
	}

	// All bypass questions should have Part 1 as source.
	for _, q := range bypass {
		hasPart1 := false
		for _, d := range q.SourceDocs {
			if d == "part1" {
				hasPart1 = true
				break
			}
		}
		if !hasPart1 {
			t.Errorf("bypass question %s should have part1 as source doc", q.Key)
		}
	}
}

func TestQuestionMap(t *testing.T) {
	m := QuestionMap()
	if len(m) != 95 {
		t.Errorf("expected 95 entries in question map, got %d", len(m))
	}

	// Check a known question.
	q, ok := m["revenue_estimate"]
	if !ok {
		t.Fatal("expected revenue_estimate in question map")
	}
	if q.Tier != 3 {
		t.Errorf("revenue_estimate should be tier 3, got %d", q.Tier)
	}
	if q.Category != CatAUMGrowth {
		t.Errorf("revenue_estimate should be category B, got %s", q.Category)
	}
}
