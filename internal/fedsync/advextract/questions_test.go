package advextract

import (
	"testing"
)

func TestAllQuestions_Count(t *testing.T) {
	qs := AllQuestions()
	if len(qs) != 238 {
		t.Errorf("expected 238 questions, got %d", len(qs))
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
		if q.Tier < 1 || q.Tier > 2 {
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
		CatConflicts: true, CatGrowth: true, CatCRS: true,
		CatCrossDoc: true, CatSynthesis: true,
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

	total := len(t1) + len(t2)
	if total != 238 {
		t.Errorf("tier distribution should sum to 238, got %d (T1=%d, T2=%d)",
			total, len(t1), len(t2))
	}

	if len(t1) != 230 {
		t.Errorf("expected 230 Tier 1 questions, got %d", len(t1))
	}
	if len(t2) != 8 {
		t.Errorf("expected 8 Tier 2 questions, got %d", len(t2))
	}
}

func TestQuestionsByScope_Distribution(t *testing.T) {
	advisor := QuestionsByScope(ScopeAdvisor)
	fund := QuestionsByScope(ScopeFund)

	total := len(advisor) + len(fund)
	if total != 238 {
		t.Errorf("scope distribution should sum to 238, got %d (advisor=%d, fund=%d)",
			total, len(advisor), len(fund))
	}

	if len(fund) != 31 {
		t.Errorf("expected 31 fund-level questions, got %d", len(fund))
	}
}

func TestStructuredBypassQuestions(t *testing.T) {
	bypass := StructuredBypassQuestions()
	if len(bypass) != 29 {
		t.Errorf("expected 29 structured bypass questions, got %d", len(bypass))
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
	if len(m) != 238 {
		t.Errorf("expected 238 entries in question map, got %d", len(m))
	}

	// Check a known Tier 2 (Sonnet) question.
	q, ok := m["integration_complexity_assessment"]
	if !ok {
		t.Fatal("expected integration_complexity_assessment in question map")
	}
	if q.Tier != 2 {
		t.Errorf("integration_complexity_assessment should be tier 2, got %d", q.Tier)
	}
	if q.Category != CatSynthesis {
		t.Errorf("integration_complexity_assessment should be category N, got %s", q.Category)
	}

	// Check a known bypass question.
	qb, ok := m["aum_current"]
	if !ok {
		t.Fatal("expected aum_current in question map")
	}
	if !qb.StructuredBypass {
		t.Error("aum_current should be a structured bypass question")
	}
}
