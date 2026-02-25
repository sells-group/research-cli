package pipeline

import (
	"regexp"
	"testing"
	"time"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/ppp"
	"github.com/stretchr/testify/assert"
)

func TestMergeAnswers_HigherTierWins(t *testing.T) {
	t1 := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "industry", Value: "Tech", Confidence: 0.8, Tier: 1},
	}
	t2 := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "industry", Value: "Technology Services", Confidence: 0.5, Tier: 2},
	}

	merged := MergeAnswers(t1, t2, nil)
	assert.Len(t, merged, 1)
	assert.Equal(t, "Technology Services", merged[0].Value)
	assert.Equal(t, 2, merged[0].Tier)
}

func TestMergeAnswers_SameTierHigherConfidenceWins(t *testing.T) {
	t1 := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "industry", Value: "Old Answer", Confidence: 0.3, Tier: 1},
		{QuestionID: "q2", FieldKey: "industry", Value: "Better Answer", Confidence: 0.9, Tier: 1},
	}

	merged := MergeAnswers(t1, nil, nil)
	assert.Len(t, merged, 1)
	assert.Equal(t, "Better Answer", merged[0].Value)
	assert.Equal(t, 0.9, merged[0].Confidence)
}

func TestMergeAnswers_LowConfidenceHigherTierIgnored(t *testing.T) {
	t1 := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "revenue", Value: "$10M", Confidence: 0.8, Tier: 1},
	}
	t2 := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "revenue", Value: "$15M", Confidence: 0.2, Tier: 2},
	}

	merged := MergeAnswers(t1, t2, nil)
	assert.Len(t, merged, 1)
	// T2 has low confidence (< 0.3), so T1 should win.
	assert.Equal(t, "$10M", merged[0].Value)
}

func TestMergeAnswers_NullT1_AnyT2Wins(t *testing.T) {
	t1 := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "year_founded", Value: nil, Confidence: 0.1, Tier: 1},
	}
	t2 := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "year_founded", Value: 2011, Confidence: 0.2, Tier: 2},
	}

	merged := MergeAnswers(t1, t2, nil)
	assert.Len(t, merged, 1)
	// T2 should win even with low confidence because T1 is null.
	assert.Equal(t, 2011, merged[0].Value)
	assert.Equal(t, 2, merged[0].Tier)
}

func TestMergeAnswers_HigherTierNullDoesNotOverride(t *testing.T) {
	t1 := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "hq_address", Value: "5021 Verdugo Way", Confidence: 0.92, Tier: 1},
	}
	t2 := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "hq_address", Value: nil, Confidence: 0.75, Tier: 2},
	}

	merged := MergeAnswers(t1, t2, nil)
	assert.Len(t, merged, 1)
	// T1's non-null value should be preserved; T2 null should NOT override.
	assert.Equal(t, "5021 Verdugo Way", merged[0].Value)
	assert.Equal(t, 0.92, merged[0].Confidence)
	assert.Equal(t, 1, merged[0].Tier)
}

func TestMergeAnswers_EmptyFieldKeySkipped(t *testing.T) {
	t1 := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "", Value: "no key", Confidence: 0.9, Tier: 1},
	}

	merged := MergeAnswers(t1, nil, nil)
	assert.Len(t, merged, 0)
}

func TestMergeAnswers_MultipleFields(t *testing.T) {
	t1 := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "industry", Value: "Tech", Confidence: 0.8, Tier: 1},
		{QuestionID: "q2", FieldKey: "revenue", Value: "$5M", Confidence: 0.6, Tier: 1},
	}
	t2 := []model.ExtractionAnswer{
		{QuestionID: "q3", FieldKey: "employees", Value: 100, Confidence: 0.9, Tier: 2},
	}

	merged := MergeAnswers(t1, t2, nil)
	assert.Len(t, merged, 3)
}

func TestValidateField_StringType(t *testing.T) {
	field := &model.FieldMapping{
		Key:      "industry",
		SFField:  "Industry",
		DataType: "string",
	}
	answer := model.ExtractionAnswer{
		FieldKey:   "industry",
		Value:      "Technology",
		Confidence: 0.9,
		SourceURL:  "https://acme.com",
		Tier:       1,
	}

	fv := ValidateField(answer, field)
	assert.NotNil(t, fv)
	assert.Equal(t, "Technology", fv.Value)
	assert.Equal(t, "Industry", fv.SFField)
}

func TestValidateField_StringMaxLength(t *testing.T) {
	field := &model.FieldMapping{
		Key:       "name",
		SFField:   "Name",
		DataType:  "string",
		MaxLength: 5,
	}
	answer := model.ExtractionAnswer{
		FieldKey: "name",
		Value:    "Long Company Name",
	}

	fv := ValidateField(answer, field)
	assert.NotNil(t, fv)
	assert.Equal(t, "Long ", fv.Value)
}

func TestValidateField_NumberType(t *testing.T) {
	field := &model.FieldMapping{Key: "employees", SFField: "NumberOfEmployees", DataType: "number"}

	tests := []struct {
		name  string
		value any
		want  any
	}{
		{"float64", float64(100), int(100)},
		{"int", 50, int(50)},
		{"string", "1,500", int(1500)},
		{"string_decimal", "99.5", float64(99.5)}, // "number" preserves fractional parts
		{"float64_with_decimal", float64(4.6), float64(4.6)},
		{"float64_whole", float64(4.0), int(4)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			answer := model.ExtractionAnswer{FieldKey: "employees", Value: tt.value}
			fv := ValidateField(answer, field)
			assert.NotNil(t, fv)
			assert.Equal(t, tt.want, fv.Value)
		})
	}
}

func TestValidateField_IntegerType(t *testing.T) {
	field := &model.FieldMapping{Key: "employees", SFField: "NumberOfEmployees", DataType: "integer"}

	tests := []struct {
		name  string
		value any
		want  int
	}{
		{"float64", float64(100), 100},
		{"int", 50, 50},
		{"string_decimal", "99.5", 99}, // "integer" truncates
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			answer := model.ExtractionAnswer{FieldKey: "employees", Value: tt.value}
			fv := ValidateField(answer, field)
			assert.NotNil(t, fv)
			assert.Equal(t, tt.want, fv.Value)
		})
	}
}

func TestValidateField_NumberType_Invalid(t *testing.T) {
	field := &model.FieldMapping{Key: "employees", SFField: "NumberOfEmployees", DataType: "number"}
	answer := model.ExtractionAnswer{FieldKey: "employees", Value: "not-a-number"}

	fv := ValidateField(answer, field)
	assert.Nil(t, fv)
}

func TestValidateField_BooleanType(t *testing.T) {
	field := &model.FieldMapping{Key: "active", SFField: "IsActive__c", DataType: "boolean"}

	tests := []struct {
		name  string
		value any
		want  bool
	}{
		{"true_bool", true, true},
		{"false_bool", false, false},
		{"yes_string", "yes", true},
		{"no_string", "no", false},
		{"1_string", "1", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			answer := model.ExtractionAnswer{FieldKey: "active", Value: tt.value}
			fv := ValidateField(answer, field)
			assert.NotNil(t, fv)
			assert.Equal(t, tt.want, fv.Value)
		})
	}
}

func TestValidateField_URLType_Invalid(t *testing.T) {
	field := &model.FieldMapping{Key: "website", SFField: "Website", DataType: "url"}
	answer := model.ExtractionAnswer{FieldKey: "website", Value: "not-a-url"}

	fv := ValidateField(answer, field)
	assert.Nil(t, fv)
}

func TestValidateField_URLType_Valid(t *testing.T) {
	field := &model.FieldMapping{Key: "website", SFField: "Website", DataType: "url"}
	answer := model.ExtractionAnswer{FieldKey: "website", Value: "https://acme.com"}

	fv := ValidateField(answer, field)
	assert.NotNil(t, fv)
	assert.Equal(t, "https://acme.com", fv.Value)
}

func TestValidateField_NilField(t *testing.T) {
	answer := model.ExtractionAnswer{FieldKey: "f", Value: "v"}
	fv := ValidateField(answer, nil)
	assert.Nil(t, fv)
}

func TestValidateField_NilValue(t *testing.T) {
	field := &model.FieldMapping{Key: "f", DataType: "string"}
	answer := model.ExtractionAnswer{FieldKey: "f", Value: nil}
	fv := ValidateField(answer, field)
	assert.Nil(t, fv)
}

func TestValidateField_FloatType(t *testing.T) {
	field := &model.FieldMapping{Key: "revenue", SFField: "AnnualRevenue", DataType: "currency"}
	answer := model.ExtractionAnswer{FieldKey: "revenue", Value: "$1,500,000"}

	fv := ValidateField(answer, field)
	assert.NotNil(t, fv)
	assert.Equal(t, 1500000.0, fv.Value)
}

func TestValidateField_ValidationRegex(t *testing.T) {
	field := &model.FieldMapping{
		Key:             "state",
		SFField:         "BillingState",
		DataType:        "string",
		Validation:      `^[A-Z]{2}$`,
		ValidationRegex: regexp.MustCompile(`^[A-Z]{2}$`),
	}

	t.Run("valid", func(t *testing.T) {
		answer := model.ExtractionAnswer{FieldKey: "state", Value: "CA"}
		fv := ValidateField(answer, field)
		assert.NotNil(t, fv)
	})

	t.Run("invalid", func(t *testing.T) {
		answer := model.ExtractionAnswer{FieldKey: "state", Value: "California"}
		fv := ValidateField(answer, field)
		assert.Nil(t, fv)
	})
}

func TestBuildFieldValues(t *testing.T) {
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry", DataType: "string"},
		{Key: "employees", SFField: "NumberOfEmployees", DataType: "number"},
		{Key: "missing", SFField: "Missing__c", DataType: "string"},
	})

	answers := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "industry", Value: "Tech", Confidence: 0.9, Tier: 1},
		{QuestionID: "q2", FieldKey: "employees", Value: float64(100), Confidence: 0.8, Tier: 1},
		{QuestionID: "q3", FieldKey: "unknown_field", Value: "ignore", Confidence: 0.7, Tier: 1},
	}

	fvs := BuildFieldValues(answers, fields)
	assert.Len(t, fvs, 2)
	assert.Equal(t, "Tech", fvs["industry"].Value)
	assert.Equal(t, 100, fvs["employees"].Value)
}

func TestBuildFieldValues_PreSeededPrecisionUpgrade(t *testing.T) {
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "google_reviews_rating", SFField: "Rating__c", DataType: "number"},
		{Key: "employees", SFField: "NumberOfEmployees", DataType: "number"},
	})

	t.Run("upgrades integer rating to decimal from pre-seeded", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "google_reviews_rating", Value: float64(4), Confidence: 0.85, Tier: 1},
		}
		company := model.Company{
			PreSeeded: map[string]any{
				"google_reviews_rating": float64(4.6),
			},
		}

		fvs := BuildFieldValues(answers, fields, company)
		assert.Equal(t, float64(4.6), fvs["google_reviews_rating"].Value)
		assert.Contains(t, fvs["google_reviews_rating"].Source, "precision_upgrade")
	})

	t.Run("does not upgrade when values differ", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "google_reviews_rating", Value: float64(3), Confidence: 0.85, Tier: 1},
		}
		company := model.Company{
			PreSeeded: map[string]any{
				"google_reviews_rating": float64(4.6), // Different integer part
			},
		}

		fvs := BuildFieldValues(answers, fields, company)
		assert.Equal(t, int(3), fvs["google_reviews_rating"].Value) // Unchanged
	})

	t.Run("does not upgrade when both are integers", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "employees", Value: float64(100), Confidence: 0.85, Tier: 1},
		}
		company := model.Company{
			PreSeeded: map[string]any{
				"employees": 100, // Same integer, no decimal
			},
		}

		fvs := BuildFieldValues(answers, fields, company)
		assert.Equal(t, int(100), fvs["employees"].Value) // Unchanged
	})

	t.Run("gap-fills missing fields from pre-seeded", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "employees", Value: float64(100), Confidence: 0.85, Tier: 1},
		}
		company := model.Company{
			PreSeeded: map[string]any{
				"google_reviews_rating": float64(4.6),
			},
		}

		fvs := BuildFieldValues(answers, fields, company)
		assert.Equal(t, float64(4.6), fvs["google_reviews_rating"].Value)
		assert.Equal(t, "grata_csv", fvs["google_reviews_rating"].Source)
	})
}

func TestEnrichFromPPP(t *testing.T) {
	loanDate := time.Date(2020, 6, 15, 0, 0, 0, 0, time.UTC)

	t.Run("empty matches returns unchanged answers", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "industry", Value: "Tech", Confidence: 0.9, Tier: 1},
		}
		result := EnrichFromPPP(answers, nil)
		assert.Len(t, result, 1)

		result = EnrichFromPPP(answers, []ppp.LoanMatch{})
		assert.Len(t, result, 1)
	})

	t.Run("single match adds revenue and employees", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "industry", Value: "Tech", Confidence: 0.9, Tier: 1},
		}
		matches := []ppp.LoanMatch{
			{
				BorrowerName:    "ACME CORP",
				CurrentApproval: 500_000,
				JobsReported:    25,
				DateApproved:    loanDate,
				MatchTier:       1,
				MatchScore:      1.0,
			},
		}

		result := EnrichFromPPP(answers, matches)
		assert.Len(t, result, 3) // original + revenue + employees

		// Find revenue answer.
		var revAnswer, empAnswer *model.ExtractionAnswer
		for i := range result {
			switch result[i].FieldKey {
			case "revenue_estimate":
				revAnswer = &result[i]
			case "employees":
				empAnswer = &result[i]
			}
		}

		assert.NotNil(t, revAnswer)
		assert.Equal(t, "$10.0M", revAnswer.Value) // 500k * 20 / 1M = 10
		assert.InDelta(t, 0.85, revAnswer.Confidence, 0.001)
		assert.Equal(t, "ppp_database", revAnswer.Source)
		assert.Equal(t, 0, revAnswer.Tier)
		assert.NotNil(t, revAnswer.DataAsOf)
		assert.Equal(t, loanDate, *revAnswer.DataAsOf)

		assert.NotNil(t, empAnswer)
		assert.Equal(t, 25, empAnswer.Value)
		assert.InDelta(t, 0.70, empAnswer.Confidence, 0.001)
		assert.Equal(t, "ppp_database", empAnswer.Source)
		assert.Equal(t, 0, empAnswer.Tier)
		assert.NotNil(t, empAnswer.DataAsOf)
		assert.Equal(t, loanDate, *empAnswer.DataAsOf)
	})

	t.Run("zero loan amount skips revenue", func(t *testing.T) {
		matches := []ppp.LoanMatch{
			{
				CurrentApproval: 0,
				JobsReported:    10,
				DateApproved:    loanDate,
				MatchScore:      1.0,
			},
		}

		result := EnrichFromPPP(nil, matches)
		assert.Len(t, result, 1) // only employees
		assert.Equal(t, "employees", result[0].FieldKey)
	})

	t.Run("zero jobs skips employees", func(t *testing.T) {
		matches := []ppp.LoanMatch{
			{
				CurrentApproval: 100_000,
				JobsReported:    0,
				DateApproved:    loanDate,
				MatchScore:      0.8,
			},
		}

		result := EnrichFromPPP(nil, matches)
		assert.Len(t, result, 1) // only revenue
		assert.Equal(t, "revenue_estimate", result[0].FieldKey)
		assert.InDelta(t, 0.68, result[0].Confidence, 0.001) // 0.8 * 0.85
	})

	t.Run("fuzzy match scales confidence", func(t *testing.T) {
		matches := []ppp.LoanMatch{
			{
				CurrentApproval: 200_000,
				JobsReported:    15,
				DateApproved:    loanDate,
				MatchTier:       3,
				MatchScore:      0.65,
			},
		}

		result := EnrichFromPPP(nil, matches)
		assert.Len(t, result, 2)

		for _, a := range result {
			switch a.FieldKey {
			case "revenue_estimate":
				assert.InDelta(t, 0.65*0.85, a.Confidence, 0.001)
			case "employees":
				assert.InDelta(t, 0.65*0.7, a.Confidence, 0.001)
			}
		}
	})
}

func TestInjectPageMetadata(t *testing.T) {
	t.Run("injects review data from metadata", func(t *testing.T) {
		pages := []model.CrawledPage{
			{
				URL:   "https://maps.google.com/test",
				Title: "[google_maps] Test Co",
				Metadata: &model.PageMetadata{
					ReviewCount: 127,
					Rating:      4.5,
				},
			},
		}
		var answers []model.ExtractionAnswer
		result := InjectPageMetadata(answers, pages)
		assert.Len(t, result, 2)

		byField := make(map[string]model.ExtractionAnswer)
		for _, a := range result {
			byField[a.FieldKey] = a
		}

		rc := byField["google_reviews_count"]
		assert.Equal(t, 127, rc.Value)
		assert.Equal(t, 0.95, rc.Confidence)
		assert.Equal(t, "google_maps_metadata", rc.Source)

		rr := byField["google_reviews_rating"]
		assert.Equal(t, 4.5, rr.Value)
		assert.Equal(t, 0.95, rr.Confidence)
	})

	t.Run("upgrades lower-confidence LLM answers", func(t *testing.T) {
		pages := []model.CrawledPage{
			{
				Metadata: &model.PageMetadata{
					ReviewCount: 200,
					Rating:      3.0,
				},
			},
		}
		answers := []model.ExtractionAnswer{
			{FieldKey: "google_reviews_count", Value: 150, Confidence: 0.8, Tier: 1},
			{FieldKey: "google_reviews_rating", Value: 4.2, Confidence: 0.8, Tier: 1},
		}
		result := InjectPageMetadata(answers, pages)
		assert.Len(t, result, 2) // no new entries, upgraded in-place

		byField := make(map[string]model.ExtractionAnswer)
		for _, a := range result {
			byField[a.FieldKey] = a
		}

		// Metadata (0.95) replaces LLM (0.8).
		assert.Equal(t, 200, byField["google_reviews_count"].Value)
		assert.Equal(t, 0.95, byField["google_reviews_count"].Confidence)
		assert.Equal(t, 3.0, byField["google_reviews_rating"].Value)
		assert.Equal(t, 0.95, byField["google_reviews_rating"].Confidence)
	})

	t.Run("does not downgrade higher-confidence answers", func(t *testing.T) {
		pages := []model.CrawledPage{
			{
				Metadata: &model.PageMetadata{
					ReviewCount: 200,
					Rating:      3.0,
				},
			},
		}
		answers := []model.ExtractionAnswer{
			{FieldKey: "google_reviews_count", Value: 150, Confidence: 0.99, Tier: 1},
			{FieldKey: "google_reviews_rating", Value: 4.2, Confidence: 0.99, Tier: 1},
		}
		result := InjectPageMetadata(answers, pages)
		assert.Len(t, result, 2)

		byField := make(map[string]model.ExtractionAnswer)
		for _, a := range result {
			byField[a.FieldKey] = a
		}

		// Original higher-confidence values preserved.
		assert.Equal(t, 150, byField["google_reviews_count"].Value)
		assert.Equal(t, 4.2, byField["google_reviews_rating"].Value)
	})

	t.Run("skips nil metadata", func(t *testing.T) {
		pages := []model.CrawledPage{
			{URL: "https://example.com", Metadata: nil},
		}
		result := InjectPageMetadata(nil, pages)
		assert.Len(t, result, 0)
	})

	t.Run("skips zero values", func(t *testing.T) {
		pages := []model.CrawledPage{
			{Metadata: &model.PageMetadata{ReviewCount: 0, Rating: 0}},
		}
		result := InjectPageMetadata(nil, pages)
		assert.Len(t, result, 0)
	})

	t.Run("regex source gets 0.95 confidence", func(t *testing.T) {
		pages := []model.CrawledPage{
			{
				Metadata: &model.PageMetadata{
					ReviewCount: 80,
					Rating:      4.2,
					Source:      "regex",
				},
			},
		}
		result := InjectPageMetadata(nil, pages)
		assert.Len(t, result, 2)

		byField := make(map[string]model.ExtractionAnswer)
		for _, a := range result {
			byField[a.FieldKey] = a
		}
		assert.Equal(t, 0.95, byField["google_reviews_count"].Confidence)
		assert.Equal(t, 0.95, byField["google_reviews_rating"].Confidence)
	})

	t.Run("perplexity source gets 0.70 confidence", func(t *testing.T) {
		pages := []model.CrawledPage{
			{
				Metadata: &model.PageMetadata{
					ReviewCount: 8,
					Rating:      4.5,
					Source:      "perplexity",
				},
			},
		}
		result := InjectPageMetadata(nil, pages)
		assert.Len(t, result, 2)

		byField := make(map[string]model.ExtractionAnswer)
		for _, a := range result {
			byField[a.FieldKey] = a
		}
		assert.Equal(t, 0.70, byField["google_reviews_count"].Confidence)
		assert.Equal(t, 0.70, byField["google_reviews_rating"].Confidence)
	})

	t.Run("perplexity review count distrusted when diverges from pre-seed", func(t *testing.T) {
		pages := []model.CrawledPage{
			{
				Metadata: &model.PageMetadata{
					ReviewCount: 8, // Perplexity hallucinated
					Rating:      4.5,
					Source:      "perplexity",
				},
			},
		}
		preSeeded := map[string]any{
			"google_reviews_count": 80, // Grata says 80
		}
		result := InjectPageMetadata(nil, pages, preSeeded)
		assert.Len(t, result, 2)

		byField := make(map[string]model.ExtractionAnswer)
		for _, a := range result {
			byField[a.FieldKey] = a
		}
		// Review count confidence should drop to 0.50 (below gap-fill 0.60)
		assert.Equal(t, 0.50, byField["google_reviews_count"].Confidence)
		// Rating is unaffected by review count cross-check
		assert.Equal(t, 0.70, byField["google_reviews_rating"].Confidence)
	})

	t.Run("perplexity review count trusted when close to pre-seed", func(t *testing.T) {
		pages := []model.CrawledPage{
			{
				Metadata: &model.PageMetadata{
					ReviewCount: 78,
					Source:      "perplexity",
				},
			},
		}
		preSeeded := map[string]any{
			"google_reviews_count": 80,
		}
		result := InjectPageMetadata(nil, pages, preSeeded)
		assert.Len(t, result, 1)

		// Close to pre-seed, confidence stays at 0.70
		assert.Equal(t, 0.70, result[0].Confidence)
	})

	t.Run("injects phone from metadata", func(t *testing.T) {
		pages := []model.CrawledPage{
			{
				Metadata: &model.PageMetadata{
					Phone: "5617936029",
				},
			},
		}
		result := InjectPageMetadata(nil, pages)
		assert.Len(t, result, 1)
		assert.Equal(t, "phone", result[0].FieldKey)
		assert.Equal(t, "5617936029", result[0].Value)
		assert.Equal(t, 0.90, result[0].Confidence)
		assert.Equal(t, "website_metadata", result[0].Source)
	})
}

func TestCrossValidateEmployeeCount(t *testing.T) {
	t.Run("boosts confidence when within LinkedIn range", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "employees", Value: 125, Confidence: 0.65, Source: "website"},
		}
		liData := &LinkedInData{EmployeeCount: "51-200"}
		result := CrossValidateEmployeeCount(answers, liData)
		assert.Len(t, result, 1)
		assert.Equal(t, 0.85, result[0].Confidence)
		assert.Contains(t, result[0].Source, "+linkedin_validated")
	})

	t.Run("no boost when outside LinkedIn range", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "employees", Value: 500, Confidence: 0.65, Source: "website"},
		}
		liData := &LinkedInData{EmployeeCount: "51-200"}
		result := CrossValidateEmployeeCount(answers, liData)
		assert.Len(t, result, 1)
		assert.Equal(t, 0.65, result[0].Confidence) // unchanged
	})

	t.Run("no boost when confidence already high", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "employees", Value: 100, Confidence: 0.90, Source: "website"},
		}
		liData := &LinkedInData{EmployeeCount: "51-200"}
		result := CrossValidateEmployeeCount(answers, liData)
		assert.Len(t, result, 1)
		assert.Equal(t, 0.90, result[0].Confidence) // unchanged
	})

	t.Run("nil linkedin data returns unchanged", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "employees", Value: 100, Confidence: 0.65},
		}
		result := CrossValidateEmployeeCount(answers, nil)
		assert.Len(t, result, 1)
		assert.Equal(t, 0.65, result[0].Confidence)
	})

	t.Run("handles 10001+ range", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "employees", Value: 15000, Confidence: 0.60, Source: "website"},
		}
		liData := &LinkedInData{EmployeeCount: "10,001+"}
		result := CrossValidateEmployeeCount(answers, liData)
		assert.Len(t, result, 1)
		assert.Equal(t, 0.85, result[0].Confidence)
	})
}

func TestValidateField_JSONType(t *testing.T) {
	field := &model.FieldMapping{Key: "contacts", DataType: "json"}

	t.Run("array value passes through", func(t *testing.T) {
		contacts := []map[string]string{
			{"first_name": "Jane", "last_name": "Doe", "title": "CEO"},
		}
		answer := model.ExtractionAnswer{FieldKey: "contacts", Value: contacts, Confidence: 0.8}
		fv := ValidateField(answer, field)
		assert.NotNil(t, fv)
		assert.Equal(t, contacts, fv.Value)
	})

	t.Run("object value passes through", func(t *testing.T) {
		obj := map[string]any{"key": "value"}
		answer := model.ExtractionAnswer{FieldKey: "contacts", Value: obj, Confidence: 0.8}
		fv := ValidateField(answer, field)
		assert.NotNil(t, fv)
		assert.Equal(t, obj, fv.Value)
	})

	t.Run("nil value returns nil", func(t *testing.T) {
		answer := model.ExtractionAnswer{FieldKey: "contacts", Value: nil}
		fv := ValidateField(answer, field)
		assert.Nil(t, fv)
	})
}

func TestPopulateOwnerFromContacts(t *testing.T) {
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "contacts", DataType: "json"},
		{Key: "owner_first_name", SFField: "FirstName", SFObject: "Contact", DataType: "string"},
		{Key: "owner_last_name", SFField: "LastName", SFObject: "Contact", DataType: "string"},
		{Key: "owner_title", SFField: "Title", SFObject: "Contact", DataType: "string"},
		{Key: "owner_email", SFField: "Email", SFObject: "Contact", DataType: "email"},
		{Key: "owner_phone", SFField: "Phone", SFObject: "Contact", DataType: "phone"},
		{Key: "owner_linkedin", SFField: "LinkedIn__c", SFObject: "Contact", DataType: "url"},
	})

	t.Run("fills owner fields from contacts[0]", func(t *testing.T) {
		result := map[string]model.FieldValue{
			"contacts": {
				FieldKey:   "contacts",
				Value:      []map[string]string{{"first_name": "Jane", "last_name": "Doe", "title": "CEO", "email": "jane@acme.com"}},
				Confidence: 0.75,
			},
		}
		populateOwnerFromContacts(result, fields)

		assert.Equal(t, "Jane", result["owner_first_name"].Value)
		assert.Equal(t, "Doe", result["owner_last_name"].Value)
		assert.Equal(t, "CEO", result["owner_title"].Value)
		assert.Equal(t, "jane@acme.com", result["owner_email"].Value)
		assert.Equal(t, "contacts[0]", result["owner_first_name"].Source)
		assert.Equal(t, 0.75, result["owner_first_name"].Confidence)
	})

	t.Run("does not override existing owner fields", func(t *testing.T) {
		result := map[string]model.FieldValue{
			"contacts": {
				FieldKey: "contacts",
				Value:    []map[string]string{{"first_name": "Jane", "last_name": "Doe", "title": "CEO"}},
			},
			"owner_first_name": {FieldKey: "owner_first_name", Value: "Alice", Confidence: 0.9},
		}
		populateOwnerFromContacts(result, fields)

		assert.Equal(t, "Alice", result["owner_first_name"].Value) // Not overwritten.
		assert.Equal(t, "Doe", result["owner_last_name"].Value)    // Filled.
	})

	t.Run("handles []any type (JSON unmarshaled)", func(t *testing.T) {
		result := map[string]model.FieldValue{
			"contacts": {
				FieldKey:   "contacts",
				Value:      []any{map[string]any{"first_name": "Bob", "last_name": "Smith", "title": "VP"}},
				Confidence: 0.7,
			},
		}
		populateOwnerFromContacts(result, fields)

		assert.Equal(t, "Bob", result["owner_first_name"].Value)
		assert.Equal(t, "Smith", result["owner_last_name"].Value)
	})

	t.Run("no contacts field is a no-op", func(t *testing.T) {
		result := map[string]model.FieldValue{
			"industry": {FieldKey: "industry", Value: "Tech"},
		}
		populateOwnerFromContacts(result, fields)
		assert.Len(t, result, 1) // Unchanged.
	})

	t.Run("empty contacts array is a no-op", func(t *testing.T) {
		result := map[string]model.FieldValue{
			"contacts": {FieldKey: "contacts", Value: []map[string]string{}},
		}
		populateOwnerFromContacts(result, fields)
		_, hasOwner := result["owner_first_name"]
		assert.False(t, hasOwner)
	})
}

func TestMergeContacts_Dedup(t *testing.T) {
	answers := []model.ExtractionAnswer{
		{FieldKey: "industry", Value: "Tech", Confidence: 0.9},
		{FieldKey: "contacts", Value: []map[string]string{
			{"first_name": "Jane", "last_name": "Doe", "title": "CEO"},
			{"first_name": "John", "last_name": "Smith", "title": "VP"},
		}, Confidence: 0.75, Source: "linkedin"},
		{FieldKey: "contacts", Value: []map[string]string{
			{"first_name": "Jane", "last_name": "Doe", "title": "CEO & Founder"},        // Duplicate.
			{"first_name": "Bob", "last_name": "Jones", "title": "Director of Finance"}, // New.
		}, Confidence: 0.7, Source: "web_extraction"},
	}

	result := MergeContacts(answers)

	// Should have industry + 1 merged contacts answer.
	contactsAnswers := 0
	var contactsAnswer model.ExtractionAnswer
	for _, a := range result {
		if a.FieldKey == "contacts" {
			contactsAnswers++
			contactsAnswer = a
		}
	}
	assert.Equal(t, 1, contactsAnswers)

	contacts := contactsAnswer.Value.([]map[string]string)
	assert.Len(t, contacts, 3) // Jane, John, Bob (deduplicated by last name).
	assert.Equal(t, "Doe", contacts[0]["last_name"])
	assert.Equal(t, "Smith", contacts[1]["last_name"])
	assert.Equal(t, "Jones", contacts[2]["last_name"])
	assert.Equal(t, 0.75, contactsAnswer.Confidence) // Best confidence.
}

func TestMergeContacts_Cap3(t *testing.T) {
	answers := []model.ExtractionAnswer{
		{FieldKey: "contacts", Value: []map[string]string{
			{"first_name": "A", "last_name": "One", "title": "CEO"},
			{"first_name": "B", "last_name": "Two", "title": "VP"},
			{"first_name": "C", "last_name": "Three", "title": "Dir"},
		}, Confidence: 0.8, Source: "linkedin"},
		{FieldKey: "contacts", Value: []map[string]string{
			{"first_name": "D", "last_name": "Four", "title": "CTO"},
			{"first_name": "E", "last_name": "Five", "title": "CFO"},
		}, Confidence: 0.7, Source: "web"},
	}

	result := MergeContacts(answers)

	var contactsAnswer model.ExtractionAnswer
	for _, a := range result {
		if a.FieldKey == "contacts" {
			contactsAnswer = a
		}
	}

	contacts := contactsAnswer.Value.([]map[string]string)
	assert.Len(t, contacts, 3) // Capped at 3.
}

func TestMergeContacts_NoContacts(t *testing.T) {
	answers := []model.ExtractionAnswer{
		{FieldKey: "industry", Value: "Tech", Confidence: 0.9},
	}

	result := MergeContacts(answers)
	assert.Len(t, result, 1) // Unchanged.
}

func TestMergeContacts_HandlesAnyType(t *testing.T) {
	// Simulates JSON-unmarshaled data where contacts is []any of map[string]any.
	answers := []model.ExtractionAnswer{
		{FieldKey: "contacts", Value: []any{
			map[string]any{"first_name": "Jane", "last_name": "Doe", "title": "CEO"},
		}, Confidence: 0.8, Source: "web"},
	}

	result := MergeContacts(answers)

	var contactsAnswer model.ExtractionAnswer
	for _, a := range result {
		if a.FieldKey == "contacts" {
			contactsAnswer = a
		}
	}

	contacts := contactsAnswer.Value.([]map[string]string)
	assert.Len(t, contacts, 1)
	assert.Equal(t, "Jane", contacts[0]["first_name"])
}

func TestParseLinkedInRange(t *testing.T) {
	tests := []struct {
		input  string
		wantLo int
		wantHi int
	}{
		{"51-200", 51, 200},
		{"201-500", 201, 500},
		{"11-50", 11, 50},
		{"10,001+", 10001, 1_000_000},
		{"1,001-5,000", 1001, 5000},
		{"invalid", 0, 0},
		{"", 0, 0},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			lo, hi := parseLinkedInRange(tc.input)
			assert.Equal(t, tc.wantLo, lo)
			assert.Equal(t, tc.wantHi, hi)
		})
	}
}
