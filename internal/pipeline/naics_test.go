package pipeline

import (
	"testing"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractSoSIndustryInfo(t *testing.T) {
	t.Run("extracts NAICS code from SoS page", func(t *testing.T) {
		page := model.CrawledPage{
			Title:    "[sos] Acme Corp - IL Secretary of State",
			Markdown: "Entity Name: Acme Corp\nNAICS Code: 541512\nStatus: Active",
		}
		info := ExtractSoSIndustryInfo(page)
		require.NotNil(t, info)
		assert.Equal(t, "541512", info.NAICSCode)
	})

	t.Run("extracts SIC code from SoS page", func(t *testing.T) {
		page := model.CrawledPage{
			Title:    "[sos] Test Company",
			Markdown: "Business Name: Test Company\nSIC Code: 6211\nState: CA",
		}
		info := ExtractSoSIndustryInfo(page)
		require.NotNil(t, info)
		assert.Equal(t, "6211", info.SICCode)
	})

	t.Run("extracts business type", func(t *testing.T) {
		page := model.CrawledPage{
			Title:    "[sos] My Insurance LLC",
			Markdown: "Entity Name: My Insurance LLC\nEntity Type: Limited Liability Company\nIndustry: Insurance",
		}
		info := ExtractSoSIndustryInfo(page)
		require.NotNil(t, info)
		assert.Equal(t, "Limited Liability Company", info.BusinessType)
		assert.Contains(t, info.IndustryKeywords, "insurance")
		assert.Contains(t, info.InferredSectors, "52")
	})

	t.Run("infers sectors from keywords in content", func(t *testing.T) {
		page := model.CrawledPage{
			Title:    "[sos] ABC Construction Inc",
			Markdown: "ABC Construction Inc is a licensed general contractor specializing in commercial building and roofing services.",
		}
		info := ExtractSoSIndustryInfo(page)
		require.NotNil(t, info)
		assert.Contains(t, info.IndustryKeywords, "construction")
		assert.Contains(t, info.IndustryKeywords, "contractor")
		assert.Contains(t, info.IndustryKeywords, "roofing")
		assert.Contains(t, info.InferredSectors, "23")
	})

	t.Run("returns nil for non-SoS pages", func(t *testing.T) {
		page := model.CrawledPage{
			Title:    "[google_maps] Acme Corp",
			Markdown: "Acme Corp - 4.5 stars - 127 reviews",
		}
		info := ExtractSoSIndustryInfo(page)
		assert.Nil(t, info)
	})

	t.Run("returns nil for empty markdown", func(t *testing.T) {
		page := model.CrawledPage{
			Title:    "[sos] Empty Page",
			Markdown: "",
		}
		info := ExtractSoSIndustryInfo(page)
		assert.Nil(t, info)
	})

	t.Run("extracts NAICS with colon separator", func(t *testing.T) {
		page := model.CrawledPage{
			Title:    "[sos] Tech Corp",
			Markdown: "Company: Tech Corp\nNAICS: 511210\nRegistered: 2020",
		}
		info := ExtractSoSIndustryInfo(page)
		require.NotNil(t, info)
		assert.Equal(t, "511210", info.NAICSCode)
	})

	t.Run("handles SoS page detected by title keywords", func(t *testing.T) {
		page := model.CrawledPage{
			Title:    "California Secretary of State - Business Entity Detail",
			Markdown: "Entity Name: Software Corp\nSIC Code: 7372\nBusiness Category: technology services",
		}
		info := ExtractSoSIndustryInfo(page)
		require.NotNil(t, info)
		assert.Equal(t, "7372", info.SICCode)
		assert.Contains(t, info.IndustryKeywords, "technology")
	})
}

func TestValidateAndCrossReferenceNAICS(t *testing.T) {
	t.Run("validates and normalizes valid NAICS code", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "company_name", Value: "Acme Corp", Confidence: 0.9, Tier: 1},
			{FieldKey: "naics_code", Value: "541512", Confidence: 0.7, Tier: 1, Source: "website"},
		}
		result := ValidateAndCrossReferenceNAICS(answers, nil)

		naics := findAnswer(result, "naics_code")
		require.NotNil(t, naics)
		assert.Equal(t, "541512", naics.Value)
		assert.Contains(t, naics.Source, "naics_validated")
		// Confidence should be boosted (valid 4-digit industry group).
		assert.Greater(t, naics.Confidence, 0.7)
	})

	t.Run("corrects invalid NAICS code to nearest valid", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "naics_code", Value: "539000", Confidence: 0.7, Tier: 1, Source: "website"},
		}
		result := ValidateAndCrossReferenceNAICS(answers, nil)

		naics := findAnswer(result, "naics_code")
		require.NotNil(t, naics)
		// Should be corrected to sector 53 (Real Estate).
		assert.Equal(t, "530000", naics.Value)
		assert.Contains(t, naics.Source, "naics_corrected")
		// Confidence should be penalized.
		assert.Less(t, naics.Confidence, 0.7)
	})

	t.Run("handles completely invalid NAICS code", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "naics_code", Value: "999999", Confidence: 0.7, Tier: 1, Source: "website"},
		}
		result := ValidateAndCrossReferenceNAICS(answers, nil)

		naics := findAnswer(result, "naics_code")
		require.NotNil(t, naics)
		assert.Contains(t, naics.Source, "naics_invalid")
		assert.Less(t, naics.Confidence, 0.7)
	})

	t.Run("boosts confidence when SoS NAICS matches", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "naics_code", Value: "541512", Confidence: 0.7, Tier: 1, Source: "website"},
		}
		pages := []model.CrawledPage{
			{
				Title:    "[sos] Acme Corp - Business Entity",
				Markdown: "Entity: Acme Corp\nNAICS Code: 541512",
			},
		}
		result := ValidateAndCrossReferenceNAICS(answers, pages)

		naics := findAnswer(result, "naics_code")
		require.NotNil(t, naics)
		assert.Contains(t, naics.Source, "sos_naics_match")
		// Should be significantly boosted.
		assert.Greater(t, naics.Confidence, 0.8)
	})

	t.Run("sector match from SoS gives moderate boost", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "naics_code", Value: "541511", Confidence: 0.7, Tier: 1, Source: "website"},
		}
		pages := []model.CrawledPage{
			{
				Title:    "[sos] Tech Corp",
				Markdown: "Entity: Tech Corp\nNAICS Code: 541611", // Same sector (54), different code
			},
		}
		result := ValidateAndCrossReferenceNAICS(answers, pages)

		naics := findAnswer(result, "naics_code")
		require.NotNil(t, naics)
		assert.Contains(t, naics.Source, "sos_sector_match")
		assert.Greater(t, naics.Confidence, 0.7)
	})

	t.Run("SoS NAICS mismatch penalizes confidence", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "naics_code", Value: "541512", Confidence: 0.8, Tier: 1, Source: "website"},
		}
		pages := []model.CrawledPage{
			{
				Title:    "[sos] Company",
				Markdown: "NAICS Code: 236115", // Construction, not Professional Services
			},
		}
		result := ValidateAndCrossReferenceNAICS(answers, pages)

		naics := findAnswer(result, "naics_code")
		require.NotNil(t, naics)
		assert.Contains(t, naics.Source, "sos_naics_mismatch")
		assert.Less(t, naics.Confidence, 0.8)
	})

	t.Run("SoS SIC crosswalk match boosts confidence", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "naics_code", Value: "523110", Confidence: 0.65, Tier: 1, Source: "website"},
		}
		pages := []model.CrawledPage{
			{
				Title:    "[sos] Broker Corp",
				Markdown: "SIC Code: 6211", // Maps to 523110 via crosswalk
			},
		}
		result := ValidateAndCrossReferenceNAICS(answers, pages)

		naics := findAnswer(result, "naics_code")
		require.NotNil(t, naics)
		assert.Contains(t, naics.Source, "sos_sic_match")
		assert.Greater(t, naics.Confidence, 0.65)
	})

	t.Run("SoS keyword match boosts confidence", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "naics_code", Value: "236115", Confidence: 0.6, Tier: 1, Source: "website"},
		}
		pages := []model.CrawledPage{
			{
				Title:    "[sos] Build It LLC",
				Markdown: "Build It LLC is a licensed construction contractor.",
			},
		}
		result := ValidateAndCrossReferenceNAICS(answers, pages)

		naics := findAnswer(result, "naics_code")
		require.NotNil(t, naics)
		assert.Contains(t, naics.Source, "sos_keyword_match")
		assert.Greater(t, naics.Confidence, 0.6)
	})

	t.Run("SoS keyword mismatch penalizes confidence", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "naics_code", Value: "541512", Confidence: 0.8, Tier: 1, Source: "website"},
		}
		pages := []model.CrawledPage{
			{
				Title:    "[sos] Dental Office",
				Markdown: "A dental and medical practice serving patients.",
			},
		}
		result := ValidateAndCrossReferenceNAICS(answers, pages)

		naics := findAnswer(result, "naics_code")
		require.NotNil(t, naics)
		assert.Contains(t, naics.Source, "sos_keyword_mismatch")
		// Validation boosts valid code (×1.1 → 0.88), then SoS mismatch penalizes (×0.92 → 0.8096).
		// Net is slightly above original 0.8, but below what validation alone would give (0.88).
		assert.Less(t, naics.Confidence, 0.88)
	})

	t.Run("no NAICS answer is no-op", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "company_name", Value: "Acme Corp", Confidence: 0.9, Tier: 1},
		}
		result := ValidateAndCrossReferenceNAICS(answers, nil)
		assert.Len(t, result, 1)
	})

	t.Run("nil NAICS value is no-op", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "naics_code", Value: nil, Confidence: 0.5, Tier: 1},
		}
		result := ValidateAndCrossReferenceNAICS(answers, nil)
		assert.Len(t, result, 1)
	})

	t.Run("NAICS code with description text is cleaned", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "naics_code", Value: "541512 Computer Systems Design", Confidence: 0.7, Tier: 1, Source: "website"},
		}
		result := ValidateAndCrossReferenceNAICS(answers, nil)

		naics := findAnswer(result, "naics_code")
		require.NotNil(t, naics)
		assert.Equal(t, "541512", naics.Value)
		assert.Contains(t, naics.Source, "naics_validated")
	})

	t.Run("4-digit NAICS is normalized to 6 digits", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "naics_code", Value: "5415", Confidence: 0.7, Tier: 1, Source: "website"},
		}
		result := ValidateAndCrossReferenceNAICS(answers, nil)

		naics := findAnswer(result, "naics_code")
		require.NotNil(t, naics)
		assert.Equal(t, "541500", naics.Value)
	})

	t.Run("confidence boost capped at 0.95", func(t *testing.T) {
		answers := []model.ExtractionAnswer{
			{FieldKey: "naics_code", Value: "541512", Confidence: 0.92, Tier: 1, Source: "website"},
		}
		pages := []model.CrawledPage{
			{
				Title:    "[sos] Corp",
				Markdown: "NAICS Code: 541512",
			},
		}
		result := ValidateAndCrossReferenceNAICS(answers, pages)

		naics := findAnswer(result, "naics_code")
		require.NotNil(t, naics)
		assert.LessOrEqual(t, naics.Confidence, 0.95)
	})
}

func TestExtractIndustryKeywords(t *testing.T) {
	t.Run("finds insurance keyword", func(t *testing.T) {
		kw := extractIndustryKeywords("we are an insurance agency providing coverage")
		assert.Contains(t, kw, "insurance")
	})

	t.Run("finds multiple keywords", func(t *testing.T) {
		kw := extractIndustryKeywords("construction and roofing contractor")
		assert.Contains(t, kw, "construction")
		assert.Contains(t, kw, "roofing")
		assert.Contains(t, kw, "contractor")
	})

	t.Run("returns empty for irrelevant text", func(t *testing.T) {
		kw := extractIndustryKeywords("active good standing registered agent")
		assert.Empty(t, kw)
	})
}

func TestBoostConfidence(t *testing.T) {
	assert.InDelta(t, 0.85, boostConfidence(0.7, 0.15), 0.001)
	assert.InDelta(t, 0.95, boostConfidence(0.9, 0.15), 0.001) // Capped
	assert.InDelta(t, 0.95, boostConfidence(0.95, 0.1), 0.001) // Already at cap
	assert.InDelta(t, 0.65, boostConfidence(0.6, 0.05), 0.001)
}

// findAnswer returns a pointer to the answer with the given field key, or nil.
func findAnswer(answers []model.ExtractionAnswer, fieldKey string) *model.ExtractionAnswer {
	for i := range answers {
		if answers[i].FieldKey == fieldKey {
			return &answers[i]
		}
	}
	return nil
}
