package pipeline

import (
	"fmt"
	"regexp"
	"strings"

	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/fedsync/transform"
	"github.com/sells-group/research-cli/internal/model"
)

// SoSIndustryInfo holds industry classification data extracted from a
// Secretary of State filing page.
type SoSIndustryInfo struct {
	// NAICSCode is a NAICS code found on the SoS page (rare but valuable).
	NAICSCode string
	// SICCode is a SIC code found on the SoS page.
	SICCode string
	// BusinessType is the entity classification (e.g., "LLC", "Corporation").
	BusinessType string
	// IndustryKeywords are industry-related keywords found in the filing.
	IndustryKeywords []string
	// InferredSectors are NAICS sectors inferred from keywords.
	InferredSectors []string
}

// naicsPattern matches 4-6 digit NAICS codes in text.
var naicsPattern = regexp.MustCompile(`(?i)(?:NAICS|naics)\s*(?:code)?[\s:]*(\d{4,6})`)

// sicPattern matches 4-digit SIC codes in text.
var sicPattern = regexp.MustCompile(`(?i)(?:SIC|sic)\s*(?:code)?[\s:]*(\d{4})`)

// businessTypePattern matches common business entity types in SoS pages.
var businessTypePattern = regexp.MustCompile(`(?i)(?:entity\s*type|business\s*type|organization\s*type|type\s*of\s*(?:entity|business|organization))[\s:]+([^\n]{3,50})`)

// industryPattern matches industry classification fields.
var industryPattern = regexp.MustCompile(`(?i)(?:industry|business\s*(?:category|classification|activity|purpose))[\s:]+([^\n]{3,80})`)

// ExtractSoSIndustryInfo parses a Secretary of State page's markdown content
// to extract any industry classification data: NAICS codes, SIC codes,
// business type, and industry keywords.
func ExtractSoSIndustryInfo(page model.CrawledPage) *SoSIndustryInfo {
	if page.Markdown == "" {
		return nil
	}
	// Only process SoS pages.
	if !strings.HasPrefix(page.Title, "[sos]") && page.Title != "" {
		// Check PageType classification as fallback.
		lower := strings.ToLower(page.Title)
		if !strings.Contains(lower, "secretary") && !strings.Contains(lower, "business entity") &&
			!strings.Contains(lower, "corporation") && !strings.Contains(lower, "sos") {
			return nil
		}
	}

	content := page.Markdown
	info := &SoSIndustryInfo{}
	found := false

	// Extract NAICS code.
	if m := naicsPattern.FindStringSubmatch(content); len(m) > 1 {
		info.NAICSCode = m[1]
		found = true
	}

	// Extract SIC code.
	if m := sicPattern.FindStringSubmatch(content); len(m) > 1 {
		info.SICCode = m[1]
		found = true
	}

	// Extract business type.
	if m := businessTypePattern.FindStringSubmatch(content); len(m) > 1 {
		info.BusinessType = strings.TrimSpace(m[1])
		found = true
	}

	// Extract industry keywords from classification fields.
	if m := industryPattern.FindStringSubmatch(content); len(m) > 1 {
		classification := strings.ToLower(strings.TrimSpace(m[1]))
		info.IndustryKeywords = extractIndustryKeywords(classification)
		found = true
	}

	// Also scan full content for industry keywords if none found yet.
	if len(info.IndustryKeywords) == 0 {
		info.IndustryKeywords = extractIndustryKeywords(strings.ToLower(content))
	}

	// Map keywords to NAICS sectors.
	sectorSet := make(map[string]bool)
	for _, kw := range info.IndustryKeywords {
		if sectors, ok := transform.NAICSSectorForKeywords[kw]; ok {
			for _, s := range sectors {
				sectorSet[s] = true
			}
		}
	}
	for s := range sectorSet {
		info.InferredSectors = append(info.InferredSectors, s)
	}

	if !found && len(info.IndustryKeywords) == 0 {
		return nil
	}
	return info
}

// extractIndustryKeywords finds industry-related keywords in text.
func extractIndustryKeywords(text string) []string {
	var found []string
	seen := make(map[string]bool)
	for keyword := range transform.NAICSSectorForKeywords {
		if strings.Contains(text, keyword) && !seen[keyword] {
			seen[keyword] = true
			found = append(found, keyword)
		}
	}
	return found
}

// ValidateAndCrossReferenceNAICS validates extracted NAICS codes against
// the official NAICS reference data and cross-references with Secretary of
// State filing data. It adjusts confidence scores based on validation results
// and multi-source agreement.
//
// This function should be called during Phase 7 (Aggregate) after MergeAnswers.
func ValidateAndCrossReferenceNAICS(answers []model.ExtractionAnswer, pages []model.CrawledPage) []model.ExtractionAnswer {
	// Find the NAICS answer.
	naicsIdx := -1
	for i, a := range answers {
		if a.FieldKey == "naics_code" {
			naicsIdx = i
			break
		}
	}
	if naicsIdx == -1 {
		return answers
	}

	naicsAnswer := &answers[naicsIdx]
	rawCode := fmt.Sprintf("%v", naicsAnswer.Value)
	if rawCode == "" || rawCode == "<nil>" {
		return answers
	}

	log := zap.L().With(zap.String("field", "naics_code"), zap.String("raw_code", rawCode))

	// Step 1: Validate the extracted NAICS code against reference data.
	validation := transform.ValidateNAICSCode(rawCode)

	if !validation.Valid {
		// Try to find closest valid code.
		corrected, reason := transform.ClosestValidNAICS(rawCode)
		if corrected != "" {
			log.Info("naics: corrected invalid code",
				zap.String("original", rawCode),
				zap.String("corrected", corrected),
				zap.String("reason", reason),
			)
			naicsAnswer.Value = corrected
			naicsAnswer.Confidence *= 0.7 // Penalty for correction
			naicsAnswer.Source += "+naics_corrected"
			// Re-validate with corrected code.
			validation = transform.ValidateNAICSCode(corrected)
		} else {
			log.Warn("naics: invalid code, no correction found",
				zap.String("code", rawCode),
				zap.String("reason", validation.Reason),
			)
			naicsAnswer.Confidence *= validation.ConfidenceAdjustment
			naicsAnswer.Source += "+naics_invalid"
			return answers
		}
	} else {
		// Apply confidence adjustment for valid codes.
		adj := validation.ConfidenceAdjustment
		if adj > 1.0 {
			// Boost capped: don't exceed 0.95.
			newConf := naicsAnswer.Confidence * adj
			if newConf > 0.95 {
				newConf = 0.95
			}
			naicsAnswer.Confidence = newConf
		} else if adj < 1.0 {
			naicsAnswer.Confidence *= adj
		}

		// Normalize the code to 6 digits.
		if validation.NormalizedCode != "" {
			naicsAnswer.Value = validation.NormalizedCode
		}

		if validation.Title != "" {
			naicsAnswer.Source += "+naics_validated"
		}
	}

	// Step 2: Cross-reference with SoS filing data.
	sosInfo := collectSoSIndustryInfo(pages)
	if sosInfo != nil {
		crossReferenceWithSoS(naicsAnswer, sosInfo, validation, log)
	}

	// Step 3: Cross-reference with pre-seeded data (from Grata CSV).
	// This is handled by the existing pre-seeded gap-fill in BuildFieldValues,
	// but we can boost confidence when the extracted code matches pre-seeded.
	// (No additional action needed here — BuildFieldValues already handles this.)

	return answers
}

// collectSoSIndustryInfo gathers industry info from any SoS pages.
func collectSoSIndustryInfo(pages []model.CrawledPage) *SoSIndustryInfo {
	for _, p := range pages {
		if strings.HasPrefix(p.Title, "[sos]") {
			if info := ExtractSoSIndustryInfo(p); info != nil {
				return info
			}
		}
	}
	return nil
}

// crossReferenceWithSoS compares the extracted NAICS code with SoS data
// and adjusts confidence accordingly.
func crossReferenceWithSoS(answer *model.ExtractionAnswer, sos *SoSIndustryInfo, _ transform.NAICSValidationResult, log *zap.Logger) {
	code := fmt.Sprintf("%v", answer.Value)
	sector := transform.NAICSToSector(code)

	// Direct NAICS match from SoS page.
	if sos.NAICSCode != "" {
		sosNorm := transform.NormalizeNAICS(sos.NAICSCode)
		extractedNorm := transform.NormalizeNAICS(code)
		if sosNorm == extractedNorm {
			// Exact match — strong confidence boost.
			answer.Confidence = boostConfidence(answer.Confidence, 0.15)
			answer.Source += "+sos_naics_match"
			log.Info("naics: SoS NAICS exact match",
				zap.String("naics", code),
			)
			return
		}
		// Sector-level match (first 2 digits agree).
		if len(sosNorm) >= 2 && len(extractedNorm) >= 2 && sosNorm[:2] == extractedNorm[:2] {
			answer.Confidence = boostConfidence(answer.Confidence, 0.05)
			answer.Source += "+sos_sector_match"
			log.Info("naics: SoS NAICS sector match",
				zap.String("extracted", code),
				zap.String("sos", sos.NAICSCode),
			)
			return
		}
		// Mismatch — penalize.
		answer.Confidence *= 0.85
		answer.Source += "+sos_naics_mismatch"
		log.Warn("naics: SoS NAICS mismatch",
			zap.String("extracted", code),
			zap.String("sos", sos.NAICSCode),
		)
		return
	}

	// SIC code cross-reference via SIC→NAICS crosswalk.
	if sos.SICCode != "" {
		crosswalked := transform.SICLookupNAICS(sos.SICCode)
		if crosswalked != "" {
			crossSector := transform.NAICSToSector(crosswalked)
			if crossSector == sector {
				answer.Confidence = boostConfidence(answer.Confidence, 0.08)
				answer.Source += "+sos_sic_match"
				log.Info("naics: SoS SIC→NAICS sector match",
					zap.String("sic", sos.SICCode),
					zap.String("crosswalked", crosswalked),
					zap.String("extracted_sector", sector),
				)
				return
			}
			// SIC crosswalk doesn't match — slight penalty.
			answer.Confidence *= 0.9
			answer.Source += "+sos_sic_mismatch"
			return
		}
	}

	// Keyword-based sector cross-reference.
	if len(sos.InferredSectors) > 0 {
		for _, s := range sos.InferredSectors {
			if s == sector {
				answer.Confidence = boostConfidence(answer.Confidence, 0.05)
				answer.Source += "+sos_keyword_match"
				log.Info("naics: SoS keyword sector match",
					zap.String("sector", sector),
					zap.Strings("keywords", sos.IndustryKeywords),
				)
				return
			}
		}
		// Keywords suggest a different sector — slight penalty.
		answer.Confidence *= 0.92
		answer.Source += "+sos_keyword_mismatch"
		log.Info("naics: SoS keyword sector mismatch",
			zap.String("extracted_sector", sector),
			zap.Strings("inferred_sectors", sos.InferredSectors),
		)
	}
}

// boostConfidence increases confidence by the given delta, capped at 0.95.
func boostConfidence(current, delta float64) float64 {
	boosted := current + delta
	if boosted > 0.95 {
		return 0.95
	}
	return boosted
}
