package pipeline

import (
	"strings"

	"github.com/sells-group/research-cli/internal/model"
)

// BusinessModel is a canonical business model category.
type BusinessModel = string

// Canonical business model constants used for taxonomy normalization.
const (
	BMServices          BusinessModel = "Services"
	BMManufacturer      BusinessModel = "Manufacturer"
	BMDistributor       BusinessModel = "Distributor"
	BMRetailer          BusinessModel = "Retailer"
	BMSaaS              BusinessModel = "SaaS"
	BMMarketplace       BusinessModel = "Marketplace"
	BMSoftware          BusinessModel = "Software"
	BMConstruction      BusinessModel = "Construction"
	BMFinancialServices BusinessModel = "Financial Services"
	BMHealthcare        BusinessModel = "Healthcare"
	BMOther             BusinessModel = "Other"
)

// canonicalModels is the set of all canonical business model labels.
var canonicalModels = map[string]BusinessModel{
	"services":           BMServices,
	"manufacturer":       BMManufacturer,
	"distributor":        BMDistributor,
	"retailer":           BMRetailer,
	"saas":               BMSaaS,
	"marketplace":        BMMarketplace,
	"software":           BMSoftware,
	"construction":       BMConstruction,
	"financial services": BMFinancialServices,
	"healthcare":         BMHealthcare,
	"other":              BMOther,
}

// businessModelKeywords maps lowercase keywords/phrases to canonical categories.
// Entries are checked by longest-match-first to avoid "financial services"
// incorrectly matching "services".
var businessModelKeywords = map[string]BusinessModel{
	// Services variants
	"services":             BMServices,
	"service provider":     BMServices,
	"consulting":           BMServices,
	"consultant":           BMServices,
	"professional service": BMServices,
	"staffing":             BMServices,
	"agency":               BMServices,
	"advisory":             BMServices,

	// Manufacturing variants
	"manufacturer":  BMManufacturer,
	"manufacturing": BMManufacturer,
	"fabricat":      BMManufacturer,

	// Distribution variants
	"distributor":  BMDistributor,
	"wholesale":    BMDistributor,
	"distribution": BMDistributor,

	// Retail variants
	"retailer":   BMRetailer,
	"e-commerce": BMRetailer,
	"ecommerce":  BMRetailer,
	"retail":     BMRetailer,

	// SaaS variants
	"saas":           BMSaaS,
	"subscription":   BMSaaS,
	"cloud platform": BMSaaS,

	// Marketplace variants
	"marketplace": BMMarketplace,
	"platform":    BMMarketplace,

	// Software variants
	"software":     BMSoftware,
	"technology":   BMSoftware,
	"tech company": BMSoftware,

	// Construction variants
	"construction":       BMConstruction,
	"general contractor": BMConstruction,
	"contractor":         BMConstruction,
	"builder":            BMConstruction,

	// Healthcare variants
	"healthcare": BMHealthcare,
	"medical":    BMHealthcare,
	"health":     BMHealthcare,
	"pharma":     BMHealthcare,
	"biotech":    BMHealthcare,
	"clinical":   BMHealthcare,

	// Financial Services variants
	"financial advisor": BMFinancialServices,
	"wealth management": BMFinancialServices,
	"investment":        BMFinancialServices,
	"insurance":         BMFinancialServices,
	"banking":           BMFinancialServices,
	"fintech":           BMFinancialServices,
	"financial":         BMFinancialServices,
}

// NormalizeBusinessModel maps any business model string to its canonical form.
// Returns the canonical label and true if a match was found, or ("Other", false)
// for unknown inputs.
func NormalizeBusinessModel(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}

	lower := strings.ToLower(raw)

	// 1. Exact canonical match.
	if canon, ok := canonicalModels[lower]; ok {
		return canon, true
	}

	// 2. Longest keyword match — iterate all keywords, pick the longest
	// matching key to avoid "financial services" losing to "services".
	bestKey := ""
	bestModel := BusinessModel("")
	for kw, bm := range businessModelKeywords {
		if strings.Contains(lower, kw) && len(kw) > len(bestKey) {
			bestKey = kw
			bestModel = bm
		}
	}
	if bestKey != "" {
		return bestModel, true
	}

	// 3. Fallback.
	return BMOther, false
}

// NormalizeBusinessModelAnswer normalizes any business_model answer in the
// extraction answer slice. Appends "+bm_normalized" to Source when a value
// is changed for provenance tracking.
func NormalizeBusinessModelAnswer(answers []model.ExtractionAnswer) []model.ExtractionAnswer {
	for i, a := range answers {
		if a.FieldKey != "business_model" {
			continue
		}
		raw, ok := a.Value.(string)
		if !ok || raw == "" {
			continue
		}
		normalized, matched := NormalizeBusinessModel(raw)
		if !matched {
			continue
		}
		if normalized != raw {
			answers[i].Value = normalized
			answers[i].Source += "+bm_normalized"
		}
	}
	return answers
}
