package transform

import (
	"strings"
	"unicode"
)

// validNAICSSectors contains all valid 2-digit NAICS sector codes.
var validNAICSSectors = map[string]bool{
	"11": true, // Agriculture, Forestry, Fishing and Hunting
	"21": true, // Mining, Quarrying, and Oil and Gas Extraction
	"22": true, // Utilities
	"23": true, // Construction
	"31": true, // Manufacturing
	"32": true, // Manufacturing
	"33": true, // Manufacturing
	"42": true, // Wholesale Trade
	"44": true, // Retail Trade
	"45": true, // Retail Trade
	"48": true, // Transportation and Warehousing
	"49": true, // Transportation and Warehousing
	"51": true, // Information
	"52": true, // Finance and Insurance
	"53": true, // Real Estate and Rental and Leasing
	"54": true, // Professional, Scientific, and Technical Services
	"55": true, // Management of Companies and Enterprises
	"56": true, // Administrative and Support
	"61": true, // Educational Services
	"62": true, // Health Care and Social Assistance
	"71": true, // Arts, Entertainment, and Recreation
	"72": true, // Accommodation and Food Services
	"81": true, // Other Services (except Public Administration)
	"92": true, // Public Administration
}

// validNAICSSubsectors contains all valid 3-digit NAICS subsector codes.
// Union of 2017 and 2022 NAICS classifications for broad compatibility.
var validNAICSSubsectors = map[string]bool{
	// 11 - Agriculture
	"111": true, "112": true, "113": true, "114": true, "115": true,
	// 21 - Mining
	"211": true, "212": true, "213": true,
	// 22 - Utilities
	"221": true,
	// 23 - Construction
	"236": true, "237": true, "238": true,
	// 31-33 - Manufacturing
	"311": true, "312": true, "313": true, "314": true, "315": true, "316": true,
	"321": true, "322": true, "323": true, "324": true, "325": true, "326": true, "327": true,
	"331": true, "332": true, "333": true, "334": true, "335": true, "336": true, "337": true, "339": true,
	// 42 - Wholesale Trade
	"423": true, "424": true, "425": true,
	// 44-45 - Retail Trade (2017 codes)
	"441": true, "442": true, "443": true, "444": true, "445": true,
	"446": true, "447": true, "448": true, "449": true,
	"451": true, "452": true, "453": true, "454": true,
	// 44-45 - Retail Trade (2022 additions)
	"455": true, "456": true, "457": true, "458": true, "459": true,
	// 48-49 - Transportation
	"481": true, "482": true, "483": true, "484": true, "485": true,
	"486": true, "487": true, "488": true, "491": true, "492": true, "493": true,
	// 51 - Information
	"511": true, "512": true, "513": true, "515": true, "516": true,
	"517": true, "518": true, "519": true,
	// 52 - Finance and Insurance
	"521": true, "522": true, "523": true, "524": true, "525": true,
	// 53 - Real Estate
	"531": true, "532": true, "533": true,
	// 54 - Professional Services
	"541": true,
	// 55 - Management
	"551": true,
	// 56 - Administrative
	"561": true, "562": true,
	// 61 - Educational Services
	"611": true,
	// 62 - Health Care
	"621": true, "622": true, "623": true, "624": true,
	// 71 - Arts/Entertainment
	"711": true, "712": true, "713": true,
	// 72 - Accommodation
	"721": true, "722": true,
	// 81 - Other Services
	"811": true, "812": true, "813": true, "814": true,
	// 92 - Public Administration
	"921": true, "922": true, "923": true, "924": true, "925": true, "926": true, "927": true, "928": true,
}

// validNAICSIndustryGroups contains valid 4-digit NAICS industry group codes.
// Covers the most common groups encountered in enrichment.
var validNAICSIndustryGroups = map[string]bool{
	// Construction
	"2361": true, "2362": true, "2371": true, "2372": true, "2373": true,
	"2379": true, "2381": true, "2382": true, "2383": true, "2389": true,
	// Manufacturing (key groups)
	"3111": true, "3112": true, "3113": true, "3114": true, "3115": true, "3116": true, "3117": true, "3118": true, "3119": true,
	"3121": true, "3122": true,
	"3131": true, "3132": true, "3133": true,
	"3141": true, "3149": true,
	"3151": true, "3152": true, "3159": true,
	"3161": true, "3162": true, "3169": true,
	"3211": true, "3212": true, "3219": true,
	"3221": true, "3222": true,
	"3231": true,
	"3241": true,
	"3251": true, "3252": true, "3253": true, "3254": true, "3255": true, "3256": true, "3259": true,
	"3261": true, "3262": true,
	"3271": true, "3272": true, "3273": true, "3274": true, "3279": true,
	"3311": true, "3312": true, "3313": true, "3314": true, "3315": true,
	"3321": true, "3322": true, "3323": true, "3324": true, "3325": true, "3326": true, "3327": true, "3328": true, "3329": true,
	"3331": true, "3332": true, "3333": true, "3334": true, "3335": true, "3336": true, "3339": true,
	"3341": true, "3342": true, "3343": true, "3344": true, "3345": true, "3346": true,
	"3351": true, "3352": true, "3353": true, "3359": true,
	"3361": true, "3362": true, "3363": true, "3364": true, "3365": true, "3366": true, "3369": true,
	"3371": true, "3372": true, "3379": true,
	"3391": true, "3399": true,
	// Wholesale Trade
	"4231": true, "4232": true, "4233": true, "4234": true, "4235": true,
	"4236": true, "4237": true, "4238": true, "4239": true,
	"4241": true, "4242": true, "4243": true, "4244": true, "4245": true,
	"4246": true, "4247": true, "4248": true, "4249": true,
	"4251": true,
	// Retail Trade (2017)
	"4411": true, "4412": true, "4413": true,
	"4421": true, "4422": true,
	"4431": true,
	"4441": true, "4442": true,
	"4451": true, "4452": true, "4453": true,
	"4461": true,
	"4471": true,
	"4481": true, "4482": true, "4483": true,
	"4511": true, "4512": true,
	"4521": true, "4529": true,
	"4531": true, "4532": true, "4533": true, "4539": true,
	"4541": true, "4542": true,
	// Retail Trade (2022 additions)
	"4491": true,
	"4551": true, "4552": true, "4553": true,
	"4561": true,
	"4571": true,
	"4581": true, "4582": true, "4583": true,
	"4591": true, "4599": true,
	// Transportation
	"4811": true, "4812": true,
	"4821": true,
	"4831": true,
	"4841": true, "4842": true,
	"4851": true, "4852": true, "4853": true, "4854": true, "4855": true, "4859": true,
	"4861": true, "4862": true, "4869": true,
	"4871": true, "4872": true, "4879": true,
	"4881": true, "4882": true, "4883": true, "4884": true, "4885": true, "4889": true,
	"4911": true,
	"4921": true, "4922": true,
	"4931": true,
	// Information
	"5111": true, "5112": true,
	"5121": true, "5122": true,
	"5131": true, "5132": true,
	"5151": true, "5152": true,
	"5161": true,
	"5171": true, "5172": true, "5174": true, "5178": true, "5179": true,
	"5182": true,
	"5191": true,
	// Finance and Insurance
	"5211": true,
	"5221": true, "5222": true, "5223": true,
	"5231": true, "5232": true, "5239": true,
	"5241": true, "5242": true,
	"5251": true, "5259": true,
	// Real Estate
	"5311": true, "5312": true, "5313": true,
	"5321": true, "5322": true, "5323": true, "5324": true,
	"5331": true,
	// Professional Services
	"5411": true, "5412": true, "5413": true, "5414": true, "5415": true,
	"5416": true, "5417": true, "5418": true, "5419": true,
	// Management
	"5511": true,
	// Administrative
	"5611": true, "5612": true, "5613": true, "5614": true, "5615": true, "5616": true, "5617": true, "5619": true,
	"5621": true, "5622": true, "5629": true,
	// Education
	"6111": true, "6112": true, "6113": true, "6114": true, "6115": true, "6116": true, "6117": true,
	// Health Care
	"6211": true, "6212": true, "6213": true, "6214": true, "6215": true, "6216": true, "6219": true,
	"6221": true, "6222": true, "6223": true,
	"6231": true, "6232": true, "6233": true, "6239": true,
	"6241": true, "6242": true, "6243": true, "6244": true,
	// Arts/Entertainment
	"7111": true, "7112": true, "7113": true, "7114": true, "7115": true,
	"7121": true, "7131": true, "7132": true, "7139": true,
	// Accommodation/Food
	"7211": true, "7212": true, "7213": true,
	"7223": true, "7224": true, "7225": true,
	// Other Services
	"8111": true, "8112": true, "8113": true, "8114": true,
	"8121": true, "8122": true, "8123": true, "8129": true,
	"8131": true, "8132": true, "8133": true, "8134": true, "8139": true,
	"8141": true,
	// Public Administration
	"9211": true, "9221": true, "9231": true, "9241": true,
	"9251": true, "9261": true, "9271": true, "9281": true,
}

// NAICSValidationResult describes the outcome of validating a NAICS code.
type NAICSValidationResult struct {
	// InputCode is the raw code that was validated.
	InputCode string
	// NormalizedCode is the 6-digit zero-padded code.
	NormalizedCode string
	// Valid is true if the code passes structural and reference validation.
	Valid bool
	// SectorValid is true if the 2-digit sector prefix is valid.
	SectorValid bool
	// SubsectorValid is true if the 3-digit subsector prefix is valid.
	SubsectorValid bool
	// IndustryGroupValid is true if the 4-digit industry group prefix is valid.
	IndustryGroupValid bool
	// Title is the official title if known.
	Title string
	// ConfidenceAdjustment is the multiplier to apply to extraction confidence.
	// 1.0 = no change, >1.0 = boost (capped), <1.0 = penalty.
	ConfidenceAdjustment float64
	// Reason describes why the confidence was adjusted.
	Reason string
}

// IsValidNAICS checks whether a NAICS code is structurally valid and exists
// in the reference data. Accepts 2-6 digit codes.
func IsValidNAICS(code string) bool {
	code = strings.TrimSpace(code)
	if code == "" {
		return false
	}
	// Must be all digits.
	for _, r := range code {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	if len(code) < 2 || len(code) > 6 {
		return false
	}

	// Check sector.
	if !validNAICSSectors[code[:2]] {
		return false
	}
	// 2-digit codes are valid at sector level.
	if len(code) == 2 {
		return true
	}
	// Check subsector.
	if !validNAICSSubsectors[code[:3]] {
		return false
	}
	if len(code) == 3 {
		return true
	}
	// Check industry group if we have reference data for it.
	if _, ok := validNAICSIndustryGroups[code[:4]]; ok {
		return true
	}
	// If the 4-digit prefix isn't in our table, the 3-digit subsector
	// is still valid — we just can't confirm the specific industry group.
	// Return true with the understanding that confidence should be slightly lower.
	return len(code) >= 4
}

// ValidateNAICSCode performs comprehensive validation on a NAICS code and
// returns detailed results including confidence adjustments.
func ValidateNAICSCode(code string) NAICSValidationResult {
	result := NAICSValidationResult{
		InputCode:            code,
		ConfidenceAdjustment: 1.0,
	}

	code = strings.TrimSpace(code)
	// Strip trailing dashes and whitespace.
	code = strings.TrimRight(code, "- ")
	if code == "" {
		result.Reason = "empty code"
		return result
	}

	// Strip non-digit characters (LLMs sometimes include descriptions).
	cleaned := extractLeadingDigits(code)
	if cleaned == "" {
		result.Reason = "no digits found"
		return result
	}

	if len(cleaned) < 2 || len(cleaned) > 6 {
		result.Reason = "invalid length"
		return result
	}

	result.NormalizedCode = NormalizeNAICS(cleaned)

	// Validate sector (2-digit).
	sector := cleaned[:2]
	if validNAICSSectors[sector] {
		result.SectorValid = true
	} else {
		result.Reason = "invalid sector prefix"
		result.ConfidenceAdjustment = 0.3
		return result
	}

	// Validate subsector (3-digit).
	if len(cleaned) >= 3 {
		subsector := cleaned[:3]
		if validNAICSSubsectors[subsector] {
			result.SubsectorValid = true
		} else {
			result.Reason = "invalid subsector"
			result.ConfidenceAdjustment = 0.5
			result.Valid = false
			// Try to find closest valid subsector in same sector.
			return result
		}
	} else {
		result.SubsectorValid = true // 2-digit code, subsector N/A
	}

	// Validate industry group (4-digit).
	if len(cleaned) >= 4 {
		ig := cleaned[:4]
		if validNAICSIndustryGroups[ig] {
			result.IndustryGroupValid = true
			result.ConfidenceAdjustment = 1.1 // Boost for confirmed 4-digit
		} else {
			// 4-digit not in our table — subsector is valid but industry group unconfirmed.
			result.ConfidenceAdjustment = 0.95
			result.Reason = "industry group not in reference table"
		}
	}

	// Look up title.
	if t := NAICSTitle(cleaned); t != "" {
		result.Title = t
	} else if t := NAICSTitle(result.NormalizedCode); t != "" {
		result.Title = t
	}

	result.Valid = true
	if result.Reason == "" {
		result.Reason = "valid"
	}
	return result
}

// ClosestValidNAICS attempts to find the closest valid NAICS code when the
// input code is invalid. Returns the corrected code and a description, or
// empty strings if no correction is possible.
func ClosestValidNAICS(code string) (string, string) {
	code = strings.TrimSpace(code)
	cleaned := extractLeadingDigits(code)
	if len(cleaned) < 2 {
		return "", ""
	}

	// If sector is invalid, no correction possible.
	if !validNAICSSectors[cleaned[:2]] {
		return "", ""
	}

	// If subsector is invalid, return the 2-digit sector.
	if len(cleaned) >= 3 && !validNAICSSubsectors[cleaned[:3]] {
		sector := cleaned[:2]
		return NormalizeNAICS(sector), "corrected to sector level (" + NAICSTitle(sector) + ")"
	}

	// If industry group is unknown but subsector valid, return 3-digit.
	if len(cleaned) >= 4 {
		if !validNAICSIndustryGroups[cleaned[:4]] {
			sub := cleaned[:3]
			return NormalizeNAICS(sub), "corrected to subsector level"
		}
	}

	// Code appears valid — return normalized.
	return NormalizeNAICS(cleaned), ""
}

// extractLeadingDigits returns the leading digit sequence from a string.
// e.g., "541512 Computer Systems Design" → "541512"
func extractLeadingDigits(s string) string {
	var b strings.Builder
	for _, r := range s {
		if unicode.IsDigit(r) {
			b.WriteRune(r)
		} else {
			break
		}
	}
	return b.String()
}

// NAICSSectorForKeywords maps industry-related keywords to likely NAICS sectors.
// Used for cross-referencing SoS business classifications with extracted NAICS codes.
var NAICSSectorForKeywords = map[string][]string{
	"insurance":      {"52"},
	"banking":        {"52"},
	"bank":           {"52"},
	"financial":      {"52"},
	"credit union":   {"52"},
	"investment":     {"52", "53"},
	"real estate":    {"53"},
	"realty":         {"53"},
	"property":       {"53"},
	"construction":   {"23"},
	"building":       {"23", "53"},
	"contractor":     {"23"},
	"plumbing":       {"23"},
	"electrical":     {"23"},
	"hvac":           {"23"},
	"roofing":        {"23"},
	"manufacturing":  {"31", "32", "33"},
	"technology":     {"51", "54"},
	"software":       {"51"},
	"consulting":     {"54"},
	"engineering":    {"54"},
	"accounting":     {"54"},
	"legal":          {"54"},
	"law firm":       {"54"},
	"attorney":       {"54"},
	"healthcare":     {"62"},
	"medical":        {"62"},
	"hospital":       {"62"},
	"dental":         {"62"},
	"physician":      {"62"},
	"pharmacy":       {"44", "45"},
	"restaurant":     {"72"},
	"hotel":          {"72"},
	"hospitality":    {"72"},
	"education":      {"61"},
	"school":         {"61"},
	"university":     {"61"},
	"transportation": {"48", "49"},
	"trucking":       {"48"},
	"logistics":      {"48", "49"},
	"warehouse":      {"49"},
	"retail":         {"44", "45"},
	"wholesale":      {"42"},
	"agriculture":    {"11"},
	"farming":        {"11"},
	"mining":         {"21"},
	"oil":            {"21"},
	"gas":            {"21"},
	"utility":        {"22"},
	"electric":       {"22"},
	"staffing":       {"56"},
	"cleaning":       {"56"},
	"janitorial":     {"56"},
	"security":       {"56"},
	"automotive":     {"44", "81"},
	"auto repair":    {"81"},
	"salon":          {"81"},
	"barber":         {"81"},
	"nonprofit":      {"81", "92"},
}
