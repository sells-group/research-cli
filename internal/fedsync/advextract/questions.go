package advextract

// Question defines an M&A-focused extraction question for ADV documents.
type Question struct {
	Key              string   // unique identifier (snake_case)
	Text             string   // the question to ask the LLM
	Tier             int      // 1=Haiku, 2=Sonnet
	Category         string   // A-N category code
	Scope            string   // "advisor" or "fund"
	SourceDocs       []string // which docs to use: "part1", "part2", "part3"
	SourceSections   []string // brochure items to route to (e.g., "item_4", "item_5")
	StructuredBypass bool     // true = answer from Part 1 data directly, no LLM
	OutputFormat     string   // expected JSON output format hint
}

// Scope constants.
const (
	ScopeAdvisor = "advisor"
	ScopeFund    = "fund"
)

// Category constants.
const (
	CatFirmIdentity = "A" // Firm Identity & Structure
	CatAUMGrowth    = "B" // AUM & Growth Trajectory
	CatInvestment   = "C" // Investment Strategy & Philosophy
	CatFees         = "D" // Fee Structure & Revenue Model
	CatClients      = "E" // Client Demographics
	CatCompliance   = "F" // Compliance & Regulatory
	CatOperations   = "G" // Operations & Technology
	CatPersonnel    = "H" // Personnel & Culture
	CatFundDetail   = "I" // Fund-Level Detail
	CatConflicts    = "J" // Conflicts of Interest
	CatGrowth       = "K" // Growth & Business Development
	CatCRS          = "L" // CRS-Specific
	CatCrossDoc     = "M" // Cross-Document Synthesis
	CatSynthesis    = "N" // Sonnet Synthesis & Assessment
)

// AllQuestions returns all M&A-focused extraction questions.
func AllQuestions() []Question {
	return allQuestions
}

// QuestionsByTier returns questions filtered by tier.
func QuestionsByTier(tier int) []Question {
	var out []Question
	for _, q := range allQuestions {
		if q.Tier == tier {
			out = append(out, q)
		}
	}
	return out
}

// QuestionsByScope returns questions filtered by scope.
func QuestionsByScope(scope string) []Question {
	var out []Question
	for _, q := range allQuestions {
		if q.Scope == scope {
			out = append(out, q)
		}
	}
	return out
}

// StructuredBypassQuestions returns questions that can be answered from Part 1 data.
func StructuredBypassQuestions() []Question {
	var out []Question
	for _, q := range allQuestions {
		if q.StructuredBypass {
			out = append(out, q)
		}
	}
	return out
}

// QuestionMap returns all questions keyed by question key.
func QuestionMap() map[string]Question {
	m := make(map[string]Question, len(allQuestions))
	for _, q := range allQuestions {
		m[q.Key] = q
	}
	return m
}

var allQuestions = []Question{

	// =========================================================================
	// Layer 0: Structured Bypass — Part 1 Data (29 questions)
	// Tier 1, StructuredBypass=true, SourceDocs=["part1"], SourceSections=[]
	// =========================================================================

	// --- Firm Identity (A) ---
	{
		Key: "office_locations", Text: "List all office locations including principal office and branch offices.",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "json",
	},
	{
		Key: "regulatory_status", Text: "What is the firm's regulatory registration status (SEC registered, state registered, exempt reporting)?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "string",
	},
	{
		Key: "key_regulatory_registrations", Text: "List all regulatory registrations and key compliance registrations (SEC, state, other).",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "json",
	},

	// --- AUM & Growth (B) ---
	{
		Key: "aum_current", Text: "What is the current AUM total, discretionary, and non-discretionary?",
		Tier: 1, Category: CatAUMGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "json",
	},
	{
		Key: "aum_discretionary_split", Text: "What percentage of AUM is managed on a discretionary vs non-discretionary basis?",
		Tier: 1, Category: CatAUMGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "json",
	},
	{
		Key: "avg_account_size", Text: "What is the average account size (AUM / number of accounts)?",
		Tier: 1, Category: CatAUMGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "number",
	},
	{
		Key: "aum_per_employee", Text: "What is the AUM per employee ratio?",
		Tier: 1, Category: CatAUMGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "number",
	},
	{
		Key: "aum_per_adviser_rep", Text: "What is the AUM per adviser representative ratio?",
		Tier: 1, Category: CatAUMGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "number",
	},
	{
		Key: "discretionary_pct", Text: "What is the discretionary AUM as a percentage of total AUM?",
		Tier: 1, Category: CatAUMGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "number",
	},

	// --- Fees (D) ---
	{
		Key: "compensation_types", Text: "What forms of compensation does the firm receive (% AUM, hourly, fixed, commissions, performance)?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "json",
	},
	{
		Key: "has_wrap_fee", Text: "Does the firm sponsor or participate in a wrap fee program?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "boolean",
	},
	{
		Key: "wrap_fee_aum", Text: "What is the wrap fee program regulatory AUM dollar amount?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "number",
	},
	{
		Key: "has_performance_fees", Text: "Does the firm receive compensation based on performance?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "boolean",
	},

	// --- Clients (E) ---
	{
		Key: "client_types_breakdown", Text: "Provide the client type breakdown by count and AUM (individuals, HNW, institutions, etc.).",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "json",
	},
	{
		Key: "hnw_concentration", Text: "What is the concentration of high-net-worth clients (>$1M) as a percentage of total clients and AUM?",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "json",
	},
	{
		Key: "institutional_vs_retail", Text: "What is the institutional vs retail client mix by count and AUM?",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "json",
	},
	{
		Key: "total_client_count", Text: "What is the total number of client accounts?",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "integer",
	},

	// --- Compliance (F) ---
	{
		Key: "disciplinary_history", Text: "Does the firm or any advisory affiliate have disciplinary history? Summarize any DRP disclosures.",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "string",
	},
	{
		Key: "cross_trading_practices", Text: "Does the firm engage in cross-trading or principal transactions?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "boolean",
	},
	{
		Key: "has_custody", Text: "Does the firm have custody of client assets based on Part 1 custody flags?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "boolean",
	},
	{
		Key: "regulatory_change_of_control", Text: "What regulatory registrations are currently active for this adviser?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "json",
	},

	// --- Operations (G) ---
	{
		Key: "total_headcount", Text: "What is the total number of employees including advisory and non-advisory?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "integer",
	},
	{
		Key: "other_business_activities", Text: "List all active other-business-activity flags from Part 1.",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "json",
	},
	{
		Key: "financial_affiliations", Text: "List all active financial industry affiliation flags from Part 1.",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "json",
	},

	// --- Fund-Level Bypass (I) ---
	{
		Key: "fund_aum", Text: "What is the fund's gross asset value (GAV) and net asset value (NAV)?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "json",
	},
	{
		Key: "fund_type_detail", Text: "What type of fund is this (hedge fund, PE fund, venture fund, real estate fund, fund of funds)?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "string",
	},
	{
		Key: "fund_regulatory_status", Text: "What is the fund's regulatory status and exemptions relied upon?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "string",
	},
	{
		Key: "total_fund_count", Text: "What is the total count of private funds managed by this adviser?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "integer",
	},
	{
		Key: "total_fund_gav", Text: "What is the sum of gross asset values across all private funds?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true, OutputFormat: "number",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Item 4: Advisory Business (20 questions)
	// Category A, Tier 1, SourceDocs=["part2"], SourceSections=[SectionAdvisoryBiz]
	// =========================================================================
	{
		Key: "year_began_advisory", Text: "What year did the firm begin providing investment advisory services?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz},
		OutputFormat: "integer",
	},
	{
		Key: "offers_financial_planning", Text: "Does the firm offer financial planning services?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "offers_portfolio_mgmt", Text: "Does the firm offer portfolio management services?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "offers_pension_consulting", Text: "Does the firm offer pension consulting services?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "offers_wealth_mgmt", Text: "Does the firm offer wealth management or comprehensive wealth advisory?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "offers_retirement_planning", Text: "Does the firm offer retirement planning services?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "offers_estate_planning", Text: "Does the firm offer estate planning services?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "offers_tax_planning", Text: "Does the firm offer tax planning or preparation services?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "offers_insurance_advisory", Text: "Does the firm offer insurance advisory or brokerage?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "uses_model_portfolios", Text: "Does the firm use model portfolios or model-based investment management?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "tamp_platform", Text: "What TAMP or model marketplace platform does the firm use (e.g., Envestnet, Orion, AssetMark)? Return null if none.",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz},
		OutputFormat: "string",
	},
	{
		Key: "firm_specialization", Text: "What is the firm's primary specialization or niche (e.g., physicians, tech executives, retirees)?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz},
		OutputFormat: "string",
	},
	{
		Key: "manages_erisa_plans", Text: "Does the firm manage ERISA plan assets?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "participates_wrap_programs", Text: "Does the firm sponsor or participate in wrap fee programs?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "sub_adviser_to_funds", Text: "Does the firm serve as a sub-adviser to other investment advisers or funds?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "num_financial_plans_annual", Text: "Approximately how many financial plans did the firm provide in the last year?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz},
		OutputFormat: "integer",
	},
	{
		Key: "has_proprietary_funds", Text: "Does the firm manage or advise proprietary funds or products?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "has_client_portal", Text: "Does the firm offer clients online access or a client portal?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "outsources_investment_mgmt", Text: "Does the firm outsource any investment management to third parties?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "firm_description_short", Text: "In one sentence, how does the firm describe its advisory business?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz},
		OutputFormat: "string",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Item 5: Fees and Compensation (18 questions)
	// Category D, Tier 1, SourceDocs=["part2"], SourceSections=[SectionFees]
	// =========================================================================
	{
		Key: "fee_schedule_aum_tiers", Text: "Extract the complete AUM-based fee schedule as JSON: [{min_aum, max_aum, annual_rate_pct}].",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees},
		OutputFormat: "json",
	},
	{
		Key: "max_fee_rate_pct", Text: "What is the highest AUM-based advisory fee rate charged (as annual percentage)?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees},
		OutputFormat: "number",
	},
	{
		Key: "min_fee_rate_pct", Text: "What is the lowest AUM-based advisory fee rate for the largest accounts?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees},
		OutputFormat: "number",
	},
	{
		Key: "charges_hourly_fees", Text: "Does the firm charge hourly fees?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees},
		OutputFormat: "boolean",
	},
	{
		Key: "hourly_fee_range", Text: "What is the hourly fee range (e.g., \"$150-$400/hour\")? Return null if no hourly fees.",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees},
		OutputFormat: "string",
	},
	{
		Key: "charges_fixed_fees", Text: "Does the firm charge fixed or flat fees for services?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees},
		OutputFormat: "boolean",
	},
	{
		Key: "fixed_fee_range", Text: "What is the fixed fee range? Return null if none.",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees},
		OutputFormat: "string",
	},
	{
		Key: "charges_financial_plan_fee", Text: "Does the firm charge a separate fee for financial plans?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees},
		OutputFormat: "boolean",
	},
	{
		Key: "financial_plan_fee", Text: "What is the financial planning fee amount or range? Return null if not disclosed.",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees},
		OutputFormat: "string",
	},
	{
		Key: "billing_frequency", Text: "How often are advisory fees billed (monthly, quarterly, annually)?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees},
		OutputFormat: "string",
	},
	{
		Key: "billing_method", Text: "Are fees billed in advance or in arrears?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees},
		OutputFormat: "string",
	},
	{
		Key: "fees_negotiable", Text: "Does the firm state that fees are negotiable?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees},
		OutputFormat: "boolean",
	},
	{
		Key: "refund_policy_on_termination", Text: "What is the refund policy if a client terminates mid-billing period?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees},
		OutputFormat: "string",
	},
	{
		Key: "termination_notice_period", Text: "What notice period is required to terminate the advisory agreement?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees},
		OutputFormat: "string",
	},
	{
		Key: "termination_fee", Text: "Is there a termination fee or penalty?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees},
		OutputFormat: "boolean",
	},
	{
		Key: "other_client_costs", Text: "What other costs do clients pay beyond advisory fees (custodian fees, fund expenses, transaction costs)?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees},
		OutputFormat: "string",
	},
	{
		Key: "bundled_vs_unbundled", Text: "Are fees bundled (all-inclusive) or unbundled (separate charges for different services)?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees},
		OutputFormat: "string",
	},
	{
		Key: "deducts_fees_from_accounts", Text: "Does the firm deduct fees directly from client accounts?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees},
		OutputFormat: "boolean",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Item 6: Performance Fees (5 questions)
	// Category D, Tier 1, SourceDocs=["part2"], SourceSections=[SectionPerformanceFees]
	// =========================================================================
	{
		Key: "has_performance_fee_detail", Text: "Does the firm charge performance-based fees or incentive allocations?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionPerformanceFees},
		OutputFormat: "boolean",
	},
	{
		Key: "performance_fee_rate", Text: "What is the performance fee rate or carried interest percentage?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionPerformanceFees},
		OutputFormat: "string",
	},
	{
		Key: "has_hurdle_rate", Text: "Is there a hurdle rate or preferred return?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionPerformanceFees},
		OutputFormat: "boolean",
	},
	{
		Key: "hurdle_rate_value", Text: "What is the hurdle rate or preferred return percentage?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionPerformanceFees},
		OutputFormat: "string",
	},
	{
		Key: "has_high_water_mark", Text: "Is there a high-water mark provision?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionPerformanceFees},
		OutputFormat: "boolean",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Item 7: Types of Clients (8 questions)
	// Category E, Tier 1, SourceDocs=["part2"], SourceSections=[SectionClientTypes]
	// =========================================================================
	{
		Key: "minimum_account_size", Text: "What is the minimum account size in dollars? Return 0 if no minimum.",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionClientTypes},
		OutputFormat: "integer",
	},
	{
		Key: "minimum_investment_amount", Text: "What is the minimum initial investment amount?",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionClientTypes},
		OutputFormat: "integer",
	},
	{
		Key: "waives_minimum", Text: "Does the firm waive the account minimum under any circumstances?",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionClientTypes},
		OutputFormat: "boolean",
	},
	{
		Key: "waiver_conditions", Text: "Under what conditions will the firm waive the account minimum?",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionClientTypes},
		OutputFormat: "string",
	},
	{
		Key: "accepts_individual_clients", Text: "Does the firm accept individual (non-HNW) clients?",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionClientTypes},
		OutputFormat: "boolean",
	},
	{
		Key: "targets_hnw", Text: "Does the firm target high-net-worth individuals?",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionClientTypes},
		OutputFormat: "boolean",
	},
	{
		Key: "hnw_threshold", Text: "What dollar threshold does the firm use to define HNW clients?",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionClientTypes},
		OutputFormat: "integer",
	},
	{
		Key: "serves_institutional", Text: "Does the firm serve institutional clients (pensions, endowments, foundations)?",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionClientTypes},
		OutputFormat: "boolean",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Item 8: Methods of Analysis & Investment (15 questions)
	// Category C, Tier 1, SourceDocs=["part2"], SourceSections=[SectionInvestment]
	// =========================================================================
	{
		Key: "uses_fundamental_analysis", Text: "Does the firm use fundamental analysis?",
		Tier: 1, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
		OutputFormat: "boolean",
	},
	{
		Key: "uses_technical_analysis", Text: "Does the firm use technical analysis?",
		Tier: 1, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
		OutputFormat: "boolean",
	},
	{
		Key: "uses_quantitative_analysis", Text: "Does the firm use quantitative or algorithmic methods?",
		Tier: 1, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
		OutputFormat: "boolean",
	},
	{
		Key: "uses_passive_indexing", Text: "Does the firm use passive or index-based investment strategies?",
		Tier: 1, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
		OutputFormat: "boolean",
	},
	{
		Key: "uses_active_management", Text: "Does the firm use active management?",
		Tier: 1, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
		OutputFormat: "boolean",
	},
	{
		Key: "uses_tactical_allocation", Text: "Does the firm use tactical asset allocation or market timing?",
		Tier: 1, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
		OutputFormat: "boolean",
	},
	{
		Key: "invests_equities", Text: "Does the firm invest client assets in equities/stocks?",
		Tier: 1, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
		OutputFormat: "boolean",
	},
	{
		Key: "invests_fixed_income", Text: "Does the firm invest in fixed income/bonds?",
		Tier: 1, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
		OutputFormat: "boolean",
	},
	{
		Key: "invests_mutual_funds", Text: "Does the firm invest in mutual funds?",
		Tier: 1, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
		OutputFormat: "boolean",
	},
	{
		Key: "invests_etfs", Text: "Does the firm invest in ETFs?",
		Tier: 1, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
		OutputFormat: "boolean",
	},
	{
		Key: "invests_alternatives", Text: "Does the firm invest in alternative investments (hedge funds, PE, real assets)?",
		Tier: 1, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
		OutputFormat: "boolean",
	},
	{
		Key: "invests_options", Text: "Does the firm use options or derivatives?",
		Tier: 1, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
		OutputFormat: "boolean",
	},
	{
		Key: "invests_private_placements", Text: "Does the firm invest in private placements?",
		Tier: 1, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
		OutputFormat: "boolean",
	},
	{
		Key: "esg_investing", Text: "Does the firm offer ESG, SRI, or impact investing?",
		Tier: 1, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
		OutputFormat: "boolean",
	},
	{
		Key: "primary_investment_approach", Text: "In one sentence, what is the firm's primary investment approach?",
		Tier: 1, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
		OutputFormat: "string",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Item 9: Disciplinary (3 questions)
	// Category F, Tier 1, SourceDocs=["part2"], SourceSections=[SectionDisciplinary]
	// =========================================================================
	{
		Key: "discloses_disciplinary_events", Text: "Does the brochure disclose any disciplinary events?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionDisciplinary},
		OutputFormat: "boolean",
	},
	{
		Key: "disciplinary_event_count", Text: "How many disciplinary events are disclosed? Return 0 if none.",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionDisciplinary},
		OutputFormat: "integer",
	},
	{
		Key: "disciplinary_summary", Text: "Briefly summarize any disclosed disciplinary events. Return null if none.",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionDisciplinary},
		OutputFormat: "string",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Item 10: Other Financial Industry Activities (8 questions)
	// Category J, Tier 1, SourceDocs=["part2"], SourceSections=[SectionAffiliations]
	// =========================================================================
	{
		Key: "affiliated_with_broker_dealer", Text: "Is the firm or its personnel affiliated with a broker-dealer?",
		Tier: 1, Category: CatConflicts, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAffiliations},
		OutputFormat: "boolean",
	},
	{
		Key: "affiliated_broker_dealer_name", Text: "What is the name of the affiliated broker-dealer? Return null if none.",
		Tier: 1, Category: CatConflicts, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAffiliations},
		OutputFormat: "string",
	},
	{
		Key: "has_insurance_licenses", Text: "Do any firm personnel hold insurance licenses?",
		Tier: 1, Category: CatConflicts, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAffiliations},
		OutputFormat: "boolean",
	},
	{
		Key: "has_cpa_affiliates", Text: "Is the firm affiliated with an accounting or CPA firm?",
		Tier: 1, Category: CatConflicts, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAffiliations},
		OutputFormat: "boolean",
	},
	{
		Key: "has_law_firm_affiliation", Text: "Is the firm affiliated with a law firm?",
		Tier: 1, Category: CatConflicts, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAffiliations},
		OutputFormat: "boolean",
	},
	{
		Key: "has_real_estate_affiliation", Text: "Is the firm involved in real estate brokerage or development?",
		Tier: 1, Category: CatConflicts, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAffiliations},
		OutputFormat: "boolean",
	},
	{
		Key: "personnel_outside_business", Text: "Do advisory personnel have outside business activities?",
		Tier: 1, Category: CatConflicts, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAffiliations},
		OutputFormat: "boolean",
	},
	{
		Key: "key_outside_activities", Text: "What are the key outside business activities of firm personnel? Return null if none.",
		Tier: 1, Category: CatConflicts, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAffiliations},
		OutputFormat: "string",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Item 11: Code of Ethics (5 questions)
	// Category F, Tier 1, SourceDocs=["part2"], SourceSections=[SectionCodeOfEthics]
	// =========================================================================
	{
		Key: "has_code_of_ethics", Text: "Does the firm maintain a code of ethics?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionCodeOfEthics},
		OutputFormat: "boolean",
	},
	{
		Key: "personal_trading_restrictions", Text: "Does the code of ethics restrict personal securities trading by employees?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionCodeOfEthics},
		OutputFormat: "boolean",
	},
	{
		Key: "pre_clearance_required", Text: "Are employees required to pre-clear personal trades?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionCodeOfEthics},
		OutputFormat: "boolean",
	},
	{
		Key: "holds_reportable_securities", Text: "Does the firm or its personnel hold reportable securities positions that could conflict with client interests?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionCodeOfEthics},
		OutputFormat: "boolean",
	},
	{
		Key: "insider_trading_policy", Text: "Does the firm have an insider trading prevention policy?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionCodeOfEthics},
		OutputFormat: "boolean",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Item 12: Brokerage Practices (10 questions)
	// Category G, Tier 1, SourceDocs=["part2"], SourceSections=[SectionBrokerage]
	// =========================================================================
	{
		Key: "primary_custodian", Text: "Who is the firm's primary custodian?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionBrokerage},
		OutputFormat: "string",
	},
	{
		Key: "secondary_custodians", Text: "List any other custodians used besides the primary. Return null if only one.",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionBrokerage},
		OutputFormat: "string",
	},
	{
		Key: "uses_soft_dollars", Text: "Does the firm use soft dollar arrangements?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionBrokerage},
		OutputFormat: "boolean",
	},
	{
		Key: "soft_dollar_services", Text: "What services does the firm receive through soft dollar arrangements? Return null if none.",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionBrokerage},
		OutputFormat: "string",
	},
	{
		Key: "best_execution_policy", Text: "Does the firm have a best execution review policy?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionBrokerage},
		OutputFormat: "boolean",
	},
	{
		Key: "aggregates_trades", Text: "Does the firm aggregate client orders (block trading)?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionBrokerage},
		OutputFormat: "boolean",
	},
	{
		Key: "directs_brokerage", Text: "Do clients direct the firm to use specific broker-dealers?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionBrokerage},
		OutputFormat: "boolean",
	},
	{
		Key: "receives_referrals_from_custodian", Text: "Does the firm receive client referrals from its custodian?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionBrokerage},
		OutputFormat: "boolean",
	},
	{
		Key: "custodian_revenue_sharing", Text: "Does the firm receive revenue sharing or service fee waivers from its custodian?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionBrokerage},
		OutputFormat: "boolean",
	},
	{
		Key: "trade_allocation_method", Text: "How are trades allocated among client accounts?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionBrokerage},
		OutputFormat: "string",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Item 13: Review of Accounts (5 questions)
	// Category G, Tier 1, SourceDocs=["part2"], SourceSections=[SectionReviewAccounts]
	// =========================================================================
	{
		Key: "account_review_frequency", Text: "How frequently are client accounts reviewed?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionReviewAccounts},
		OutputFormat: "string",
	},
	{
		Key: "review_triggers", Text: "What events trigger additional account reviews?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionReviewAccounts},
		OutputFormat: "string",
	},
	{
		Key: "primary_reviewer_title", Text: "What is the title/role of the person who reviews client accounts?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionReviewAccounts},
		OutputFormat: "string",
	},
	{
		Key: "provides_written_reports", Text: "Does the firm provide written reports to clients?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionReviewAccounts},
		OutputFormat: "boolean",
	},
	{
		Key: "report_frequency", Text: "How often are written reports provided to clients?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionReviewAccounts},
		OutputFormat: "string",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Item 14: Client Referrals (6 questions)
	// Category K, Tier 1, SourceDocs=["part2"], SourceSections=[SectionReferrals]
	// =========================================================================
	{
		Key: "pays_for_referrals", Text: "Does the firm pay for client referrals?",
		Tier: 1, Category: CatGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionReferrals},
		OutputFormat: "boolean",
	},
	{
		Key: "referral_fee_structure", Text: "What is the compensation structure for referrals? Return null if none.",
		Tier: 1, Category: CatGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionReferrals},
		OutputFormat: "string",
	},
	{
		Key: "receives_referral_income", Text: "Does the firm receive income for referring clients to other service providers?",
		Tier: 1, Category: CatGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionReferrals},
		OutputFormat: "boolean",
	},
	{
		Key: "has_solicitor_agreements", Text: "Does the firm have solicitor or promoter agreements?",
		Tier: 1, Category: CatGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionReferrals},
		OutputFormat: "boolean",
	},
	{
		Key: "solicitor_compensation_type", Text: "How are solicitors compensated (cash, revenue share, flat fee)? Return null if none.",
		Tier: 1, Category: CatGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionReferrals},
		OutputFormat: "string",
	},
	{
		Key: "receives_other_economic_benefits", Text: "Does the firm receive other economic benefits from non-clients?",
		Tier: 1, Category: CatGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionReferrals},
		OutputFormat: "boolean",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Item 15: Custody (4 questions)
	// Category F, Tier 1, SourceDocs=["part2"], SourceSections=[SectionCustody]
	// =========================================================================
	{
		Key: "deemed_custody", Text: "Does the firm have deemed custody (e.g., ability to debit fees, standing letters of authorization)?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionCustody},
		OutputFormat: "boolean",
	},
	{
		Key: "qualified_custodian_name", Text: "Who is the qualified custodian?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionCustody},
		OutputFormat: "string",
	},
	{
		Key: "receives_account_statements", Text: "Do clients receive account statements directly from the custodian?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionCustody},
		OutputFormat: "boolean",
	},
	{
		Key: "surprise_audit_firm", Text: "What firm conducts the annual surprise audit (if applicable)? Return null if no surprise audit.",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionCustody},
		OutputFormat: "string",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Item 16: Investment Discretion (3 questions)
	// Category G, Tier 1, SourceDocs=["part2"], SourceSections=[SectionDiscretion]
	// =========================================================================
	{
		Key: "has_discretionary_authority", Text: "Does the firm have discretionary investment authority?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionDiscretion},
		OutputFormat: "boolean",
	},
	{
		Key: "discretion_limitations", Text: "Are there any limitations on the firm's discretionary authority? Return null if no limitations.",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionDiscretion},
		OutputFormat: "string",
	},
	{
		Key: "clients_may_restrict", Text: "May clients impose restrictions on investing in certain securities?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionDiscretion},
		OutputFormat: "boolean",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Item 17: Voting Client Securities (3 questions)
	// Category F, Tier 1, SourceDocs=["part2"], SourceSections=[SectionProxyVoting]
	// =========================================================================
	{
		Key: "votes_client_proxies", Text: "Does the firm vote proxies on behalf of clients?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionProxyVoting},
		OutputFormat: "boolean",
	},
	{
		Key: "proxy_voting_policy_summary", Text: "Briefly describe the proxy voting policy. Return null if firm does not vote proxies.",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionProxyVoting},
		OutputFormat: "string",
	},
	{
		Key: "clients_may_direct_votes", Text: "May clients direct the firm on how to vote?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionProxyVoting},
		OutputFormat: "boolean",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Item 18: Financial Information (8 questions)
	// Category F, Tier 1, SourceDocs=["part2"], SourceSections=[SectionFinancialInfo]
	// =========================================================================
	{
		Key: "has_balance_sheet_requirement", Text: "Is the firm required to provide a balance sheet?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFinancialInfo},
		OutputFormat: "boolean",
	},
	{
		Key: "has_financial_condition_issues", Text: "Are there any financial conditions that could impair the firm's ability to meet commitments?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFinancialInfo},
		OutputFormat: "boolean",
	},
	{
		Key: "has_been_bankrupt", Text: "Has the firm been the subject of a bankruptcy petition in the last 10 years?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFinancialInfo},
		OutputFormat: "boolean",
	},
	{
		Key: "has_eo_insurance", Text: "Does the firm maintain errors and omissions (E&O) or professional liability insurance?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFinancialInfo},
		OutputFormat: "boolean",
	},
	{
		Key: "eo_coverage_amount", Text: "What is the E&O insurance coverage limit? Return null if not disclosed.",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFinancialInfo},
		OutputFormat: "string",
	},
	{
		Key: "has_fidelity_bond", Text: "Does the firm maintain a fidelity bond?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFinancialInfo},
		OutputFormat: "boolean",
	},
	{
		Key: "has_cyber_insurance", Text: "Does the firm disclose cybersecurity insurance coverage?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFinancialInfo},
		OutputFormat: "boolean",
	},
	{
		Key: "succession_plan_disclosed", Text: "Does the firm disclose a succession plan or continuity arrangement?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFinancialInfo},
		OutputFormat: "boolean",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Part 3 CRS-Specific (8 questions)
	// Category L, Tier 1, SourceDocs=["part3"], SourceSections=[]
	// =========================================================================
	{
		Key: "crs_firm_type", Text: "How does the CRS describe the firm type (investment adviser, broker-dealer, both)?",
		Tier: 1, Category: CatCRS, Scope: ScopeAdvisor,
		SourceDocs: []string{"part3"}, SourceSections: []string{},
		OutputFormat: "string",
	},
	{
		Key: "crs_key_services", Text: "What are the key services listed in the CRS?",
		Tier: 1, Category: CatCRS, Scope: ScopeAdvisor,
		SourceDocs: []string{"part3"}, SourceSections: []string{},
		OutputFormat: "string",
	},
	{
		Key: "crs_standard_of_conduct", Text: "What standard of conduct does the CRS describe (fiduciary, suitability)?",
		Tier: 1, Category: CatCRS, Scope: ScopeAdvisor,
		SourceDocs: []string{"part3"}, SourceSections: []string{},
		OutputFormat: "string",
	},
	{
		Key: "crs_main_fees", Text: "How does the CRS summarize the firm's main fees?",
		Tier: 1, Category: CatCRS, Scope: ScopeAdvisor,
		SourceDocs: []string{"part3"}, SourceSections: []string{},
		OutputFormat: "string",
	},
	{
		Key: "crs_has_conflicts", Text: "Does the CRS disclose conflicts of interest?",
		Tier: 1, Category: CatCRS, Scope: ScopeAdvisor,
		SourceDocs: []string{"part3"}, SourceSections: []string{},
		OutputFormat: "boolean",
	},
	{
		Key: "crs_conflicts_list", Text: "What specific conflicts does the CRS disclose?",
		Tier: 1, Category: CatCRS, Scope: ScopeAdvisor,
		SourceDocs: []string{"part3"}, SourceSections: []string{},
		OutputFormat: "string",
	},
	{
		Key: "crs_disciplinary_flag", Text: "Does the CRS disclose disciplinary history?",
		Tier: 1, Category: CatCRS, Scope: ScopeAdvisor,
		SourceDocs: []string{"part3"}, SourceSections: []string{},
		OutputFormat: "boolean",
	},
	{
		Key: "crs_conversation_starters", Text: "What conversation starter questions does the CRS suggest?",
		Tier: 1, Category: CatCRS, Scope: ScopeAdvisor,
		SourceDocs: []string{"part3"}, SourceSections: []string{},
		OutputFormat: "string",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Cross-Document Synthesis (10 questions)
	// Category M, Tier 1, SourceDocs and SourceSections vary per question
	// =========================================================================
	{
		Key: "ownership_structure_detail", Text: "Describe the ownership structure including all owners by name, percentage, and role.",
		Tier: 1, Category: CatCrossDoc, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionAffiliations},
		OutputFormat: "string",
	},
	{
		Key: "key_personnel_names", Text: "List key investment decision-makers as JSON: [{name, title, years_experience}].",
		Tier: 1, Category: CatCrossDoc, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionMaterialChanges},
		OutputFormat: "json",
	},
	{
		Key: "key_personnel_count", Text: "How many key investment professionals does the firm have?",
		Tier: 1, Category: CatCrossDoc, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionAffiliations},
		OutputFormat: "integer",
	},
	{
		Key: "num_support_staff", Text: "How many non-advisory/support staff does the firm have? Return null if not disclosed.",
		Tier: 1, Category: CatCrossDoc, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionAffiliations},
		OutputFormat: "integer",
	},
	{
		Key: "professional_certifications_list", Text: "List professional certifications held by key personnel as JSON: [{name, certification}].",
		Tier: 1, Category: CatCrossDoc, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionMaterialChanges},
		OutputFormat: "json",
	},
	{
		Key: "primary_investment_strategies_list", Text: "List all named investment strategies as JSON array of strings.",
		Tier: 1, Category: CatCrossDoc, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionAffiliations},
		OutputFormat: "json",
	},
	{
		Key: "all_custodians_list", Text: "List all custodians mentioned anywhere in the documents as JSON array.",
		Tier: 1, Category: CatCrossDoc, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionBrokerage, SectionCustody},
		OutputFormat: "json",
	},
	{
		Key: "all_affiliated_entities", Text: "List all affiliated entities as JSON: [{name, relationship}].",
		Tier: 1, Category: CatCrossDoc, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionAffiliations},
		OutputFormat: "json",
	},
	{
		Key: "year_most_recent_material_change", Text: "What year was the most recent material change disclosed in Item 2?",
		Tier: 1, Category: CatCrossDoc, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionMaterialChanges},
		OutputFormat: "integer",
	},
	{
		Key: "all_regulatory_registrations", Text: "List all regulatory registrations/licenses mentioned: [{type, jurisdiction}].",
		Tier: 1, Category: CatCrossDoc, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionAffiliations},
		OutputFormat: "json",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Fund-Level Questions (20 questions)
	// Category I, Tier 1, Scope=fund
	// SourceDocs=["part2"], SourceSections=[SectionInvestment, SectionAdvisoryBiz]
	// =========================================================================
	{
		Key: "fund_investment_strategy", Text: "In one sentence, what is this fund's investment strategy?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "string",
	},
	{
		Key: "fund_target_return", Text: "What is the fund's target return? Return null if not disclosed.",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "string",
	},
	{
		Key: "fund_benchmark", Text: "What benchmark does the fund use? Return null if not disclosed.",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "string",
	},
	{
		Key: "fund_uses_leverage", Text: "Does the fund use leverage?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "fund_max_leverage_ratio", Text: "What is the maximum leverage ratio? Return null if not disclosed.",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "string",
	},
	{
		Key: "fund_lock_up_period", Text: "What is the fund lock-up period? Return null if none.",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "string",
	},
	{
		Key: "fund_redemption_notice_days", Text: "How many days notice is required for redemption? Return 0 if no restriction.",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "integer",
	},
	{
		Key: "fund_redemption_frequency", Text: "How often can investors redeem (monthly, quarterly, annually)?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "string",
	},
	{
		Key: "fund_has_gate", Text: "Does the fund have redemption gates?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "fund_mgmt_fee_pct", Text: "What is the management fee percentage?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "number",
	},
	{
		Key: "fund_performance_fee_pct", Text: "What is the performance fee/carried interest percentage? Return null if none.",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "number",
	},
	{
		Key: "fund_hurdle_rate", Text: "What is the fund's hurdle rate? Return null if none.",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "string",
	},
	{
		Key: "fund_high_water_mark", Text: "Does the fund have a high-water mark?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "fund_min_investment", Text: "What is the minimum investment for the fund?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "integer",
	},
	{
		Key: "fund_auditor", Text: "Who is the fund's auditor? Return null if not disclosed.",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "string",
	},
	{
		Key: "fund_administrator", Text: "Who is the fund's administrator? Return null if not disclosed.",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "string",
	},
	{
		Key: "fund_prime_broker", Text: "Who is the fund's prime broker? Return null if not disclosed.",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "string",
	},
	{
		Key: "fund_side_letters_exist", Text: "Are there side letter arrangements granting preferential terms?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "fund_gp_commitment_exists", Text: "Does the GP/manager have a co-investment commitment in the fund?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "fund_concentration_limits", Text: "Does the fund have position concentration limits?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},

	// =========================================================================
	// Layer 3: Sonnet Synthesis & Assessment (8 questions)
	// Category N, Tier 2, SourceDocs=["part1","part2","part3"], SourceSections=[]
	// =========================================================================
	{
		Key: "integration_complexity_assessment", Text: "Given all extracted facts, assess the integration complexity for an acquirer (technology, custodians, compliance, culture). Rate 1-10 with reasoning.",
		Tier: 2, Category: CatSynthesis, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2", "part3"}, SourceSections: []string{},
		OutputFormat: "string",
	},
	{
		Key: "client_retention_risk", Text: "Assess the risk of client attrition post-acquisition based on client type, key person dependency, and contract terms.",
		Tier: 2, Category: CatSynthesis, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2", "part3"}, SourceSections: []string{},
		OutputFormat: "string",
	},
	{
		Key: "competitive_positioning", Text: "How does this firm position itself vs. competitors based on its disclosed strategy, fees, and target clients?",
		Tier: 2, Category: CatSynthesis, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2", "part3"}, SourceSections: []string{},
		OutputFormat: "string",
	},
	{
		Key: "growth_trajectory_assessment", Text: "Based on AUM trends, client growth, and disclosed strategy, assess the organic growth trajectory.",
		Tier: 2, Category: CatSynthesis, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2", "part3"}, SourceSections: []string{},
		OutputFormat: "string",
	},
	{
		Key: "key_person_risk_assessment", Text: "Assess the key-person risk based on ownership, team structure, and succession planning. Rate 1-10.",
		Tier: 2, Category: CatSynthesis, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2", "part3"}, SourceSections: []string{},
		OutputFormat: "string",
	},
	{
		Key: "regulatory_risk_profile", Text: "Assess the overall regulatory risk based on DRP history, custody arrangements, and cross-trading.",
		Tier: 2, Category: CatSynthesis, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2", "part3"}, SourceSections: []string{},
		OutputFormat: "string",
	},
	{
		Key: "valuation_considerations", Text: "What factors would most influence this firm's valuation multiple? Consider recurring revenue quality, growth, and client quality.",
		Tier: 2, Category: CatSynthesis, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2", "part3"}, SourceSections: []string{},
		OutputFormat: "string",
	},
	{
		Key: "acquisition_recommendation", Text: "Based on all available data, provide a brief M&A attractiveness assessment for this advisor.",
		Tier: 2, Category: CatSynthesis, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2", "part3"}, SourceSections: []string{},
		OutputFormat: "string",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Contract Terms & Change-of-Control (8 questions)
	// Category D/F, Tier 1, SourceDocs=["part2"], SourceSections=[SectionFees, SectionAdvisoryBiz]
	// =========================================================================
	{
		Key: "contract_assignment_clause", Text: "Does the advisory agreement contain an assignment clause? Describe the assignment provisions.",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees, SectionAdvisoryBiz},
		OutputFormat: "string",
	},
	{
		Key: "contract_consent_requirement", Text: "Does the advisory agreement require client consent for assignment or change of control?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees, SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "contract_negative_consent", Text: "Does the firm use negative consent provisions (deemed consent if client does not object)?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees, SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "contract_change_of_control", Text: "Does the advisory agreement address change of control events?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees, SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "contract_non_solicit_period", Text: "Is there a non-solicitation period mentioned in the advisory agreement? What is the duration?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees, SectionAdvisoryBiz},
		OutputFormat: "string",
	},
	{
		Key: "contract_non_compete_scope", Text: "Is there a non-compete clause? What is the scope and duration?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAffiliations, SectionAdvisoryBiz},
		OutputFormat: "string",
	},
	{
		Key: "contract_transition_provision", Text: "Does the advisory agreement include transition provisions for ownership changes?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees, SectionAdvisoryBiz},
		OutputFormat: "string",
	},
	{
		Key: "contract_client_portability", Text: "Are client accounts portable (can clients easily transfer to a new advisor)?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees, SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Fund Performance Track Record (8 questions)
	// Category I, Tier 1, Scope=fund, SourceDocs=["part2"]
	// =========================================================================
	{
		Key: "fund_track_record_years", Text: "How many years of track record does this fund have?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "integer",
	},
	{
		Key: "fund_historical_returns", Text: "Extract any disclosed historical returns as JSON: [{period, return_pct}]. Return null if not disclosed.",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "json",
	},
	{
		Key: "fund_benchmark_comparison", Text: "What benchmark does this fund compare its performance against? Return null if not disclosed.",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "string",
	},
	{
		Key: "fund_vintage_year", Text: "What is the fund's vintage year (year of first close/inception)?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "integer",
	},
	{
		Key: "fund_irr_disclosed", Text: "Does the fund disclose IRR or similar return metrics? What is the disclosed IRR?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "string",
	},
	{
		Key: "fund_loss_disclosure", Text: "Does the fund disclose any material investment losses or significant drawdowns?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "fund_investor_count", Text: "How many investors does this fund have? Return null if not disclosed.",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "integer",
	},
	{
		Key: "fund_capital_committed", Text: "What is the total capital committed to this fund? Return null if not disclosed.",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
		OutputFormat: "integer",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Client Concentration Risk (8 questions)
	// Category E, Tier 1, SourceDocs=["part2"], SourceSections=[SectionClientTypes, SectionFees]
	// =========================================================================
	{
		Key: "client_largest_pct_aum", Text: "What percentage of AUM does the largest single client represent? Return null if not disclosed.",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionClientTypes, SectionFees},
		OutputFormat: "number",
	},
	{
		Key: "client_top5_pct_aum", Text: "What percentage of AUM do the top 5 clients represent? Return null if not disclosed.",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionClientTypes, SectionFees},
		OutputFormat: "number",
	},
	{
		Key: "client_avg_tenure_years", Text: "What is the average client relationship tenure in years? Return null if not disclosed.",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionClientTypes, SectionFees},
		OutputFormat: "number",
	},
	{
		Key: "client_retention_rate", Text: "What is the client retention rate? Return null if not disclosed.",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionClientTypes, SectionFees},
		OutputFormat: "number",
	},
	{
		Key: "client_median_account_size", Text: "What is the median account size? Return null if not disclosed.",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionClientTypes, SectionFees},
		OutputFormat: "integer",
	},
	{
		Key: "client_size_distribution", Text: "Extract client account size distribution as JSON: [{range, count_or_pct}]. Return null if not disclosed.",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionClientTypes, SectionFees},
		OutputFormat: "json",
	},
	{
		Key: "client_geographic_concentration", Text: "Are clients geographically concentrated (majority in one state or region)?",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionClientTypes, SectionFees},
		OutputFormat: "boolean",
	},
	{
		Key: "client_age_demographics", Text: "What is the general age demographic of the client base (e.g., pre-retirees, retirees, young professionals)? Return null if not disclosed.",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionClientTypes, SectionFees},
		OutputFormat: "string",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Personnel Compensation & Retention (10 questions)
	// Category H, Tier 1, SourceDocs=["part2"]
	// =========================================================================
	{
		Key: "personnel_comp_structure", Text: "How are advisory personnel compensated (salary, bonus, revenue share, equity)?",
		Tier: 1, Category: CatPersonnel, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionAffiliations},
		OutputFormat: "string",
	},
	{
		Key: "personnel_has_equity_incentives", Text: "Does the firm offer equity ownership or profit-sharing incentives to key personnel?",
		Tier: 1, Category: CatPersonnel, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionAffiliations},
		OutputFormat: "boolean",
	},
	{
		Key: "personnel_non_compete_exists", Text: "Do advisory personnel have non-compete or non-solicitation agreements?",
		Tier: 1, Category: CatPersonnel, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionAffiliations},
		OutputFormat: "boolean",
	},
	{
		Key: "personnel_avg_tenure_key", Text: "What is the average tenure of key investment personnel in years? Return null if not disclosed.",
		Tier: 1, Category: CatPersonnel, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionAffiliations},
		OutputFormat: "number",
	},
	{
		Key: "personnel_turnover_disclosed", Text: "Does the firm disclose personnel turnover rates or recent departures?",
		Tier: 1, Category: CatPersonnel, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionMaterialChanges},
		OutputFormat: "boolean",
	},
	{
		Key: "personnel_training_program", Text: "Does the firm have a formal training or professional development program?",
		Tier: 1, Category: CatPersonnel, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionCodeOfEthics},
		OutputFormat: "boolean",
	},
	{
		Key: "personnel_cfp_cfa_count", Text: "How many personnel hold CFP, CFA, or equivalent certifications? Return null if not disclosed.",
		Tier: 1, Category: CatPersonnel, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionAffiliations},
		OutputFormat: "integer",
	},
	{
		Key: "personnel_succession_identified", Text: "Has the firm identified specific successors for key leadership positions?",
		Tier: 1, Category: CatPersonnel, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFinancialInfo, SectionAdvisoryBiz},
		OutputFormat: "boolean",
	},
	{
		Key: "personnel_comp_disclosure_level", Text: "How detailed is the firm's disclosure of personnel compensation (none, general, specific)?",
		Tier: 1, Category: CatPersonnel, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionAffiliations},
		OutputFormat: "string",
	},
	{
		Key: "personnel_key_person_pct_aum", Text: "What percentage of AUM is managed by the single most important person? Return null if not disclosed.",
		Tier: 1, Category: CatPersonnel, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionAffiliations},
		OutputFormat: "number",
	},

	// =========================================================================
	// Layer 1: Haiku Factual — Technology Infrastructure (8 questions)
	// Category G, Tier 1, SourceDocs=["part2"]
	// =========================================================================
	{
		Key: "tech_portfolio_accounting", Text: "What portfolio accounting or reporting system does the firm use (e.g., Orion, Black Diamond, Tamarac)?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionBrokerage, SectionAdvisoryBiz},
		OutputFormat: "string",
	},
	{
		Key: "tech_crm_platform", Text: "What CRM platform does the firm use (e.g., Salesforce, Redtail, Wealthbox)?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionBrokerage},
		OutputFormat: "string",
	},
	{
		Key: "tech_trading_platform", Text: "What trading or order management system does the firm use?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionBrokerage, SectionAdvisoryBiz},
		OutputFormat: "string",
	},
	{
		Key: "tech_financial_planning_sw", Text: "What financial planning software does the firm use (e.g., eMoney, MoneyGuidePro, RightCapital)?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionBrokerage},
		OutputFormat: "string",
	},
	{
		Key: "tech_reporting_tools", Text: "What client reporting tools or platforms does the firm use?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionBrokerage, SectionReviewAccounts},
		OutputFormat: "string",
	},
	{
		Key: "tech_cybersecurity_program", Text: "Does the firm disclose a cybersecurity program or information security practices?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFinancialInfo, SectionCodeOfEthics},
		OutputFormat: "boolean",
	},
	{
		Key: "tech_data_backup_disaster_recov", Text: "Does the firm disclose data backup or disaster recovery procedures?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFinancialInfo, SectionCodeOfEthics},
		OutputFormat: "boolean",
	},
	{
		Key: "tech_cloud_provider", Text: "What cloud or technology infrastructure provider does the firm use? Return null if not disclosed.",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionFinancialInfo},
		OutputFormat: "string",
	},
}
