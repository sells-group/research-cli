package advextract

// Question defines an M&A-focused extraction question for ADV documents.
type Question struct {
	Key              string   // unique identifier (snake_case)
	Text             string   // the question to ask the LLM
	Tier             int      // 1=Haiku, 2=Sonnet, 3=Opus
	Category         string   // A-K category code
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
	CatFirmIdentity    = "A" // Firm Identity & Structure
	CatAUMGrowth       = "B" // AUM & Growth Trajectory
	CatInvestment      = "C" // Investment Strategy & Philosophy
	CatFees            = "D" // Fee Structure & Revenue Model
	CatClients         = "E" // Client Demographics
	CatCompliance      = "F" // Compliance & Regulatory
	CatOperations      = "G" // Operations & Technology
	CatPersonnel       = "H" // Personnel & Culture
	CatFundDetail      = "I" // Fund-Level Detail
	CatConflicts       = "J" // Conflicts of Interest
	CatGrowth          = "K" // Growth & Business Development
)

// AllQuestions returns all 95 M&A-focused extraction questions.
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
	// A. Firm Identity & Structure (10 questions)
	// =========================================================================
	{
		Key: "legal_structure", Text: "What is the firm's legal structure and form of organization (e.g., LLC, corporation, partnership)?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionCoverPage, SectionAdvisoryBiz},
	},
	{
		Key: "ownership_structure", Text: "Describe the ownership structure including all owners with 25%+ stakes, their ownership percentages, and whether they are active in management.",
		Tier: 2, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionAffiliations},
	},
	{
		Key: "succession_plan", Text: "What succession plan details are disclosed? Include key-person dependencies, transition timeline, and continuity arrangements.",
		Tier: 2, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFinancialInfo},
	},
	{
		Key: "year_founded", Text: "What year was the firm founded or began providing investment advisory services?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionCoverPage, SectionAdvisoryBiz},
	},
	{
		Key: "parent_organization", Text: "Is the firm part of a holding company or does it have a parent organization? If so, identify the parent entity.",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAffiliations},
	},
	{
		Key: "operating_subsidiaries", Text: "List all operating subsidiaries, related entities, and affiliated companies. For each, describe the relationship and services provided.",
		Tier: 2, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionAffiliations},
	},
	{
		Key: "business_continuity", Text: "Describe the firm's business continuity plan including disaster recovery provisions and how client assets/accounts would be protected.",
		Tier: 2, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFinancialInfo},
	},
	{
		Key: "office_locations", Text: "List all office locations including principal office and branch offices.",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true,
	},
	{
		Key: "regulatory_status", Text: "What is the firm's regulatory registration status (SEC registered, state registered, exempt reporting)?",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true,
	},
	{
		Key: "key_regulatory_registrations", Text: "List all regulatory registrations and key compliance registrations (SEC, state, other).",
		Tier: 1, Category: CatFirmIdentity, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true,
	},

	// =========================================================================
	// B. AUM & Growth Trajectory (8 questions)
	// =========================================================================
	{
		Key: "aum_current", Text: "What is the current AUM total, discretionary, and non-discretionary?",
		Tier: 1, Category: CatAUMGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true,
	},
	{
		Key: "aum_discretionary_split", Text: "What percentage of AUM is managed on a discretionary vs non-discretionary basis?",
		Tier: 1, Category: CatAUMGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true,
	},
	{
		Key: "aum_growth_trend", Text: "What is the AUM compound annual growth rate (CAGR) based on available historical filing data? Describe the growth trajectory.",
		Tier: 2, Category: CatAUMGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionAdvisoryBiz},
	},
	{
		Key: "client_account_growth", Text: "How has the number of client accounts changed over time? Identify the growth trend.",
		Tier: 2, Category: CatAUMGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionAdvisoryBiz},
	},
	{
		Key: "revenue_estimate", Text: "Estimate the firm's annual advisory revenue based on AUM, fee schedule, and client mix. Show your calculation methodology.",
		Tier: 3, Category: CatAUMGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionFees, SectionAdvisoryBiz},
	},
	{
		Key: "revenue_per_client", Text: "Estimate the average revenue per client based on AUM per account and fee schedule. Identify high-value vs standard client segments.",
		Tier: 3, Category: CatAUMGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionFees, SectionClientTypes},
	},
	{
		Key: "organic_growth_vs_acquisition", Text: "Has the firm grown primarily organically or through acquisitions? Identify any disclosed acquisition history or team hires.",
		Tier: 3, Category: CatAUMGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionAffiliations},
	},
	{
		Key: "avg_account_size", Text: "What is the average account size (AUM / number of accounts)?",
		Tier: 1, Category: CatAUMGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true,
	},

	// =========================================================================
	// C. Investment Strategy & Philosophy (10 questions)
	// =========================================================================
	{
		Key: "investment_philosophy", Text: "What is the firm's core investment philosophy? Summarize the guiding principles behind their approach.",
		Tier: 1, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
	},
	{
		Key: "primary_strategies", Text: "What are the primary investment strategies employed (e.g., value, growth, income, tactical, passive)?",
		Tier: 1, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
	},
	{
		Key: "asset_classes", Text: "What asset classes does the firm invest in (equities, fixed income, alternatives, real estate, etc.)?",
		Tier: 1, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
	},
	{
		Key: "alternatives_usage", Text: "Does the firm use alternative investments (hedge funds, private equity, real assets)? If so, describe the approach and allocation.",
		Tier: 1, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
	},
	{
		Key: "proprietary_vs_third_party", Text: "Does the firm use proprietary products, third-party products, or both? What is the mix and are there conflicts of interest from proprietary product usage?",
		Tier: 2, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees, SectionInvestment, SectionAffiliations},
	},
	{
		Key: "esg_approach", Text: "Does the firm incorporate ESG/SRI/impact investing into their process? Describe the approach if applicable.",
		Tier: 2, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
	},
	{
		Key: "portfolio_construction", Text: "Describe the portfolio construction methodology including diversification approach, rebalancing frequency, and risk management.",
		Tier: 2, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
	},
	{
		Key: "performance_track_record", Text: "Is there any performance track record or benchmarking information disclosed? Summarize if available.",
		Tier: 3, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
	},
	{
		Key: "risk_management", Text: "What risk management practices and controls are described? Include any risk limits, monitoring, or mitigation strategies.",
		Tier: 2, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
	},
	{
		Key: "analysis_methods", Text: "What methods of analysis does the firm use (fundamental, technical, quantitative, charting)? Describe the research process.",
		Tier: 1, Category: CatInvestment, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
	},

	// =========================================================================
	// D. Fee Structure & Revenue Model (10 questions)
	// =========================================================================
	{
		Key: "fee_schedule_complete", Text: "Provide the complete fee schedule including all tiers, breakpoints, and minimums. Include percentage rates for each AUM tier.",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees},
	},
	{
		Key: "fee_billing_method", Text: "How are fees billed? Include billing frequency (monthly, quarterly), billing method (advance/arrears), and calculation basis.",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees},
	},
	{
		Key: "performance_fee_structure", Text: "Does the firm charge performance-based fees? If so, describe the structure, hurdle rates, and high-water marks.",
		Tier: 2, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionPerformanceFees},
	},
	{
		Key: "wrap_fee_details", Text: "Does the firm participate in wrap fee programs? Describe the structure, included services, and any additional costs to clients.",
		Tier: 2, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionFees},
	},
	{
		Key: "revenue_concentration", Text: "Analyze revenue concentration across client segments. What percentage of estimated revenue comes from HNW, institutional, and other segments?",
		Tier: 3, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionFees, SectionClientTypes},
	},
	{
		Key: "blended_avg_fee_rate", Text: "Estimate the blended average fee rate based on the fee schedule and AUM distribution across tiers.",
		Tier: 3, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionFees},
	},
	{
		Key: "minimum_account_size", Text: "What is the minimum account size or investment minimum?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionClientTypes},
	},
	{
		Key: "compensation_types", Text: "What forms of compensation does the firm receive (% AUM, hourly, fixed, commissions, performance)?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true,
	},
	{
		Key: "other_fees_expenses", Text: "What other fees and expenses are clients expected to pay beyond advisory fees (custodian fees, fund expenses, transaction costs)?",
		Tier: 1, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees},
	},
	{
		Key: "fee_negotiability", Text: "Are fees negotiable? Under what circumstances might the firm reduce or waive fees?",
		Tier: 2, Category: CatFees, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees},
	},

	// =========================================================================
	// E. Client Demographics (8 questions)
	// =========================================================================
	{
		Key: "client_types_breakdown", Text: "Provide the client type breakdown by count and AUM (individuals, HNW, institutions, etc.).",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true,
	},
	{
		Key: "hnw_concentration", Text: "What is the concentration of high-net-worth clients (>$1M) as a percentage of total clients and AUM?",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true,
	},
	{
		Key: "institutional_vs_retail", Text: "What is the institutional vs retail client mix by count and AUM?",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true,
	},
	{
		Key: "total_client_count", Text: "What is the total number of client accounts?",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true,
	},
	{
		Key: "target_client_profile", Text: "Describe the firm's target client profile. What characteristics define their ideal client (net worth, profession, life stage)?",
		Tier: 2, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2", "part3"}, SourceSections: []string{SectionClientTypes},
	},
	{
		Key: "client_retention", Text: "What indicators of client retention and satisfaction are available? Look for tenure mentions, growth in existing accounts, or referral-based growth.",
		Tier: 3, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionReferrals},
	},
	{
		Key: "geographic_concentration", Text: "Is the client base geographically concentrated? Identify primary markets and any regional focus.",
		Tier: 2, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionClientTypes},
	},
	{
		Key: "services_offered", Text: "What advisory services does the firm offer (financial planning, portfolio management, pension consulting, etc.)?",
		Tier: 1, Category: CatClients, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2", "part3"}, SourceSections: []string{SectionAdvisoryBiz},
	},

	// =========================================================================
	// F. Compliance & Regulatory (8 questions)
	// =========================================================================
	{
		Key: "disciplinary_history", Text: "Does the firm or any advisory affiliate have disciplinary history? Summarize any DRP disclosures.",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part3"}, SourceSections: []string{SectionDisciplinary},
		StructuredBypass: true,
	},
	{
		Key: "custody_arrangements", Text: "Describe the firm's custody arrangements. Does the firm have custody of client assets? Who is the qualified custodian?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionCustody},
	},
	{
		Key: "compliance_program", Text: "Describe the compliance program including the role of the Chief Compliance Officer and key compliance policies.",
		Tier: 2, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFinancialInfo, SectionCodeOfEthics},
	},
	{
		Key: "conflicts_disclosed", Text: "What conflicts of interest are disclosed? Provide a comprehensive list from ADV Part 2 Items 10-12 and CRS.",
		Tier: 2, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2", "part3"}, SourceSections: []string{SectionAffiliations, SectionCodeOfEthics, SectionBrokerage},
	},
	{
		Key: "related_party_transactions", Text: "Are there related party transactions disclosed? Describe any transactions between the firm and affiliated entities.",
		Tier: 2, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionAffiliations, SectionCodeOfEthics},
	},
	{
		Key: "proxy_voting", Text: "What is the firm's proxy voting policy? Does the firm vote proxies on behalf of clients?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionProxyVoting},
	},
	{
		Key: "cross_trading_practices", Text: "Does the firm engage in cross-trading or principal transactions?",
		Tier: 1, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true,
	},
	{
		Key: "code_of_ethics_summary", Text: "Summarize the firm's Code of Ethics, including personal trading policies and insider trading prevention measures.",
		Tier: 2, Category: CatCompliance, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionCodeOfEthics},
	},

	// =========================================================================
	// G. Operations & Technology (8 questions)
	// =========================================================================
	{
		Key: "primary_custodians", Text: "Who are the primary custodians used by the firm? List all named custodians.",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionBrokerage},
	},
	{
		Key: "broker_selection_soft_dollars", Text: "How does the firm select broker-dealers for client transactions? Does the firm use soft dollar arrangements? Describe the practices.",
		Tier: 2, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionBrokerage},
	},
	{
		Key: "technology_platform", Text: "What technology platforms, portfolio management systems, or financial planning tools does the firm use?",
		Tier: 2, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionBrokerage},
	},
	{
		Key: "trade_execution", Text: "Describe the firm's trade execution and allocation practices including how orders are aggregated and allocated among clients.",
		Tier: 2, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionBrokerage},
	},
	{
		Key: "client_reporting", Text: "What client reporting capabilities does the firm offer? Include frequency, format, and content of reports.",
		Tier: 2, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionReviewAccounts},
	},
	{
		Key: "account_review_process", Text: "How frequently are client accounts reviewed and by whom? What triggers additional reviews?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionReviewAccounts},
	},
	{
		Key: "total_headcount", Text: "What is the total number of employees including advisory and non-advisory?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true,
	},
	{
		Key: "investment_discretion_scope", Text: "What is the scope of the firm's investment discretion? Are there any limitations on discretionary authority?",
		Tier: 1, Category: CatOperations, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionDiscretion},
	},

	// =========================================================================
	// H. Personnel & Culture (8 questions)
	// =========================================================================
	{
		Key: "key_decision_makers", Text: "Who are the key investment decision-makers? Provide names, titles, years of experience, and educational backgrounds.",
		Tier: 1, Category: CatPersonnel, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionMaterialChanges, SectionAdvisoryBiz},
	},
	{
		Key: "adviser_compensation_structure", Text: "How are advisers compensated? Describe the compensation model (salary, bonus, equity, revenue sharing).",
		Tier: 2, Category: CatPersonnel, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees, SectionAffiliations},
	},
	{
		Key: "employee_ownership", Text: "Do employees have ownership stakes in the firm? Describe equity participation and incentive structures.",
		Tier: 2, Category: CatPersonnel, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionAffiliations},
	},
	{
		Key: "next_gen_leadership", Text: "Is there evidence of next-generation leadership development? Identify potential successors and leadership pipeline.",
		Tier: 3, Category: CatPersonnel, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionFinancialInfo},
	},
	{
		Key: "professional_certifications", Text: "List professional certifications held by key personnel (CFA, CFP, CPA, CIMA, etc.).",
		Tier: 1, Category: CatPersonnel, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionMaterialChanges, SectionAdvisoryBiz},
	},
	{
		Key: "team_structure", Text: "Describe the organizational structure of the advisory team. How are teams organized (by client, strategy, function)?",
		Tier: 2, Category: CatPersonnel, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz},
	},
	{
		Key: "investment_committee", Text: "Does the firm have an investment committee? Describe its composition, role, and decision-making process.",
		Tier: 2, Category: CatPersonnel, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
	},
	{
		Key: "key_person_risk_advisor", Text: "Assess the key-person risk. How dependent is the firm on specific individuals for investment decisions, client relationships, or operations?",
		Tier: 3, Category: CatPersonnel, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionFinancialInfo},
	},

	// =========================================================================
	// I. Fund-Level Detail (15 questions, scope=fund)
	// =========================================================================
	{
		Key: "fund_investment_strategy", Text: "What is the fund's investment strategy? Describe the approach, target markets, and investment thesis.",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
	},
	{
		Key: "fund_aum", Text: "What is the fund's gross asset value (GAV) and net asset value (NAV)?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true,
	},
	{
		Key: "fund_type_detail", Text: "What type of fund is this (hedge fund, PE fund, venture fund, real estate fund, fund of funds)?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true,
	},
	{
		Key: "fund_investor_types", Text: "Who are the fund's investors? Describe the investor base composition (HNW, institutional, pension, endowment).",
		Tier: 2, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionClientTypes, SectionAdvisoryBiz},
	},
	{
		Key: "fund_fee_structure", Text: "What is the fund's fee structure including management fee, performance allocation/carried interest, and any other fees?",
		Tier: 2, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionPerformanceFees, SectionFees},
	},
	{
		Key: "fund_minimum_investment", Text: "What is the minimum investment amount for the fund?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionClientTypes, SectionFees},
	},
	{
		Key: "fund_liquidity_terms", Text: "What are the fund's liquidity and redemption terms (lock-up period, redemption notice, redemption frequency, gates)?",
		Tier: 2, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionFinancialInfo},
	},
	{
		Key: "fund_leverage", Text: "Does the fund use leverage? Describe the leverage policy, limits, and current usage if disclosed.",
		Tier: 2, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
	},
	{
		Key: "fund_key_person_risk", Text: "Assess key-person risk for this fund. How dependent is the fund's strategy on specific individuals?",
		Tier: 3, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionFinancialInfo},
	},
	{
		Key: "fund_valuation_methodology", Text: "How are fund assets valued? Describe the valuation methodology, frequency, and any use of third-party valuation agents.",
		Tier: 2, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFinancialInfo},
	},
	{
		Key: "fund_service_providers", Text: "Who are the fund's key service providers (auditor, administrator, legal counsel, prime broker)?",
		Tier: 2, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionBrokerage, SectionCustody},
	},
	{
		Key: "fund_regulatory_status", Text: "What is the fund's regulatory status and exemptions relied upon?",
		Tier: 1, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part1"}, SourceSections: []string{},
		StructuredBypass: true,
	},
	{
		Key: "fund_concentration_limits", Text: "Does the fund have concentration limits or diversification requirements?",
		Tier: 2, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment},
	},
	{
		Key: "fund_side_pockets", Text: "Does the fund use side pockets or special purpose vehicles? If so, describe the structure.",
		Tier: 2, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz},
	},
	{
		Key: "fund_track_record", Text: "Is there any fund performance data or track record disclosed? Summarize if available.",
		Tier: 3, Category: CatFundDetail, Scope: ScopeFund,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionInvestment, SectionAdvisoryBiz},
	},

	// =========================================================================
	// J. Conflicts of Interest (5 questions)
	// =========================================================================
	{
		Key: "revenue_sharing_conflicts", Text: "Does the firm receive revenue sharing from custodians, broker-dealers, or fund companies? Describe the arrangements and how conflicts are managed.",
		Tier: 2, Category: CatConflicts, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionReferrals, SectionBrokerage},
	},
	{
		Key: "proprietary_product_conflicts", Text: "Are there conflicts from the use of proprietary products or affiliated funds? How are these conflicts mitigated?",
		Tier: 2, Category: CatConflicts, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionFees, SectionAffiliations},
	},
	{
		Key: "soft_dollar_practices", Text: "Describe all soft dollar arrangements. What services are received and how do these arrangements benefit clients?",
		Tier: 2, Category: CatConflicts, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionBrokerage},
	},
	{
		Key: "outside_business_activities", Text: "Do advisory personnel have outside business activities that create conflicts? Describe any disclosed activities.",
		Tier: 2, Category: CatConflicts, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAffiliations, SectionCodeOfEthics},
	},
	{
		Key: "conflict_mitigation_overall", Text: "Overall, how effectively does the firm identify, disclose, and mitigate conflicts of interest? Rate the comprehensiveness of disclosure.",
		Tier: 3, Category: CatConflicts, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2", "part3"}, SourceSections: []string{SectionAffiliations, SectionCodeOfEthics, SectionBrokerage, SectionFees},
	},

	// =========================================================================
	// K. Growth & Business Development (5 questions)
	// =========================================================================
	{
		Key: "referral_arrangements", Text: "Does the firm have referral arrangements or solicitor agreements? Describe compensation paid for client referrals.",
		Tier: 1, Category: CatGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionReferrals},
	},
	{
		Key: "growth_strategy", Text: "What is the firm's growth strategy? Analyze organic growth drivers, M&A appetite, and market expansion plans based on all available disclosures.",
		Tier: 3, Category: CatGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionAffiliations},
	},
	{
		Key: "acquisition_history", Text: "Has the firm made acquisitions or been acquired? Identify any disclosed ownership changes, mergers, or acquisitions.",
		Tier: 3, Category: CatGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part1", "part2"}, SourceSections: []string{SectionAdvisoryBiz, SectionAffiliations},
	},
	{
		Key: "brand_differentiation", Text: "What differentiates this firm from competitors? Identify unique value propositions, niche expertise, or competitive advantages from brochure and CRS.",
		Tier: 3, Category: CatGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2", "part3"}, SourceSections: []string{SectionAdvisoryBiz, SectionInvestment},
	},
	{
		Key: "market_positioning", Text: "How does the firm position itself in the market? Identify the market segment (mass affluent, HNW, UHNW, institutional) and competitive positioning.",
		Tier: 2, Category: CatGrowth, Scope: ScopeAdvisor,
		SourceDocs: []string{"part2", "part3"}, SourceSections: []string{SectionAdvisoryBiz, SectionClientTypes},
	},
}
