package peextract

// Question defines a PE-firm-focused extraction question.
type Question struct {
	Key          string   // unique identifier (snake_case, pe_ prefix)
	Text         string   // the question to ask the LLM
	Tier         int      // 1=Haiku, 2=Sonnet
	Category     string   // A-F category code
	PageTypes    []string // which page types to route to
	OutputFormat string   // expected output format hint: json, string, integer
}

// Category constants.
const (
	CatFirmIdentity  = "A" // Firm Identity
	CatKeyPeople     = "B" // Key People
	CatPortfolio     = "C" // Portfolio & Strategy
	CatFundFinancial = "D" // Fund & Financial
	CatContactMisc   = "E" // Contact & Misc
	CatSynthesis     = "F" // Synthesis (Sonnet)
	CatBlogIntel     = "G" // Blog & Content Intelligence (Sonnet)
)

// AllQuestions returns all PE extraction questions.
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

// QuestionMap returns all questions keyed by question key.
func QuestionMap() map[string]Question {
	m := make(map[string]Question, len(allQuestions))
	for _, q := range allQuestions {
		m[q.Key] = q
	}
	return m
}

// filterByTier returns questions matching a specific tier.
func filterByTier(qs []Question, tier int) []Question {
	var out []Question
	for _, q := range qs {
		if q.Tier == tier {
			out = append(out, q)
		}
	}
	return out
}

var allQuestions = []Question{
	// ─── Category A: Firm Identity (5 questions, T1) ───

	{
		Key:          "pe_hq_address",
		Text:         "What is the firm's headquarters address? Return as JSON with fields: street, city, state, zip, country.",
		Tier:         1,
		Category:     CatFirmIdentity,
		PageTypes:    []string{"homepage", "about", "contact"},
		OutputFormat: "json",
	},
	{
		Key:          "pe_year_founded",
		Text:         "What year was this firm founded or established?",
		Tier:         1,
		Category:     CatFirmIdentity,
		PageTypes:    []string{"about", "homepage"},
		OutputFormat: "integer",
	},
	{
		Key:          "pe_firm_description",
		Text:         "Provide a one-paragraph description of this firm, including what they do, their focus, and their market position.",
		Tier:         1,
		Category:     CatFirmIdentity,
		PageTypes:    []string{"about", "homepage"},
		OutputFormat: "string",
	},
	{
		Key:          "pe_firm_type",
		Text:         "What type of firm is this? Classify as one of: private_equity, aggregator, holding_company, family_office, or other. Return just the classification string.",
		Tier:         1,
		Category:     CatFirmIdentity,
		PageTypes:    []string{"about", "homepage", "strategy"},
		OutputFormat: "string",
	},
	{
		Key:          "pe_office_locations",
		Text:         "What are all the office locations for this firm? Return as a JSON array of objects with fields: city, state, country.",
		Tier:         1,
		Category:     CatFirmIdentity,
		PageTypes:    []string{"about", "contact", "homepage"},
		OutputFormat: "json",
	},

	// ─── Category B: Key People (5 questions, T1) ───

	{
		Key:          "pe_managing_partners",
		Text:         "Who are the managing partners, principals, or senior leadership? Return as a JSON array of objects with fields: name, title, bio_summary (one sentence).",
		Tier:         1,
		Category:     CatKeyPeople,
		PageTypes:    []string{"team", "about"},
		OutputFormat: "json",
	},
	{
		Key:          "pe_investment_team",
		Text:         "Who are the members of the investment or deal team? Return as a JSON array of objects with fields: name, title, focus_area.",
		Tier:         1,
		Category:     CatKeyPeople,
		PageTypes:    []string{"team", "about"},
		OutputFormat: "json",
	},
	{
		Key:          "pe_team_size",
		Text:         "What is the total number of employees or team members at this firm?",
		Tier:         1,
		Category:     CatKeyPeople,
		PageTypes:    []string{"about", "team", "careers"},
		OutputFormat: "integer",
	},
	{
		Key:          "pe_founder_names",
		Text:         "Who are the founders of this firm? Return as a JSON array of name strings.",
		Tier:         1,
		Category:     CatKeyPeople,
		PageTypes:    []string{"about", "team", "homepage"},
		OutputFormat: "json",
	},
	{
		Key:          "pe_key_person_backgrounds",
		Text:         "Summarize the professional backgrounds of the top 3 leaders at this firm. Include their prior firms, education, and notable experience.",
		Tier:         1,
		Category:     CatKeyPeople,
		PageTypes:    []string{"team", "about"},
		OutputFormat: "string",
	},

	// ─── Category C: Portfolio & Strategy (10 questions, T1) ───

	{
		Key:          "pe_portfolio_companies",
		Text:         "What are the current portfolio companies? Return as a JSON array of objects with fields: name, description (one sentence), sector.",
		Tier:         1,
		Category:     CatPortfolio,
		PageTypes:    []string{"portfolio"},
		OutputFormat: "json",
	},
	{
		Key:          "pe_portfolio_count",
		Text:         "How many portfolio companies does this firm currently have?",
		Tier:         1,
		Category:     CatPortfolio,
		PageTypes:    []string{"portfolio", "about", "homepage"},
		OutputFormat: "integer",
	},
	{
		Key:          "pe_investment_strategy",
		Text:         "What is this firm's investment strategy or thesis? Describe their approach to acquiring and growing companies.",
		Tier:         1,
		Category:     CatPortfolio,
		PageTypes:    []string{"strategy", "about", "homepage"},
		OutputFormat: "string",
	},
	{
		Key:          "pe_target_sectors",
		Text:         "What sectors or industries does this firm target for investment? Return as a JSON array of sector name strings.",
		Tier:         1,
		Category:     CatPortfolio,
		PageTypes:    []string{"strategy", "about", "homepage", "portfolio"},
		OutputFormat: "json",
	},
	{
		Key:          "pe_target_geography",
		Text:         "What geographic regions does this firm target for investments?",
		Tier:         1,
		Category:     CatPortfolio,
		PageTypes:    []string{"strategy", "about", "homepage"},
		OutputFormat: "string",
	},
	{
		Key:          "pe_deal_size_range",
		Text:         "What is the typical deal size range for this firm's investments? Include any stated minimums or maximums.",
		Tier:         1,
		Category:     CatPortfolio,
		PageTypes:    []string{"strategy", "about", "homepage"},
		OutputFormat: "string",
	},
	{
		Key:          "pe_investment_criteria",
		Text:         "What criteria does this firm use to evaluate potential investments? Include any stated requirements around revenue, EBITDA, growth, or other metrics.",
		Tier:         1,
		Category:     CatPortfolio,
		PageTypes:    []string{"strategy", "about"},
		OutputFormat: "string",
	},
	{
		Key:          "pe_recent_acquisitions",
		Text:         "What companies has this firm acquired in the last 3 years? Return as a JSON array of objects with fields: company_name, date, deal_type (platform/add-on/unknown), size_if_stated.",
		Tier:         1,
		Category:     CatPortfolio,
		PageTypes:    []string{"portfolio", "news", "blog", "about"},
		OutputFormat: "json",
	},
	{
		Key:          "pe_deal_velocity",
		Text:         "How many acquisitions has this firm completed in the last 3 years? Return the count as an integer.",
		Tier:         1,
		Category:     CatPortfolio,
		PageTypes:    []string{"portfolio", "news", "blog", "about", "homepage"},
		OutputFormat: "integer",
	},
	{
		Key:          "pe_integration_approach",
		Text:         "How does this firm integrate acquired companies? Do they rebrand, centralize operations, or let firms operate independently? Describe their integration philosophy and approach.",
		Tier:         1,
		Category:     CatPortfolio,
		PageTypes:    []string{"strategy", "about", "portfolio"},
		OutputFormat: "string",
	},

	// ─── Category D: Fund & Financial (6 questions, T1) ───

	{
		Key:          "pe_total_aum",
		Text:         "What is the total assets under management (AUM) or capital under management for this firm?",
		Tier:         1,
		Category:     CatFundFinancial,
		PageTypes:    []string{"about", "homepage", "strategy"},
		OutputFormat: "string",
	},
	{
		Key:          "pe_fund_names",
		Text:         "What are the names of this firm's funds? Return as a JSON array of objects with fields: name, year (vintage year if stated), size (fund size if stated).",
		Tier:         1,
		Category:     CatFundFinancial,
		PageTypes:    []string{"strategy", "about", "homepage"},
		OutputFormat: "json",
	},
	{
		Key:          "pe_recent_fundraise",
		Text:         "Has this firm recently raised a new fund or completed a fundraise? Describe any recent fundraising activity.",
		Tier:         1,
		Category:     CatFundFinancial,
		PageTypes:    []string{"news", "blog", "about", "homepage"},
		OutputFormat: "string",
	},
	{
		Key:          "pe_exits_notable",
		Text:         "What notable exits or realizations has this firm completed? Return as a JSON array of objects with fields: company_name, exit_type (IPO/sale/merger), year.",
		Tier:         1,
		Category:     CatFundFinancial,
		PageTypes:    []string{"portfolio", "news", "blog", "about"},
		OutputFormat: "json",
	},
	{
		Key:          "pe_dry_powder",
		Text:         "How much capital does this firm have available for new investments (dry powder, uncommitted capital, available fund capacity)?",
		Tier:         1,
		Category:     CatFundFinancial,
		PageTypes:    []string{"about", "homepage", "strategy", "news", "blog"},
		OutputFormat: "string",
	},
	{
		Key:          "pe_valuation_approach",
		Text:         "What valuation metrics or multiples does this firm typically use when evaluating acquisitions? Include any stated preferences for AUM multiples, revenue multiples, EBITDA multiples, or other valuation frameworks.",
		Tier:         1,
		Category:     CatFundFinancial,
		PageTypes:    []string{"strategy", "about"},
		OutputFormat: "string",
	},

	// ─── Category E: Contact & Misc (4 questions, T1) ───

	{
		Key:          "pe_contact_email",
		Text:         "What is the general contact email address for this firm?",
		Tier:         1,
		Category:     CatContactMisc,
		PageTypes:    []string{"contact", "homepage"},
		OutputFormat: "string",
	},
	{
		Key:          "pe_contact_phone",
		Text:         "What is the main phone number for this firm?",
		Tier:         1,
		Category:     CatContactMisc,
		PageTypes:    []string{"contact", "homepage"},
		OutputFormat: "string",
	},
	{
		Key:          "pe_news_recent",
		Text:         "Summarize the most recent news, press releases, or announcements from this firm.",
		Tier:         1,
		Category:     CatContactMisc,
		PageTypes:    []string{"news", "blog", "homepage"},
		OutputFormat: "string",
	},
	{
		Key:          "pe_careers_hiring",
		Text:         "Is this firm actively hiring? Describe any open positions or hiring information visible on their website.",
		Tier:         1,
		Category:     CatContactMisc,
		PageTypes:    []string{"careers", "homepage"},
		OutputFormat: "string",
	},

	// ─── Category F: Synthesis (4 questions, T2 Sonnet) ───

	{
		Key:          "pe_acquisition_pattern",
		Text:         "Analyze this firm's acquisition pattern. Consider: How frequently do they acquire? What types of deals do they pursue (platform vs. add-on, majority vs. minority)? How do they appear to integrate acquisitions? What is their typical holding period?",
		Tier:         2,
		Category:     CatSynthesis,
		PageTypes:    []string{"portfolio", "strategy", "about", "news", "blog"},
		OutputFormat: "string",
	},
	{
		Key:          "pe_strategic_assessment",
		Text:         "Provide a strategic assessment of this firm as a competitor or partner in the RIA M&A space. Consider their stated strategy, portfolio composition, deal capacity, geographic focus, and any advantages or vulnerabilities.",
		Tier:         2,
		Category:     CatSynthesis,
		PageTypes:    []string{"about", "strategy", "portfolio", "homepage"},
		OutputFormat: "string",
	},
	{
		Key:          "pe_portfolio_gap_analysis",
		Text:         "Based on this firm's existing portfolio, geographic presence, and stated strategy, what types of RIAs would fill gaps in their platform? Consider gaps in geography, AUM size, service model, or client type.",
		Tier:         2,
		Category:     CatSynthesis,
		PageTypes:    []string{"portfolio", "strategy", "about"},
		OutputFormat: "string",
	},
	{
		Key:          "pe_competitive_position",
		Text:         "How does this firm differentiate itself from other PE acquirers in the RIA space? What is their value proposition to potential sellers? Consider their integration approach, growth resources, technology, and cultural philosophy.",
		Tier:         2,
		Category:     CatSynthesis,
		PageTypes:    []string{"about", "strategy", "homepage"},
		OutputFormat: "string",
	},

	// ─── Category G: Blog & Content Intelligence (10 questions, T2 Sonnet) ───

	{
		Key:          "pe_investment_themes",
		Text:         "What investment themes or theses does this firm discuss in their blog posts or insights content? Identify specific themes (e.g., wealth management consolidation, fee-based advisory, succession planning) with supporting quotes or examples.",
		Tier:         2,
		Category:     CatBlogIntel,
		PageTypes:    []string{"blog", "strategy"},
		OutputFormat: "string",
	},
	{
		Key:          "pe_market_views",
		Text:         "What is this firm's stated view on current market conditions, M&A trends, or industry outlook? Summarize any commentary on valuations, deal flow, regulatory environment, or competitive dynamics.",
		Tier:         2,
		Category:     CatBlogIntel,
		PageTypes:    []string{"blog", "news", "strategy"},
		OutputFormat: "string",
	},
	{
		Key:          "pe_target_profile_signals",
		Text:         "Based on blog content and insights, what specific characteristics does this firm seek in acquisition targets? Look for signals about ideal revenue size, client demographics, geographic preferences, service model, or cultural fit criteria.",
		Tier:         2,
		Category:     CatBlogIntel,
		PageTypes:    []string{"blog", "strategy", "portfolio"},
		OutputFormat: "string",
	},
	{
		Key:          "pe_deal_announcements",
		Text:         "List specific deals, acquisitions, or partnerships announced in press releases or blog posts. Return as a JSON array of objects with fields: company_name, date (if stated), deal_type (acquisition/partnership/investment), details (one sentence).",
		Tier:         2,
		Category:     CatBlogIntel,
		PageTypes:    []string{"news", "blog", "portfolio"},
		OutputFormat: "json",
	},
	{
		Key:          "pe_fundraise_signals",
		Text:         "Are there any fundraising announcements, fund closings, capital raise activity, or LP-related updates in the firm's blog or press content? Include specific amounts, fund names, and dates if mentioned.",
		Tier:         2,
		Category:     CatBlogIntel,
		PageTypes:    []string{"news", "blog", "homepage"},
		OutputFormat: "string",
	},
	{
		Key:          "pe_portfolio_updates",
		Text:         "What portfolio company updates, milestones, growth metrics, or exits are discussed in blog posts or press releases? Return as a JSON array of objects with fields: company_name, update_type (growth/milestone/exit/hire/expansion), details (one sentence), date (if stated).",
		Tier:         2,
		Category:     CatBlogIntel,
		PageTypes:    []string{"blog", "news", "portfolio"},
		OutputFormat: "json",
	},
	{
		Key:          "pe_hiring_expansion",
		Text:         "Are there any signals of team growth, new hires, office openings, or geographic expansion mentioned in blog posts or press releases? Describe specific people, roles, or locations.",
		Tier:         2,
		Category:     CatBlogIntel,
		PageTypes:    []string{"blog", "news", "careers"},
		OutputFormat: "string",
	},
	{
		Key:          "pe_thought_leadership",
		Text:         "Summarize the firm's top 3 most substantive blog posts or insights pieces. For each, provide: the topic, key argument or insight, and relevance to their M&A strategy.",
		Tier:         2,
		Category:     CatBlogIntel,
		PageTypes:    []string{"blog"},
		OutputFormat: "string",
	},
	{
		Key:          "pe_content_recency",
		Text:         "What is the date of the most recent blog post or press release? If multiple dates are visible, also state the second and third most recent. This indicates the firm's content activity level.",
		Tier:         2,
		Category:     CatBlogIntel,
		PageTypes:    []string{"blog", "news"},
		OutputFormat: "string",
	},
	{
		Key:          "pe_competitive_intel",
		Text:         "What does the firm say about competitors, market positioning, or their differentiation in blog content? Look for mentions of other PE firms, competitive advantages they claim, or market share commentary.",
		Tier:         2,
		Category:     CatBlogIntel,
		PageTypes:    []string{"blog", "strategy", "about"},
		OutputFormat: "string",
	},
}
