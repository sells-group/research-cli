package advextract

import (
	"encoding/json"
	"fmt"
	"strings"
)

// AdvisorDocs holds all assembled documents for a single advisor.
type AdvisorDocs struct {
	CRDNumber int
	FirmName  string

	// Part 1: Structured data formatted as readable text
	Part1Formatted string

	// Part 2: Brochure text sectioned by Item 1-18
	BrochureSections map[string]string // section key → text

	// Part 3: CRS full text
	CRSText string

	// Owners from Schedule A/B
	OwnersFormatted string

	// Funds
	Funds []FundRow

	// Raw advisor row for structured bypass
	Advisor *AdvisorRow
}

// AssembleDocs builds an AdvisorDocs from store data.
func AssembleDocs(advisor *AdvisorRow, brochures []BrochureRow, crs []CRSRow, owners []OwnerRow, funds []FundRow) *AdvisorDocs {
	docs := &AdvisorDocs{
		CRDNumber: advisor.CRDNumber,
		FirmName:  advisor.FirmName,
		Advisor:   advisor,
		Funds:     funds,
	}

	// Format Part 1 structured data.
	docs.Part1Formatted = FormatPart1Structured(advisor)

	// Section the best brochure (most recent with content).
	if len(brochures) > 0 {
		docs.BrochureSections = SectionBrochure(brochures[0].TextContent)
	} else {
		docs.BrochureSections = make(map[string]string)
	}

	// Use the most recent CRS.
	if len(crs) > 0 {
		docs.CRSText = crs[0].TextContent
	}

	// Format owners.
	docs.OwnersFormatted = formatOwners(owners)

	return docs
}

// FormatPart1Structured converts the advisor's filing data into readable text
// for LLM context, organized by section.
func FormatPart1Structured(a *AdvisorRow) string {
	if a == nil {
		return ""
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "=== ADV Part 1 Structured Data for CRD %d ===\n\n", a.CRDNumber)

	// Firm Identity
	sb.WriteString("--- Firm Identity ---\n")
	fmt.Fprintf(&sb, "Firm Name: %s\n", a.FirmName)
	fmt.Fprintf(&sb, "CRD Number: %d\n", a.CRDNumber)
	fmt.Fprintf(&sb, "City: %s\n", a.City)
	fmt.Fprintf(&sb, "State: %s\n", a.State)
	fmt.Fprintf(&sb, "Website: %s\n", a.Website)
	if a.FilingDate != nil {
		fmt.Fprintf(&sb, "Filing Date: %s\n", a.FilingDate.Format("2006-01-02"))
	}

	// AUM
	sb.WriteString("\n--- Assets Under Management ---\n")
	if a.AUMTotal != nil {
		fmt.Fprintf(&sb, "Total AUM: $%s\n", formatDollars(*a.AUMTotal))
	}
	if a.AUMDiscretionary != nil {
		fmt.Fprintf(&sb, "Discretionary AUM: $%s\n", formatDollars(*a.AUMDiscretionary))
	}
	if a.AUMNonDiscretionary != nil {
		fmt.Fprintf(&sb, "Non-Discretionary AUM: $%s\n", formatDollars(*a.AUMNonDiscretionary))
	}
	if a.NumAccounts != nil {
		fmt.Fprintf(&sb, "Number of Accounts: %d\n", *a.NumAccounts)
	}
	if a.AUMTotal != nil && a.NumAccounts != nil && *a.NumAccounts > 0 {
		avg := *a.AUMTotal / int64(*a.NumAccounts)
		fmt.Fprintf(&sb, "Average Account Size: $%s\n", formatDollars(avg))
	}

	// Employees
	sb.WriteString("\n--- Employees ---\n")
	if a.TotalEmployees != nil {
		fmt.Fprintf(&sb, "Total Employees: %d\n", *a.TotalEmployees)
	}

	// Client Types (if JSONB data available)
	if len(a.ClientTypes) > 0 && string(a.ClientTypes) != "null" {
		sb.WriteString("\n--- Client Types ---\n")
		sb.WriteString(formatClientTypes(a.ClientTypes))
	}

	// Full filing data for structured bypass (selected fields)
	if a.Filing != nil {
		sb.WriteString("\n--- Compensation Types ---\n")
		compFields := []struct{ key, label string }{
			{"comp_pct_aum", "% of AUM"},
			{"comp_hourly", "Hourly"},
			{"comp_subscription", "Subscription"},
			{"comp_fixed", "Fixed"},
			{"comp_commissions", "Commissions"},
			{"comp_performance", "Performance"},
			{"comp_other", "Other"},
		}
		for _, cf := range compFields {
			if v, ok := a.Filing[cf.key]; ok && v != nil && v != false {
				fmt.Fprintf(&sb, "  %s: %v\n", cf.label, v)
			}
		}

		sb.WriteString("\n--- Registration Status ---\n")
		regFields := []struct{ key, label string }{
			{"sec_registered", "SEC Registered"},
			{"exempt_reporting", "Exempt Reporting Adviser"},
			{"state_registered", "State Registered"},
		}
		for _, rf := range regFields {
			if v, ok := a.Filing[rf.key]; ok && v != nil {
				fmt.Fprintf(&sb, "  %s: %v\n", rf.label, v)
			}
		}

		sb.WriteString("\n--- Disciplinary History (DRP Flags) ---\n")
		drpFields := []struct{ key, label string }{
			{"has_any_drp", "Has Any DRP"},
			{"drp_criminal_firm", "Criminal - Firm"},
			{"drp_regulatory_firm", "Regulatory - Firm"},
			{"drp_civil_firm", "Civil - Firm"},
			{"drp_complaint_firm", "Complaint - Firm"},
		}
		for _, df := range drpFields {
			if v, ok := a.Filing[df.key]; ok && v != nil {
				fmt.Fprintf(&sb, "  %s: %v\n", df.label, v)
			}
		}

		sb.WriteString("\n--- Custody ---\n")
		custodyFields := []struct{ key, label string }{
			{"custody_client_cash", "Custody of Client Cash"},
			{"custody_client_securities", "Custody of Client Securities"},
			{"custody_qualified_custodian", "Qualified Custodian"},
			{"custody_surprise_exam", "Surprise Exam"},
		}
		for _, cf := range custodyFields {
			if v, ok := a.Filing[cf.key]; ok && v != nil {
				fmt.Fprintf(&sb, "  %s: %v\n", cf.label, v)
			}
		}

		sb.WriteString("\n--- Transactions & Cross-Trading ---\n")
		txnFields := []struct{ key, label string }{
			{"txn_agency_cross", "Agency Cross Transactions"},
			{"txn_principal", "Principal Transactions"},
			{"txn_proprietary_interest", "Proprietary Interest"},
			{"txn_referral_compensation", "Referral Compensation"},
			{"txn_revenue_sharing", "Revenue Sharing"},
		}
		for _, tf := range txnFields {
			if v, ok := a.Filing[tf.key]; ok && v != nil {
				fmt.Fprintf(&sb, "  %s: %v\n", tf.label, v)
			}
		}

		sb.WriteString("\n--- Other Business Activities ---\n")
		bizFields := []struct{ key, label string }{
			{"biz_broker_dealer", "Broker-Dealer"},
			{"biz_insurance", "Insurance"},
			{"biz_real_estate", "Real Estate"},
			{"biz_accountant", "Accountant"},
			{"biz_lawyer", "Lawyer"},
			{"biz_bank", "Bank"},
			{"biz_trust_company", "Trust Company"},
		}
		for _, bf := range bizFields {
			if v, ok := a.Filing[bf.key]; ok && v != nil && v != false {
				fmt.Fprintf(&sb, "  %s: %v\n", bf.label, v)
			}
		}

		sb.WriteString("\n--- Offices ---\n")
		if v, ok := a.Filing["num_other_offices"]; ok && v != nil {
			fmt.Fprintf(&sb, "  Number of Other Offices: %v\n", v)
		}
	}

	return sb.String()
}

// DocumentForQuestion assembles the relevant document context for a question.
func DocumentForQuestion(docs *AdvisorDocs, q Question) string {
	var parts []string

	for _, src := range q.SourceDocs {
		switch src {
		case "part1":
			if docs.Part1Formatted != "" {
				parts = append(parts, docs.Part1Formatted)
			}
		case "part2":
			if len(q.SourceSections) > 0 {
				text := SectionsForItems(docs.BrochureSections, q.SourceSections...)
				if text != "" {
					parts = append(parts, "=== ADV Part 2 Brochure (Relevant Sections) ===\n\n"+text)
				}
			} else if full, ok := docs.BrochureSections[SectionFull]; ok {
				parts = append(parts, "=== ADV Part 2 Brochure ===\n\n"+truncateText(full, 15000))
			}
		case "part3":
			if docs.CRSText != "" {
				parts = append(parts, "=== ADV Part 3 CRS ===\n\n"+truncateText(docs.CRSText, 8000))
			}
		}
	}

	// Add owner context for ownership-related questions.
	if docs.OwnersFormatted != "" && needsOwnerContext(q) {
		parts = append(parts, docs.OwnersFormatted)
	}

	return strings.Join(parts, "\n\n")
}

// FundContext assembles context for a fund-level question.
func FundContext(docs *AdvisorDocs, fund FundRow) string {
	var sb strings.Builder

	// Fund structured data
	fmt.Fprintf(&sb, "=== Fund: %s (ID: %s) ===\n", fund.FundName, fund.FundID)
	fmt.Fprintf(&sb, "Type: %s\n", fund.FundType)
	if fund.GrossAssetValue != nil {
		fmt.Fprintf(&sb, "Gross Asset Value: $%s\n", formatDollars(*fund.GrossAssetValue))
	}
	if fund.NetAssetValue != nil {
		fmt.Fprintf(&sb, "Net Asset Value: $%s\n", formatDollars(*fund.NetAssetValue))
	}

	// Find brochure sections mentioning this fund
	if len(docs.BrochureSections) > 0 {
		fundMentions := findFundMentions(docs.BrochureSections, fund.FundName)
		if fundMentions != "" {
			sb.WriteString("\n--- Brochure Sections Mentioning This Fund ---\n")
			sb.WriteString(fundMentions)
		}
	}

	return sb.String()
}

// findFundMentions searches brochure sections for text mentioning the fund name.
func findFundMentions(sections map[string]string, fundName string) string {
	if fundName == "" {
		return ""
	}

	nameLower := strings.ToLower(fundName)
	var parts []string

	for key, text := range sections {
		if key == SectionFull {
			continue
		}
		if strings.Contains(strings.ToLower(text), nameLower) {
			header := itemHeaders[key]
			if header == "" {
				header = key
			}
			parts = append(parts, fmt.Sprintf("--- %s ---\n%s", header, text))
		}
	}

	return strings.Join(parts, "\n\n")
}

func formatOwners(owners []OwnerRow) string {
	if len(owners) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("=== Ownership (Schedule A/B) ===\n")
	for _, o := range owners {
		pct := "N/A"
		if o.OwnershipPct != nil {
			pct = fmt.Sprintf("%.1f%%", *o.OwnershipPct)
		}
		control := ""
		if o.IsControl {
			control = " [CONTROL PERSON]"
		}
		fmt.Fprintf(&sb, "- %s (%s): %s%s\n", o.OwnerName, o.OwnerType, pct, control)
	}
	return sb.String()
}

func formatClientTypes(raw json.RawMessage) string {
	var clients []map[string]any
	if err := json.Unmarshal(raw, &clients); err != nil {
		return string(raw) + "\n"
	}

	var sb strings.Builder
	for _, c := range clients {
		name, _ := c["type"].(string)
		count, _ := c["count"].(float64)
		pctRAUM, _ := c["pct_raum"].(float64)
		if name != "" && (count > 0 || pctRAUM > 0) {
			fmt.Fprintf(&sb, "  %s: %d clients, %.1f%% RAUM\n", name, int(count), pctRAUM)
		}
	}
	return sb.String()
}

func formatDollars(v int64) string {
	if v == 0 {
		return "0"
	}
	s := fmt.Sprintf("%d", v)
	if len(s) <= 3 {
		return s
	}

	var result []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}

func truncateText(text string, maxLen int) string {
	if len(text) <= maxLen {
		return text
	}
	return text[:maxLen] + "\n... [truncated]"
}

func needsOwnerContext(q Question) bool {
	ownerKeys := map[string]bool{
		"ownership_structure":    true,
		"employee_ownership":     true,
		"operating_subsidiaries": true,
		"acquisition_history":    true,
	}
	return ownerKeys[q.Key]
}
