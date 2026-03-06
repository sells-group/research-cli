package advextract

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// --- helpers ---

func ptr[T any](v T) *T { return &v }

func testAdvisorMinimal() *AdvisorRow {
	return &AdvisorRow{
		CRDNumber: 12345,
		FirmName:  "Test Advisors LLC",
	}
}

func testAdvisorFull() *AdvisorRow {
	fd := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
	return &AdvisorRow{
		CRDNumber:           12345,
		FirmName:            "Test Advisors LLC",
		City:                "New York",
		State:               "NY",
		Website:             "https://testadvisors.com",
		AUMTotal:            ptr(int64(5000000000)),
		AUMDiscretionary:    ptr(int64(4500000000)),
		AUMNonDiscretionary: ptr(int64(500000000)),
		NumAccounts:         ptr(1000),
		TotalEmployees:      ptr(50),
		FilingDate:          &fd,
		ClientTypes: json.RawMessage(`[
			{"type":"Individuals","count":500,"pct_raum":30.0},
			{"type":"High Net Worth","count":200,"pct_raum":50.0}
		]`),
		Filing: map[string]any{
			"comp_pct_aum":                true,
			"comp_fixed":                  true,
			"comp_commissions":            false,
			"sec_registered":              true,
			"exempt_reporting":            false,
			"has_any_drp":                 false,
			"drp_criminal_firm":           false,
			"custody_client_cash":         true,
			"custody_client_securities":   true,
			"custody_qualified_custodian": true,
			"custody_surprise_exam":       false,
			"txn_agency_cross":            false,
			"txn_principal":               false,
			"txn_referral_compensation":   true,
			"biz_broker_dealer":           true,
			"biz_insurance":               true,
			"num_other_offices":           float64(3),
		},
	}
}

func testAdvisorDocs() *AdvisorDocs {
	return &AdvisorDocs{
		CRDNumber: 12345,
		FirmName:  "Test Advisors LLC",
		Part1Formatted: "=== ADV Part 1 Structured Data for CRD 12345 ===\n\n" +
			"--- Firm Identity ---\nFirm Name: Test Advisors LLC\n",
		BrochureSections: map[string]string{
			"item_4": "We provide investment advisory services.",
			"item_5": "Our fees are based on a percentage of AUM.",
			"full":   "Full brochure text here.",
		},
		CRSText:         "This is the CRS document text.",
		OwnersFormatted: "=== Ownership (Schedule A/B) ===\n- John Doe (Individual): 60.0%\n",
		Funds: []FundRow{
			{CRDNumber: 12345, FundID: "F1", FundName: "Alpha Fund", FundType: "Hedge Fund",
				GrossAssetValue: ptr(int64(100000000)), NetAssetValue: ptr(int64(90000000))},
		},
		Advisor: testAdvisorMinimal(),
	}
}

// --- FormatPart1Structured ---

func TestFormatPart1Structured_NilAdvisor(t *testing.T) {
	require.Equal(t, "", FormatPart1Structured(nil))
}

func TestFormatPart1Structured_Minimal(t *testing.T) {
	out := FormatPart1Structured(testAdvisorMinimal())

	require.Contains(t, out, "Firm Name: Test Advisors LLC")
	require.Contains(t, out, "CRD Number: 12345")
	require.Contains(t, out, "--- Firm Identity ---")
	require.Contains(t, out, "--- Assets Under Management ---")
	// No AUM values set.
	require.NotContains(t, out, "Total AUM:")
	require.NotContains(t, out, "Number of Accounts:")
	// No filing data.
	require.NotContains(t, out, "--- Compensation Types ---")
}

func TestFormatPart1Structured_Full(t *testing.T) {
	out := FormatPart1Structured(testAdvisorFull())

	// Firm identity.
	require.Contains(t, out, "Firm Name: Test Advisors LLC")
	require.Contains(t, out, "City: New York")
	require.Contains(t, out, "State: NY")
	require.Contains(t, out, "Website: https://testadvisors.com")
	require.Contains(t, out, "Filing Date: 2025-06-15")

	// AUM.
	require.Contains(t, out, "Total AUM: $5,000,000,000")
	require.Contains(t, out, "Discretionary AUM: $4,500,000,000")
	require.Contains(t, out, "Non-Discretionary AUM: $500,000,000")
	require.Contains(t, out, "Number of Accounts: 1000")
	require.Contains(t, out, "Average Account Size: $5,000,000")

	// Employees.
	require.Contains(t, out, "Total Employees: 50")

	// Client types.
	require.Contains(t, out, "--- Client Types ---")
	require.Contains(t, out, "Individuals: 500 clients, 30.0% RAUM")
	require.Contains(t, out, "High Net Worth: 200 clients, 50.0% RAUM")

	// Filing sections.
	require.Contains(t, out, "--- Compensation Types ---")
	require.Contains(t, out, "% of AUM: true")
	require.Contains(t, out, "Fixed: true")
	// comp_commissions is false, should be filtered by != false check.
	require.NotContains(t, out, "Commissions: false")

	require.Contains(t, out, "--- Registration Status ---")
	require.Contains(t, out, "SEC Registered: true")

	require.Contains(t, out, "--- Disciplinary History (DRP Flags) ---")
	require.Contains(t, out, "Has Any DRP: false")

	require.Contains(t, out, "--- Custody ---")
	require.Contains(t, out, "Custody of Client Cash: true")

	require.Contains(t, out, "--- Transactions & Cross-Trading ---")
	require.Contains(t, out, "Referral Compensation: true")

	require.Contains(t, out, "--- Other Business Activities ---")
	require.Contains(t, out, "Broker-Dealer: true")
	require.Contains(t, out, "Insurance: true")

	require.Contains(t, out, "--- Offices ---")
	require.Contains(t, out, "Number of Other Offices: 3")
}

// --- DocumentForQuestion ---

func TestDocumentForQuestion_Part1Source(t *testing.T) {
	docs := testAdvisorDocs()
	q := Question{Key: "firm_name", SourceDocs: []string{"part1"}}

	out := DocumentForQuestion(docs, q)

	require.Contains(t, out, "ADV Part 1 Structured Data")
	require.NotContains(t, out, "Brochure")
	require.NotContains(t, out, "CRS")
}

func TestDocumentForQuestion_Part2WithSections(t *testing.T) {
	docs := testAdvisorDocs()
	q := Question{
		Key:            "fee_structure",
		SourceDocs:     []string{"part2"},
		SourceSections: []string{"item_4", "item_5"},
	}

	out := DocumentForQuestion(docs, q)

	require.Contains(t, out, "ADV Part 2 Brochure (Relevant Sections)")
	require.Contains(t, out, "Advisory Business")
	require.Contains(t, out, "investment advisory services")
	require.Contains(t, out, "Fees and Compensation")
	require.Contains(t, out, "percentage of AUM")
}

func TestDocumentForQuestion_Part2FullFallback(t *testing.T) {
	docs := testAdvisorDocs()
	q := Question{
		Key:        "general_info",
		SourceDocs: []string{"part2"},
		// No SourceSections → falls back to full brochure.
	}

	out := DocumentForQuestion(docs, q)

	require.Contains(t, out, "ADV Part 2 Brochure ===")
	require.Contains(t, out, "Full brochure text here.")
}

func TestDocumentForQuestion_Part3Source(t *testing.T) {
	docs := testAdvisorDocs()
	q := Question{Key: "crs_info", SourceDocs: []string{"part3"}}

	out := DocumentForQuestion(docs, q)

	require.Contains(t, out, "ADV Part 3 CRS")
	require.Contains(t, out, "CRS document text")
}

func TestDocumentForQuestion_MultipleSources(t *testing.T) {
	docs := testAdvisorDocs()
	q := Question{
		Key:        "cross_doc",
		SourceDocs: []string{"part1", "part2", "part3"},
	}

	out := DocumentForQuestion(docs, q)

	require.Contains(t, out, "ADV Part 1")
	require.Contains(t, out, "ADV Part 2 Brochure")
	require.Contains(t, out, "ADV Part 3 CRS")
}

func TestDocumentForQuestion_OwnerContextInjected(t *testing.T) {
	docs := testAdvisorDocs()
	q := Question{
		Key:        "ownership_structure",
		SourceDocs: []string{"part1"},
	}

	out := DocumentForQuestion(docs, q)

	require.Contains(t, out, "Ownership (Schedule A/B)")
	require.Contains(t, out, "John Doe")
}

func TestDocumentForQuestion_NoOwnerContextForUnrelatedKey(t *testing.T) {
	docs := testAdvisorDocs()
	q := Question{
		Key:        "fee_structure",
		SourceDocs: []string{"part1"},
	}

	out := DocumentForQuestion(docs, q)

	require.NotContains(t, out, "Ownership (Schedule A/B)")
}

// --- FundContext ---

func TestFundContext_WithAssets(t *testing.T) {
	docs := testAdvisorDocs()
	fund := FundRow{
		CRDNumber:       12345,
		FundID:          "F1",
		FundName:        "Alpha Fund",
		FundType:        "Hedge Fund",
		GrossAssetValue: ptr(int64(100000000)),
		NetAssetValue:   ptr(int64(90000000)),
	}

	out := FundContext(docs, fund)

	require.Contains(t, out, "Fund: Alpha Fund (ID: F1)")
	require.Contains(t, out, "Type: Hedge Fund")
	require.Contains(t, out, "Gross Asset Value: $100,000,000")
	require.Contains(t, out, "Net Asset Value: $90,000,000")
}

func TestFundContext_WithoutAssets(t *testing.T) {
	docs := testAdvisorDocs()
	fund := FundRow{
		CRDNumber: 12345,
		FundID:    "F2",
		FundName:  "Beta Fund",
		FundType:  "Private Equity",
	}

	out := FundContext(docs, fund)

	require.Contains(t, out, "Fund: Beta Fund")
	require.NotContains(t, out, "Gross Asset Value")
	require.NotContains(t, out, "Net Asset Value")
}

func TestFundContext_BrochureMentions(t *testing.T) {
	docs := &AdvisorDocs{
		BrochureSections: map[string]string{
			"item_4": "We manage the Alpha Fund for accredited investors.",
			"item_8": "The Alpha Fund uses a long/short equity strategy.",
			"full":   "Full brochure text mentioning Alpha Fund.",
		},
	}
	fund := FundRow{
		FundID:   "F1",
		FundName: "Alpha Fund",
		FundType: "Hedge Fund",
	}

	out := FundContext(docs, fund)

	require.Contains(t, out, "Brochure Sections Mentioning This Fund")
	require.Contains(t, out, "Alpha Fund")
}

// --- formatDollars ---

func TestFormatDollars(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0"},
		{123, "123"},
		{1234, "1,234"},
		{1234567, "1,234,567"},
		{1234567890, "1,234,567,890"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			require.Equal(t, tt.want, formatDollars(tt.input))
		})
	}
}

// --- truncateText ---

func TestTruncateText_Short(t *testing.T) {
	text := "short text"
	require.Equal(t, text, truncateText(text, 100))
}

func TestTruncateText_Long(t *testing.T) {
	text := strings.Repeat("x", 200)
	out := truncateText(text, 50)

	require.Len(t, out, 50+len("\n... [truncated]"))
	require.True(t, strings.HasSuffix(out, "\n... [truncated]"))
}

// --- formatOwners ---

func TestFormatOwners_Empty(t *testing.T) {
	require.Equal(t, "", formatOwners(nil))
	require.Equal(t, "", formatOwners([]OwnerRow{}))
}

func TestFormatOwners_SingleWithPct(t *testing.T) {
	owners := []OwnerRow{
		{OwnerName: "Jane Smith", OwnerType: "Individual", OwnershipPct: ptr(75.5)},
	}
	out := formatOwners(owners)

	require.Contains(t, out, "=== Ownership (Schedule A/B) ===")
	require.Contains(t, out, "- Jane Smith (Individual): 75.5%")
}

func TestFormatOwners_MultipleWithControl(t *testing.T) {
	owners := []OwnerRow{
		{OwnerName: "Jane Smith", OwnerType: "Individual", OwnershipPct: ptr(75.0), IsControl: true},
		{OwnerName: "Bob Jones", OwnerType: "Individual", OwnershipPct: ptr(25.0)},
	}
	out := formatOwners(owners)

	require.Contains(t, out, "Jane Smith (Individual): 75.0% [CONTROL PERSON]")
	require.Contains(t, out, "Bob Jones (Individual): 25.0%")
	require.Equal(t, 1, strings.Count(out, "[CONTROL PERSON]"))
}

func TestFormatOwners_NilPct(t *testing.T) {
	owners := []OwnerRow{
		{OwnerName: "Unknown LLC", OwnerType: "Entity"},
	}
	out := formatOwners(owners)

	require.Contains(t, out, "- Unknown LLC (Entity): N/A")
}

// --- formatClientTypes ---

func TestFormatClientTypes_ValidJSON(t *testing.T) {
	raw := json.RawMessage(`[{"type":"Individuals","count":100,"pct_raum":40.0},{"type":"HNW","count":50,"pct_raum":60.0}]`)
	out := formatClientTypes(raw)

	require.Contains(t, out, "Individuals: 100 clients, 40.0% RAUM")
	require.Contains(t, out, "HNW: 50 clients, 60.0% RAUM")
}

func TestFormatClientTypes_InvalidJSON(t *testing.T) {
	raw := json.RawMessage(`not json`)
	out := formatClientTypes(raw)

	require.Equal(t, "not json\n", out)
}

func TestFormatClientTypes_EmptyArray(t *testing.T) {
	raw := json.RawMessage(`[]`)
	out := formatClientTypes(raw)

	require.Equal(t, "", out)
}

// --- needsOwnerContext ---

func TestNeedsOwnerContext(t *testing.T) {
	tests := []struct {
		key  string
		want bool
	}{
		{"ownership_structure", true},
		{"employee_ownership", true},
		{"operating_subsidiaries", true},
		{"acquisition_history", true},
		{"fee_structure", false},
		{"aum_total", false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			q := Question{Key: tt.key}
			require.Equal(t, tt.want, needsOwnerContext(q))
		})
	}
}
