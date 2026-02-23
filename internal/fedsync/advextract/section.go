package advextract

import (
	"fmt"
	"regexp"
	"strings"
)

// BrochureSection keys used in question routing.
const (
	SectionCoverPage       = "item_1"
	SectionMaterialChanges = "item_2"
	SectionTOC             = "item_3"
	SectionAdvisoryBiz     = "item_4"
	SectionFees            = "item_5"
	SectionPerformanceFees = "item_6"
	SectionClientTypes     = "item_7"
	SectionInvestment      = "item_8"
	SectionDisciplinary    = "item_9"
	SectionAffiliations    = "item_10"
	SectionCodeOfEthics    = "item_11"
	SectionBrokerage       = "item_12"
	SectionReviewAccounts  = "item_13"
	SectionReferrals       = "item_14"
	SectionCustody         = "item_15"
	SectionDiscretion      = "item_16"
	SectionProxyVoting     = "item_17"
	SectionFinancialInfo   = "item_18"
	SectionFull            = "full" // fallback: entire brochure
)

// itemHeaders maps section keys to their canonical titles for display.
var itemHeaders = map[string]string{
	SectionCoverPage:       "Cover Page",
	SectionMaterialChanges: "Material Changes",
	SectionTOC:             "Table of Contents",
	SectionAdvisoryBiz:     "Advisory Business",
	SectionFees:            "Fees and Compensation",
	SectionPerformanceFees: "Performance-Based Fees and Side-By-Side Management",
	SectionClientTypes:     "Types of Clients",
	SectionInvestment:      "Methods of Analysis, Investment Strategies and Risk of Loss",
	SectionDisciplinary:    "Disciplinary Information",
	SectionAffiliations:    "Other Financial Industry Activities and Affiliations",
	SectionCodeOfEthics:    "Code of Ethics, Participation or Interest in Client Transactions and Personal Trading",
	SectionBrokerage:       "Brokerage Practices",
	SectionReviewAccounts:  "Review of Accounts",
	SectionReferrals:       "Client Referrals and Other Compensation",
	SectionCustody:         "Custody",
	SectionDiscretion:      "Investment Discretion",
	SectionProxyVoting:     "Voting Client Securities",
	SectionFinancialInfo:   "Financial Information",
}

// itemPattern matches ADV Part 2 item headers in brochure text.
// Handles variants like:
//   - "Item 4 – Advisory Business"
//   - "Item 4: Advisory Business"
//   - "ITEM 4 ADVISORY BUSINESS"
//   - "Item 4 - Advisory Business"
//   - "Item 4. Advisory Business"
var itemPattern = regexp.MustCompile(
	`(?im)^[\s]*item\s+(\d{1,2})\s*[:\-–—.\s]+\s*(.*)$`,
)

// SectionBrochure splits ADV Part 2 brochure text into sections by Item header.
// Returns a map from section key (e.g., "item_4") to the text of that section.
// The "full" key always contains the complete text.
// If no items are detected, only "full" is returned.
func SectionBrochure(text string) map[string]string {
	sections := make(map[string]string)
	sections[SectionFull] = text

	if text == "" {
		return sections
	}

	// Find all item header positions.
	matches := itemPattern.FindAllStringSubmatchIndex(text, -1)
	if len(matches) == 0 {
		return sections
	}

	type headerMatch struct {
		itemNum int
		start   int // start of header line
		end     int // end of header line (start of content)
	}

	var headers []headerMatch
	for _, m := range matches {
		numStr := text[m[2]:m[3]]
		num := parseItemNumber(numStr)
		if num < 1 || num > 18 {
			continue
		}
		headers = append(headers, headerMatch{
			itemNum: num,
			start:   m[0],
			end:     m[1],
		})
	}

	if len(headers) == 0 {
		return sections
	}

	// Extract text between consecutive headers.
	for i, h := range headers {
		key := itemKey(h.itemNum)
		var content string
		if i+1 < len(headers) {
			content = text[h.end:headers[i+1].start]
		} else {
			content = text[h.end:]
		}
		content = strings.TrimSpace(content)
		if content != "" {
			sections[key] = content
		}
	}

	return sections
}

// SectionsForItems returns concatenated text from the specified section keys.
// Falls back to full text if no matching sections are found.
func SectionsForItems(sections map[string]string, keys ...string) string {
	var parts []string
	for _, key := range keys {
		if text, ok := sections[key]; ok {
			header := itemHeaders[key]
			if header == "" {
				header = key
			}
			parts = append(parts, "--- "+header+" ---\n"+text)
		}
	}
	if len(parts) == 0 {
		if full, ok := sections[SectionFull]; ok {
			return full
		}
		return ""
	}
	return strings.Join(parts, "\n\n")
}

// itemKey converts an item number (1-18) to a section key like "item_4".
func itemKey(num int) string {
	return fmt.Sprintf("item_%d", num)
}

func parseItemNumber(s string) int {
	s = strings.TrimSpace(s)
	n := 0
	for _, c := range s {
		if c >= '0' && c <= '9' {
			n = n*10 + int(c-'0')
		}
	}
	return n
}
