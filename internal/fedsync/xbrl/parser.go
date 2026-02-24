// Package xbrl parses XBRL JSON-LD fact data from EDGAR filings.
package xbrl

import (
	"encoding/json"
	"io"

	"github.com/rotisserie/eris"
)

// CompanyFacts represents the EDGAR company facts JSON-LD structure.
type CompanyFacts struct {
	CIK        int               `json:"cik"`
	EntityName string            `json:"entityName"`
	Facts      map[string]FactNS `json:"facts"`
}

// FactNS groups facts by namespace (e.g., "us-gaap", "dei").
type FactNS map[string]Fact

// Fact is a single XBRL fact with its units and values.
type Fact struct {
	Label       string                 `json:"label"`
	Description string                 `json:"description"`
	Units       map[string][]FactValue `json:"units"`
}

// FactValue is a single data point for a fact.
type FactValue struct {
	End   string `json:"end"`
	Val   any    `json:"val"`
	Accn  string `json:"accn"`
	FY    int    `json:"fy"`
	FP    string `json:"fp"`
	Form  string `json:"form"`
	Filed string `json:"filed"`
	Frame string `json:"frame,omitempty"`
}

// ExtractedFact is a flattened fact ready for database insertion.
type ExtractedFact struct {
	CIK      int
	FactName string
	Period   string
	Value    any
	Unit     string
	Form     string
	Filed    string
	FY       int
}

// ParseCompanyFacts parses EDGAR Company Facts JSON-LD from a reader.
func ParseCompanyFacts(r io.Reader) (*CompanyFacts, error) {
	var facts CompanyFacts
	if err := json.NewDecoder(r).Decode(&facts); err != nil {
		return nil, eris.Wrap(err, "xbrl: parse company facts")
	}
	return &facts, nil
}

// ExtractTargetFacts extracts specific US-GAAP facts matching the target taxonomy.
func ExtractTargetFacts(facts *CompanyFacts, targets []string) []ExtractedFact {
	if facts == nil || len(facts.Facts) == 0 {
		return nil
	}

	targetSet := make(map[string]bool, len(targets))
	for _, t := range targets {
		targetSet[t] = true
	}

	var result []ExtractedFact

	// Check us-gaap namespace first, then dei
	for _, ns := range []string{"us-gaap", "dei"} {
		nsMap, ok := facts.Facts[ns]
		if !ok {
			continue
		}

		for factName, fact := range nsMap {
			if !targetSet[factName] {
				continue
			}

			for unit, values := range fact.Units {
				for _, v := range values {
					if v.End == "" {
						continue
					}
					result = append(result, ExtractedFact{
						CIK:      facts.CIK,
						FactName: factName,
						Period:   v.End,
						Value:    v.Val,
						Unit:     unit,
						Form:     v.Form,
						Filed:    v.Filed,
						FY:       v.FY,
					})
				}
			}
		}
	}

	return result
}
