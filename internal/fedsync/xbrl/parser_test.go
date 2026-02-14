package xbrl

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sampleCompanyFacts = `{
  "cik": 320193,
  "entityName": "Apple Inc.",
  "facts": {
    "us-gaap": {
      "Assets": {
        "label": "Assets",
        "description": "Total assets",
        "units": {
          "USD": [
            {
              "end": "2023-09-30",
              "val": 352583000000,
              "accn": "0000320193-23-000106",
              "fy": 2023,
              "fp": "FY",
              "form": "10-K",
              "filed": "2023-11-03",
              "frame": "CY2023Q3I"
            },
            {
              "end": "2022-09-24",
              "val": 352755000000,
              "accn": "0000320193-22-000108",
              "fy": 2022,
              "fp": "FY",
              "form": "10-K",
              "filed": "2022-10-28"
            }
          ]
        }
      },
      "NetIncomeLoss": {
        "label": "Net Income (Loss)",
        "description": "Net income or loss",
        "units": {
          "USD": [
            {
              "end": "2023-09-30",
              "val": 96995000000,
              "accn": "0000320193-23-000106",
              "fy": 2023,
              "fp": "FY",
              "form": "10-K",
              "filed": "2023-11-03"
            }
          ]
        }
      },
      "SomeOtherFact": {
        "label": "Some Other",
        "description": "Not a target",
        "units": {
          "USD": [
            {
              "end": "2023-09-30",
              "val": 100,
              "accn": "0000320193-23-000106",
              "fy": 2023,
              "fp": "FY",
              "form": "10-K",
              "filed": "2023-11-03"
            }
          ]
        }
      }
    },
    "dei": {
      "NumberOfEmployees": {
        "label": "Number of Employees",
        "description": "Total employees",
        "units": {
          "pure": [
            {
              "end": "2023-09-30",
              "val": 161000,
              "accn": "0000320193-23-000106",
              "fy": 2023,
              "fp": "FY",
              "form": "10-K",
              "filed": "2023-11-03"
            }
          ]
        }
      }
    }
  }
}`

func TestParseCompanyFacts(t *testing.T) {
	facts, err := ParseCompanyFacts(strings.NewReader(sampleCompanyFacts))
	require.NoError(t, err)

	assert.Equal(t, 320193, facts.CIK)
	assert.Equal(t, "Apple Inc.", facts.EntityName)
	assert.Contains(t, facts.Facts, "us-gaap")
	assert.Contains(t, facts.Facts, "dei")

	usGaap := facts.Facts["us-gaap"]
	assert.Contains(t, usGaap, "Assets")
	assert.Contains(t, usGaap, "NetIncomeLoss")

	assets := usGaap["Assets"]
	assert.Equal(t, "Assets", assets.Label)
	assert.Contains(t, assets.Units, "USD")
	assert.Len(t, assets.Units["USD"], 2)
	assert.Equal(t, "2023-09-30", assets.Units["USD"][0].End)
}

func TestParseCompanyFacts_Invalid(t *testing.T) {
	_, err := ParseCompanyFacts(strings.NewReader("not json"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "xbrl: parse company facts")
}

func TestExtractTargetFacts(t *testing.T) {
	facts, err := ParseCompanyFacts(strings.NewReader(sampleCompanyFacts))
	require.NoError(t, err)

	targets := []string{"Assets", "NetIncomeLoss", "NumberOfEmployees"}
	extracted := ExtractTargetFacts(facts, targets)

	// Should get: 2 Assets + 1 NetIncomeLoss + 1 NumberOfEmployees = 4
	assert.Len(t, extracted, 4)

	// Verify all are target facts
	for _, ef := range extracted {
		assert.Equal(t, 320193, ef.CIK)
		assert.Contains(t, targets, ef.FactName)
		assert.NotEmpty(t, ef.Period)
		assert.NotNil(t, ef.Value)
	}

	// Check that SomeOtherFact was not extracted
	for _, ef := range extracted {
		assert.NotEqual(t, "SomeOtherFact", ef.FactName)
	}
}

func TestExtractTargetFacts_NoTargets(t *testing.T) {
	facts, err := ParseCompanyFacts(strings.NewReader(sampleCompanyFacts))
	require.NoError(t, err)

	extracted := ExtractTargetFacts(facts, []string{"NonexistentFact"})
	assert.Empty(t, extracted)
}

func TestExtractTargetFacts_NilFacts(t *testing.T) {
	extracted := ExtractTargetFacts(nil, TargetFacts)
	assert.Empty(t, extracted)
}

func TestExtractTargetFacts_EmptyFacts(t *testing.T) {
	facts := &CompanyFacts{CIK: 1, EntityName: "Test", Facts: map[string]FactNS{}}
	extracted := ExtractTargetFacts(facts, TargetFacts)
	assert.Empty(t, extracted)
}

func TestExtractTargetFacts_SkipsEmptyEnd(t *testing.T) {
	factsJSON := `{
		"cik": 1,
		"entityName": "Test",
		"facts": {
			"us-gaap": {
				"Assets": {
					"label": "Assets",
					"description": "test",
					"units": {
						"USD": [
							{"end": "", "val": 100, "accn": "x", "fy": 2023, "fp": "FY", "form": "10-K", "filed": "2023-01-01"},
							{"end": "2023-12-31", "val": 200, "accn": "y", "fy": 2023, "fp": "FY", "form": "10-K", "filed": "2023-01-01"}
						]
					}
				}
			}
		}
	}`

	facts, err := ParseCompanyFacts(strings.NewReader(factsJSON))
	require.NoError(t, err)

	extracted := ExtractTargetFacts(facts, []string{"Assets"})
	assert.Len(t, extracted, 1)
	assert.Equal(t, "2023-12-31", extracted[0].Period)
}

func TestTargetFacts_NotEmpty(t *testing.T) {
	assert.NotEmpty(t, TargetFacts)
	assert.Contains(t, TargetFacts, "Assets")
	assert.Contains(t, TargetFacts, "NetIncomeLoss")
	assert.Contains(t, TargetFacts, "NumberOfEmployees")
}
