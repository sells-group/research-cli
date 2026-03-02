package salesforce

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cannedReportJSON is a minimal SF Analytics report response with tabular format.
const cannedReportJSON = `{
	"reportMetadata": {
		"id": "00OPo00000g8kDcMAI",
		"name": "Test Report",
		"detailColumns": ["Account.Id", "Account.Name", "Account.Website", "Account.BillingCity", "Account.BillingState"],
		"reportFormat": "TABULAR"
	},
	"factMap": {
		"T!T": {
			"rows": [
				{
					"dataCells": [
						{"label": "001xx0000001", "value": "001xx0000001"},
						{"label": "Acme Corp", "value": "Acme Corp"},
						{"label": "https://acme.com", "value": "https://acme.com"},
						{"label": "Springfield", "value": "Springfield"},
						{"label": "IL", "value": "IL"}
					]
				},
				{
					"dataCells": [
						{"label": "001xx0000002", "value": "001xx0000002"},
						{"label": "Beta Inc", "value": "Beta Inc"},
						{"label": "https://beta.io", "value": "https://beta.io"},
						{"label": "Chicago", "value": "Chicago"},
						{"label": "IL", "value": "IL"}
					]
				}
			]
		}
	}
}`

func TestRunReport(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/analytics/reports/00OPo00000g8kDcMAI?includeDetails=true", r.URL.String())
		assert.Equal(t, http.MethodGet, r.Method)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(cannedReportJSON))
	}))
	defer srv.Close()

	// Use mockClient to test RunReport via the interface.
	mc := &mockClient{
		runReportFn: func(_ context.Context, reportID string) (*ReportResult, error) {
			assert.Equal(t, "00OPo00000g8kDcMAI", reportID)
			var result ReportResult
			require.NoError(t, json.Unmarshal([]byte(cannedReportJSON), &result))
			return &result, nil
		},
	}

	result, err := mc.RunReport(context.Background(), "00OPo00000g8kDcMAI")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "00OPo00000g8kDcMAI", result.ReportMetadata.ID)
	assert.Equal(t, "Test Report", result.ReportMetadata.Name)
	assert.Len(t, result.ReportMetadata.DetailColumns, 5)
}

func TestParseReportAccounts_Tabular(t *testing.T) {
	t.Parallel()

	var result ReportResult
	require.NoError(t, json.Unmarshal([]byte(cannedReportJSON), &result))

	accounts, err := ParseReportAccounts(&result)
	require.NoError(t, err)
	require.Len(t, accounts, 2)

	assert.Equal(t, "001xx0000001", accounts[0].ID)
	assert.Equal(t, "Acme Corp", accounts[0].Name)
	assert.Equal(t, "https://acme.com", accounts[0].Website)
	assert.Equal(t, "Springfield", accounts[0].City)
	assert.Equal(t, "IL", accounts[0].State)

	assert.Equal(t, "001xx0000002", accounts[1].ID)
	assert.Equal(t, "Beta Inc", accounts[1].Name)
	assert.Equal(t, "https://beta.io", accounts[1].Website)
}

func TestParseReportAccounts_MissingIDColumn(t *testing.T) {
	t.Parallel()

	result := ReportResult{
		ReportMetadata: ReportMetadata{
			DetailColumns: []string{"Account.Name", "Account.Website"},
		},
		FactMap: map[string]json.RawMessage{
			"T!T": json.RawMessage(`{"rows": []}`),
		},
	}

	_, err := ParseReportAccounts(&result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing Account ID column")
}

func TestParseReportAccounts_SkipsEmptyID(t *testing.T) {
	t.Parallel()

	reportJSON := `{
		"reportMetadata": {
			"detailColumns": ["Account.Id", "Account.Name", "Account.Website"],
			"reportFormat": "TABULAR"
		},
		"factMap": {
			"T!T": {
				"rows": [
					{
						"dataCells": [
							{"label": "001xx0000001", "value": "001xx0000001"},
							{"label": "Acme Corp", "value": "Acme Corp"},
							{"label": "https://acme.com", "value": "https://acme.com"}
						]
					},
					{
						"dataCells": [
							{"label": "", "value": ""},
							{"label": "No ID Corp", "value": "No ID Corp"},
							{"label": "https://noid.com", "value": "https://noid.com"}
						]
					}
				]
			}
		}
	}`

	var result ReportResult
	require.NoError(t, json.Unmarshal([]byte(reportJSON), &result))

	accounts, err := ParseReportAccounts(&result)
	require.NoError(t, err)
	require.Len(t, accounts, 1)
	assert.Equal(t, "001xx0000001", accounts[0].ID)
}

func TestParseReportAccounts_AlternateColumnNames(t *testing.T) {
	t.Parallel()

	reportJSON := `{
		"reportMetadata": {
			"detailColumns": ["Id", "Name", "Website", "BillingCity", "BillingState"],
			"reportFormat": "TABULAR"
		},
		"factMap": {
			"T!T": {
				"rows": [
					{
						"dataCells": [
							{"label": "001xx0000099", "value": "001xx0000099"},
							{"label": "Alt Names Inc", "value": "Alt Names Inc"},
							{"label": "https://alt.com", "value": "https://alt.com"},
							{"label": "Austin", "value": "Austin"},
							{"label": "TX", "value": "TX"}
						]
					}
				]
			}
		}
	}`

	var result ReportResult
	require.NoError(t, json.Unmarshal([]byte(reportJSON), &result))

	accounts, err := ParseReportAccounts(&result)
	require.NoError(t, err)
	require.Len(t, accounts, 1)
	assert.Equal(t, "001xx0000099", accounts[0].ID)
	assert.Equal(t, "Alt Names Inc", accounts[0].Name)
	assert.Equal(t, "https://alt.com", accounts[0].Website)
	assert.Equal(t, "Austin", accounts[0].City)
	assert.Equal(t, "TX", accounts[0].State)
}
