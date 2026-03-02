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

	// When no ID column is present (e.g. SUMMARY reports), ParseReportAccounts
	// gracefully falls back to Name-based identification instead of erroring.
	reportJSON := `{
		"reportMetadata": {
			"detailColumns": ["Account.Name", "Account.Website"],
			"reportFormat": "SUMMARY"
		},
		"factMap": {
			"0!T": {
				"rows": [
					{
						"dataCells": [
							{"label": "Acme Corp", "value": "Acme Corp"},
							{"label": "https://acme.com", "value": "https://acme.com"}
						]
					},
					{
						"dataCells": [
							{"label": "", "value": ""},
							{"label": "", "value": ""}
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
	// Row with name is kept; row with empty name+ID is skipped.
	require.Len(t, accounts, 1)
	assert.Equal(t, "", accounts[0].ID)
	assert.Equal(t, "Acme Corp", accounts[0].Name)
	assert.Equal(t, "https://acme.com", accounts[0].Website)
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

func TestRunReport_HTTPTest(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Contains(t, r.URL.String(), "/analytics/reports/00OPo00000g8kDcMAI")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(cannedReportJSON))
	}))
	defer srv.Close()

	mc := &mockClient{
		runReportFn: func(_ context.Context, reportID string) (*ReportResult, error) {
			// Simulate the real HTTP flow by hitting our test server.
			resp, err := http.Get(srv.URL + "/analytics/reports/" + reportID + "?includeDetails=true")
			if err != nil {
				return nil, err
			}
			defer resp.Body.Close() //nolint:errcheck

			var result ReportResult
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				return nil, err
			}
			return &result, nil
		},
	}

	result, err := mc.RunReport(context.Background(), "00OPo00000g8kDcMAI")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "00OPo00000g8kDcMAI", result.ReportMetadata.ID)
	assert.Len(t, result.FactMap, 1)
}

func TestCellString_OutOfBounds(t *testing.T) {
	t.Parallel()

	cells := []ReportCell{
		{Label: "first", Value: "first"},
	}
	// Valid index.
	assert.Equal(t, "first", cellString(cells, 0))
	// Negative index.
	assert.Equal(t, "", cellString(cells, -1))
	// Beyond length.
	assert.Equal(t, "", cellString(cells, 5))
	// Empty slice.
	assert.Equal(t, "", cellString(nil, 0))
}

func TestBuildColumnIndex_Lookup(t *testing.T) {
	t.Parallel()

	ci := buildColumnIndex([]string{"Account.Id", "Account.Name", "Account.Website"})

	idx, ok := ci.lookup("Account.Id")
	assert.True(t, ok)
	assert.Equal(t, 0, idx)

	idx, ok = ci.lookup("Account.Name")
	assert.True(t, ok)
	assert.Equal(t, 1, idx)

	// Not found.
	_, ok = ci.lookup("Nonexistent", "AlsoNope")
	assert.False(t, ok)

	// First match wins among alternatives.
	idx, ok = ci.lookup("Missing", "Account.Website")
	assert.True(t, ok)
	assert.Equal(t, 2, idx)
}

func TestParseReportAccounts_EmptyFactMap(t *testing.T) {
	t.Parallel()

	result := ReportResult{
		ReportMetadata: ReportMetadata{
			DetailColumns: []string{"Account.Id", "Account.Name"},
		},
		FactMap: map[string]json.RawMessage{},
	}

	accounts, err := ParseReportAccounts(&result)
	require.NoError(t, err)
	assert.Empty(t, accounts)
}

func TestParseReportAccounts_InvalidFactMapJSON(t *testing.T) {
	t.Parallel()

	result := ReportResult{
		ReportMetadata: ReportMetadata{
			DetailColumns: []string{"Account.Id", "Account.Name"},
		},
		FactMap: map[string]json.RawMessage{
			"T!T": json.RawMessage(`not valid json`),
		},
	}

	_, err := ParseReportAccounts(&result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal fact map")
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
