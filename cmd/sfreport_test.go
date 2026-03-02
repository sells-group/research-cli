package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/pkg/salesforce"
)

func TestReportAccountsToCompanies(t *testing.T) {
	t.Parallel()

	accounts := []salesforce.ReportAccount{
		{
			ID:      "001xx0000001",
			Name:    "Acme Corp",
			Website: "https://acme.com",
			City:    "Springfield",
			State:   "IL",
		},
		{
			ID:      "001xx0000002",
			Name:    "Beta Inc",
			Website: "https://beta.io",
			City:    "",
			State:   "CA",
		},
		{
			ID:      "001xx0000003",
			Name:    "Gamma LLC",
			Website: "https://gamma.dev",
			City:    "Austin",
			State:   "",
		},
		{
			ID:      "001xx0000004",
			Name:    "Delta Co",
			Website: "https://delta.net",
			City:    "",
			State:   "",
		},
	}

	companies := reportAccountsToCompanies(accounts)
	require.Len(t, companies, 4)

	// Full city + state.
	assert.Equal(t, "001xx0000001", companies[0].SalesforceID)
	assert.Equal(t, "Acme Corp", companies[0].Name)
	assert.Equal(t, "https://acme.com", companies[0].URL)
	assert.Equal(t, "Springfield", companies[0].City)
	assert.Equal(t, "IL", companies[0].State)
	assert.Equal(t, "Springfield, IL", companies[0].Location)

	// State only.
	assert.Equal(t, "CA", companies[1].Location)

	// City only.
	assert.Equal(t, "Austin", companies[2].Location)

	// Neither city nor state.
	assert.Equal(t, "", companies[3].Location)
}
