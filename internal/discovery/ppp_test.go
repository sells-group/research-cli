package discovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/pkg/google"
)

func TestPPPStrategy_Run(t *testing.T) {
	store := &mockStore{
		borrowers: []PPPBorrower{
			{
				BorrowerName: "Acme Corp",
				City:         "Springfield",
				State:        "IL",
				Zip:          "62701",
				NAICSCode:    "541211",
				Approval:     250000,
				LoanNumber:   "LN001",
			},
			{
				BorrowerName: "Beta Industries",
				City:         "Chicago",
				State:        "IL",
				Zip:          "60601",
				NAICSCode:    "541330",
				Approval:     500000,
				LoanNumber:   "LN002",
			},
		},
		createdRunID: "test-run-123",
	}

	gClient := &mockGoogleClient{
		responses: map[string]*google.DiscoverySearchResponse{
			"Acme Corp Springfield IL": {
				Places: []google.DiscoveryPlace{
					{
						ID:               "ChIJ-acme",
						DisplayName:      google.DisplayName{Text: "Acme Corp"},
						WebsiteURI:       "https://acmecorp.com",
						FormattedAddress: "123 Main St, Springfield, IL 62701",
					},
				},
			},
			"Beta Industries Chicago IL": {
				Places: []google.DiscoveryPlace{
					{
						ID:               "ChIJ-beta",
						DisplayName:      google.DisplayName{Text: "Beta Industries LLC"},
						WebsiteURI:       "https://betaindustries.com",
						FormattedAddress: "456 State St, Chicago, IL 60601",
					},
				},
			},
		},
	}

	cfg := &config.DiscoveryConfig{
		GooglePlacesRateLimit: 100, // High rate for tests.
		MaxCandidatesPerRun:   10000,
		PPPMinApproval:        150000,
		DirectoryBlocklist:    []string{"yelp.com", "facebook.com"},
	}

	strategy := NewPPPStrategy(store, gClient, cfg)

	runCfg := RunConfig{
		Strategy: "ppp",
		Params: map[string]any{
			"naics":  []string{"541211", "541330"},
			"states": []string{"IL"},
		},
	}

	result, err := strategy.Run(context.Background(), runCfg)
	require.NoError(t, err)

	assert.Equal(t, 2, result.CandidatesFound)
	assert.True(t, result.CostUSD > 0)

	// Verify candidates were inserted.
	assert.Equal(t, 1, store.bulkInsertCalls)
	require.Len(t, store.insertedCandidates, 2)

	assert.Equal(t, "ChIJ-acme", store.insertedCandidates[0].GooglePlaceID)
	assert.Equal(t, "acmecorp.com", store.insertedCandidates[0].Domain)
	assert.Equal(t, "ppp", store.insertedCandidates[0].Source)

	assert.Equal(t, "ChIJ-beta", store.insertedCandidates[1].GooglePlaceID)
	assert.Equal(t, "betaindustries.com", store.insertedCandidates[1].Domain)
}

func TestPPPStrategy_SkipsDirectoryURLs(t *testing.T) {
	store := &mockStore{
		borrowers: []PPPBorrower{
			{BorrowerName: "Test Corp", City: "LA", State: "CA", NAICSCode: "541211", Approval: 200000, LoanNumber: "LN003"},
		},
		createdRunID: "test-run-dir",
	}

	gClient := &mockGoogleClient{
		responses: map[string]*google.DiscoverySearchResponse{
			"Test Corp LA CA": {
				Places: []google.DiscoveryPlace{
					{ID: "ChIJ-dir", DisplayName: google.DisplayName{Text: "Test Corp"}, WebsiteURI: "https://yelp.com/biz/test-corp"},
				},
			},
		},
	}

	cfg := &config.DiscoveryConfig{
		GooglePlacesRateLimit: 100,
		MaxCandidatesPerRun:   10000,
		PPPMinApproval:        150000,
		DirectoryBlocklist:    []string{"yelp.com"},
	}

	strategy := NewPPPStrategy(store, gClient, cfg)
	result, err := strategy.Run(context.Background(), RunConfig{Strategy: "ppp", Params: map[string]any{}})
	require.NoError(t, err)

	assert.Equal(t, 0, result.CandidatesFound)
}

func TestPPPStrategy_NoBorrowers(t *testing.T) {
	store := &mockStore{borrowers: nil}
	gClient := &mockGoogleClient{}
	cfg := &config.DiscoveryConfig{GooglePlacesRateLimit: 100, PPPMinApproval: 150000, MaxCandidatesPerRun: 10000}

	strategy := NewPPPStrategy(store, gClient, cfg)
	result, err := strategy.Run(context.Background(), RunConfig{Strategy: "ppp", Params: map[string]any{}})
	require.NoError(t, err)

	assert.Equal(t, 0, result.CandidatesFound)
}

func TestNameSimilarity(t *testing.T) {
	tests := []struct {
		name   string
		a, b   string
		minSim float64
		maxSim float64
	}{
		{"exact match", "Acme Corp", "Acme Corp", 1.0, 1.0},
		{"case insensitive", "ACME CORP", "acme corp", 1.0, 1.0},
		{"partial overlap", "Acme Corp LLC", "Acme Corp", 0.5, 1.0},
		{"no overlap", "Acme Corp", "Beta Industries", 0.0, 0.1},
		{"empty strings", "", "", 0.0, 0.0},
		{"one empty", "Acme", "", 0.0, 0.0},
		{"with punctuation", "Smith & Co.", "Smith Co", 0.5, 1.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sim := nameSimilarity(tt.a, tt.b)
			assert.GreaterOrEqual(t, sim, tt.minSim, "similarity too low")
			assert.LessOrEqual(t, sim, tt.maxSim, "similarity too high")
		})
	}
}

func TestExtractDomain(t *testing.T) {
	tests := []struct {
		url    string
		domain string
	}{
		{"https://www.acmecorp.com/about", "acmecorp.com"},
		{"https://acmecorp.com", "acmecorp.com"},
		{"http://WWW.ACME.COM", "acme.com"},
		{"invalid-url", ""},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			assert.Equal(t, tt.domain, extractDomain(tt.url))
		})
	}
}

func TestToStringSlice(t *testing.T) {
	assert.Equal(t, []string{"a", "b"}, toStringSlice([]string{"a", "b"}))
	assert.Equal(t, []string{"a", "b"}, toStringSlice("a,b"))
	assert.Equal(t, []string{"x"}, toStringSlice([]any{"x"}))
	assert.Nil(t, toStringSlice(nil))
	assert.Nil(t, toStringSlice(""))
	assert.Nil(t, toStringSlice(42))
}
