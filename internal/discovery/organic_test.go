package discovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/pkg/google"
)

func TestOrganicStrategy_Run(t *testing.T) {
	store := &mockStore{
		createdRunID: "organic-run-1",
		unsearchedCells: []GridCell{
			{ID: 1, CBSACode: "16980", CellKM: 2.0, SWLat: 41.8, SWLon: -87.7, NELat: 41.82, NELon: -87.68},
			{ID: 2, CBSACode: "16980", CellKM: 2.0, SWLat: 41.82, SWLon: -87.7, NELat: 41.84, NELon: -87.68},
		},
	}

	gClient := &mockGoogleClient{
		responses: map[string]*google.DiscoverySearchResponse{
			"accounting firms": {
				Places: []google.DiscoveryPlace{
					{
						ID:               "ChIJ-acc1",
						DisplayName:      google.DisplayName{Text: "Chicago Accounting LLC"},
						WebsiteURI:       "https://chicagoaccounting.com",
						FormattedAddress: "100 N Michigan Ave, Chicago, IL 60601",
					},
					{
						ID:               "ChIJ-acc2",
						DisplayName:      google.DisplayName{Text: "Midwest Tax Group"},
						WebsiteURI:       "https://midwesttax.com",
						FormattedAddress: "200 S Wacker Dr, Chicago, IL 60606",
					},
				},
			},
		},
	}

	cfg := &config.DiscoveryConfig{
		GooglePlacesRateLimit: 100,
		MaxCandidatesPerRun:   10000,
		DirectoryBlocklist:    []string{"yelp.com"},
	}

	strategy := NewOrganicStrategy(store, gClient, cfg)

	runCfg := RunConfig{
		Strategy: "organic",
		Params: map[string]any{
			"cbsa_code":  "16980",
			"text_query": "accounting firms",
			"max_cells":  float64(10),
		},
	}

	result, err := strategy.Run(context.Background(), runCfg)
	require.NoError(t, err)

	// Two cells, each returning 2 results = 4 candidates.
	assert.Equal(t, 4, result.CandidatesFound)
	assert.True(t, result.CostUSD > 0)

	// Verify cells were marked as searched.
	assert.Len(t, store.updatedCellIDs, 2)
	assert.Contains(t, store.updatedCellIDs, int64(1))
	assert.Contains(t, store.updatedCellIDs, int64(2))
}

func TestOrganicStrategy_MissingParams(t *testing.T) {
	store := &mockStore{createdRunID: "organic-run-err"}
	gClient := &mockGoogleClient{}
	cfg := &config.DiscoveryConfig{GooglePlacesRateLimit: 100}

	strategy := NewOrganicStrategy(store, gClient, cfg)

	// Missing cbsa_code.
	_, err := strategy.Run(context.Background(), RunConfig{
		Strategy: "organic",
		Params:   map[string]any{"text_query": "test"},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cbsa_code")

	// Missing text_query.
	_, err = strategy.Run(context.Background(), RunConfig{
		Strategy: "organic",
		Params:   map[string]any{"cbsa_code": "16980"},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "text_query")
}

func TestOrganicStrategy_SkipsEmptyWebsite(t *testing.T) {
	store := &mockStore{
		createdRunID: "organic-run-skip",
		unsearchedCells: []GridCell{
			{ID: 1, CBSACode: "16980", CellKM: 2.0, SWLat: 41.8, SWLon: -87.7, NELat: 41.82, NELon: -87.68},
		},
	}

	gClient := &mockGoogleClient{
		responses: map[string]*google.DiscoverySearchResponse{
			"test query": {
				Places: []google.DiscoveryPlace{
					{ID: "ChIJ-nosite", DisplayName: google.DisplayName{Text: "No Website LLC"}, WebsiteURI: ""},
					{ID: "ChIJ-hassite", DisplayName: google.DisplayName{Text: "Has Website LLC"}, WebsiteURI: "https://haswebsite.com"},
				},
			},
		},
	}

	cfg := &config.DiscoveryConfig{GooglePlacesRateLimit: 100, DirectoryBlocklist: []string{}}

	strategy := NewOrganicStrategy(store, gClient, cfg)
	result, err := strategy.Run(context.Background(), RunConfig{
		Strategy: "organic",
		Params:   map[string]any{"cbsa_code": "16980", "text_query": "test query", "max_cells": float64(10)},
	})
	require.NoError(t, err)

	// Only the one with a website should be included.
	assert.Equal(t, 1, result.CandidatesFound)
}

func TestParseAddress(t *testing.T) {
	tests := []struct {
		name      string
		addr      string
		wantCity  string
		wantState string
		wantZip   string
	}{
		{"full US address", "123 Main St, Springfield, IL 62701, USA", "Springfield", "IL", "62701"},
		{"no zip", "456 Oak Ave, Chicago, IL, USA", "Chicago", "IL", ""},
		{"short address", "Springfield, IL", "Springfield", "IL", ""},
		{"empty", "", "", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			city, state, zip := parseAddress(tt.addr)
			assert.Equal(t, tt.wantCity, city)
			assert.Equal(t, tt.wantState, state)
			assert.Equal(t, tt.wantZip, zip)
		})
	}
}
