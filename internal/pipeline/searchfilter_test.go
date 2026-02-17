package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/jina"
)

func TestFilterBBBResult(t *testing.T) {
	company := model.Company{Name: "ABC Construction, Inc.", City: "Springfield", State: "IL"}

	tests := []struct {
		name    string
		results []jina.SearchResult
		wantURL string
	}{
		{
			name: "profile URL with name match",
			results: []jina.SearchResult{
				{Title: "Search Results", URL: "https://www.bbb.org/search?find_text=abc", Content: "search page"},
				{Title: "ABC Construction BBB Profile", URL: "https://www.bbb.org/us/il/springfield/profile/construction/abc-construction-0001-12345", Content: "profile"},
			},
			wantURL: "https://www.bbb.org/us/il/springfield/profile/construction/abc-construction-0001-12345",
		},
		{
			name: "profile URL without name match — no blanket fallback",
			results: []jina.SearchResult{
				{Title: "Some Other Company", URL: "https://www.bbb.org/us/il/springfield/profile/plumbing/other-co-0001-99999", Content: "profile"},
			},
			wantURL: "",
		},
		{
			name: "profile URL with name in URL slug",
			results: []jina.SearchResult{
				{Title: "BBB Business Profile", URL: "https://www.bbb.org/us/il/springfield/profile/construction/abc-construction-0001-12345", Content: "profile"},
			},
			wantURL: "https://www.bbb.org/us/il/springfield/profile/construction/abc-construction-0001-12345",
		},
		{
			name: "search page only — no match",
			results: []jina.SearchResult{
				{Title: "BBB Search", URL: "https://www.bbb.org/search?find_text=abc", Content: "search"},
			},
			wantURL: "",
		},
		{
			name:    "empty results",
			results: []jina.SearchResult{},
			wantURL: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := filterBBBResult(tc.results, company)
			if tc.wantURL == "" {
				assert.Nil(t, got)
			} else {
				assert.NotNil(t, got)
				assert.Equal(t, tc.wantURL, got.URL)
			}
		})
	}
}

func TestFilterSoSResult(t *testing.T) {
	company := model.Company{Name: "ABC Construction, LLC"}

	tests := []struct {
		name    string
		results []jina.SearchResult
		wantURL string
	}{
		{
			name: "gov URL with name in title",
			results: []jina.SearchResult{
				{Title: "ABC Construction - Illinois SoS", URL: "https://www.ilsos.gov/corporatellc/abc123", Content: "filing"},
			},
			wantURL: "https://www.ilsos.gov/corporatellc/abc123",
		},
		{
			name: "gov URL with name in content",
			results: []jina.SearchResult{
				{Title: "Business Search", URL: "https://www.sos.il.gov/corp/abc", Content: "ABC Construction filing details"},
			},
			wantURL: "https://www.sos.il.gov/corp/abc",
		},
		{
			name: "state.xx.us URL with name match",
			results: []jina.SearchResult{
				{Title: "ABC Construction", URL: "https://secure.utah.gov/bes/abc", Content: "ABC Construction filing"},
			},
			wantURL: "https://secure.utah.gov/bes/abc",
		},
		{
			name: ".us domain without name match — no match",
			results: []jina.SearchResult{
				{Title: "Business Search", URL: "https://corporations.utah.us/search/abc", Content: "results"},
			},
			wantURL: "",
		},
		{
			name: ".state. domain with name match",
			results: []jina.SearchResult{
				{Title: "ABC Construction", URL: "https://sos.state.il.us/corp/abc", Content: "ABC Construction"},
			},
			wantURL: "https://sos.state.il.us/corp/abc",
		},
		{
			name: "gov URL without name match — no match, no entity pattern",
			results: []jina.SearchResult{
				{Title: "Business Search", URL: "https://www.sos.gov/search", Content: "results page"},
			},
			wantURL: "",
		},
		{
			name: "gov URL with entity ID pattern",
			results: []jina.SearchResult{
				{Title: "Business Search", URL: "https://www.ilsos.gov/corporatellc/CorporateLlcController?command=detail&id=12345", Content: "results page"},
			},
			wantURL: "https://www.ilsos.gov/corporatellc/CorporateLlcController?command=detail&id=12345",
		},
		{
			name: "gov URL without entity ID or name match",
			results: []jina.SearchResult{
				{Title: "SoS Homepage", URL: "https://www.sos.gov/about", Content: "about us"},
			},
			wantURL: "",
		},
		{
			name: "no gov or state URL",
			results: []jina.SearchResult{
				{Title: "ABC Construction", URL: "https://www.example.com/abc", Content: "not gov"},
			},
			wantURL: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := filterSoSResult(tc.results, company)
			if tc.wantURL == "" {
				assert.Nil(t, got)
			} else {
				assert.NotNil(t, got)
				assert.Equal(t, tc.wantURL, got.URL)
			}
		})
	}
}

func TestURLSlug(t *testing.T) {
	tests := []struct {
		rawURL string
		want   string
	}{
		{"https://www.bbb.org/us/il/springfield/profile/construction/abc-construction-0001-12345", "abc-construction-0001-12345"},
		{"https://www.bbb.org/us/il/profile/plumbing/other-co-0001-99999/", "other-co-0001-99999"},
		{"https://www.bbb.org/", ""},
		{"https://www.bbb.org", ""},
		{"not a url ://bad", ""},
	}

	for _, tc := range tests {
		got := urlSlug(tc.rawURL)
		assert.Equal(t, tc.want, got, "urlSlug(%q)", tc.rawURL)
	}
}

func TestIsGovOrStateURL(t *testing.T) {
	tests := []struct {
		url  string
		want bool
	}{
		{"https://www.ilsos.gov/corp", true},
		{"https://secure.utah.gov/bes", true},
		{"https://sos.state.il.us/corp", true},
		{"https://corporations.utah.us/search", true},
		{"https://www.example.com/abc", false},
		{"https://www.bbb.org/profile", false},
	}
	for _, tc := range tests {
		got := isGovOrStateURL(tc.url)
		assert.Equal(t, tc.want, got, "isGovOrStateURL(%q)", tc.url)
	}
}

func TestFuzzyNameMatch(t *testing.T) {
	tests := []struct {
		text        string
		companyName string
		want        bool
	}{
		{"ABC Construction Company profile page", "ABC Construction, Inc.", true},
		{"ABC Construction profile", "ABC Construction, LLC", true},
		{"XYZ Plumbing profile", "ABC Construction", false},
		{"", "ABC Construction", false},
		{"ABC Construction", "", false},
	}

	for _, tc := range tests {
		got := fuzzyNameMatch(tc.text, tc.companyName)
		assert.Equal(t, tc.want, got, "fuzzyNameMatch(%q, %q)", tc.text, tc.companyName)
	}
}

func TestNormalizeName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"ABC Construction, Inc.", "abc construction"},
		{"ABC Construction, LLC", "abc construction"},
		{"ABC Corp.", "abc"},
		{"Simple Name", "simple name"},
		{"Test Company Corporation", "test company"},
		{"  Spaces, Inc. ", "spaces"},
	}

	for _, tc := range tests {
		got := normalizeName(tc.input)
		assert.Equal(t, tc.want, got, "normalizeName(%q)", tc.input)
	}
}
