package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sells-group/research-cli/internal/model"
)

func TestCrossReferenceAddress_AllMatch(t *testing.T) {
	company := model.Company{
		Name:    "ABC Construction",
		City:    "Springfield",
		State:   "IL",
		ZipCode: "62701",
	}

	pages := []model.CrawledPage{
		{
			Title:    "[bbb] ABC Construction BBB Profile",
			Markdown: "ABC Construction Company, 123 Main St, Springfield, IL 62701. BBB A+ rated.",
		},
	}

	matches := CrossReferenceAddress(company, pages)

	assert.Len(t, matches, 1)
	assert.True(t, matches[0].CityMatch)
	assert.True(t, matches[0].StateMatch)
	assert.True(t, matches[0].ZipMatch)
	assert.InDelta(t, 1.0, matches[0].Score, 0.01)
}

func TestCrossReferenceAddress_PartialMatch(t *testing.T) {
	company := model.Company{
		Name:    "ABC Construction",
		City:    "Springfield",
		State:   "IL",
		ZipCode: "62701",
	}

	pages := []model.CrawledPage{
		{
			Title:    "[google_maps] Google Maps",
			Markdown: "ABC Construction, Springfield, Illinois",
		},
	}

	matches := CrossReferenceAddress(company, pages)

	assert.Len(t, matches, 1)
	assert.True(t, matches[0].CityMatch)
	assert.True(t, matches[0].StateMatch) // "IL" → stateVariants → "illinois" matches via containsWord
	assert.False(t, matches[0].ZipMatch)
	assert.InDelta(t, 0.7, matches[0].Score, 0.01)
}

func TestCrossReferenceAddress_NoAddress(t *testing.T) {
	company := model.Company{
		Name: "ABC Construction",
	}

	pages := []model.CrawledPage{
		{Title: "Page", Markdown: "Some content"},
	}

	matches := CrossReferenceAddress(company, pages)
	assert.Nil(t, matches)
}

func TestCrossReferenceAddress_NoMatch(t *testing.T) {
	company := model.Company{
		Name:    "ABC Construction",
		City:    "Springfield",
		State:   "IL",
		ZipCode: "62701",
	}

	pages := []model.CrawledPage{
		{
			Title:    "[sos] Secretary of State",
			Markdown: "Business record for XYZ Company registered at 99 Oak St, Denver, CO 80201",
		},
	}

	matches := CrossReferenceAddress(company, pages)
	assert.Empty(t, matches)
}

func TestCrossReferenceAddress_MultiplePages(t *testing.T) {
	company := model.Company{
		Name:    "Test Corp",
		City:    "Houston",
		State:   "TX",
		ZipCode: "77001",
	}

	pages := []model.CrawledPage{
		{Title: "Page 1", Markdown: "Test Corp in Houston, TX 77001"},
		{Title: "Page 2", Markdown: "No relevant content here"},
		{Title: "Page 3", Markdown: "Located in Houston area"},
	}

	matches := CrossReferenceAddress(company, pages)
	assert.Len(t, matches, 2) // Page 1 and Page 3
}

func TestCrossReferenceAddress_StateAbbreviation(t *testing.T) {
	company := model.Company{
		Name:  "Test Corp",
		City:  "Dallas",
		State: "TX",
	}

	pages := []model.CrawledPage{
		{Title: "Page", Markdown: "Test Corp is headquartered in Dallas, Texas."},
	}

	matches := CrossReferenceAddress(company, pages)

	assert.Len(t, matches, 1)
	assert.True(t, matches[0].CityMatch)
	assert.True(t, matches[0].StateMatch) // "TX" → stateVariants → "texas" matches
}

func TestCrossReferenceAddress_FalsePositivePrevented(t *testing.T) {
	company := model.Company{
		Name:  "Test Corp",
		State: "IL",
	}

	pages := []model.CrawledPage{
		{Title: "Page", Markdown: "This is a BUILDING in FILING status"},
	}

	matches := CrossReferenceAddress(company, pages)

	// "IL" should NOT match inside "BUILDING" or "FILING" thanks to word boundary checks.
	assert.Empty(t, matches)
}

func TestContainsWord(t *testing.T) {
	tests := []struct {
		text   string
		needle string
		want   bool
	}{
		{"hello world", "hello", true},
		{"hello world", "world", true},
		{"hello world", "lo wo", false},  // not word-bounded
		{"building permit", "il", false},  // "il" inside "building"
		{"filing status", "il", false},    // "il" inside "filing"
		{"springfield, il 62701", "il", true},
		{"tx is great", "tx", true},
		{"extra text", "tx", false}, // "tx" inside "extra" and "text"
		{"", "hello", false},
		{"hello", "", false},
		{"il", "il", true},                  // exact match
		{"il il il", "il", true},            // multiple occurrences
		{"foil il bar", "il", true},         // second occurrence is word-bounded
		{"foil bail", "il", false},          // not word-bounded in either occurrence
		{"springfield, il.", "il", true},    // period boundary
		{"(il)", "il", true},               // parens boundary
		{"abc-il-xyz", "il", true},         // hyphen boundary
	}

	for _, tc := range tests {
		got := containsWord(tc.text, tc.needle)
		assert.Equal(t, tc.want, got, "containsWord(%q, %q)", tc.text, tc.needle)
	}
}

func TestStateVariants(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"IL", []string{"il", "illinois"}},
		{"il", []string{"il", "illinois"}},
		{"Illinois", []string{"il", "illinois"}},
		{"illinois", []string{"il", "illinois"}},
		{"TX", []string{"tx", "texas"}},
		{"texas", []string{"tx", "texas"}},
		{"CA", []string{"ca", "california"}},
		{"Unknown", []string{"unknown"}},
		{"", nil},
	}

	for _, tc := range tests {
		got := stateVariants(tc.input)
		assert.Equal(t, tc.want, got, "stateVariants(%q)", tc.input)
	}
}
