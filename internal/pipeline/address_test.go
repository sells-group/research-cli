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
		{"building permit", "il", false}, // "il" inside "building"
		{"filing status", "il", false},   // "il" inside "filing"
		{"springfield, il 62701", "il", true},
		{"tx is great", "tx", true},
		{"extra text", "tx", false}, // "tx" inside "extra" and "text"
		{"", "hello", false},
		{"hello", "", false},
		{"il", "il", true},               // exact match
		{"il il il", "il", true},         // multiple occurrences
		{"foil il bar", "il", true},      // second occurrence is word-bounded
		{"foil bail", "il", false},       // not word-bounded in either occurrence
		{"springfield, il.", "il", true}, // period boundary
		{"(il)", "il", true},             // parens boundary
		{"abc-il-xyz", "il", true},       // hyphen boundary
	}

	for _, tc := range tests {
		got := containsWord(tc.text, tc.needle)
		assert.Equal(t, tc.want, got, "containsWord(%q, %q)", tc.text, tc.needle)
	}
}

func TestExtractStructuredAddress(t *testing.T) {
	tests := []struct {
		name       string
		markdown   string
		source     string
		wantStreet string
		wantCity   string
		wantState  string
		wantZip    string
		wantOK     bool
	}{
		{
			name: "BBB business address block",
			markdown: `# ABC Construction BBB Profile

**Business Address**
123 Main Street
Suite 100
Austin, TX 78701

**Phone:** (512) 555-1234`,
			source:     "[bbb] ABC Construction BBB Profile",
			wantStreet: "123 Main Street, Suite 100",
			wantCity:   "Austin",
			wantState:  "TX",
			wantZip:    "78701",
			wantOK:     true,
		},
		{
			name: "BBB single line street",
			markdown: `**Business Address**
456 Oak Avenue
Dallas, TX 75201

BBB Rating: A+`,
			source:     "[bbb] Test Corp BBB",
			wantStreet: "456 Oak Avenue",
			wantCity:   "Dallas",
			wantState:  "TX",
			wantZip:    "75201",
			wantOK:     true,
		},
		{
			name: "SoS principal office",
			markdown: `## Business Entity Detail

**Entity Name:** ABC Corp
**Status:** Active
**Principal Office**
789 Elm Boulevard
Floor 3
Springfield, IL 62701

**Registered Agent:** John Smith`,
			source:     "[sos] ABC Corp Filing",
			wantStreet: "789 Elm Boulevard, Floor 3",
			wantCity:   "Springfield",
			wantState:  "IL",
			wantZip:    "62701",
			wantOK:     true,
		},
		{
			name:       "No address in markdown",
			markdown:   "This is a company page with no address information whatsoever.",
			source:     "[bbb] Test",
			wantStreet: "",
			wantCity:   "",
			wantState:  "",
			wantZip:    "",
			wantOK:     false,
		},
		{
			name:       "Partial address - city but no zip",
			markdown:   "Located in Springfield, Illinois. No further details.",
			source:     "[sos] Test",
			wantStreet: "",
			wantCity:   "",
			wantState:  "",
			wantZip:    "",
			wantOK:     false,
		},
		{
			name: "Multiple addresses returns first near header",
			markdown: `**Address**
100 First Street
Denver, CO 80201

**Mailing Address**
PO Box 999
Boulder, CO 80302`,
			source:     "[bbb] Multi Address",
			wantStreet: "100 First Street",
			wantCity:   "Denver",
			wantState:  "CO",
			wantZip:    "80201",
			wantOK:     true,
		},
		{
			name:       "Empty markdown",
			markdown:   "",
			source:     "[bbb] Empty",
			wantStreet: "",
			wantCity:   "",
			wantState:  "",
			wantZip:    "",
			wantOK:     false,
		},
		{
			name: "Zip+4 format",
			markdown: `**Business Address**
200 Commerce Drive
Houston, TX 77001-4321`,
			source:     "[bbb] Zip4 Test",
			wantStreet: "200 Commerce Drive",
			wantCity:   "Houston",
			wantState:  "TX",
			wantZip:    "77001-4321",
			wantOK:     true,
		},
		{
			name: "Inline address without header",
			markdown: `ABC Corp is located at 500 Market Street
San Francisco, CA 94105`,
			source:     "[sos] ABC Corp",
			wantStreet: "500 Market Street",
			wantCity:   "San Francisco",
			wantState:  "CA",
			wantZip:    "94105",
			wantOK:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			street, city, state, zip, ok := ExtractStructuredAddress(tc.markdown, tc.source)
			assert.Equal(t, tc.wantOK, ok, "ok")
			if tc.wantOK {
				assert.Equal(t, tc.wantCity, city, "city")
				assert.Equal(t, tc.wantState, state, "state")
				assert.Equal(t, tc.wantZip, zip, "zip")
				assert.Equal(t, tc.wantStreet, street, "street")
			}
		})
	}
}

func TestIsSuiteLine(t *testing.T) {
	tests := []struct {
		line string
		want bool
	}{
		{"Suite 100", true},
		{"Ste 200", true},
		{"Ste. 300", true},
		{"#400", true},
		{"Unit 5B", true},
		{"Floor 3", true},
		{"Fl 2", true},
		{"Apt 1A", true},
		{"Bldg 7", true},
		{"Building A", true},
		{"123 Main Street", false},
		{"Some random text", false},
		{"", false},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.want, isSuiteLine(tc.line), "isSuiteLine(%q)", tc.line)
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
