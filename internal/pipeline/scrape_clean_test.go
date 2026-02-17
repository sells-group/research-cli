package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCleanBBBMarkdown_CookieBanner(t *testing.T) {
	md := `Some content before

Cookies on BBB.org We use cookies to give you the best experience.
Accept All Cookies
Manage Cookies

# Business Profile

Business Started: 2/22/2011`

	result := cleanBBBMarkdown(md)
	assert.NotContains(t, result, "Cookies on BBB.org")
	assert.NotContains(t, result, "Accept All Cookies")
	assert.Contains(t, result, "Business Started: 2/22/2011")
}

func TestCleanBBBMarkdown_CookiePreferencesModal(t *testing.T) {
	// Realistic BBB page with both cookie banner AND cookie preferences modal.
	md := `V 3 Construction Inc | BBB Business Profile
[Skip to main content](#content)

Cookies on BBB.org
We use cookies to give you the best experience.
Accept All Cookies Manage Cookies

Cookie Preferences
Many websites use cookies or similar tools.

### Necessary Cookies
These cookies are necessary for the site.

### Functional Cookies
These cookies enable enhanced functionality.

### Marketing Cookies
These cookies may be set by advertising partners.

Save Changes Accept All Cookies

[Better Business Bureau Homepage](https://www.bbb.org/)

*   Consumers
    *   [Get a Quote](https://www.bbb.org/get-a-quote)
    *   [Leave a Review](https://www.bbb.org/leave-a-review)

*   Businesses
    *   [Get Your Business Listed](https://www.bbb.org/get-listed)

Language Sign In

Business Profile
General Contractor

V 3 Construction Inc

West Jordan, UT 84088

Serving the following areas:
*   Salt Lake County, UT

Business Started:2/22/2011
Business Management:Mr. Kyle Vance, CEO`

	result := cleanBBBMarkdown(md)
	assert.NotContains(t, result, "Cookies on BBB.org")
	assert.NotContains(t, result, "Accept All Cookies")
	assert.NotContains(t, result, "Cookie Preferences")
	assert.NotContains(t, result, "Necessary Cookies")
	assert.NotContains(t, result, "Marketing Cookies")
	assert.NotContains(t, result, "Get a Quote")
	assert.NotContains(t, result, "Get Your Business Listed")
	assert.Contains(t, result, "Business Profile")
	assert.Contains(t, result, "V 3 Construction Inc")
	assert.Contains(t, result, "Salt Lake County, UT")
	assert.Contains(t, result, "Business Started:2/22/2011")
	assert.Contains(t, result, "Mr. Kyle Vance, CEO")
	// Verify it's compact enough for 4000-char truncation to reach business data.
	assert.Less(t, len(result), 500, "cleaned BBB page should be compact")
}

func TestCleanBBBMarkdown_Footer(t *testing.T) {
	md := `# Acme Corp BBB Profile

Business Started: 2/22/2011
Serving: Salt Lake County, UT

---
BBB Business Profiles may not be reproduced for sales purposes.
Copyright 2024 BBB.`

	result := cleanBBBMarkdown(md)
	assert.Contains(t, result, "Business Started: 2/22/2011")
	assert.Contains(t, result, "Serving: Salt Lake County, UT")
	assert.NotContains(t, result, "BBB Business Profiles may not be reproduced")
}

func TestCleanBBBMarkdown_SidebarSections(t *testing.T) {
	md := `# Business Profile

Business Started: 2/22/2011

## Industry Tip

Some industry tips here that add noise.

## Business Details

Important business details here.`

	result := cleanBBBMarkdown(md)
	assert.Contains(t, result, "Business Started: 2/22/2011")
	assert.NotContains(t, result, "Some industry tips here")
	assert.Contains(t, result, "Important business details here")
}

func TestCleanBBBMarkdown_NoBoilerplate(t *testing.T) {
	md := `# Acme Corp

Business Started: 2/22/2011
Phone: (801) 555-1234`

	result := cleanBBBMarkdown(md)
	assert.Contains(t, result, "Business Started: 2/22/2011")
	assert.Contains(t, result, "Phone: (801) 555-1234")
}

func TestCleanGoogleMapsMarkdown_SignInFooter(t *testing.T) {
	md := `# Acme Corp

4.6 stars (120 reviews)
123 Main St, Salt Lake City, UT

Get the most out of Google Maps
Sign in to save your favorite places.`

	result := cleanGoogleMapsMarkdown(md)
	assert.Contains(t, result, "4.6 stars")
	assert.Contains(t, result, "123 Main St")
	assert.NotContains(t, result, "Get the most out of Google Maps")
}

func TestCleanGoogleMapsMarkdown_NoFooter(t *testing.T) {
	md := `# Acme Corp

4.6 stars (120 reviews)`

	result := cleanGoogleMapsMarkdown(md)
	assert.Equal(t, md, result)
}

func TestCleanExternalMarkdown_Dispatch(t *testing.T) {
	tests := []struct {
		name   string
		source string
		input  string
		want   string
	}{
		{"bbb", "bbb", "# Business\nBusiness Started: 2011\n---\nBBB Business Profiles may not be reproduced\nfooter", "# Business\nBusiness Started: 2011"},
		{"google_maps", "google_maps", "content\nGet the most out of Google Maps", "content"},
		{"unknown", "sos", "content unchanged", "content unchanged"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CleanExternalMarkdown(tt.source, tt.input)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestCleanExternalMarkdown_UnknownSource(t *testing.T) {
	input := "some content"
	result := CleanExternalMarkdown("sos", input)
	assert.Equal(t, input, result)
}
