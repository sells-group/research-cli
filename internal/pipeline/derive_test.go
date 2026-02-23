package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sells-group/research-cli/internal/model"
)

func TestDeriveCompanyInfo_OGSiteName(t *testing.T) {
	html := `<html><head>
		<meta property="og:site_name" content="Acme Construction">
		<title>Acme Construction - Home</title>
	</head><body></body></html>`

	name, city, state := DeriveCompanyInfo([]byte(html), "https://acme-construction.com")

	assert.Equal(t, "Acme Construction", name)
	assert.Empty(t, city)
	assert.Empty(t, state)
}

func TestDeriveCompanyInfo_OGTitle(t *testing.T) {
	html := `<html><head>
		<meta property="og:title" content="Smith & Associates - Professional Services">
		<title>Smith & Associates - Home</title>
	</head><body></body></html>`

	name, _, _ := DeriveCompanyInfo([]byte(html), "https://smith.com")

	assert.Equal(t, "Smith & Associates", name)
}

func TestDeriveCompanyInfo_TitleTag(t *testing.T) {
	tests := []struct {
		name     string
		html     string
		expected string
	}{
		{
			"strips Home suffix",
			`<html><head><title>Acme Corp - Home</title></head><body></body></html>`,
			"Acme Corp",
		},
		{
			"strips Official Site suffix",
			`<html><head><title>Acme Corp | Official Site</title></head><body></body></html>`,
			"Acme Corp",
		},
		{
			"strips Welcome suffix",
			`<html><head><title>Acme Corp - Welcome</title></head><body></body></html>`,
			"Acme Corp",
		},
		{
			"pipe separator",
			`<html><head><title>Acme Corp | Leading Technology Provider</title></head><body></body></html>`,
			"Acme Corp",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, _, _ := DeriveCompanyInfo([]byte(tt.html), "https://acme.com")
			assert.Equal(t, tt.expected, name)
		})
	}
}

func TestDeriveCompanyInfo_JSONLD(t *testing.T) {
	html := `<html><head><title></title>
	<script type="application/ld+json">
	{
		"@type": "Organization",
		"name": "Acme Industries",
		"address": {
			"@type": "PostalAddress",
			"addressLocality": "Springfield",
			"addressRegion": "IL"
		}
	}
	</script>
	</head><body></body></html>`

	name, city, state := DeriveCompanyInfo([]byte(html), "https://acme.com")

	assert.Equal(t, "Acme Industries", name)
	assert.Equal(t, "Springfield", city)
	assert.Equal(t, "IL", state)
}

func TestDeriveCompanyInfo_JSONLDArray(t *testing.T) {
	html := `<html><head>
	<script type="application/ld+json">
	[
		{"@type": "WebSite", "name": "Ignore This"},
		{"@type": "LocalBusiness", "name": "Bob's Auto Repair", "address": {"addressLocality": "Austin", "addressRegion": "TX"}}
	]
	</script>
	</head><body></body></html>`

	name, city, state := DeriveCompanyInfo([]byte(html), "https://bobs-auto.com")

	assert.Equal(t, "Bob's Auto Repair", name)
	assert.Equal(t, "Austin", city)
	assert.Equal(t, "TX", state)
}

func TestDeriveCompanyInfo_DomainFallback(t *testing.T) {
	html := `<html><head></head><body><p>Loading...</p></body></html>`

	name, _, _ := DeriveCompanyInfo([]byte(html), "https://www.acme-construction.com")

	assert.Equal(t, "Acme Construction", name)
}

func TestDeriveCompanyInfo_Blocked(t *testing.T) {
	// Cloudflare challenge HTML with no useful meta tags.
	html := `<html><head><title>Attention Required! | Cloudflare</title></head>
	<body><h1>Checking your browser before accessing the site</h1></body></html>`

	name, _, _ := DeriveCompanyInfo([]byte(html), "https://blocked.com")

	// Should fall through all parsers; title "Attention Required!" contains " | " so first part is used.
	// The domain fallback provides the actual name.
	assert.NotEmpty(t, name)
}

func TestDeriveCompanyInfo_Location(t *testing.T) {
	html := `<html><head>
	<meta property="og:site_name" content="Acme Corp">
	<script type="application/ld+json">
	{"@type": "Organization", "name": "Acme Corp", "address": {"addressLocality": "Denver", "addressRegion": "CO"}}
	</script>
	</head><body></body></html>`

	name, city, state := DeriveCompanyInfo([]byte(html), "https://acme.com")

	assert.Equal(t, "Acme Corp", name)
	assert.Equal(t, "Denver", city)
	assert.Equal(t, "CO", state)
}

func TestDeriveCompanyInfo_MetaContentReversedOrder(t *testing.T) {
	// Some sites put content before property.
	html := `<html><head>
		<meta content="Reverse Corp" property="og:site_name">
	</head><body></body></html>`

	name, _, _ := DeriveCompanyInfo([]byte(html), "https://reverse.com")

	assert.Equal(t, "Reverse Corp", name)
}

func TestDeriveNameFromPages(t *testing.T) {
	pages := []model.CrawledPage{
		{URL: "https://acme.com/about", Title: "About Us - Acme Industries"},
		{URL: "https://acme.com", Title: "Acme Industries - Home"},
	}

	name := deriveNameFromPages(pages, "https://acme.com")

	assert.Equal(t, "Acme Industries", name)
}

func TestDeriveNameFromPages_NoHomepageMatch(t *testing.T) {
	pages := []model.CrawledPage{
		{URL: "https://acme.com/about", Title: "About Us | Acme Industries"},
	}

	name := deriveNameFromPages(pages, "https://acme.com")

	assert.Equal(t, "About Us", name)
}

func TestDeriveNameFromPages_Empty(t *testing.T) {
	name := deriveNameFromPages(nil, "https://acme.com")
	assert.Equal(t, "Acme", name) // Falls back to domainToName
}

func TestDeriveNameFromPages_WWWPrefix(t *testing.T) {
	pages := []model.CrawledPage{
		{URL: "https://www.acme.com/", Title: "Acme Corp - Home"},
	}

	name := deriveNameFromPages(pages, "acme.com")

	assert.Equal(t, "Acme Corp", name)
}

func TestDomainToName(t *testing.T) {
	tests := []struct {
		url      string
		expected string
	}{
		{"https://acme-construction.com", "Acme Construction"},
		{"https://www.acme-construction.com", "Acme Construction"},
		{"acme-construction.com", "Acme Construction"},
		{"https://smith.com", "Smith"},
		{"https://my-great-company.io", "My Great Company"},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			assert.Equal(t, tt.expected, domainToName(tt.url))
		})
	}
}

func TestDetectInputMode(t *testing.T) {
	tests := []struct {
		name     string
		company  model.Company
		expected model.InputMode
	}{
		{
			"url only",
			model.Company{URL: "https://acme.com"},
			model.InputModeURLOnly,
		},
		{
			"minimal",
			model.Company{URL: "https://acme.com", Name: "Acme"},
			model.InputModeMinimal,
		},
		{
			"standard with location",
			model.Company{URL: "https://acme.com", Name: "Acme", Location: "Springfield, IL"},
			model.InputModeStandard,
		},
		{
			"standard with city/state",
			model.Company{URL: "https://acme.com", Name: "Acme", City: "Springfield", State: "IL"},
			model.InputModeStandard,
		},
		{
			"pre-seeded",
			model.Company{URL: "https://acme.com", Name: "Acme", Location: "Springfield, IL", PreSeeded: map[string]any{"industry": "Tech"}},
			model.InputModePreSeeded,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, DetectInputMode(tt.company))
		})
	}
}

func TestCleanTitleToName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"Acme Corp - Home", "Acme Corp"},
		{"Acme Corp | Official Site", "Acme Corp"},
		{"Acme Corp | Welcome", "Acme Corp"},
		{"Acme Corp - Leading Provider of Things", "Acme Corp"},
		{"Simple Name", "Simple Name"},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, cleanTitleToName(tt.input))
		})
	}
}

func TestExtractTitle(t *testing.T) {
	html := `<html><head><title>My Company</title></head></html>`
	assert.Equal(t, "My Company", extractTitle(html))

	html2 := `<html><head></head></html>`
	assert.Equal(t, "", extractTitle(html2))
}

func TestExtractMetaContent(t *testing.T) {
	html := `<html><head>
		<meta property="og:site_name" content="Test Corp">
		<meta property="og:title" content="Test Title">
		<meta name="description" content="A description">
	</head></html>`

	assert.Equal(t, "Test Corp", extractMetaContent(html, "og:site_name"))
	assert.Equal(t, "Test Title", extractMetaContent(html, "og:title"))
	assert.Equal(t, "A description", extractMetaContent(html, "description"))
	assert.Equal(t, "", extractMetaContent(html, "nonexistent"))
}

func TestExtractJSONLDOrg(t *testing.T) {
	t.Run("single org", func(t *testing.T) {
		html := `<script type="application/ld+json">{"@type":"Organization","name":"Test Org","address":{"addressLocality":"NYC","addressRegion":"NY"}}</script>`
		name, city, state := extractJSONLDOrg(html)
		assert.Equal(t, "Test Org", name)
		assert.Equal(t, "NYC", city)
		assert.Equal(t, "NY", state)
	})

	t.Run("string address", func(t *testing.T) {
		html := `<script type="application/ld+json">{"@type":"Organization","name":"Test","address":"Chicago, IL"}</script>`
		name, city, state := extractJSONLDOrg(html)
		assert.Equal(t, "Test", name)
		assert.Equal(t, "Chicago", city)
		assert.Equal(t, "IL", state)
	})

	t.Run("no org type", func(t *testing.T) {
		html := `<script type="application/ld+json">{"@type":"WebSite","name":"Not Org"}</script>`
		name, _, _ := extractJSONLDOrg(html)
		assert.Empty(t, name)
	})

	t.Run("no json-ld", func(t *testing.T) {
		html := `<html><head></head></html>`
		name, _, _ := extractJSONLDOrg(html)
		assert.Empty(t, name)
	})

	t.Run("LocalBusiness type", func(t *testing.T) {
		html := `<script type="application/ld+json">{"@type":"LocalBusiness","name":"Local Shop"}</script>`
		name, _, _ := extractJSONLDOrg(html)
		assert.Equal(t, "Local Shop", name)
	})
}
