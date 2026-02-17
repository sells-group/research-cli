package model

import "time"

// PageType represents a classified page category.
type PageType string

const (
	PageTypeHomepage     PageType = "homepage"
	PageTypeAbout        PageType = "about"
	PageTypeServices     PageType = "services"
	PageTypeProducts     PageType = "products"
	PageTypePricing      PageType = "pricing"
	PageTypeCareers      PageType = "careers"
	PageTypeContact      PageType = "contact"
	PageTypeTeam         PageType = "team"
	PageTypeBlog         PageType = "blog"
	PageTypeNews         PageType = "news"
	PageTypeFAQ          PageType = "faq"
	PageTypeTestimonials PageType = "testimonials"
	PageTypeCaseStudies  PageType = "case_studies"
	PageTypePartners     PageType = "partners"
	PageTypeLegal        PageType = "legal"
	PageTypeInvestors    PageType = "investors"
	PageTypeOther        PageType = "other"

	// External source page types (auto-classified by title prefix).
	PageTypeBBB        PageType = "bbb_profile"
	PageTypeGoogleMaps PageType = "google_maps"
	PageTypeSoS        PageType = "government_registry"
	PageTypeLinkedIn   PageType = "linkedin"
)

// AllPageTypes returns all defined page types.
func AllPageTypes() []PageType {
	return []PageType{
		PageTypeHomepage,
		PageTypeAbout,
		PageTypeServices,
		PageTypeProducts,
		PageTypePricing,
		PageTypeCareers,
		PageTypeContact,
		PageTypeTeam,
		PageTypeBlog,
		PageTypeNews,
		PageTypeFAQ,
		PageTypeTestimonials,
		PageTypeCaseStudies,
		PageTypePartners,
		PageTypeLegal,
		PageTypeInvestors,
		PageTypeOther,
		PageTypeBBB,
		PageTypeGoogleMaps,
		PageTypeSoS,
		PageTypeLinkedIn,
	}
}

// ExternalPageTypes returns the page types for external sources.
// These are always included as supplementary context during routing.
func ExternalPageTypes() []PageType {
	return []PageType{
		PageTypeBBB,
		PageTypeGoogleMaps,
		PageTypeSoS,
		PageTypeLinkedIn,
	}
}

// IsExternalPageType returns true if the page type is an external source.
func IsExternalPageType(pt PageType) bool {
	switch pt {
	case PageTypeBBB, PageTypeGoogleMaps, PageTypeSoS, PageTypeLinkedIn:
		return true
	}
	return false
}

// CrawledPage represents a page fetched during crawling.
type CrawledPage struct {
	URL      string `json:"url"`
	Title    string `json:"title"`
	Markdown string `json:"markdown"`
	HTML     string `json:"html,omitempty"`
	StatusCode int  `json:"status_code"`
}

// PageClassification holds the result of page type classification.
type PageClassification struct {
	PageType   PageType `json:"page_type"`
	Confidence float64  `json:"confidence"`
}

// ClassifiedPage is a crawled page with its classification.
type ClassifiedPage struct {
	CrawledPage
	Classification PageClassification `json:"classification"`
}

// PageIndex maps page types to their classified pages.
type PageIndex map[PageType][]ClassifiedPage

// CrawlCache stores a cached crawl result.
type CrawlCache struct {
	ID         string       `json:"id"`
	CompanyURL string       `json:"company_url"`
	Pages      []CrawledPage `json:"pages"`
	CrawledAt  time.Time    `json:"crawled_at"`
	ExpiresAt  time.Time    `json:"expires_at"`
}

// CrawlResult holds the outcome of a crawl phase.
type CrawlResult struct {
	Pages      []CrawledPage `json:"pages"`
	Source     string        `json:"source"` // "local" or "firecrawl"
	FromCache  bool          `json:"from_cache"`
	PagesCount int           `json:"pages_count"`
}

// ProbeResult holds the outcome of an HTTP probe.
type ProbeResult struct {
	Reachable  bool   `json:"reachable"`
	StatusCode int    `json:"status_code"`
	HasRobots  bool   `json:"has_robots"`
	HasSitemap bool   `json:"has_sitemap"`
	Blocked    bool   `json:"blocked"`
	BlockType  string `json:"block_type,omitempty"`
	FinalURL   string `json:"final_url"`
}
