package peextract

// PEPageType represents a PE-firm-specific page classification.
type PEPageType string

// PE page type constants classify crawled pages by their content role.
const (
	PEPageTypeHomepage  PEPageType = "homepage"
	PEPageTypeAbout     PEPageType = "about"
	PEPageTypeTeam      PEPageType = "team"
	PEPageTypePortfolio PEPageType = "portfolio"
	PEPageTypeStrategy  PEPageType = "strategy"
	PEPageTypeNews      PEPageType = "news"
	PEPageTypeBlog      PEPageType = "blog"
	PEPageTypeContact   PEPageType = "contact"
	PEPageTypeCareers   PEPageType = "careers"
	PEPageTypeOther     PEPageType = "other"
)

// AllPEPageTypes returns all PE page types.
func AllPEPageTypes() []PEPageType {
	return []PEPageType{
		PEPageTypeHomepage,
		PEPageTypeAbout,
		PEPageTypeTeam,
		PEPageTypePortfolio,
		PEPageTypeStrategy,
		PEPageTypeNews,
		PEPageTypeBlog,
		PEPageTypeContact,
		PEPageTypeCareers,
		PEPageTypeOther,
	}
}
