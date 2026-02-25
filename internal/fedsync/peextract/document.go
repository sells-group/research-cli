package peextract

import (
	"fmt"
	"strings"
)

const (
	maxDocLen     = 15000 // max characters per document context
	maxDocLenBlog = 20000 // larger context for blog intelligence questions (more articles = more signal)
)

// PEFirmDocs holds assembled documents for a PE firm ready for extraction.
type PEFirmDocs struct {
	PEFirmID int64
	FirmName string

	// Pages indexed by page type.
	PagesByType map[PEPageType][]ClassifiedPage
}

// AssembleDocs builds PEFirmDocs from classified pages.
func AssembleDocs(firmID int64, firmName string, pages []ClassifiedPage) *PEFirmDocs {
	docs := &PEFirmDocs{
		PEFirmID:    firmID,
		FirmName:    firmName,
		PagesByType: make(map[PEPageType][]ClassifiedPage),
	}

	for _, p := range pages {
		docs.PagesByType[p.PageType] = append(docs.PagesByType[p.PageType], p)
	}

	return docs
}

// AssembleDocsFromCache builds PEFirmDocs from cached crawl rows.
func AssembleDocsFromCache(firmID int64, firmName string, cache []CrawlCacheRow) *PEFirmDocs {
	var pages []ClassifiedPage
	for _, c := range cache {
		pages = append(pages, ClassifiedPage{
			URL:      c.URL,
			Title:    c.Title,
			Markdown: c.Markdown,
			PageType: PEPageType(c.PageType),
		})
	}
	return AssembleDocs(firmID, firmName, pages)
}

// DocumentForQuestion assembles the document context for a specific question.
// Routes based on the question's PageTypes, truncated to maxDocLen (or maxDocLenBlog for blog questions).
func DocumentForQuestion(docs *PEFirmDocs, q Question) string {
	var sb strings.Builder
	limit := maxDocLen
	if q.Category == CatBlogIntel {
		limit = maxDocLenBlog
	}
	remaining := limit

	// Gather pages from question's preferred page types in order.
	for _, pt := range q.PageTypes {
		pages, ok := docs.PagesByType[PEPageType(pt)]
		if !ok {
			continue
		}

		for _, p := range pages {
			if remaining <= 0 {
				break
			}

			section := formatPageContext(p)
			if len(section) > remaining {
				section = section[:remaining]
			}

			sb.WriteString(section)
			sb.WriteString("\n\n")
			remaining -= len(section) + 2
		}
	}

	// If no content found from preferred pages, try homepage as fallback.
	if sb.Len() == 0 {
		if pages, ok := docs.PagesByType[PEPageTypeHomepage]; ok && len(pages) > 0 {
			section := formatPageContext(pages[0])
			if len(section) > limit {
				section = section[:limit]
			}
			sb.WriteString(section)
		}
	}

	return sb.String()
}

// formatPageContext formats a single page for inclusion in the LLM context.
func formatPageContext(p ClassifiedPage) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "--- Page: %s [%s] ---\n", p.Title, p.PageType)
	fmt.Fprintf(&sb, "URL: %s\n\n", p.URL)
	sb.WriteString(p.Markdown)
	return sb.String()
}

// TotalDocTokenEstimate returns a rough estimate of total document tokens.
func TotalDocTokenEstimate(docs *PEFirmDocs) int {
	total := 0
	for _, pages := range docs.PagesByType {
		for _, p := range pages {
			total += len(p.Markdown) / 4 // rough token estimate
		}
	}
	return total
}

// HasPages returns true if docs has any non-empty pages.
func HasPages(docs *PEFirmDocs) bool {
	for _, pages := range docs.PagesByType {
		for _, p := range pages {
			if strings.TrimSpace(p.Markdown) != "" {
				return true
			}
		}
	}
	return false
}
