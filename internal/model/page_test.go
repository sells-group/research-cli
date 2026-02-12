package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAllPageTypes(t *testing.T) {
	t.Parallel()

	types := AllPageTypes()

	t.Run("has expected count", func(t *testing.T) {
		t.Parallel()
		assert.Len(t, types, 17)
	})

	t.Run("contains all known types", func(t *testing.T) {
		t.Parallel()
		expected := []PageType{
			PageTypeHomepage, PageTypeAbout, PageTypeServices,
			PageTypeProducts, PageTypePricing, PageTypeCareers,
			PageTypeContact, PageTypeTeam, PageTypeBlog,
			PageTypeNews, PageTypeFAQ, PageTypeTestimonials,
			PageTypeCaseStudies, PageTypePartners, PageTypeLegal,
			PageTypeInvestors, PageTypeOther,
		}
		assert.Equal(t, expected, types)
	})

	t.Run("no duplicates", func(t *testing.T) {
		t.Parallel()
		seen := make(map[PageType]bool)
		for _, pt := range types {
			assert.False(t, seen[pt], "duplicate page type: %s", pt)
			seen[pt] = true
		}
	})
}

func TestPageTypeStringValues(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "homepage", string(PageTypeHomepage))
	assert.Equal(t, "case_studies", string(PageTypeCaseStudies))
	assert.Equal(t, "other", string(PageTypeOther))
}
