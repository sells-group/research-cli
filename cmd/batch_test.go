package main

import (
	"testing"

	"github.com/jomei/notionapi"
	"github.com/stretchr/testify/assert"
)

func TestLeadToCompany_AllFields(t *testing.T) {
	page := notionapi.Page{
		ID: "page-123",
		Properties: notionapi.Properties{
			"Name": &notionapi.TitleProperty{
				Title: []notionapi.RichText{
					{PlainText: "Acme"},
					{PlainText: " Corp"},
				},
			},
			"URL": &notionapi.URLProperty{
				URL: "https://acme.com",
			},
			"SalesforceID": &notionapi.RichTextProperty{
				RichText: []notionapi.RichText{
					{PlainText: "001ABC"},
				},
			},
			"Location": &notionapi.RichTextProperty{
				RichText: []notionapi.RichText{
					{PlainText: "New York, NY"},
				},
			},
		},
	}

	c := leadToCompany(page)
	assert.Equal(t, "page-123", c.NotionPageID)
	assert.Equal(t, "Acme Corp", c.Name)
	assert.Equal(t, "https://acme.com", c.URL)
	assert.Equal(t, "001ABC", c.SalesforceID)
	assert.Equal(t, "New York, NY", c.Location)
}

func TestLeadToCompany_MissingFields(t *testing.T) {
	page := notionapi.Page{
		ID:         "page-456",
		Properties: notionapi.Properties{},
	}

	c := leadToCompany(page)
	assert.Equal(t, "page-456", c.NotionPageID)
	assert.Empty(t, c.Name)
	assert.Empty(t, c.URL)
	assert.Empty(t, c.SalesforceID)
	assert.Empty(t, c.Location)
}

func TestLeadToCompany_WhitespaceTrimed(t *testing.T) {
	page := notionapi.Page{
		ID: "page-789",
		Properties: notionapi.Properties{
			"Name": &notionapi.TitleProperty{
				Title: []notionapi.RichText{
					{PlainText: "  Trimmed  "},
				},
			},
			"URL": &notionapi.URLProperty{
				URL: "  https://trimmed.com  ",
			},
			"SalesforceID": &notionapi.RichTextProperty{
				RichText: []notionapi.RichText{
					{PlainText: "  001XYZ  "},
				},
			},
			"Location": &notionapi.RichTextProperty{
				RichText: []notionapi.RichText{
					{PlainText: "  Austin, TX  "},
				},
			},
		},
	}

	c := leadToCompany(page)
	assert.Equal(t, "Trimmed", c.Name)
	assert.Equal(t, "https://trimmed.com", c.URL)
	assert.Equal(t, "001XYZ", c.SalesforceID)
	assert.Equal(t, "Austin, TX", c.Location)
}

func TestLeadToCompany_WrongPropertyType(t *testing.T) {
	// Properties exist but with wrong types - should not panic
	page := notionapi.Page{
		ID: "page-wrong",
		Properties: notionapi.Properties{
			"Name": &notionapi.RichTextProperty{
				RichText: []notionapi.RichText{{PlainText: "wrong type"}},
			},
			"URL": &notionapi.TitleProperty{
				Title: []notionapi.RichText{{PlainText: "wrong type"}},
			},
		},
	}

	c := leadToCompany(page)
	assert.Equal(t, "page-wrong", c.NotionPageID)
	assert.Empty(t, c.Name)
	assert.Empty(t, c.URL)
}

func TestLeadToCompany_MultipleRichTextSegments(t *testing.T) {
	page := notionapi.Page{
		ID: "page-multi",
		Properties: notionapi.Properties{
			"SalesforceID": &notionapi.RichTextProperty{
				RichText: []notionapi.RichText{
					{PlainText: "001"},
					{PlainText: "ABC"},
					{PlainText: "XYZ"},
				},
			},
		},
	}

	c := leadToCompany(page)
	assert.Equal(t, "001ABCXYZ", c.SalesforceID)
}
