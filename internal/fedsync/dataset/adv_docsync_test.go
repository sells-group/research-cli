package dataset

import (
	"testing"

	"github.com/sells-group/research-cli/internal/ocr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractSectionMetadata(t *testing.T) {
	text := `Our management fee is 1.00% of AUM, with a minimum of $250,000.
Call us at (555) 123-4567 or visit https://example.com for details.
Do you have a fiduciary duty? Yes.`

	meta := extractSectionMetadata(text)
	require.NotNil(t, meta)

	dollars, ok := meta["dollar_amounts"].([]string)
	require.True(t, ok)
	assert.Contains(t, dollars, "$250,000")

	pcts, ok := meta["percentages"].([]string)
	require.True(t, ok)
	assert.Contains(t, pcts, "1.00%")

	phones, ok := meta["phone_numbers"].([]string)
	require.True(t, ok)
	assert.Contains(t, phones, "(555) 123-4567")

	urls, ok := meta["urls"].([]string)
	require.True(t, ok)
	assert.Contains(t, urls, "https://example.com")

	yns, ok := meta["yes_no_flags"].([]string)
	require.True(t, ok)
	assert.Contains(t, yns, "Yes")
}

func TestExtractSectionMetadata_Empty(t *testing.T) {
	meta := extractSectionMetadata("plain text without any special patterns")
	assert.Nil(t, meta)
}

func TestMarshalJSONB(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		assert.Nil(t, marshalJSONB(nil))
	})

	t.Run("map", func(t *testing.T) {
		result := marshalJSONB(map[string]string{"key": "val"})
		assert.JSONEq(t, `{"key":"val"}`, string(result))
	})
}

func TestTablesToJSON(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		assert.Nil(t, tablesToJSON(nil))
	})

	t.Run("empty", func(t *testing.T) {
		assert.Nil(t, tablesToJSON([]ocr.TableData{}))
	})

	t.Run("with data", func(t *testing.T) {
		result := tablesToJSON([]ocr.TableData{
			{Headers: []string{"A", "B"}, Rows: [][]string{{"1", "2"}}},
		})
		assert.NotNil(t, result)
	})
}

func TestParseBrochureSections(t *testing.T) {
	doc := &ocr.StructuredDocument{
		FullText: "test",
		Sections: []ocr.DocumentSection{
			{Title: "Item 4 – Advisory Business", Text: "We provide advisory services.", Tables: nil},
			{Title: "Item 5 - Fees and Compensation", Text: "Our fee is 1.00% of AUM."},
			{Title: "Not an item", Text: "Random section"},
			{Title: "Item 8 - Methods of Analysis", Text: "We use fundamental analysis."},
		},
	}

	rows := parseBrochureSections(doc, 12345, "broch_001")
	assert.Len(t, rows, 3)

	assert.Equal(t, "item_4", rows[0].SectionKey)
	assert.Equal(t, 12345, rows[0].CRDNumber)
	assert.Equal(t, "broch_001", rows[0].DocID)
	assert.Equal(t, "We provide advisory services.", rows[0].TextContent)

	assert.Equal(t, "item_5", rows[1].SectionKey)
	assert.NotNil(t, rows[1].Metadata) // should have percentages

	assert.Equal(t, "item_8", rows[2].SectionKey)
}

func TestParseBrochureSections_Nil(t *testing.T) {
	assert.Nil(t, parseBrochureSections(nil, 0, ""))
	assert.Nil(t, parseBrochureSections(&ocr.StructuredDocument{}, 0, ""))
}

func TestMatchBrochureItemKey(t *testing.T) {
	tests := []struct {
		title string
		want  string
	}{
		{"Item 4 – Advisory Business", "item_4"},
		{"ITEM 5: Fees and Compensation", "item_5"},
		{"Item 18 - Financial Information", "item_18"},
		{"Item 0 - Invalid", ""},
		{"Item 19 - Out of Range", ""},
		{"Not an item", ""},
	}

	for _, tt := range tests {
		t.Run(tt.title, func(t *testing.T) {
			assert.Equal(t, tt.want, matchBrochureItemKey(tt.title))
		})
	}
}

func TestParseCRSSections(t *testing.T) {
	doc := &ocr.StructuredDocument{
		FullText: "test",
		Sections: []ocr.DocumentSection{
			{Title: "What investment services and advice can you provide me?", Text: "We offer portfolio management."},
			{Title: "What fees will I pay?", Text: "Our fee is 1.00% annually."},
			{Title: "Do you or your financial professionals have legal or disciplinary history?", Text: "No."},
			{Title: "Conversation Starters", Text: "Ask us about our services."},
			{Title: "Additional Information", Text: "Visit our website."},
		},
	}

	rows := parseCRSSections(doc, 99999, "crs_001")
	assert.Len(t, rows, 5)

	assert.Equal(t, crsSectionRelationshipsServices, rows[0].SectionKey)
	assert.Equal(t, crsSectionFeesCosts, rows[1].SectionKey)
	assert.Equal(t, crsSectionDisciplinary, rows[2].SectionKey)
	assert.Equal(t, crsSectionConversationStarters, rows[3].SectionKey)
	assert.Equal(t, crsSectionAdditionalInfo, rows[4].SectionKey)
}

func TestMatchCRSSectionKey(t *testing.T) {
	tests := []struct {
		title string
		want  string
	}{
		{"What investment services and advice can you provide me?", crsSectionRelationshipsServices},
		{"Relationships and Services", crsSectionRelationshipsServices},
		{"What fees will I pay?", crsSectionFeesCosts},
		{"Fees, Costs, Conflicts, and Standard of Conduct", crsSectionFeesCosts},
		{"Disciplinary History", crsSectionDisciplinary},
		{"Conversation Starters", crsSectionConversationStarters},
		{"Additional Information", crsSectionAdditionalInfo},
		{"Something Unrelated", ""},
	}

	for _, tt := range tests {
		t.Run(tt.title, func(t *testing.T) {
			assert.Equal(t, tt.want, matchCRSSectionKey(tt.title))
		})
	}
}

func TestCrsMappingsFromPDFsDoc(t *testing.T) {
	files := []string{
		"/tmp/extract/crs_12345.pdf",
		"/tmp/extract/crs_67890.pdf",
		"/tmp/extract/readme.txt",
		"/tmp/extract/invalid.pdf",
	}

	mappings := crsMappingsFromPDFsDoc(files)
	assert.Len(t, mappings, 2)
	assert.Equal(t, 12345, mappings[0].CRDNumber)
	assert.Equal(t, "crs_12345", mappings[0].DocID)
	assert.Equal(t, 67890, mappings[1].CRDNumber)
}
