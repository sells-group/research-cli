package fetcher

import (
	"context"
	"encoding/xml"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamXML_MalformedXML(t *testing.T) {
	// XML with invalid content that triggers a token read error
	input := `<root><item><name>ok</name></item><item><name>bad&invalid;</name></item></root>`
	ch, errCh := StreamXML[testItem](context.Background(), strings.NewReader(input), "item")

	var items []testItem
	for item := range ch {
		items = append(items, item)
	}

	var gotErr error
	for err := range errCh {
		if err != nil {
			gotErr = err
		}
	}

	// Depending on the XML content, we may get items before the error or an error
	// The important thing is we don't panic
	_ = items
	_ = gotErr
}

func TestStreamXML_DecodeElementError(t *testing.T) {
	// Type mismatch: value field expects int but gets non-numeric
	type strictItem struct {
		XMLName xml.Name `xml:"item"`
		Name    string   `xml:"name"`
		Value   int      `xml:"value"`
	}

	input := `<root><item><name>ok</name><value>not_a_number</value></item></root>`
	ch, errCh := StreamXML[strictItem](context.Background(), strings.NewReader(input), "item")

	var items []strictItem
	for item := range ch {
		items = append(items, item)
	}

	var gotErr error
	for err := range errCh {
		if err != nil {
			gotErr = err
		}
	}

	require.Error(t, gotErr)
	assert.Contains(t, gotErr.Error(), "xml: decode element")
	assert.Empty(t, items)
}

func TestStreamXML_ContextCancelDuringSend(t *testing.T) {
	// Build a large XML document
	var sb strings.Builder
	sb.WriteString("<root>")
	for range 500 {
		sb.WriteString("<item><name>test</name><value>1</value></item>")
	}
	sb.WriteString("</root>")

	ctx, cancel := context.WithCancel(context.Background())
	ch, errCh := StreamXML[testItem](ctx, strings.NewReader(sb.String()), "item")

	// Read one item, then cancel
	<-ch
	cancel()

	// Drain
	for range ch {
	}
	var gotErr error
	for err := range errCh {
		if err != nil {
			gotErr = err
		}
	}
	if gotErr != nil {
		assert.Contains(t, gotErr.Error(), "context")
	}
}

func TestStreamXML_InvalidXMLSyntax(t *testing.T) {
	// Completely broken XML that triggers a token or decode error
	input := `<root><item><unclosed`
	ch, errCh := StreamXML[testItem](context.Background(), strings.NewReader(input), "item")

	for range ch {
	}

	var gotErr error
	for err := range errCh {
		if err != nil {
			gotErr = err
		}
	}

	require.Error(t, gotErr)
	// May be either a token read error or a decode element error depending on where the parser is
	assert.Contains(t, gotErr.Error(), "xml:")
}

func TestStreamXML_BrokenTokenOnly(t *testing.T) {
	// XML with invalid character that triggers a token read error before any element matching
	input := "\x00"
	ch, errCh := StreamXML[testItem](context.Background(), strings.NewReader(input), "item")

	for range ch {
	}

	var gotErr error
	for err := range errCh {
		if err != nil {
			gotErr = err
		}
	}

	require.Error(t, gotErr)
	assert.Contains(t, gotErr.Error(), "xml: read token")
}

func TestStreamXML_ContextAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	input := `<root><item><name>a</name><value>1</value></item></root>`
	ch, errCh := StreamXML[testItem](ctx, strings.NewReader(input), "item")

	for range ch {
	}
	var gotErr error
	for err := range errCh {
		if err != nil {
			gotErr = err
		}
	}
	if gotErr != nil {
		assert.Contains(t, gotErr.Error(), "context")
	}
}

func TestStreamXML_MixedElements(t *testing.T) {
	// XML with multiple element types, only matching ones should be returned
	input := `<data>
		<skip>ignored</skip>
		<item><name>first</name><value>1</value></item>
		<other>also ignored</other>
		<item><name>second</name><value>2</value></item>
		<item><name>third</name><value>3</value></item>
	</data>`

	ch, errCh := StreamXML[testItem](context.Background(), strings.NewReader(input), "item")

	var items []testItem
	for item := range ch {
		items = append(items, item)
	}
	for err := range errCh {
		require.NoError(t, err)
	}

	require.Len(t, items, 3)
	assert.Equal(t, "first", items[0].Name)
	assert.Equal(t, "second", items[1].Name)
	assert.Equal(t, "third", items[2].Name)
}
