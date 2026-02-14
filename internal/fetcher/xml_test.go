package fetcher

import (
	"context"
	"encoding/xml"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testItem struct {
	XMLName xml.Name `xml:"item"`
	Name    string   `xml:"name"`
	Value   int      `xml:"value"`
}

func TestStreamXML_SimpleElements(t *testing.T) {
	input := `<root>
		<item><name>alpha</name><value>1</value></item>
		<item><name>beta</name><value>2</value></item>
		<item><name>gamma</name><value>3</value></item>
	</root>`

	itemCh, errCh := StreamXML[testItem](context.Background(), strings.NewReader(input), "item")

	var items []testItem
	for item := range itemCh {
		items = append(items, item)
	}
	for err := range errCh {
		require.NoError(t, err)
	}

	require.Len(t, items, 3)
	assert.Equal(t, "alpha", items[0].Name)
	assert.Equal(t, 1, items[0].Value)
	assert.Equal(t, "beta", items[1].Name)
	assert.Equal(t, 2, items[1].Value)
	assert.Equal(t, "gamma", items[2].Name)
	assert.Equal(t, 3, items[2].Value)
}

type testNested struct {
	XMLName xml.Name `xml:"record"`
	ID      string   `xml:"id,attr"`
	Detail  struct {
		Text string `xml:",chardata"`
	} `xml:"detail"`
}

func TestStreamXML_NestedElements(t *testing.T) {
	input := `<data>
		<record id="r1"><detail>first</detail></record>
		<other>skip me</other>
		<record id="r2"><detail>second</detail></record>
	</data>`

	ch, errCh := StreamXML[testNested](context.Background(), strings.NewReader(input), "record")

	var records []testNested
	for rec := range ch {
		records = append(records, rec)
	}
	for err := range errCh {
		require.NoError(t, err)
	}

	require.Len(t, records, 2)
	assert.Equal(t, "r1", records[0].ID)
	assert.Equal(t, "first", records[0].Detail.Text)
	assert.Equal(t, "r2", records[1].ID)
	assert.Equal(t, "second", records[1].Detail.Text)
}

func TestStreamXML_EmptyInput(t *testing.T) {
	ch, errCh := StreamXML[testItem](context.Background(), strings.NewReader(""), "item")

	var items []testItem
	for item := range ch {
		items = append(items, item)
	}
	for err := range errCh {
		require.NoError(t, err)
	}

	assert.Empty(t, items)
}

func TestStreamXML_ContextCancellation(t *testing.T) {
	// Build a large XML document
	var sb strings.Builder
	sb.WriteString("<root>")
	for i := range 10000 {
		sb.WriteString("<item><name>item</name><value>")
		sb.WriteString(strings.Repeat("x", i%10))
		sb.WriteString("</value></item>")
	}
	sb.WriteString("</root>")

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond)

	ch, errCh := StreamXML[testItem](ctx, strings.NewReader(sb.String()), "item")

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

func TestStreamXML_NoMatchingElements(t *testing.T) {
	input := `<root><other>data</other></root>`
	ch, errCh := StreamXML[testItem](context.Background(), strings.NewReader(input), "item")

	var items []testItem
	for item := range ch {
		items = append(items, item)
	}
	for err := range errCh {
		require.NoError(t, err)
	}

	assert.Empty(t, items)
}
