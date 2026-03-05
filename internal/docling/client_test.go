package docling

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

const cannedResponse = `{
  "document": {
    "pages": [{"page_no": 1, "size": {"width": 612, "height": 792}}],
    "body": [
      {"type": "heading", "text": "Item 4 – Advisory Business", "level": 1},
      {"type": "paragraph", "text": "We provide investment advisory services to individuals and institutions."},
      {"type": "table", "data": {"headers": ["Fee Type", "Rate"], "rows": [["Management Fee", "1.00%"], ["Performance Fee", "20%"]]}}
    ]
  }
}`

func TestConvert_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/api/v1/convert" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		ct := r.Header.Get("Content-Type")
		if ct == "" {
			t.Error("missing Content-Type header")
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(cannedResponse))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	doc, err := c.Convert(context.Background(), []byte("%PDF-fake"), ConvertOpts{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(doc.Pages) != 1 {
		t.Fatalf("expected 1 page, got %d", len(doc.Pages))
	}

	elements := doc.Pages[0].Elements
	if len(elements) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(elements))
	}

	// Heading
	if elements[0].Type != "heading" {
		t.Errorf("expected heading, got %s", elements[0].Type)
	}
	if elements[0].Level != 1 {
		t.Errorf("expected level 1, got %d", elements[0].Level)
	}
	if elements[0].Text != "Item 4 \u2013 Advisory Business" {
		t.Errorf("unexpected heading text: %s", elements[0].Text)
	}

	// Paragraph
	if elements[1].Type != "paragraph" {
		t.Errorf("expected paragraph, got %s", elements[1].Type)
	}

	// Table
	if elements[2].Type != "table" {
		t.Errorf("expected table, got %s", elements[2].Type)
	}
	if elements[2].Table == nil {
		t.Fatal("expected non-nil Table")
	}
	if len(elements[2].Table.Headers) != 2 {
		t.Errorf("expected 2 headers, got %d", len(elements[2].Table.Headers))
	}
	if len(elements[2].Table.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(elements[2].Table.Rows))
	}
	if elements[2].Table.Rows[0][1] != "1.00%" {
		t.Errorf("unexpected first row rate: %s", elements[2].Table.Rows[0][1])
	}
}

func TestConvert_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	_, err := c.Convert(context.Background(), []byte("%PDF-fake"), ConvertOpts{})
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestConvert_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("not json at all"))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	_, err := c.Convert(context.Background(), []byte("%PDF-fake"), ConvertOpts{})
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestConvert_RequestError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	srv.Close() // close immediately so the HTTP call fails

	c := NewClient(srv.URL)
	_, err := c.Convert(context.Background(), []byte("%PDF-fake"), ConvertOpts{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "docling: API call")
}

func TestConvert_EmptyResponse(t *testing.T) {
	emptyDoc := `{"document": {"pages": [], "body": []}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(emptyDoc))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	doc, err := c.Convert(context.Background(), []byte("%PDF-fake"), ConvertOpts{})
	require.NoError(t, err)
	// Empty pages list defaults to 1 page with empty elements.
	require.Len(t, doc.Pages, 1)
	require.Empty(t, doc.Pages[0].Elements)
}

func TestMapResponse_TableElements(t *testing.T) {
	resp := &apiResponse{
		Document: apiDocument{
			Pages: []apiPage{{PageNo: 1}},
			Body: []apiElement{
				{Type: "paragraph", Text: "intro"},
				{
					Type: "table",
					Text: "",
					Data: &apiTableData{
						Headers: []string{"A", "B", "C"},
						Rows: [][]string{
							{"1", "2", "3"},
							{"4", "5", "6"},
						},
					},
				},
				{
					Type: "table",
					Text: "",
					Data: &apiTableData{
						Headers: nil,
						Rows:    [][]string{{"only", "row"}},
					},
				},
			},
		},
	}

	doc := mapResponse(resp)
	require.Len(t, doc.Pages, 1)
	elems := doc.Pages[0].Elements
	require.Len(t, elems, 3)

	// First element: paragraph with nil Table.
	require.Nil(t, elems[0].Table)

	// Second element: full table with headers and rows.
	require.NotNil(t, elems[1].Table)
	require.Equal(t, []string{"A", "B", "C"}, elems[1].Table.Headers)
	require.Len(t, elems[1].Table.Rows, 2)
	require.Equal(t, []string{"1", "2", "3"}, elems[1].Table.Rows[0])

	// Third element: table with nil headers but valid rows.
	require.NotNil(t, elems[2].Table)
	require.Nil(t, elems[2].Table.Headers)
	require.Len(t, elems[2].Table.Rows, 1)
	require.Equal(t, []string{"only", "row"}, elems[2].Table.Rows[0])
}
