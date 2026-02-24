package scrape

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalScraper_CleanHTML(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`<html><head><title>Acme Corp</title></head>
<body><nav>Menu</nav><h1>Welcome</h1><p>We build great products.</p>
<footer>Copyright 2024</footer></body></html>`))
	}))
	defer srv.Close()

	s := NewLocalScraper()
	result, err := s.Scrape(context.Background(), srv.URL)
	require.NoError(t, err)
	assert.Equal(t, "local_http", result.Source)
	assert.Equal(t, "Acme Corp", result.Page.Title)
	assert.Equal(t, 200, result.Page.StatusCode)
	assert.Contains(t, result.Page.Markdown, "Welcome")
	assert.Contains(t, result.Page.Markdown, "great products")
	// Nav and footer should be stripped.
	assert.NotContains(t, result.Page.Markdown, "Menu")
	assert.NotContains(t, result.Page.Markdown, "Copyright 2024")
}

func TestLocalScraper_Cloudflare(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cf-Ray", "abc123")
		w.WriteHeader(403)
		_, _ = w.Write([]byte(`<html><body>Access denied</body></html>`))
	}))
	defer srv.Close()

	s := NewLocalScraper()
	_, err := s.Scrape(context.Background(), srv.URL)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "blocked")
}

func TestLocalScraper_Captcha(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`<html><body>Please complete the reCAPTCHA to continue</body></html>`))
	}))
	defer srv.Close()

	s := NewLocalScraper()
	_, err := s.Scrape(context.Background(), srv.URL)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "blocked")
}

func TestLocalScraper_EmptyBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`<html></html>`))
	}))
	defer srv.Close()

	s := NewLocalScraper()
	_, err := s.Scrape(context.Background(), srv.URL)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "empty")
}

func TestLocalScraper_HTTP404(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		_, _ = w.Write([]byte(`<html><body>Not found page with lots of content here to exceed threshold</body></html>`))
	}))
	defer srv.Close()

	s := NewLocalScraper()
	_, err := s.Scrape(context.Background(), srv.URL)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "status 404")
}

func TestLocalScraper_Name(t *testing.T) {
	s := NewLocalScraper()
	assert.Equal(t, "local_http", s.Name())
}

func TestLocalScraper_Supports(t *testing.T) {
	s := NewLocalScraper()
	assert.True(t, s.Supports("https://example.com"))
	assert.True(t, s.Supports("http://localhost"))
}

func TestStripHTML_Basic(t *testing.T) {
	input := `<html><head><style>body{color:red}</style></head>
<body><script>alert('hi')</script><h1>Hello</h1><p>World &amp; friends</p></body></html>`
	result := stripHTML(input)
	assert.Contains(t, result, "Hello")
	assert.Contains(t, result, "World & friends")
	assert.NotContains(t, result, "alert")
	assert.NotContains(t, result, "color:red")
	assert.NotContains(t, result, "<h1>")
}

func TestStripHTML_Entities(t *testing.T) {
	input := `&lt;tag&gt; &amp; &quot;quoted&quot; &#39;apos&#39; &nbsp;space`
	result := stripHTML(input)
	assert.Contains(t, result, `<tag>`)
	assert.Contains(t, result, `& "quoted"`)
	assert.Contains(t, result, `'apos'`)
}

func TestStripHTML_WhitespaceCollapse(t *testing.T) {
	input := "Hello     world\n\n\n\n\nfoo"
	result := stripHTML(input)
	assert.NotContains(t, result, "     ")
	assert.NotContains(t, result, "\n\n\n")
}

func TestExtractTitle(t *testing.T) {
	body := []byte(`<html><head><title>My Page Title</title></head><body></body></html>`)
	assert.Equal(t, "My Page Title", extractTitle(body))
}

func TestExtractTitle_Missing(t *testing.T) {
	body := []byte(`<html><body>no title here</body></html>`)
	assert.Equal(t, "", extractTitle(body))
}
