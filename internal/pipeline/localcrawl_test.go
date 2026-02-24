package pipeline

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/scrape"
)

func TestNormalizeURL(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"bare domain", "acme.com", "https://acme.com/"},
		{"with https", "https://acme.com", "https://acme.com/"},
		{"with http", "http://acme.com", "http://acme.com/"},
		{"with path", "https://acme.com/about", "https://acme.com/about"},
		{"with trailing slash", "https://acme.com/", "https://acme.com/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := normalizeURL(tt.input)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestBaseURL(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"with path", "https://acme.com/about", "https://acme.com"},
		{"with port", "http://localhost:8080/test", "http://localhost:8080"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := baseURL(tt.input)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestIsExcluded_WithPathMatcher(t *testing.T) {
	lc := NewLocalCrawler()

	tests := []struct {
		name     string
		link     string
		excluded bool
	}{
		{"blog path", "https://acme.com/blog/post1", true},
		{"blog root", "https://acme.com/blog", true},
		{"news path", "https://acme.com/news/article", true},
		{"careers", "https://acme.com/careers/job1", true},
		{"press", "https://acme.com/press/release", true},
		{"about page", "https://acme.com/about", false},
		{"services", "https://acme.com/services", false},
		{"homepage", "https://acme.com/", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := lc.IsExcludedURL(tt.link)
			assert.Equal(t, tt.excluded, result)
		})
	}
}

func TestNewLocalCrawlerWithExcludes(t *testing.T) {
	lc := NewLocalCrawlerWithExcludes([]string{"/blog/*", "/events/*"})

	assert.True(t, lc.IsExcludedURL("https://acme.com/blog/post1"))
	assert.True(t, lc.IsExcludedURL("https://acme.com/events/2024"))
	assert.False(t, lc.IsExcludedURL("https://acme.com/about"))
	// Default patterns should NOT be present when custom ones are provided.
	assert.False(t, lc.IsExcludedURL("https://acme.com/careers/job1"))
}

func TestNewLocalCrawlerWithMatcher(t *testing.T) {
	matcher := scrape.NewPathMatcher([]string{"/tmp/*"})
	lc := NewLocalCrawlerWithMatcher(matcher)

	assert.True(t, lc.IsExcludedURL("https://acme.com/tmp/file"))
	assert.False(t, lc.IsExcludedURL("https://acme.com/about"))
}

func TestParseLinks(t *testing.T) {
	base, _ := url.Parse("https://acme.com")

	html := `
	<html>
	<body>
		<a href="/about">About</a>
		<a href="/services">Services</a>
		<a href="https://acme.com/team">Team</a>
		<a href="https://external.com/page">External</a>
		<a href="#section">Anchor</a>
		<a href="javascript:void(0)">JS</a>
		<a href="mailto:info@acme.com">Email</a>
	</body>
	</html>`

	links := parseLinks(html, base)

	assert.Len(t, links, 3) // about, services, team (same host)
	assert.Contains(t, links, "https://acme.com/about")
	assert.Contains(t, links, "https://acme.com/services")
	assert.Contains(t, links, "https://acme.com/team")
}

func TestParseLinks_Deduplication(t *testing.T) {
	base, _ := url.Parse("https://acme.com")

	html := `
	<a href="/about">About</a>
	<a href="/about">About Again</a>
	<a href="https://acme.com/about">About Full URL</a>`

	links := parseLinks(html, base)
	assert.Len(t, links, 1)
}

func TestParseLinks_RelativeResolution(t *testing.T) {
	base, _ := url.Parse("https://acme.com/pages/")

	html := `<a href="sub/page">Sub Page</a>`

	links := parseLinks(html, base)
	assert.Len(t, links, 1)
	assert.Equal(t, "https://acme.com/pages/sub/page", links[0])
}

func TestNewLocalCrawler(t *testing.T) {
	lc := NewLocalCrawler()
	assert.NotNil(t, lc)
	assert.NotNil(t, lc.http)
}

// --- httptest-based tests for checkExists, Probe, DiscoverLinks ---

func TestCheckExists_Found(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/robots.txt":
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "User-agent: *\nAllow: /") //nolint:errcheck
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client(), matcher: scrape.NewPathMatcher(nil)}
	ctx := context.Background()

	assert.True(t, lc.checkExists(ctx, srv.URL+"/robots.txt"))
}

func TestCheckExists_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client(), matcher: scrape.NewPathMatcher(nil)}
	ctx := context.Background()

	assert.False(t, lc.checkExists(ctx, srv.URL+"/sitemap.xml"))
}

func TestCheckExists_ConnectionRefused(t *testing.T) {
	lc := &LocalCrawler{http: http.DefaultClient, matcher: scrape.NewPathMatcher(nil)}
	ctx := context.Background()

	assert.False(t, lc.checkExists(ctx, "http://127.0.0.1:1/bad"))
}

func TestProbe_Reachable(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "<html><body><h1>Welcome to Acme</h1><p>We are a technology company.</p></body></html>") //nolint:errcheck
	})
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "User-agent: *\nAllow: /") //nolint:errcheck
	})
	mux.HandleFunc("/sitemap.xml", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "<urlset></urlset>") //nolint:errcheck
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client(), matcher: scrape.NewPathMatcher(nil)}
	ctx := context.Background()

	result, err := lc.Probe(ctx, srv.URL)
	require.NoError(t, err)
	assert.True(t, result.Reachable)
	assert.Equal(t, http.StatusOK, result.StatusCode)
	assert.True(t, result.HasRobots)
	assert.True(t, result.HasSitemap)
	assert.False(t, result.Blocked)
}

func TestProbe_Unreachable(t *testing.T) {
	lc := &LocalCrawler{http: http.DefaultClient, matcher: scrape.NewPathMatcher(nil)}
	ctx := context.Background()

	result, err := lc.Probe(ctx, "http://127.0.0.1:1")
	require.NoError(t, err)
	assert.False(t, result.Reachable)
}

func TestProbe_Blocked_Cloudflare(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Cf-Ray", "abc123-IAD")
		w.Header().Set("Server", "cloudflare")
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, "<html><body>Checking your browser before accessing the site. Cloudflare challenge page.</body></html>") //nolint:errcheck
	}))
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client(), matcher: scrape.NewPathMatcher(nil)}
	ctx := context.Background()

	result, err := lc.Probe(ctx, srv.URL)
	require.NoError(t, err)
	assert.True(t, result.Reachable)
	assert.True(t, result.Blocked)
	assert.Equal(t, "cloudflare", result.BlockType)
}

func TestProbe_NoRobotsNoSitemap(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" && r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "<html><body>Normal page</body></html>") //nolint:errcheck
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client(), matcher: scrape.NewPathMatcher(nil)}
	ctx := context.Background()

	result, err := lc.Probe(ctx, srv.URL)
	require.NoError(t, err)
	assert.True(t, result.Reachable)
	assert.False(t, result.HasRobots)
	assert.False(t, result.HasSitemap)
	assert.False(t, result.Blocked)
}

func TestDiscoverLinks_BFS(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `<html><body><a href="/about">About</a><a href="/services">Services</a><a href="/blog/post">Blog Post</a></body></html>`)
	})
	mux.HandleFunc("/about", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `<html><body><a href="/team">Team</a></body></html>`)
	})
	mux.HandleFunc("/services", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `<html><body>Our services</body></html>`)
	})
	mux.HandleFunc("/team", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `<html><body>Our team</body></html>`)
	})
	mux.HandleFunc("/sitemap.xml", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client(), matcher: scrape.NewPathMatcher([]string{"/blog/*", "/news/*", "/press/*", "/careers/*"})}
	ctx := context.Background()

	urls, err := lc.DiscoverLinks(ctx, srv.URL, 10, 2)
	require.NoError(t, err)

	// Should find: /, /about, /services, /team (blog is excluded)
	assert.Contains(t, urls, srv.URL+"/")
	assert.Contains(t, urls, srv.URL+"/about")
	assert.Contains(t, urls, srv.URL+"/services")
	assert.Contains(t, urls, srv.URL+"/team")

	// Blog should be excluded.
	for _, u := range urls {
		assert.NotContains(t, u, "/blog")
	}
}

func TestDiscoverLinks_MaxPages(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `<html><body><a href="/about">About</a><a href="/services">Services</a><a href="/contact">Contact</a></body></html>`)
	})
	mux.HandleFunc("/about", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `<html><body>About us</body></html>`)
	})
	mux.HandleFunc("/services", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `<html><body>Services</body></html>`)
	})
	mux.HandleFunc("/contact", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `<html><body>Contact</body></html>`)
	})
	mux.HandleFunc("/sitemap.xml", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client(), matcher: scrape.NewPathMatcher(nil)}
	ctx := context.Background()

	urls, err := lc.DiscoverLinks(ctx, srv.URL, 2, 2)
	require.NoError(t, err)
	assert.Len(t, urls, 2)
}

func TestDiscoverLinks_MaxDepth(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `<html><body><a href="/about">About</a></body></html>`)
	})
	mux.HandleFunc("/about", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `<html><body><a href="/team">Team</a></body></html>`)
	})
	mux.HandleFunc("/team", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `<html><body>Team page</body></html>`)
	})
	mux.HandleFunc("/sitemap.xml", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client(), matcher: scrape.NewPathMatcher(nil)}
	ctx := context.Background()

	// maxDepth=1: should get / (depth 0) and /about (depth 1), but NOT /team (depth 2).
	urls, err := lc.DiscoverLinks(ctx, srv.URL, 10, 1)
	require.NoError(t, err)

	assert.Contains(t, urls, srv.URL+"/")
	assert.Contains(t, urls, srv.URL+"/about")

	hasTeam := false
	for _, u := range urls {
		if u == srv.URL+"/team" {
			hasTeam = true
		}
	}
	assert.False(t, hasTeam, "/team should not be found at maxDepth=1")
}

func TestDiscoverLinks_ContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `<html><body><a href="/about">About</a></body></html>`)
	}))
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client(), matcher: scrape.NewPathMatcher(nil)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	urls, err := lc.DiscoverLinks(ctx, srv.URL, 10, 2)
	assert.NoError(t, err)
	// With cancelled context, may return empty or partial results.
	assert.True(t, len(urls) <= 1)
}

func TestExtractLinks_Non200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprint(w, "Not Found")
	}))
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client(), matcher: scrape.NewPathMatcher(nil)}
	ctx := context.Background()

	base, _ := url.Parse(srv.URL)
	links, err := lc.extractLinks(ctx, srv.URL+"/missing", base)

	assert.NoError(t, err)
	assert.Nil(t, links)
}

func TestExtractLinks_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `<html><body><a href="/about">About</a><a href="/services">Services</a></body></html>`)
	}))
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client(), matcher: scrape.NewPathMatcher(nil)}
	ctx := context.Background()

	base, _ := url.Parse(srv.URL)
	links, err := lc.extractLinks(ctx, srv.URL+"/", base)

	require.NoError(t, err)
	assert.Len(t, links, 2)
}

func TestFetchSitemapURLs(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/sitemap.xml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://acme.com/about</loc></url>
  <url><loc>https://acme.com/services</loc></url>
  <url><loc>https://external.com/page</loc></url>
</urlset>`)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client(), matcher: scrape.NewPathMatcher(nil)}
	ctx := context.Background()

	base, _ := url.Parse("https://acme.com")
	urls := lc.fetchSitemapURLs(ctx, srv.URL+"/sitemap.xml", base)

	// Should only include same-host URLs (acme.com), not external.com.
	assert.Len(t, urls, 2)
	assert.Contains(t, urls, "https://acme.com/about")
	assert.Contains(t, urls, "https://acme.com/services")
}

func TestFetchSitemapURLs_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client(), matcher: scrape.NewPathMatcher(nil)}
	ctx := context.Background()

	base, _ := url.Parse("https://acme.com")
	urls := lc.fetchSitemapURLs(ctx, srv.URL+"/sitemap.xml", base)

	assert.Nil(t, urls)
}

func TestFetchSitemapURLs_InvalidXML(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "not xml at all")
	}))
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client(), matcher: scrape.NewPathMatcher(nil)}
	ctx := context.Background()

	base, _ := url.Parse("https://acme.com")
	urls := lc.fetchSitemapURLs(ctx, srv.URL+"/sitemap.xml", base)

	assert.Nil(t, urls)
}

func TestDiscoverLinks_WithSitemap(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `<html><body><a href="/about">About</a></body></html>`)
	})
	mux.HandleFunc("/about", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `<html><body>About us</body></html>`)
	})
	mux.HandleFunc("/services", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `<html><body>Services</body></html>`)
	})
	mux.HandleFunc("/sitemap.xml", func(w http.ResponseWriter, r *http.Request) {
		// The sitemap URLs must use the httptest server host.
		srvURL := "http://" + r.Host
		_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><url><loc>%s/services</loc></url></urlset>`, srvURL)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client(), matcher: scrape.NewPathMatcher(nil)}
	ctx := context.Background()

	urls, err := lc.DiscoverLinks(ctx, srv.URL, 10, 2)
	require.NoError(t, err)

	// Should find / (BFS seed), /about (from HTML links), and /services (from sitemap).
	assert.Contains(t, urls, srv.URL+"/")
	assert.Contains(t, urls, srv.URL+"/about")
	assert.Contains(t, urls, srv.URL+"/services")
}

func TestDiscoverLinks_ParallelFetch(t *testing.T) {
	const pageCount = 12
	const perPageDelay = 50 * time.Millisecond

	mux := http.NewServeMux()

	// Homepage links to all pages.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var links string
		for i := 0; i < pageCount; i++ {
			links += fmt.Sprintf(`<a href="/page%d">Page %d</a>`, i, i)
		}
		fmt.Fprintf(w, `<html><body>%s</body></html>`, links) //nolint:errcheck
	})

	// Each page has a small delay to simulate slow responses.
	for i := 0; i < pageCount; i++ {
		i := i
		mux.HandleFunc(fmt.Sprintf("/page%d", i), func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(perPageDelay)
			fmt.Fprintf(w, `<html><body>Page %d content</body></html>`, i) //nolint:errcheck
		})
	}

	// No sitemap.
	mux.HandleFunc("/sitemap.xml", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client(), matcher: scrape.NewPathMatcher(nil)}
	ctx := context.Background()

	start := time.Now()
	urls, err := lc.DiscoverLinks(ctx, srv.URL, 50, 2)
	elapsed := time.Since(start)

	require.NoError(t, err)

	// Should discover homepage + all linked pages.
	assert.GreaterOrEqual(t, len(urls), pageCount+1)
	assert.Contains(t, urls, srv.URL+"/")
	for i := 0; i < pageCount; i++ {
		assert.Contains(t, urls, fmt.Sprintf("%s/page%d", srv.URL, i))
	}

	// Serial would take pageCount * perPageDelay = 600ms+. Parallel should be
	// significantly faster. Use 400ms as threshold to prove parallelism.
	serialDuration := time.Duration(pageCount) * perPageDelay
	assert.Less(t, elapsed, serialDuration,
		"parallel crawl took %v; serial would be ~%v", elapsed, serialDuration)
}

func TestDiscoverLinks_ParallelRespectMaxPages(t *testing.T) {
	const totalPages = 25
	const maxPages = 5

	mux := http.NewServeMux()

	// Homepage links to all pages.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var links string
		for i := 0; i < totalPages; i++ {
			links += fmt.Sprintf(`<a href="/page%d">Page %d</a>`, i, i)
		}
		fmt.Fprintf(w, `<html><body>%s</body></html>`, links) //nolint:errcheck
	})

	for i := 0; i < totalPages; i++ {
		i := i
		mux.HandleFunc(fmt.Sprintf("/page%d", i), func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, `<html><body>Page %d content</body></html>`, i) //nolint:errcheck
		})
	}

	mux.HandleFunc("/sitemap.xml", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client(), matcher: scrape.NewPathMatcher(nil)}
	ctx := context.Background()

	urls, err := lc.DiscoverLinks(ctx, srv.URL, maxPages, 2)
	require.NoError(t, err)

	// Must not exceed maxPages despite many available pages.
	assert.LessOrEqual(t, len(urls), maxPages,
		"expected at most %d pages, got %d", maxPages, len(urls))
}

func TestDiscoverLinks_ParallelErrorsNonFatal(t *testing.T) {
	const goodPages = 6
	const badPages = 6

	mux := http.NewServeMux()

	// Homepage links to good and bad pages.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var links string
		for i := 0; i < goodPages; i++ {
			links += fmt.Sprintf(`<a href="/good%d">Good %d</a>`, i, i)
		}
		for i := 0; i < badPages; i++ {
			links += fmt.Sprintf(`<a href="/bad%d">Bad %d</a>`, i, i)
		}
		fmt.Fprintf(w, `<html><body>%s</body></html>`, links) //nolint:errcheck
	})

	// Good pages return 200.
	for i := 0; i < goodPages; i++ {
		i := i
		mux.HandleFunc(fmt.Sprintf("/good%d", i), func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, `<html><body>Good page %d</body></html>`, i) //nolint:errcheck
		})
	}

	// Bad pages return 500.
	for i := 0; i < badPages; i++ {
		mux.HandleFunc(fmt.Sprintf("/bad%d", i), func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = fmt.Fprint(w, "Internal Server Error")
		})
	}

	mux.HandleFunc("/sitemap.xml", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client(), matcher: scrape.NewPathMatcher(nil)}
	ctx := context.Background()

	urls, err := lc.DiscoverLinks(ctx, srv.URL, 50, 2)
	require.NoError(t, err)

	// Should still discover homepage + all good and bad pages (they are in the
	// queue regardless of HTTP status — extractLinks returns nil links for
	// non-200, but the URL itself is already added to the result list).
	assert.GreaterOrEqual(t, len(urls), 1+goodPages,
		"should discover at least homepage + good pages despite 500 errors")

	// Verify good pages are present.
	for i := 0; i < goodPages; i++ {
		assert.Contains(t, urls, fmt.Sprintf("%s/good%d", srv.URL, i))
	}
}

func TestDiscoverLinks_ParallelContextCancel(t *testing.T) {
	const pageCount = 20

	mux := http.NewServeMux()

	// Homepage links to many slow pages.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var links string
		for i := 0; i < pageCount; i++ {
			links += fmt.Sprintf(`<a href="/slow%d">Slow %d</a>`, i, i)
		}
		_, _ = fmt.Fprintf(w, `<html><body>%s</body></html>`, links)
	})

	// Each slow page takes 2 seconds.
	for i := 0; i < pageCount; i++ {
		i := i
		mux.HandleFunc(fmt.Sprintf("/slow%d", i), func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(2 * time.Second):
			}
			_, _ = fmt.Fprintf(w, `<html><body>Slow page %d</body></html>`, i)
		})
	}

	mux.HandleFunc("/sitemap.xml", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client(), matcher: scrape.NewPathMatcher(nil)}

	// Cancel after 200ms — mid-crawl.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	urls, err := lc.DiscoverLinks(ctx, srv.URL, 50, 2)
	elapsed := time.Since(start)

	// Should not error — DiscoverLinks treats context cancellation gracefully.
	assert.NoError(t, err)

	// Should return partial results (at least the homepage was queued).
	assert.GreaterOrEqual(t, len(urls), 1, "should have at least partial results")

	// Must not hang — should return well within 1 second (context cancelled at 200ms).
	assert.Less(t, elapsed, 1*time.Second,
		"DiscoverLinks should return promptly after context cancellation, took %v", elapsed)
}
