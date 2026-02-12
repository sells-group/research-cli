package pipeline

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestIsExcluded(t *testing.T) {
	base, _ := url.Parse("https://acme.com")

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
			result := isExcluded(tt.link, base)
			assert.Equal(t, tt.excluded, result)
		})
	}
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
			fmt.Fprint(w, "User-agent: *\nAllow: /")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client()}
	ctx := context.Background()

	assert.True(t, lc.checkExists(ctx, srv.URL+"/robots.txt"))
}

func TestCheckExists_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client()}
	ctx := context.Background()

	assert.False(t, lc.checkExists(ctx, srv.URL+"/sitemap.xml"))
}

func TestCheckExists_ConnectionRefused(t *testing.T) {
	lc := &LocalCrawler{http: http.DefaultClient}
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
		fmt.Fprint(w, "<html><body><h1>Welcome to Acme</h1><p>We are a technology company.</p></body></html>")
	})
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "User-agent: *\nAllow: /")
	})
	mux.HandleFunc("/sitemap.xml", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "<urlset></urlset>")
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client()}
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
	lc := &LocalCrawler{http: http.DefaultClient}
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
		fmt.Fprint(w, "<html><body>Checking your browser before accessing the site. Cloudflare challenge page.</body></html>")
	}))
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client()}
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
			fmt.Fprint(w, "<html><body>Normal page</body></html>")
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client()}
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
		fmt.Fprint(w, `<html><body>
			<a href="/about">About</a>
			<a href="/services">Services</a>
			<a href="/blog/post">Blog Post</a>
		</body></html>`)
	})
	mux.HandleFunc("/about", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><body>
			<a href="/team">Team</a>
		</body></html>`)
	})
	mux.HandleFunc("/services", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><body>Our services</body></html>`)
	})
	mux.HandleFunc("/team", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><body>Our team</body></html>`)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client()}
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
		fmt.Fprint(w, `<html><body>
			<a href="/about">About</a>
			<a href="/services">Services</a>
			<a href="/contact">Contact</a>
		</body></html>`)
	})
	mux.HandleFunc("/about", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><body>About us</body></html>`)
	})
	mux.HandleFunc("/services", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><body>Services</body></html>`)
	})
	mux.HandleFunc("/contact", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><body>Contact</body></html>`)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client()}
	ctx := context.Background()

	urls, err := lc.DiscoverLinks(ctx, srv.URL, 2, 2)
	require.NoError(t, err)
	assert.Len(t, urls, 2)
}

func TestDiscoverLinks_MaxDepth(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><body><a href="/about">About</a></body></html>`)
	})
	mux.HandleFunc("/about", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><body><a href="/team">Team</a></body></html>`)
	})
	mux.HandleFunc("/team", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><body>Team page</body></html>`)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client()}
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
		fmt.Fprint(w, `<html><body><a href="/about">About</a></body></html>`)
	}))
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client()}
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
		fmt.Fprint(w, "Not Found")
	}))
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client()}
	ctx := context.Background()

	base, _ := url.Parse(srv.URL)
	links, err := lc.extractLinks(ctx, srv.URL+"/missing", base)

	assert.NoError(t, err)
	assert.Nil(t, links)
}

func TestExtractLinks_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><body><a href="/about">About</a><a href="/services">Services</a></body></html>`)
	}))
	defer srv.Close()

	lc := &LocalCrawler{http: srv.Client()}
	ctx := context.Background()

	base, _ := url.Parse(srv.URL)
	links, err := lc.extractLinks(ctx, srv.URL+"/", base)

	require.NoError(t, err)
	assert.Len(t, links, 2)
}
