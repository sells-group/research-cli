package geocode

import (
	"net/http"
	"strings"

	"golang.org/x/time/rate"
)

// newTestLimiter creates a rate limiter that effectively does not limit for tests.
func newTestLimiter() *rate.Limiter {
	return rate.NewLimiter(rate.Inf, 1)
}

// newRewriteClient creates an HTTP client that rewrites requests to a test server URL.
// All requests matching the target prefix are redirected to the test server.
func newRewriteClient(testServerURL, targetPrefix string) *http.Client {
	return &http.Client{
		Transport: &rewriteTransport{
			base:         http.DefaultTransport,
			testServer:   testServerURL,
			targetPrefix: targetPrefix,
		},
	}
}

type rewriteTransport struct {
	base         http.RoundTripper
	testServer   string
	targetPrefix string
}

func (t *rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	origURL := req.URL.String()
	if strings.HasPrefix(origURL, t.targetPrefix) {
		suffix := origURL[len(t.targetPrefix):]
		newURL := t.testServer + suffix
		newReq := req.Clone(req.Context())
		parsed, err := req.URL.Parse(newURL)
		if err != nil {
			return nil, err
		}
		newReq.URL = parsed
		newReq.Host = parsed.Host
		return t.base.RoundTrip(newReq)
	}
	return t.base.RoundTrip(req)
}
