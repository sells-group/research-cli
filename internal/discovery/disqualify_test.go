package discovery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sells-group/research-cli/internal/config"
)

func TestDisqualifyT0_NoWebsite(t *testing.T) {
	store := &mockStore{}
	cfg := &config.DiscoveryConfig{T0URLTimeoutSecs: 1, DirectoryBlocklist: []string{}}
	c := &Candidate{Name: "Test Corp", Website: ""}

	dq, reason := DisqualifyT0(context.Background(), c, store, cfg)
	assert.True(t, dq)
	assert.Equal(t, ReasonNoWebsite, reason)
}

func TestDisqualifyT0_DirectoryURL(t *testing.T) {
	store := &mockStore{}
	cfg := &config.DiscoveryConfig{
		T0URLTimeoutSecs:   1,
		DirectoryBlocklist: []string{"yelp.com", "facebook.com", "linkedin.com"},
	}

	tests := []struct {
		url    string
		expect bool
	}{
		{"https://yelp.com/biz/test-corp", true},
		{"https://www.facebook.com/testcorp", true},
		{"https://www.linkedin.com/company/test", true},
		{"https://testcorp.com", false},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			c := &Candidate{Name: "Test Corp", Website: tt.url, Domain: "testcorp.com"}
			dq, reason := DisqualifyT0(context.Background(), c, store, cfg)
			if tt.expect {
				assert.True(t, dq)
				assert.Equal(t, ReasonDirectoryURL, reason)
			} else {
				// Would fall through to other checks; not testing full chain here.
				if dq {
					assert.NotEqual(t, ReasonDirectoryURL, reason)
				}
			}
		})
	}
}

func TestDisqualifyT0_SoleProp(t *testing.T) {
	store := &mockStore{}
	cfg := &config.DiscoveryConfig{T0URLTimeoutSecs: 1, DirectoryBlocklist: []string{}}

	tests := []struct {
		name   string
		expect bool
	}{
		{"John D.", true},
		{"Jane Sm", true},
		{"John D", true},
		{"Acme Corp", false},
		{"John David Smith", false},
		{"Mary Ann O'Brien LLC", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Candidate{Name: tt.name, Website: "https://example.com", Domain: "example.com"}
			dq, reason := DisqualifyT0(context.Background(), c, store, cfg)
			if tt.expect {
				assert.True(t, dq, "expected disqualification for %q", tt.name)
				assert.Equal(t, ReasonSoleProp, reason)
			} else {
				if dq {
					assert.NotEqual(t, ReasonSoleProp, reason, "unexpected sole_prop for %q", tt.name)
				}
			}
		})
	}
}

func TestDisqualifyT0_AlreadyEnriched(t *testing.T) {
	store := &mockStore{
		domainExistsResults: map[string]bool{"existing.com": true},
	}
	cfg := &config.DiscoveryConfig{T0URLTimeoutSecs: 1, DirectoryBlocklist: []string{}}

	c := &Candidate{Name: "Existing Corp", Website: "https://existing.com", Domain: "existing.com"}
	dq, reason := DisqualifyT0(context.Background(), c, store, cfg)
	assert.True(t, dq)
	assert.Equal(t, ReasonAlreadyEnrich, reason)
}

func TestDisqualifyT0_DuplicatePlace(t *testing.T) {
	store := &mockStore{
		placeExistsResults: map[string]bool{"ChIJ-dup": true},
	}
	cfg := &config.DiscoveryConfig{T0URLTimeoutSecs: 1, DirectoryBlocklist: []string{}}

	c := &Candidate{
		Name:          "Dup Corp",
		Website:       "https://dupcorp.com",
		Domain:        "dupcorp.com",
		GooglePlaceID: "ChIJ-dup",
	}
	dq, reason := DisqualifyT0(context.Background(), c, store, cfg)
	assert.True(t, dq)
	assert.Equal(t, ReasonDuplicatePlace, reason)
}

func TestDisqualifyT0_URLDead(t *testing.T) {
	// Server that immediately returns 500.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	store := &mockStore{}
	cfg := &config.DiscoveryConfig{T0URLTimeoutSecs: 1, DirectoryBlocklist: []string{}}

	c := &Candidate{Name: "Dead Corp", Website: srv.URL, Domain: "dead.com"}
	dq, reason := DisqualifyT0(context.Background(), c, store, cfg)
	assert.True(t, dq)
	assert.Equal(t, ReasonURLDead, reason)
}

func TestDisqualifyT0_URLReachable(t *testing.T) {
	// Server that returns 200.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	store := &mockStore{}
	cfg := &config.DiscoveryConfig{T0URLTimeoutSecs: 2, DirectoryBlocklist: []string{}}

	c := &Candidate{Name: "Live Corp", Website: srv.URL, Domain: "live.com"}
	dq, _ := DisqualifyT0(context.Background(), c, store, cfg)
	assert.False(t, dq)
}

func TestIsDirectoryURL(t *testing.T) {
	blocklist := []string{"yelp.com", "facebook.com", "bbb.org"}

	tests := []struct {
		url    string
		expect bool
	}{
		{"https://yelp.com/biz/test", true},
		{"https://www.yelp.com/biz/test", true},
		{"https://m.yelp.com/biz/test", true},
		{"https://facebook.com/test", true},
		{"https://www.bbb.org/test", true},
		{"https://testcorp.com", false},
		{"https://notyelp.com", false},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			assert.Equal(t, tt.expect, isDirectoryURL(tt.url, blocklist))
		})
	}
}

func TestRunT0(t *testing.T) {
	// Server for reachable URLs.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	store := &mockStore{
		candidates: []Candidate{
			{ID: 1, Name: "Good Corp", Website: srv.URL, Domain: "good.com"},
			{ID: 2, Name: "No Site Inc", Website: "", Domain: ""},
			{ID: 3, Name: "Yelp Biz", Website: "https://yelp.com/biz/test", Domain: "yelp.com"},
		},
	}

	cfg := &config.DiscoveryConfig{
		T0URLTimeoutSecs:   2,
		DirectoryBlocklist: []string{"yelp.com"},
	}

	q, dq, err := RunT0(context.Background(), store, cfg, "test-run", 100)
	assert.NoError(t, err)
	assert.Equal(t, 1, q)  // Good Corp
	assert.Equal(t, 2, dq) // No Site + Yelp
}
