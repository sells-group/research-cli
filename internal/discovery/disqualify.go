package discovery

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
)

// T0 disqualification reason codes.
const (
	ReasonNoWebsite      = "no_website"
	ReasonDirectoryURL   = "directory_url"
	ReasonSoleProp       = "sole_prop"
	ReasonAlreadyEnrich  = "already_enriched"
	ReasonDuplicatePlace = "duplicate_place"
	ReasonURLDead        = "url_dead"
)

// solePropPattern matches names like "John D." or "Jane Sm" — likely sole proprietorships.
var solePropPattern = regexp.MustCompile(`^\w+ \w{1,2}\.?$`)

// DisqualifyT0 applies programmatic disqualification checks to a candidate.
// Returns true and the reason if the candidate should be disqualified.
func DisqualifyT0(ctx context.Context, candidate *Candidate, store Store, cfg *config.DiscoveryConfig) (bool, string) {
	// 1. No website.
	if candidate.Website == "" {
		return true, ReasonNoWebsite
	}

	// 2. Directory URL.
	if isDirectoryURL(candidate.Website, cfg.DirectoryBlocklist) {
		return true, ReasonDirectoryURL
	}

	// 3. Sole proprietorship name pattern.
	if solePropPattern.MatchString(candidate.Name) {
		return true, ReasonSoleProp
	}

	// 4. Domain already in companies table.
	if candidate.Domain != "" {
		exists, err := store.DomainExists(ctx, candidate.Domain)
		if err == nil && exists {
			return true, ReasonAlreadyEnrich
		}
	}

	// 5. Duplicate Google Place ID.
	if candidate.GooglePlaceID != "" {
		exists, err := store.PlaceIDExists(ctx, candidate.GooglePlaceID)
		if err == nil && exists {
			return true, ReasonDuplicatePlace
		}
	}

	// 6. URL unreachable.
	timeout := time.Duration(cfg.T0URLTimeoutSecs) * time.Second
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	if !isURLReachable(ctx, candidate.Website, timeout) {
		return true, ReasonURLDead
	}

	return false, ""
}

// RunT0 applies T0 disqualification to all non-disqualified candidates in a run.
func RunT0(ctx context.Context, store Store, cfg *config.DiscoveryConfig, runID string, limit int) (qualified, disqualified int, err error) {
	log := zap.L().With(zap.String("phase", "t0"), zap.String("run_id", runID))

	notDisqualified := false
	candidates, err := store.ListCandidates(ctx, runID, ListOpts{
		Disqualified: &notDisqualified,
		Limit:        limit,
	})
	if err != nil {
		return 0, 0, err
	}

	log.Info("running T0 disqualification", zap.Int("candidates", len(candidates)))

	for i := range candidates {
		if ctx.Err() != nil {
			return qualified, disqualified, ctx.Err() //nolint:nilerr // return context error
		}

		dq, reason := DisqualifyT0(ctx, &candidates[i], store, cfg)
		if dq {
			if err := store.DisqualifyCandidate(ctx, candidates[i].ID, reason); err != nil {
				log.Warn("disqualify failed", zap.Int64("id", candidates[i].ID), zap.Error(err))
			}
			disqualified++
		} else {
			// Assign a passing T0 score.
			score := 1.0
			if err := store.UpdateCandidateScore(ctx, candidates[i].ID, "t0", score); err != nil {
				log.Warn("update score failed", zap.Int64("id", candidates[i].ID), zap.Error(err))
			}
			qualified++
		}
	}

	log.Info("T0 complete", zap.Int("qualified", qualified), zap.Int("disqualified", disqualified))
	return qualified, disqualified, nil
}

// isDirectoryURL checks if a URL's hostname matches any entry in the blocklist.
func isDirectoryURL(website string, blocklist []string) bool {
	u, err := url.Parse(website)
	if err != nil {
		return false
	}

	host := strings.ToLower(u.Hostname())
	host = strings.TrimPrefix(host, "www.")

	for _, blocked := range blocklist {
		blocked = strings.ToLower(blocked)
		if host == blocked || strings.HasSuffix(host, "."+blocked) {
			return true
		}
	}
	return false
}

// isURLReachable performs an HTTP HEAD request to check if a URL is reachable.
func isURLReachable(ctx context.Context, rawURL string, timeout time.Duration) bool {
	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: timeout,
			}).DialContext,
			TLSHandshakeTimeout: timeout,
		},
		CheckRedirect: func(_ *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, rawURL, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; research-cli/1.0)")

	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close() //nolint:errcheck

	// Consider 2xx and 3xx as reachable.
	return resp.StatusCode < 400
}
