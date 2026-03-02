package discovery

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/pkg/google"
)

// PPPStrategy discovers leads by finding PPP borrowers that are not yet in the
// companies table and looking them up via Google Places to obtain a website.
type PPPStrategy struct {
	store   Store
	google  google.Client
	limiter *rate.Limiter
	cfg     *config.DiscoveryConfig
}

// NewPPPStrategy creates a PPPStrategy with the given dependencies.
func NewPPPStrategy(store Store, g google.Client, cfg *config.DiscoveryConfig) *PPPStrategy {
	rateLimit := cfg.GooglePlacesRateLimit
	if rateLimit <= 0 {
		rateLimit = 10
	}
	return &PPPStrategy{
		store:   store,
		google:  g,
		limiter: rate.NewLimiter(rate.Limit(rateLimit), 1),
		cfg:     cfg,
	}
}

// Run executes the PPP exhaustion strategy.
func (s *PPPStrategy) Run(ctx context.Context, runCfg RunConfig) (*RunResult, error) {
	log := zap.L().With(zap.String("strategy", "ppp"))

	// Extract params.
	naics := toStringSlice(runCfg.Params["naics"])
	states := toStringSlice(runCfg.Params["states"])
	minApproval := s.cfg.PPPMinApproval
	if v, ok := runCfg.Params["min_approval"].(float64); ok && v > 0 {
		minApproval = v
	}
	limit := s.cfg.MaxCandidatesPerRun
	if v, ok := runCfg.Params["limit"].(float64); ok && v > 0 {
		limit = int(v)
	}

	// Find new PPP borrowers.
	borrowers, err := s.store.FindNewPPPBorrowers(ctx, naics, states, minApproval, limit)
	if err != nil {
		return nil, eris.Wrap(err, "ppp: find borrowers")
	}
	log.Info("found PPP borrowers", zap.Int("count", len(borrowers)))

	if len(borrowers) == 0 {
		return &RunResult{}, nil
	}

	// Create run.
	runID, err := s.store.CreateRun(ctx, runCfg)
	if err != nil {
		return nil, eris.Wrap(err, "ppp: create run")
	}

	var (
		apiCalls   int
		batch      []Candidate
		totalFound int
	)

	for i, b := range borrowers {
		if ctx.Err() != nil {
			break
		}

		// Rate limit Google API calls.
		if err := s.limiter.Wait(ctx); err != nil {
			break
		}

		query := b.BorrowerName
		if b.City != "" && b.State != "" {
			query += " " + b.City + " " + b.State
		}

		resp, err := s.google.DiscoverySearch(ctx, google.DiscoverySearchRequest{
			TextQuery: query,
		})
		apiCalls++

		if err != nil {
			log.Warn("google search failed", zap.String("borrower", b.BorrowerName), zap.Error(err))
			continue
		}

		// Match by name similarity.
		for _, place := range resp.Places {
			sim := nameSimilarity(b.BorrowerName, place.DisplayName.Text)
			if sim < 0.6 {
				continue
			}

			// Skip if no website or directory URL.
			if place.WebsiteURI == "" {
				continue
			}
			if isDirectoryURL(place.WebsiteURI, s.cfg.DirectoryBlocklist) {
				continue
			}

			domain := extractDomain(place.WebsiteURI)

			sourceRecord, _ := json.Marshal(b)

			batch = append(batch, Candidate{
				RunID:         runID,
				GooglePlaceID: place.ID,
				Name:          place.DisplayName.Text,
				Domain:        domain,
				Website:       place.WebsiteURI,
				Street:        b.Street,
				City:          b.City,
				State:         b.State,
				ZipCode:       b.Zip,
				NAICSCode:     b.NAICSCode,
				Source:        "ppp",
				SourceRecord:  sourceRecord,
			})
		}

		// Flush in batches of 1000.
		if len(batch) >= 1000 {
			n, err := s.store.BulkInsertCandidates(ctx, batch)
			if err != nil {
				log.Error("bulk insert failed", zap.Error(err))
			} else {
				totalFound += int(n)
			}
			batch = batch[:0]
		}

		if (i+1)%100 == 0 {
			log.Info("progress", zap.Int("processed", i+1), zap.Int("total", len(borrowers)))
		}
	}

	// Flush remaining.
	if len(batch) > 0 {
		n, err := s.store.BulkInsertCandidates(ctx, batch)
		if err != nil {
			log.Error("bulk insert failed", zap.Error(err))
		} else {
			totalFound += int(n)
		}
	}

	result := &RunResult{
		CandidatesFound: totalFound,
		CostUSD:         float64(apiCalls) * 0.032, // Places API per-search cost
	}

	if err := s.store.CompleteRun(ctx, runID, result); err != nil {
		log.Error("complete run failed", zap.Error(err))
	}

	log.Info("PPP run complete",
		zap.Int("borrowers_processed", len(borrowers)),
		zap.Int("candidates_found", totalFound),
		zap.Int("api_calls", apiCalls),
	)

	return result, nil
}

// nameSimilarity computes Jaccard similarity on lowercased word sets.
func nameSimilarity(a, b string) float64 {
	wordsA := wordSet(strings.ToLower(a))
	wordsB := wordSet(strings.ToLower(b))

	if len(wordsA) == 0 || len(wordsB) == 0 {
		return 0
	}

	intersection := 0
	for w := range wordsA {
		if wordsB[w] {
			intersection++
		}
	}

	union := len(wordsA)
	for w := range wordsB {
		if !wordsA[w] {
			union++
		}
	}

	if union == 0 {
		return 0
	}
	return float64(intersection) / float64(union)
}

func wordSet(s string) map[string]bool {
	words := strings.Fields(s)
	set := make(map[string]bool, len(words))
	for _, w := range words {
		// Strip common punctuation.
		w = strings.Trim(w, ".,;:!?()[]{}\"'")
		if w != "" {
			set[w] = true
		}
	}
	return set
}

func extractDomain(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	host := strings.ToLower(u.Hostname())
	host = strings.TrimPrefix(host, "www.")
	return host
}

func toStringSlice(v any) []string {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case []string:
		return val
	case []any:
		result := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result
	case string:
		if val == "" {
			return nil
		}
		parts := strings.Split(val, ",")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		return parts
	default:
		return nil
	}
}
