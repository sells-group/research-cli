package discovery

import (
	"context"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/pkg/google"
)

const (
	// maxPagesPerCell limits pagination to avoid excessive API costs per cell.
	maxPagesPerCell = 3
	// splitThreshold is the result count that triggers sub-cell splitting.
	splitThreshold = 60
)

// OrganicStrategy discovers leads by searching Google Places within MSA grid cells.
type OrganicStrategy struct {
	store   Store
	google  google.Client
	limiter *rate.Limiter
	cfg     *config.DiscoveryConfig
}

// NewOrganicStrategy creates an OrganicStrategy with the given dependencies.
func NewOrganicStrategy(store Store, g google.Client, cfg *config.DiscoveryConfig) *OrganicStrategy {
	rateLimit := cfg.GooglePlacesRateLimit
	if rateLimit <= 0 {
		rateLimit = 10
	}
	return &OrganicStrategy{
		store:   store,
		google:  g,
		limiter: rate.NewLimiter(rate.Limit(rateLimit), 1),
		cfg:     cfg,
	}
}

// Run executes the organic grid search strategy.
func (s *OrganicStrategy) Run(ctx context.Context, runCfg RunConfig) (*RunResult, error) {
	log := zap.L().With(zap.String("strategy", "organic"))

	cbsaCode, _ := runCfg.Params["cbsa_code"].(string)
	textQuery, _ := runCfg.Params["text_query"].(string)
	maxCells := 100
	if v, ok := runCfg.Params["max_cells"].(float64); ok && v > 0 {
		maxCells = int(v)
	}

	if cbsaCode == "" {
		return nil, eris.New("organic: cbsa_code is required")
	}
	if textQuery == "" {
		return nil, eris.New("organic: text_query is required")
	}

	// Create run.
	runID, err := s.store.CreateRun(ctx, runCfg)
	if err != nil {
		return nil, eris.Wrap(err, "organic: create run")
	}

	cells, err := s.store.GetUnsearchedCells(ctx, cbsaCode, maxCells)
	if err != nil {
		return nil, eris.Wrap(err, "organic: get unsearched cells")
	}
	log.Info("fetched unsearched cells", zap.Int("count", len(cells)), zap.String("cbsa", cbsaCode))

	var (
		apiCalls   int
		totalFound int
		batch      []Candidate
	)

	for i, cell := range cells {
		if ctx.Err() != nil {
			break
		}

		cellResults, calls, err := s.searchCell(ctx, runID, textQuery, cell)
		apiCalls += calls
		if err != nil {
			log.Warn("cell search failed", zap.Int64("cell_id", cell.ID), zap.Error(err))
			continue
		}

		batch = append(batch, cellResults...)

		// Update cell as searched.
		if err := s.store.UpdateCellSearched(ctx, cell.ID, len(cellResults)); err != nil {
			log.Warn("update cell searched failed", zap.Int64("cell_id", cell.ID), zap.Error(err))
		}

		// If cell returned many results, it may need splitting.
		// Log a warning; actual sub-cell generation would require PostGIS INSERT.
		if len(cellResults) >= splitThreshold {
			log.Warn("cell saturated, consider splitting",
				zap.Int64("cell_id", cell.ID),
				zap.Int("results", len(cellResults)),
			)
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

		if (i+1)%10 == 0 {
			log.Info("progress", zap.Int("cells_searched", i+1), zap.Int("total_cells", len(cells)))
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
		CostUSD:         float64(apiCalls) * 0.032,
	}

	if err := s.store.CompleteRun(ctx, runID, result); err != nil {
		log.Error("complete run failed", zap.Error(err))
	}

	log.Info("organic run complete",
		zap.Int("cells_searched", len(cells)),
		zap.Int("candidates_found", totalFound),
		zap.Int("api_calls", apiCalls),
	)

	return result, nil
}

// searchCell searches a single grid cell, paginating up to maxPagesPerCell.
func (s *OrganicStrategy) searchCell(ctx context.Context, runID, textQuery string, cell GridCell) ([]Candidate, int, error) {
	var (
		candidates []Candidate
		pageToken  string
		apiCalls   int
	)

	for page := 0; page < maxPagesPerCell; page++ {
		if err := s.limiter.Wait(ctx); err != nil {
			return candidates, apiCalls, eris.Wrap(err, "organic: rate limit wait")
		}

		req := google.DiscoverySearchRequest{
			TextQuery: textQuery,
			LocationRestriction: &google.LocationRect{
				Rectangle: google.Rectangle{
					Low:  google.LatLng{Latitude: cell.SWLat, Longitude: cell.SWLon},
					High: google.LatLng{Latitude: cell.NELat, Longitude: cell.NELon},
				},
			},
			PageToken: pageToken,
		}

		resp, err := s.google.DiscoverySearch(ctx, req)
		apiCalls++
		if err != nil {
			return candidates, apiCalls, eris.Wrap(err, "organic: discovery search")
		}

		for _, place := range resp.Places {
			if place.WebsiteURI == "" {
				continue
			}
			if isDirectoryURL(place.WebsiteURI, s.cfg.DirectoryBlocklist) {
				continue
			}

			domain := extractDomain(place.WebsiteURI)
			addr := place.FormattedAddress
			city, state, zip := parseAddress(addr)

			candidates = append(candidates, Candidate{
				RunID:         runID,
				GooglePlaceID: place.ID,
				Name:          place.DisplayName.Text,
				Domain:        domain,
				Website:       place.WebsiteURI,
				City:          city,
				State:         state,
				ZipCode:       zip,
				Source:        "organic",
			})
		}

		// Stop if no more pages.
		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}

	return candidates, apiCalls, nil
}

// parseAddress performs a best-effort extraction of city, state, zip from a
// formatted address string like "123 Main St, Springfield, IL 62701, USA".
func parseAddress(addr string) (city, state, zip string) {
	parts := splitAddress(addr)
	if len(parts) < 2 {
		return "", "", ""
	}

	// Typically: street, city, state+zip, country
	// We want the second-to-last US-style segment.
	for i := len(parts) - 1; i >= 0; i-- {
		if s, z := parseStateZip(parts[i]); s != "" {
			state = s
			zip = z
			if i > 0 {
				city = parts[i-1]
			}
			return city, state, zip
		}
	}

	// Fallback: assume last element is city.
	if len(parts) >= 2 {
		city = parts[len(parts)-2]
	}
	return city, state, zip
}

func splitAddress(addr string) []string {
	raw := make([]string, 0)
	for _, p := range splitComma(addr) {
		trimmed := trimSpace(p)
		if trimmed != "" {
			raw = append(raw, trimmed)
		}
	}
	return raw
}

func splitComma(s string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}

// parseStateZip tries to parse "IL 62701" or "IL" from a string.
func parseStateZip(s string) (state, zip string) {
	s = trimSpace(s)
	fields := splitFields(s)
	if len(fields) == 0 {
		return "", ""
	}
	candidate := fields[0]
	if len(candidate) != 2 {
		return "", ""
	}
	// Must be uppercase letters.
	if candidate[0] < 'A' || candidate[0] > 'Z' || candidate[1] < 'A' || candidate[1] > 'Z' {
		return "", ""
	}
	state = candidate
	if len(fields) >= 2 && isZipCode(fields[1]) {
		zip = fields[1]
	}
	return state, zip
}

func splitFields(s string) []string {
	var fields []string
	start := -1
	for i := 0; i < len(s); i++ {
		if s[i] == ' ' || s[i] == '\t' {
			if start >= 0 {
				fields = append(fields, s[start:i])
				start = -1
			}
		} else if start < 0 {
			start = i
		}
	}
	if start >= 0 {
		fields = append(fields, s[start:])
	}
	return fields
}

func isZipCode(s string) bool {
	if len(s) < 5 || len(s) > 10 {
		return false
	}
	for _, c := range s {
		if c != '-' && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}
