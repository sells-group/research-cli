package pipeline

import (
	"context"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/ppp"
)

// PPPPhase implements Phase 1D: PPP loan lookup for a company.
func PPPPhase(ctx context.Context, company model.Company, pppClient ppp.Querier) ([]ppp.LoanMatch, error) {
	if pppClient == nil {
		return nil, nil // PPP not configured
	}
	if company.Location == "" {
		zap.L().Debug("ppp: skipping, no location", zap.String("company", company.Name))
		return nil, nil
	}

	// Parse state and city from location (expect "City, ST" format).
	state, city := parseLocation(company.Location)
	if state == "" {
		zap.L().Debug("ppp: skipping, no state in location", zap.String("company", company.Name))
		return nil, nil
	}

	start := time.Now()
	matches, err := pppClient.FindLoans(ctx, company.Name, state, city)
	duration := time.Since(start)

	if err != nil {
		return nil, eris.Wrap(err, "ppp: find loans")
	}

	zap.L().Info("ppp: phase complete",
		zap.String("company", company.Name),
		zap.Int("matches", len(matches)),
		zap.Int64("duration_ms", duration.Milliseconds()),
	)
	if len(matches) > 0 {
		zap.L().Info("ppp: best match",
			zap.String("borrower", matches[0].BorrowerName),
			zap.Int("tier", matches[0].MatchTier),
			zap.Float64("score", matches[0].MatchScore),
		)
	}

	return matches, nil
}

// parseLocation splits a location string into state and city components.
// Expected formats: "City, ST", "ST", "City, State" (only 2-letter codes accepted).
func parseLocation(loc string) (state, city string) {
	parts := strings.Split(loc, ",")
	if len(parts) >= 2 {
		city = strings.TrimSpace(parts[0])
		state = strings.TrimSpace(parts[len(parts)-1])
	} else {
		state = strings.TrimSpace(loc)
	}
	state = strings.ToUpper(state)
	// Ensure 2-letter state code.
	if len(state) > 2 {
		state = "" // Can't parse
	}
	return state, city
}
