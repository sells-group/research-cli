package discovery

import (
	"context"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/company"
)

// Promote moves qualified discovery candidates into the enrichment pipeline
// by creating CompanyRecord entries and marking them as promoted.
func Promote(ctx context.Context, dStore Store, cStore company.CompanyStore, runID string, minScore float64) (*PromoteResult, error) {
	log := zap.L().With(zap.String("phase", "promote"), zap.String("run_id", runID))

	notDisqualified := false
	score := minScore
	candidates, err := dStore.ListCandidates(ctx, runID, ListOpts{
		Disqualified: &notDisqualified,
		MinScore:     &score,
		Limit:        10000,
	})
	if err != nil {
		return nil, eris.Wrap(err, "promote: list candidates")
	}

	// Filter to non-promoted candidates.
	var eligible []Candidate
	for _, c := range candidates {
		if c.PromotedAt == nil {
			eligible = append(eligible, c)
		}
	}

	log.Info("promoting candidates", zap.Int("eligible", len(eligible)))

	result := &PromoteResult{}
	var promotedIDs []int64

	for _, c := range eligible {
		if ctx.Err() != nil {
			break
		}

		// Skip if domain already exists in companies.
		if c.Domain != "" {
			existing, err := cStore.GetCompanyByDomain(ctx, c.Domain)
			if err != nil {
				log.Warn("domain check failed", zap.String("domain", c.Domain), zap.Error(err))
				result.Errors++
				continue
			}
			if existing != nil {
				result.Skipped++
				continue
			}
		}

		// Create company record.
		rec := &company.CompanyRecord{
			Name:      c.Name,
			Domain:    c.Domain,
			Website:   c.Website,
			Street:    c.Street,
			City:      c.City,
			State:     c.State,
			ZipCode:   c.ZipCode,
			NAICSCode: c.NAICSCode,
		}

		if err := cStore.CreateCompany(ctx, rec); err != nil {
			log.Warn("create company failed", zap.String("name", c.Name), zap.Error(err))
			result.Errors++
			continue
		}

		// Link Google Place ID.
		if c.GooglePlaceID != "" {
			id := &company.Identifier{
				CompanyID:  rec.ID,
				System:     company.SystemGooglePlace,
				Identifier: c.GooglePlaceID,
			}
			if err := cStore.UpsertIdentifier(ctx, id); err != nil {
				log.Warn("upsert identifier failed", zap.Int64("company_id", rec.ID), zap.Error(err))
			}
		}

		promotedIDs = append(promotedIDs, c.ID)
		result.Promoted++
	}

	// Mark all promoted candidates.
	if len(promotedIDs) > 0 {
		if err := dStore.MarkPromoted(ctx, promotedIDs); err != nil {
			log.Error("mark promoted failed", zap.Error(err))
		}
	}

	log.Info("promote complete",
		zap.Int("promoted", result.Promoted),
		zap.Int("skipped", result.Skipped),
		zap.Int("errors", result.Errors),
	)

	return result, nil
}
