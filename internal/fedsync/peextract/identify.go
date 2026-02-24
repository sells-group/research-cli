package peextract

import (
	"context"
	"strings"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
)

// PEFirmCandidate represents a PE firm identified from adv_owners data.
type PEFirmCandidate struct {
	OwnerName    string
	OwnerType    string
	RIACount     int
	OwnedCRDs    []int
	MaxOwnership float64
	HasControl   bool
	WebsiteURL   string // resolved or empty
	Source       string // adv_owners, manual
	// Social media URLs captured from rejected website URLs.
	LinkedInURL   string
	TwitterURL    string
	FacebookURL   string
	InstagramURL  string
	YouTubeURL    string
	CrunchbaseURL string
}

// IdentifyPEFirms identifies PE firm candidates from adv_owners.
// It finds entity owners with >=50% control of 2+ RIAs.
func IdentifyPEFirms(ctx context.Context, pool db.Pool, minRIAs int) ([]PEFirmCandidate, error) {
	if minRIAs < 2 {
		minRIAs = 2
	}

	query := `SELECT
		owner_name,
		owner_type,
		count(DISTINCT crd_number) AS ria_count,
		max(ownership_pct) AS max_ownership_pct,
		bool_or(is_control) AS has_control,
		array_agg(DISTINCT crd_number ORDER BY crd_number) AS owned_crds
	FROM fed_data.adv_owners
	WHERE owner_type IN ('DE', 'FE')
	AND (ownership_pct >= 50.0 OR ownership_pct IS NULL)
	AND is_control = true
	GROUP BY owner_name, owner_type
	HAVING count(DISTINCT crd_number) >= $1
	ORDER BY count(DISTINCT crd_number) DESC`

	rows, err := pool.Query(ctx, query, minRIAs)
	if err != nil {
		return nil, eris.Wrap(err, "peextract: identify PE firms")
	}
	defer rows.Close()

	var candidates []PEFirmCandidate
	for rows.Next() {
		var c PEFirmCandidate
		var ownershipPct *float64
		if err := rows.Scan(&c.OwnerName, &c.OwnerType, &c.RIACount, &ownershipPct, &c.HasControl, &c.OwnedCRDs); err != nil {
			return nil, eris.Wrap(err, "peextract: scan PE firm candidate")
		}
		if ownershipPct != nil {
			c.MaxOwnership = *ownershipPct
		}
		c.Source = "adv_owners"
		candidates = append(candidates, c)
	}

	return candidates, rows.Err()
}

// validateURL checks if a URL is appropriate for a PE firm website.
// Returns (valid, reason) where reason explains rejection.
func validateURL(rawURL, firmName string) (bool, string) {
	u := strings.ToLower(strings.TrimSpace(rawURL))

	// Reject empty.
	if u == "" {
		return false, "empty URL"
	}

	// Reject social media.
	socialDomains := []string{"linkedin.com", "twitter.com", "x.com", "facebook.com", "instagram.com", "youtube.com"}
	for _, d := range socialDomains {
		if strings.Contains(u, d) {
			return false, "social media URL: " + d
		}
	}

	// Reject charitable foundations.
	if strings.Contains(u, ".org") && strings.Contains(u, "foundation") {
		return false, "charitable foundation"
	}

	// Reject government/regulatory sites.
	govDomains := []string{".gov", "sec.gov", "finra.org"}
	for _, d := range govDomains {
		if strings.Contains(u, d) {
			return false, "government/regulatory site"
		}
	}

	return true, ""
}

// ResolveWebsites batch-resolves website URLs for all candidates.
// Uses owned CRD numbers to look up RIA websites, then falls back to owner name match.
// If store is non-nil, overrides from pe_firm_overrides are checked first.
func ResolveWebsites(ctx context.Context, pool db.Pool, store *Store, candidates []PEFirmCandidate) {
	// Phase 0: Check overrides first.
	if store != nil {
		for i := range candidates {
			overrideURL, err := store.LoadOverrideURL(ctx, candidates[i].OwnerName)
			if err != nil {
				zap.L().Debug("override lookup failed", zap.String("firm", candidates[i].OwnerName), zap.Error(err))
				continue
			}
			if overrideURL != nil {
				candidates[i].WebsiteURL = *overrideURL
				candidates[i].Source = "override"
			}
		}
	}

	// Build CRD → candidate index for batch lookup.
	crdToIdx := make(map[int][]int)
	for i, c := range candidates {
		for _, crd := range c.OwnedCRDs {
			crdToIdx[crd] = append(crdToIdx[crd], i)
		}
	}

	// Phase 1: Batch lookup from adv_firms via owned CRDs.
	allCRDs := make([]int, 0, len(crdToIdx))
	for crd := range crdToIdx {
		allCRDs = append(allCRDs, crd)
	}

	if len(allCRDs) > 0 {
		rows, err := pool.Query(ctx, `SELECT crd_number, website FROM fed_data.adv_firms
			WHERE crd_number = ANY($1) AND website IS NOT NULL AND website != ''`, allCRDs)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var crd int
				var url string
				if err := rows.Scan(&crd, &url); err != nil {
					continue
				}
				for _, idx := range crdToIdx[crd] {
					if candidates[idx].WebsiteURL == "" {
						if valid, reason := validateURL(url, candidates[idx].OwnerName); !valid {
							captureSocialURL(&candidates[idx], url)
							zap.L().Debug("rejected URL from adv_firms",
								zap.String("firm", candidates[idx].OwnerName),
								zap.String("url", url),
								zap.String("reason", reason))
							continue
						}
						candidates[idx].WebsiteURL = url
						candidates[idx].Source = "adv_firms"
					}
				}
			}
		}
	}

	// Phase 2: For candidates still without a website, try edgar_entities by name.
	for i := range candidates {
		if candidates[i].WebsiteURL != "" {
			continue
		}
		var url string
		err := pool.QueryRow(ctx, `SELECT e.website FROM fed_data.edgar_entities e
			WHERE e.company_name ILIKE $1 AND e.website IS NOT NULL AND e.website != ''
			LIMIT 1`, "%"+normalizeForSearch(candidates[i].OwnerName)+"%").Scan(&url)
		if err == nil && url != "" {
			if valid, reason := validateURL(url, candidates[i].OwnerName); !valid {
				captureSocialURL(&candidates[i], url)
				zap.L().Debug("rejected URL from edgar",
					zap.String("firm", candidates[i].OwnerName),
					zap.String("url", url),
					zap.String("reason", reason))
				continue
			}
			candidates[i].WebsiteURL = url
			candidates[i].Source = "edgar"
		}
	}

	resolved := 0
	for _, c := range candidates {
		if c.WebsiteURL != "" {
			resolved++
		}
	}
	zap.L().Info("websites resolved", zap.Int("resolved", resolved), zap.Int("total", len(candidates)))
}

// normalizeForSearch strips common suffixes for fuzzy matching.
func normalizeForSearch(name string) string {
	name = strings.TrimSpace(name)
	for _, suffix := range []string{
		", LLC", ", LP", ", Inc.", ", Inc", ", Corp.", ", Corp",
		" LLC", " LP", " Inc.", " Inc", " Corp.", " Corp",
		", L.L.C.", ", L.P.",
	} {
		name = strings.TrimSuffix(name, suffix)
	}
	return name
}

// captureSocialURL stores a social media URL in the appropriate field.
// First non-empty URL per platform wins (dedup across multiple RIAs).
func captureSocialURL(c *PEFirmCandidate, rawURL string) {
	u := strings.ToLower(strings.TrimSpace(rawURL))
	switch {
	case strings.Contains(u, "linkedin.com"):
		if c.LinkedInURL == "" {
			c.LinkedInURL = strings.TrimSpace(rawURL)
		}
	case strings.Contains(u, "twitter.com"), strings.Contains(u, "x.com"):
		if c.TwitterURL == "" {
			c.TwitterURL = strings.TrimSpace(rawURL)
		}
	case strings.Contains(u, "facebook.com"):
		if c.FacebookURL == "" {
			c.FacebookURL = strings.TrimSpace(rawURL)
		}
	case strings.Contains(u, "instagram.com"):
		if c.InstagramURL == "" {
			c.InstagramURL = strings.TrimSpace(rawURL)
		}
	case strings.Contains(u, "youtube.com"):
		if c.YouTubeURL == "" {
			c.YouTubeURL = strings.TrimSpace(rawURL)
		}
	case strings.Contains(u, "crunchbase.com"):
		if c.CrunchbaseURL == "" {
			c.CrunchbaseURL = strings.TrimSpace(rawURL)
		}
	}
}

// PersistCandidates upserts PE firm candidates into pe_firms and pe_firm_rias.
func PersistCandidates(ctx context.Context, store *Store, candidates []PEFirmCandidate) (int, error) {
	var persisted int
	for _, c := range candidates {
		firmID, err := store.UpsertFirm(ctx, c)
		if err != nil {
			zap.L().Warn("failed to upsert PE firm",
				zap.String("firm", c.OwnerName), zap.Error(err))
			continue
		}

		if err := store.LinkFirmRIAs(ctx, firmID, c); err != nil {
			zap.L().Warn("failed to link RIAs",
				zap.String("firm", c.OwnerName), zap.Error(err))
			continue
		}

		persisted++
	}
	return persisted, nil
}
