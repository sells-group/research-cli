package scraper

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

// fdicAPIURL is the default FDIC BankFind Summary of Deposits API endpoint.
const fdicAPIURL = "https://banks.data.fdic.gov/api/sod"

// fdicAPIFields is the comma-separated list of fields requested from the API.
const fdicAPIFields = "UNINUMBR,NAMEFULL,SIMS_LATITUDE,SIMS_LONGITUDE,DEPSUMBR,BKCLASS,ADDRESBR,CITY,STALPBR,ZIPBR,BRSERTYP,BKMO,CBSA_DIV_NAMB,ASSET"

// fdicPageLimit is the maximum number of records per API page.
const fdicPageLimit = 10000

// fdicBranchExclude lists API fields stored in dedicated DB columns.
var fdicBranchExclude = map[string]bool{
	"UNINUMBR":       true,
	"NAMEFULL":       true,
	"BKCLASS":        true,
	"DEPSUMBR":       true,
	"SIMS_LATITUDE":  true,
	"SIMS_LONGITUDE": true,
}

// fdicResponse represents the top-level FDIC BankFind API response.
type fdicResponse struct {
	Meta struct {
		Total int `json:"total"`
	} `json:"meta"`
	Data []fdicRecord `json:"data"`
}

// fdicRecord represents a single record in the FDIC API response.
type fdicRecord struct {
	Data map[string]any `json:"data"`
}

// FDICBranches scrapes FDIC Summary of Deposits branch location data
// via the FDIC BankFind API.
type FDICBranches struct {
	baseURL   string // override for testing; empty uses default FDIC endpoint
	pageLimit int    // override for testing; 0 uses fdicPageLimit
}

// Name implements GeoScraper.
func (s *FDICBranches) Name() string { return "fdic_branches" }

// Table implements GeoScraper.
func (s *FDICBranches) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (s *FDICBranches) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (s *FDICBranches) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (s *FDICBranches) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.October)
}

// Sync implements GeoScraper.
func (s *FDICBranches) Sync(ctx context.Context, pool db.Pool, _ fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting fdic_branches sync")

	base := s.baseURL
	if base == "" {
		base = fdicAPIURL
	}

	// Try current year, then fall back to year-1 and year-2.
	now := time.Now()
	var total int
	var year int
	var probeErr error
	for offset := 0; offset <= 2; offset++ {
		year = now.Year() - offset
		total, probeErr = s.probeYear(ctx, base, year)
		if probeErr == nil && total > 0 {
			log.Info("found FDIC SOD data", zap.Int("year", year), zap.Int("total", total))
			break
		}
	}
	if probeErr != nil {
		return nil, eris.Wrap(probeErr, "fdic_branches: probe API")
	}
	if total == 0 {
		log.Warn("no FDIC SOD data found for any year")
		return &geoscraper.SyncResult{RowsSynced: 0}, nil
	}

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, uErr := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        s.Table(),
			Columns:      infraCols,
			ConflictKeys: infraConflictKeys,
		}, batch)
		if uErr != nil {
			return eris.Wrap(uErr, "fdic_branches: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	limit := s.pageLimit
	if limit == 0 {
		limit = fdicPageLimit
	}

	for pageOffset := 0; pageOffset < total; pageOffset += limit {
		records, err := s.fetchPage(ctx, base, year, limit, pageOffset)
		if err != nil {
			return nil, eris.Wrapf(err, "fdic_branches: fetch page offset=%d", pageOffset)
		}

		for _, rec := range records {
			lat := fdicFloat64(rec.Data, "SIMS_LATITUDE")
			lon := fdicFloat64(rec.Data, "SIMS_LONGITUDE")
			if lat == 0 && lon == 0 {
				continue
			}

			sourceID := fdicString(rec.Data, "UNINUMBR")
			if sourceID == "" {
				continue
			}

			batch = append(batch, []any{
				fdicString(rec.Data, "NAMEFULL"),
				"bank_branch",
				fdicString(rec.Data, "BKCLASS"),
				fdicFloat64(rec.Data, "DEPSUMBR"),
				lat,
				lon,
				"fdic",
				sourceID,
				fdicProperties(rec.Data, fdicBranchExclude),
			})

			if len(batch) >= hifldBatchSize {
				if err := flush(); err != nil {
					return nil, err
				}
			}
		}
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("fdic_branches sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}

// probeYear fetches a single record to check if data exists for the given year.
func (s *FDICBranches) probeYear(ctx context.Context, base string, year int) (int, error) {
	url := fmt.Sprintf("%s?filters=YEAR:%d&fields=UNINUMBR&limit=1&offset=0", base, year)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, eris.Wrap(err, "build probe request")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, eris.Wrap(err, "probe request")
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return 0, eris.Errorf("probe: status %d", resp.StatusCode)
	}

	var body fdicResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, eris.Wrap(err, "decode probe response")
	}

	return body.Meta.Total, nil
}

// fetchPage retrieves a single page of FDIC SOD records.
func (s *FDICBranches) fetchPage(ctx context.Context, base string, year, limit, offset int) ([]fdicRecord, error) {
	url := fmt.Sprintf("%s?filters=YEAR:%d&fields=%s&limit=%d&offset=%d", base, year, fdicAPIFields, limit, offset)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, eris.Wrap(err, "build page request")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, eris.Wrap(err, "page request")
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return nil, eris.Errorf("page: status %d", resp.StatusCode)
	}

	var body fdicResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, eris.Wrap(err, "decode page response")
	}

	return body.Data, nil
}

// fdicString extracts a string from FDIC API data, handling numeric and string values.
func fdicString(data map[string]any, key string) string {
	v, ok := data[key]
	if !ok || v == nil {
		return ""
	}
	switch n := v.(type) {
	case string:
		return n
	case float64:
		if n == float64(int64(n)) {
			return strconv.FormatInt(int64(n), 10)
		}
		return strconv.FormatFloat(n, 'f', -1, 64)
	case json.Number:
		return n.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}

// fdicFloat64 extracts a float64 from FDIC API data.
func fdicFloat64(data map[string]any, key string) float64 {
	v, ok := data[key]
	if !ok || v == nil {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return n
	case json.Number:
		f, _ := n.Float64()
		return f
	default:
		return 0
	}
}

// fdicProperties builds a JSONB byte slice from API data, excluding dedicated columns.
func fdicProperties(data map[string]any, exclude map[string]bool) []byte {
	props := make(map[string]any)
	for k, v := range data {
		if exclude[k] || v == nil {
			continue
		}
		props[k] = v
	}
	b, err := json.Marshal(props)
	if err != nil {
		return []byte("{}")
	}
	return b
}
