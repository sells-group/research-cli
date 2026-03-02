package scraper

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
	"github.com/sells-group/research-cli/internal/geoscraper/arcgis"
)

// CensusDemographics scrapes ACS 5-year tract-level demographics and joins
// polygon geometries from the Census TIGERweb ArcGIS service.
type CensusDemographics struct {
	apiKey       string // Census API key, injected from config
	acsBaseURL   string // override for testing; empty uses default
	tigerBaseURL string // override for testing; empty uses default
	year         int    // ACS vintage; 0 defaults to censusACSYear
}

// Name implements GeoScraper.
func (c *CensusDemographics) Name() string { return "census_demographics" }

// Table implements GeoScraper.
func (c *CensusDemographics) Table() string { return "geo.demographics" }

// Category implements GeoScraper.
func (c *CensusDemographics) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (c *CensusDemographics) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (c *CensusDemographics) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}

// acsYear returns the configured ACS vintage year, defaulting to censusACSYear.
func (c *CensusDemographics) acsYear() int {
	if c.year > 0 {
		return c.year
	}
	return censusACSYear
}

// acsURL returns the ACS base URL, falling back to the default.
func (c *CensusDemographics) acsURL() string {
	if c.acsBaseURL != "" {
		return c.acsBaseURL
	}
	return fmt.Sprintf(defaultACSBaseURL, c.acsYear())
}

// tigerURL returns the TIGERweb base URL, falling back to the default.
func (c *CensusDemographics) tigerURL() string {
	if c.tigerBaseURL != "" {
		return c.tigerBaseURL
	}
	return defaultTigerBaseURL
}

// Sync implements GeoScraper.
func (c *CensusDemographics) Sync(ctx context.Context, pool db.Pool, ft fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	if c.apiKey == "" {
		return nil, eris.New("census_demographics: Census API key is required")
	}

	log := zap.L().With(zap.String("scraper", c.Name()))
	log.Info("starting census demographics sync", zap.Int("year", c.acsYear()))

	var totalRows int64
	var batch [][]any
	year := c.acsYear()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := demoUpsert(ctx, pool, c.Table(), batch)
		if err != nil {
			return eris.Wrap(err, "census_demographics: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	for _, fips := range stateFIPS {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		log.Debug("fetching ACS data", zap.String("fips", fips))

		// Step 1: Fetch ACS tabular data for this state.
		acsURL := buildACSURL(c.acsURL(), c.apiKey, fips)
		demoMap, err := c.fetchACS(ctx, ft, acsURL)
		if err != nil {
			log.Warn("ACS fetch failed, skipping state", zap.String("fips", fips), zap.Error(err))
			continue
		}
		if len(demoMap) == 0 {
			continue
		}

		// Step 2: Query TIGERweb for tract polygons in this state.
		log.Debug("fetching TIGERweb tracts", zap.String("fips", fips))

		err = arcgis.QueryAll(ctx, ft, arcgis.QueryConfig{
			BaseURL: c.tigerURL(),
			Where:   fmt.Sprintf("STATE='%s'", fips),
			OutSR:   4326,
		}, func(features []arcgis.Feature) error {
			for _, feat := range features {
				row, ok := newDemoRow(feat, demoMap, year)
				if !ok {
					continue
				}
				batch = append(batch, row)

				if len(batch) >= censusBatchSize {
					if err := flush(); err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			return nil, eris.Wrapf(err, "census_demographics: TIGERweb query state %s", fips)
		}
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("census demographics sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}

// fetchACS downloads and parses the Census ACS API response for a single state.
func (c *CensusDemographics) fetchACS(ctx context.Context, ft fetcher.Fetcher, url string) (map[string]*demoRow, error) {
	body, err := ft.Download(ctx, url)
	if err != nil {
		return nil, eris.Wrap(err, "census: download ACS")
	}
	defer body.Close() //nolint:errcheck

	data, err := io.ReadAll(body)
	if err != nil {
		return nil, eris.Wrap(err, "census: read ACS response")
	}

	return parseACSResponse(data)
}

// newDemoRow builds a row for the demographics temp table by joining a TIGERweb
// feature with ACS data. Returns nil, false if the feature has no geometry,
// no GEOID, or no matching ACS row.
func newDemoRow(feat arcgis.Feature, demoMap map[string]*demoRow, year int) ([]any, bool) {
	if feat.Geometry == nil || len(feat.Geometry.Rings) == 0 {
		return nil, false
	}

	geoid := hifldString(feat.Attributes, "GEOID")
	if geoid == "" {
		return nil, false
	}

	dr, ok := demoMap[geoid]
	if !ok {
		return nil, false
	}

	geomWKT := feat.Geometry.RingsToEWKT()
	sourceID := geoid

	return []any{
		geoid,
		censusGeoLevel,
		year,
		dr.population,
		dr.income,
		dr.age,
		dr.housing,
		geomWKT,
		censusSource,
		sourceID,
		[]byte("{}"),
	}, true
}
