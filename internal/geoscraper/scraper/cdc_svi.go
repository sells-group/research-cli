package scraper

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
	"github.com/sells-group/research-cli/internal/geoscraper/arcgis"
)

// cdcSviBaseURL is the ArcGIS FeatureServer endpoint for CDC SVI 2022 tract-level data (Layer 2).
const cdcSviBaseURL = "https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/CDC_SVI_2022_%28Archive%29/FeatureServer/2/query"

// cdcSviOutFields lists the ArcGIS attribute fields to request.
var cdcSviOutFields = []string{
	"FIPS", "RPL_THEMES", "RPL_THEME1", "RPL_THEME2", "RPL_THEME3", "RPL_THEME4",
	"E_TOTPOP", "E_POV150", "E_UNEMP", "E_HBURD", "E_NOHSDP", "E_UNINSUR",
	"EP_POV150", "EP_UNEMP", "EP_HBURD", "EP_NOHSDP", "EP_UNINSUR",
}

// sviExclude lists attribute keys stored in dedicated DB columns (excluded from properties JSONB).
var sviExclude = map[string]bool{
	"FIPS":       true,
	"RPL_THEMES": true,
	"RPL_THEME1": true,
	"RPL_THEME2": true,
	"RPL_THEME3": true,
	"RPL_THEME4": true,
	"E_TOTPOP":   true,
	"E_POV150":   true,
	"E_UNEMP":    true,
	"E_HBURD":    true,
	"E_NOHSDP":   true,
	"E_UNINSUR":  true,
	"EP_POV150":  true,
	"EP_UNEMP":   true,
	"EP_HBURD":   true,
	"EP_NOHSDP":  true,
	"EP_UNINSUR": true,
}

var sviCols = []string{
	"fips", "state_fips", "county_fips", "year",
	"rpl_themes", "rpl_theme1", "rpl_theme2", "rpl_theme3", "rpl_theme4",
	"e_totpop", "e_pov150", "e_unemp", "e_hburd", "e_nohsdp", "e_uninsur",
	"ep_pov150", "ep_unemp", "ep_hburd", "ep_nohsdp", "ep_uninsur",
	"source", "source_id", "properties",
}

var sviConflictKeys = []string{"fips", "year"}

// CDCSvi scrapes CDC Social Vulnerability Index data by census tract from the
// ArcGIS FeatureServer API.
type CDCSvi struct {
	baseURL string // override for testing; empty uses default ArcGIS endpoint
	year    int    // override for testing; 0 uses auto-detection
}

// Name implements GeoScraper.
func (s *CDCSvi) Name() string { return "cdc_svi" }

// Table implements GeoScraper.
func (s *CDCSvi) Table() string { return "geo.svi" }

// Category implements GeoScraper.
func (s *CDCSvi) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (s *CDCSvi) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (s *CDCSvi) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.March)
}

// sviDataYear picks the most recent even year for SVI (biennial: 2018, 2020, 2022, 2024).
func sviDataYear(now time.Time) int {
	y := now.Year() - 2
	if y%2 != 0 {
		y--
	}
	return y
}

// Sync implements GeoScraper.
func (s *CDCSvi) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting cdc_svi sync")

	year := s.year
	if year == 0 {
		year = sviDataYear(time.Now())
	}

	baseURL := s.baseURL
	if baseURL == "" {
		baseURL = cdcSviBaseURL
	}

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, uErr := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        s.Table(),
			Columns:      sviCols,
			ConflictKeys: sviConflictKeys,
		}, batch)
		if uErr != nil {
			return eris.Wrap(uErr, "cdc_svi: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	err := arcgis.QueryAll(ctx, f, arcgis.QueryConfig{
		BaseURL:   baseURL,
		OutFields: cdcSviOutFields,
		PageSize:  1000,
	}, func(features []arcgis.Feature) error {
		for _, feat := range features {
			fips := sviAttrString(feat.Attributes, "FIPS")
			if fips == "" {
				continue
			}

			// CDC uses -999 as a null sentinel for rank values.
			rplThemes := hifldFloat64(feat.Attributes, "RPL_THEMES")
			if rplThemes == -999 {
				continue
			}

			stateFIPS := ""
			countyFIPS := ""
			if len(fips) >= 5 {
				stateFIPS = fips[:2]
				countyFIPS = fips[2:5]
			}

			batch = append(batch, []any{
				fips,
				stateFIPS,
				countyFIPS,
				year,
				rplThemes,
				hifldFloat64(feat.Attributes, "RPL_THEME1"),
				hifldFloat64(feat.Attributes, "RPL_THEME2"),
				hifldFloat64(feat.Attributes, "RPL_THEME3"),
				hifldFloat64(feat.Attributes, "RPL_THEME4"),
				sviAttrInt(feat.Attributes, "E_TOTPOP"),
				sviAttrInt(feat.Attributes, "E_POV150"),
				sviAttrInt(feat.Attributes, "E_UNEMP"),
				sviAttrInt(feat.Attributes, "E_HBURD"),
				sviAttrInt(feat.Attributes, "E_NOHSDP"),
				sviAttrInt(feat.Attributes, "E_UNINSUR"),
				hifldFloat64(feat.Attributes, "EP_POV150"),
				hifldFloat64(feat.Attributes, "EP_UNEMP"),
				hifldFloat64(feat.Attributes, "EP_HBURD"),
				hifldFloat64(feat.Attributes, "EP_NOHSDP"),
				hifldFloat64(feat.Attributes, "EP_UNINSUR"),
				"cdc",
				fips,
				hifldProperties(feat.Attributes, sviExclude),
			})

			if len(batch) >= hifldBatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, eris.Wrap(err, "cdc_svi: query arcgis")
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("cdc_svi sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}

// sviAttrString extracts a string FIPS code from ArcGIS attributes. ArcGIS may
// return FIPS as a float64 (numeric field) or a string, so this handles both.
func sviAttrString(attrs map[string]any, key string) string {
	v, ok := attrs[key]
	if !ok || v == nil {
		return ""
	}
	switch n := v.(type) {
	case string:
		return n
	case float64:
		// FIPS codes are integers; format without decimal.
		return strconv.FormatInt(int64(n), 10)
	case json.Number:
		return n.String()
	default:
		return ""
	}
}

// sviAttrInt extracts an integer from ArcGIS attributes, handling float64 and
// json.Number values. Returns 0 if the key is missing or nil.
func sviAttrInt(attrs map[string]any, key string) int {
	v, ok := attrs[key]
	if !ok || v == nil {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	default:
		return 0
	}
}
