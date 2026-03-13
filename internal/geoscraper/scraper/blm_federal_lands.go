package scraper

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
	"github.com/sells-group/research-cli/internal/tiger"
)

var smaProduct = tiger.Product{
	Name:     "BLM_SMA",
	Columns:  []string{"ADMIN_AGEN", "ADMIN_UNIT", "GIS_ACRES", "STATE_FIPS"},
	GeomType: "MULTIPOLYGON",
}

var federalLandsCols = []string{
	"name", "admin_agency", "state", "acres", "geom",
	"latitude", "longitude",
	"source", "source_id", "properties",
}

var federalLandsConflictKeys = []string{"source", "source_id"}

// BLMFederalLands scrapes Surface Management Agency data from BLM.
type BLMFederalLands struct {
	baseURL string // override for testing; empty uses default BLM endpoint
}

// Name implements GeoScraper.
func (s *BLMFederalLands) Name() string { return "blm_federal_lands" }

// Table implements GeoScraper.
func (s *BLMFederalLands) Table() string { return "geo.federal_lands" }

// Category implements GeoScraper.
func (s *BLMFederalLands) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (s *BLMFederalLands) Cadence() geoscraper.Cadence { return geoscraper.Quarterly }

// ShouldRun implements GeoScraper.
func (s *BLMFederalLands) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.QuarterlyAfterDelay(now, lastSync, 0)
}

// Sync implements GeoScraper.
func (s *BLMFederalLands) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting blm_federal_lands sync")

	url := s.baseURL
	if url == "" {
		url = "https://gbp-blm-egis.hub.arcgis.com/datasets/blm-natl-sma-surface-mgmt-agency/content?format=shp"
	}

	zipPath := filepath.Join(tempDir, "blm_sma.zip")
	if _, err := f.DownloadToFile(ctx, url, zipPath); err != nil {
		return nil, eris.Wrap(err, "blm_federal_lands: download")
	}

	extractDir := filepath.Join(tempDir, "blm_sma")
	if err := os.MkdirAll(extractDir, 0o750); err != nil {
		return nil, eris.Wrap(err, "blm_federal_lands: create extract dir")
	}

	if _, err := fetcher.ExtractZIP(zipPath, extractDir); err != nil {
		return nil, eris.Wrap(err, "blm_federal_lands: extract zip")
	}

	shpPath, err := findShapefile(extractDir, "sma")
	if err != nil {
		return nil, eris.Wrap(err, "blm_federal_lands: find shapefile")
	}

	result, err := tiger.ParseShapefile(shpPath, smaProduct)
	if err != nil {
		return nil, eris.Wrap(err, "blm_federal_lands: parse shapefile")
	}

	cols := make(map[string]int)
	for i, c := range result.Columns {
		cols[c] = i
	}

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, uErr := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        s.Table(),
			Columns:      federalLandsCols,
			ConflictKeys: federalLandsConflictKeys,
		}, batch)
		if uErr != nil {
			return eris.Wrap(uErr, "blm_federal_lands: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	for i, raw := range result.Rows {
		adminAgency := strVal(raw, cols["admin_agen"])
		adminUnit := strVal(raw, cols["admin_unit"])
		acres := floatVal(raw, cols["gis_acres"])
		stateFIPS := strVal(raw, cols["state_fips"])

		geomIdx, hasGeom := cols["the_geom"]
		if !hasGeom || geomIdx >= len(raw) {
			continue
		}
		wkb, ok := raw[geomIdx].([]byte)
		if !ok || wkb == nil {
			continue
		}

		sourceID := fmt.Sprintf("sma_%d", i)
		if adminUnit != "" {
			sourceID = "sma_" + adminUnit
		}

		props, _ := json.Marshal(map[string]any{
			"admin_unit": adminUnit,
			"state_fips": stateFIPS,
		})

		batch = append(batch, []any{
			adminUnit,
			adminAgency,
			stateFIPS,
			acres,
			wkb,
			0.0, 0.0,
			"blm",
			sourceID,
			props,
		})

		if len(batch) >= hifldBatchSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("blm_federal_lands sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}

// findShapefile searches for a .shp file containing the given name fragment.
func findShapefile(dir, fragment string) (string, error) {
	var found string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return nil //nolint:nilerr
		}
		name := strings.ToLower(info.Name())
		if !info.IsDir() && strings.HasSuffix(name, ".shp") && strings.Contains(name, fragment) {
			found = path
			return filepath.SkipAll
		}
		return nil
	})
	if err != nil {
		return "", eris.Wrap(err, "walk extract dir")
	}
	if found == "" {
		return "", eris.Errorf("no .shp file containing %q found", fragment)
	}
	return found, nil
}

// floatVal extracts a float64 from a row element. Handles string and float64 types.
func floatVal(row []any, idx int) float64 {
	if idx < 0 || idx >= len(row) || row[idx] == nil {
		return 0
	}
	switch v := row[idx].(type) {
	case float64:
		return v
	case string:
		f := csvFloat64([]string{v}, 0)
		return f
	default:
		return 0
	}
}
