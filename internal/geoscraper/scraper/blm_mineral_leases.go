package scraper

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
	"github.com/sells-group/research-cli/internal/tiger"
)

var mineralLeaseProduct = tiger.Product{
	Name:     "BLM_MINERAL_LEASE",
	Columns:  []string{"LEASE_NUM", "LEASE_TYPE", "COMMODITY", "STATE", "GIS_ACRES"},
	GeomType: "MULTIPOLYGON",
}

var mineralLeasesCols = []string{
	"lease_number", "lease_type", "commodity", "state", "acres", "geom",
	"latitude", "longitude",
	"source", "source_id", "properties",
}

var mineralLeasesConflictKeys = []string{"source", "source_id"}

// BLMMineralLeases scrapes mineral lease data from BLM.
type BLMMineralLeases struct {
	baseURL string // override for testing; empty uses default BLM endpoint
}

// Name implements GeoScraper.
func (s *BLMMineralLeases) Name() string { return "blm_mineral_leases" }

// Table implements GeoScraper.
func (s *BLMMineralLeases) Table() string { return "geo.mineral_leases" }

// Category implements GeoScraper.
func (s *BLMMineralLeases) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (s *BLMMineralLeases) Cadence() geoscraper.Cadence { return geoscraper.Quarterly }

// ShouldRun implements GeoScraper.
func (s *BLMMineralLeases) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.QuarterlyAfterDelay(now, lastSync, 0)
}

// Sync implements GeoScraper.
func (s *BLMMineralLeases) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting blm_mineral_leases sync")

	url := s.baseURL
	if url == "" {
		url = "https://gbp-blm-egis.hub.arcgis.com/datasets/blm-natl-mining-claims/content?format=shp"
	}

	zipPath := filepath.Join(tempDir, "blm_mineral.zip")
	if _, err := f.DownloadToFile(ctx, url, zipPath); err != nil {
		return nil, eris.Wrap(err, "blm_mineral_leases: download")
	}

	extractDir := filepath.Join(tempDir, "blm_mineral")
	if err := os.MkdirAll(extractDir, 0o750); err != nil {
		return nil, eris.Wrap(err, "blm_mineral_leases: create extract dir")
	}

	if _, err := fetcher.ExtractZIP(zipPath, extractDir); err != nil {
		return nil, eris.Wrap(err, "blm_mineral_leases: extract zip")
	}

	shpPath, err := findShapefile(extractDir, "mining")
	if err != nil {
		// Try alternate name patterns.
		shpPath, err = findShapefile(extractDir, "mineral")
		if err != nil {
			return nil, eris.Wrap(err, "blm_mineral_leases: find shapefile")
		}
	}

	result, err := tiger.ParseShapefile(shpPath, mineralLeaseProduct)
	if err != nil {
		return nil, eris.Wrap(err, "blm_mineral_leases: parse shapefile")
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
			Columns:      mineralLeasesCols,
			ConflictKeys: mineralLeasesConflictKeys,
		}, batch)
		if uErr != nil {
			return eris.Wrap(uErr, "blm_mineral_leases: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	for i, raw := range result.Rows {
		leaseNum := strVal(raw, cols["lease_num"])
		leaseType := strVal(raw, cols["lease_type"])
		commodity := strVal(raw, cols["commodity"])
		state := strVal(raw, cols["state"])
		acres := floatVal(raw, cols["gis_acres"])

		geomIdx, hasGeom := cols["the_geom"]
		if !hasGeom || geomIdx >= len(raw) {
			continue
		}
		wkb, ok := raw[geomIdx].([]byte)
		if !ok || wkb == nil {
			continue
		}

		sourceID := leaseNum
		if sourceID == "" {
			sourceID = fmt.Sprintf("mineral_%d", i)
		}

		props, _ := json.Marshal(map[string]any{
			"lease_type": leaseType,
			"commodity":  commodity,
		})

		batch = append(batch, []any{
			leaseNum,
			leaseType,
			commodity,
			state,
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

	log.Info("blm_mineral_leases sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}
