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

// femaDownloadBaseURL is the FEMA MSC county-level NFHL download endpoint.
const femaDownloadBaseURL = "https://msc.fema.gov/portal/downloadProduct"

// nfhlProduct defines the shapefile columns for the S_Fld_Haz_Ar layer.
var nfhlProduct = tiger.Product{
	Name:     "NFHL_FLOOD",
	Columns:  []string{"fld_zone", "fld_ar_id", "sfha_tf", "zone_subty", "dfirm_id"},
	GeomType: "MULTIPOLYGON",
}

var floodBulkCols = []string{
	"zone_code", "flood_type", "geom",
	"source", "source_id", "properties",
}

var floodBulkConflictKeys = []string{"source", "source_id"}

// FEMAFloodBulk scrapes flood hazard areas from FEMA NFHL county-level
// shapefile downloads, replacing the slow ArcGIS pagination (100/page).
type FEMAFloodBulk struct {
	downloadBaseURL string   // override for testing; empty uses FEMA MSC
	countyFIPS      []string // override for testing; empty uses DB lookup
}

// Name implements GeoScraper.
func (f *FEMAFloodBulk) Name() string { return "fema_flood_bulk" }

// Table implements GeoScraper.
func (f *FEMAFloodBulk) Table() string { return "geo.flood_zones" }

// Category implements GeoScraper.
func (f *FEMAFloodBulk) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (f *FEMAFloodBulk) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (f *FEMAFloodBulk) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}

// Sync implements GeoScraper.
func (f *FEMAFloodBulk) Sync(ctx context.Context, pool db.Pool, ft fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", f.Name()))
	log.Info("starting FEMA NFHL bulk sync")

	counties := f.countyFIPS
	if len(counties) == 0 {
		var err error
		counties, err = allCountyFIPS(ctx, pool)
		if err != nil {
			return nil, eris.Wrap(err, "fema_flood_bulk: list county FIPS")
		}
	}

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, uErr := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        f.Table(),
			Columns:      floodBulkCols,
			ConflictKeys: floodBulkConflictKeys,
		}, batch)
		if uErr != nil {
			return eris.Wrap(uErr, "fema_flood_bulk: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	for _, fips := range counties {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		rows, err := f.syncCounty(ctx, ft, fips, tempDir)
		if err != nil {
			log.Debug("NFHL download failed, skipping county",
				zap.String("fips", fips), zap.Error(err))
			continue
		}

		batch = append(batch, rows...)
		if len(batch) >= femaBatchSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("FEMA NFHL bulk sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}

func (f *FEMAFloodBulk) syncCounty(ctx context.Context, ft fetcher.Fetcher, fips, tempDir string) ([][]any, error) {
	url := f.buildURL(fips)
	zipPath := filepath.Join(tempDir, fmt.Sprintf("nfhl_%s.zip", fips))

	if _, err := ft.DownloadToFile(ctx, url, zipPath); err != nil {
		return nil, eris.Wrapf(err, "download NFHL %s", fips)
	}

	extractDir := filepath.Join(tempDir, "nfhl_"+fips)
	if err := os.MkdirAll(extractDir, 0o750); err != nil {
		return nil, eris.Wrap(err, "create extract dir")
	}

	if _, err := fetcher.ExtractZIP(zipPath, extractDir); err != nil {
		return nil, eris.Wrapf(err, "extract NFHL %s", fips)
	}

	shpPath, err := findNFHLShapefile(extractDir)
	if err != nil {
		return nil, eris.Wrapf(err, "find S_Fld_Haz_Ar in %s", fips)
	}

	result, err := tiger.ParseShapefile(shpPath, nfhlProduct)
	if err != nil {
		return nil, eris.Wrapf(err, "parse NFHL %s", fips)
	}
	result = filterToProductColumns(result, nfhlProduct)

	var rows [][]any
	for _, raw := range result.Rows {
		row, ok := newNFHLRow(raw)
		if !ok {
			continue
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (f *FEMAFloodBulk) buildURL(fips string) string {
	if f.downloadBaseURL != "" {
		return f.downloadBaseURL + "/" + fips + ".zip"
	}
	return fmt.Sprintf("%s?productID=NFHL_%sC", femaDownloadBaseURL, fips)
}

// findNFHLShapefile finds the S_Fld_Haz_Ar shapefile within an extracted NFHL ZIP.
// NFHL ZIPs contain multiple shapefiles; we want the flood hazard areas layer.
func findNFHLShapefile(dir string) (string, error) {
	// Walk directory tree (NFHL ZIPs may have nested directories).
	var found string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return nil //nolint:nilerr // skip unreadable entries
		}
		name := strings.ToLower(info.Name())
		if !info.IsDir() && strings.HasSuffix(name, ".shp") && strings.Contains(name, "s_fld_haz_ar") {
			found = path
			return filepath.SkipAll
		}
		return nil
	})
	if err != nil {
		return "", eris.Wrap(err, "walk NFHL extract dir")
	}
	if found == "" {
		return "", eris.New("no S_Fld_Haz_Ar.shp found in NFHL archive")
	}
	return found, nil
}

func newNFHLRow(raw []any) ([]any, bool) {
	// raw: fld_zone, fld_ar_id, sfha_tf, zone_subty, dfirm_id, wkb
	if len(raw) < 6 {
		return nil, false
	}
	wkb, ok := raw[5].([]byte)
	if !ok || wkb == nil {
		return nil, false
	}

	zoneCode := strVal(raw, 0)
	floodArID := strVal(raw, 1)
	sfhaTF := strVal(raw, 2)
	zoneSubty := strVal(raw, 3)
	dfirmID := strVal(raw, 4)
	floodType := femaFloodType(zoneCode, sfhaTF, zoneSubty)

	sourceID := floodArID
	if sourceID == "" {
		sourceID = fmt.Sprintf("%s_%s", dfirmID, zoneCode)
	}

	props, _ := json.Marshal(map[string]any{
		"sfha_tf":    sfhaTF,
		"zone_subty": zoneSubty,
		"dfirm_id":   dfirmID,
	})

	return []any{
		zoneCode,
		floodType,
		wkb, // geom (WKB → PostGIS auto-casts to geometry)
		femaSource,
		sourceID,
		props,
	}, true
}
