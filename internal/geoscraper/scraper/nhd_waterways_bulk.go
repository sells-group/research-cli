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

// nhdStates maps state abbreviations to NHD download identifiers.
// NHD uses state names with underscores for multi-word names.
var nhdStates = []string{
	"AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
	"GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
	"MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
	"NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
	"SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
}

// NHDWaterwaysBulk scrapes waterway data from NHD state-level GDB files,
// replacing the slow ArcGIS pagination approach.
type NHDWaterwaysBulk struct {
	baseURL string   // override for testing; empty uses default S3 endpoint
	states  []string // override for testing; empty uses all states
}

// Name implements GeoScraper.
func (s *NHDWaterwaysBulk) Name() string { return "nhd_waterways_bulk" }

// Table implements GeoScraper.
func (s *NHDWaterwaysBulk) Table() string { return "geo.infrastructure" }

// Category implements GeoScraper.
func (s *NHDWaterwaysBulk) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (s *NHDWaterwaysBulk) Cadence() geoscraper.Cadence { return geoscraper.Annual }

// ShouldRun implements GeoScraper.
func (s *NHDWaterwaysBulk) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.AnnualAfter(now, lastSync, time.January)
}

// Sync implements GeoScraper.
func (s *NHDWaterwaysBulk) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting nhd_waterways_bulk sync")

	states := s.states
	if len(states) == 0 {
		states = nhdStates
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
			return eris.Wrap(uErr, "nhd_waterways_bulk: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	for _, st := range states {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		rows, err := s.syncState(ctx, f, st, tempDir)
		if err != nil {
			log.Debug("NHD download failed, skipping state",
				zap.String("state", st), zap.Error(err))
			continue
		}

		batch = append(batch, rows...)
		if len(batch) >= hifldBatchSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("nhd_waterways_bulk sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}

func (s *NHDWaterwaysBulk) syncState(ctx context.Context, f fetcher.Fetcher, state, tempDir string) ([][]any, error) {
	url := s.buildURL(state)
	zipPath := filepath.Join(tempDir, fmt.Sprintf("nhd_%s.zip", state))

	if _, err := f.DownloadToFile(ctx, url, zipPath); err != nil {
		return nil, eris.Wrapf(err, "download NHD %s", state)
	}

	extractDir := filepath.Join(tempDir, "nhd_"+state)
	if err := os.MkdirAll(extractDir, 0o750); err != nil {
		return nil, eris.Wrap(err, "create extract dir")
	}

	if _, err := fetcher.ExtractZIP(zipPath, extractDir); err != nil {
		return nil, eris.Wrapf(err, "extract NHD %s", state)
	}

	gdbPath := findGDB(extractDir)
	if gdbPath == "" {
		return nil, eris.Errorf("no GDB found in NHD %s archive", state)
	}

	result, err := tiger.ParseGDB(ctx, gdbPath, "NHDFlowline", extractDir)
	if err != nil {
		return nil, eris.Wrapf(err, "parse NHD %s", state)
	}

	cols := make(map[string]int)
	for i, c := range result.Columns {
		cols[c] = i
	}

	var rows [][]any
	for _, raw := range result.Rows {
		name := strValFromMap(raw, cols, "gnis_name")
		ftype := strValFromMap(raw, cols, "ftype")
		gnisID := strValFromMap(raw, cols, "gnis_id")
		sourceID := gnisID
		if sourceID == "" {
			sourceID = fmt.Sprintf("nhd_%s_%d", state, len(rows))
		}

		props, _ := json.Marshal(map[string]any{
			"state": state,
			"ftype": ftype,
			"fcode": strValFromMap(raw, cols, "fcode"),
		})

		rows = append(rows, []any{
			name,
			"waterway",
			ftype,
			0.0,
			0.0, 0.0, // NHD flowlines are line features; lat/lon not meaningful
			"nhd",
			sourceID,
			props,
		})
	}
	return rows, nil
}

func (s *NHDWaterwaysBulk) buildURL(state string) string {
	if s.baseURL != "" {
		return s.baseURL + "/" + state + ".zip"
	}
	return fmt.Sprintf("https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHD/State/GDB/NHD_H_%s_State_GDB.zip", state)
}

// findGDB searches for a .gdb directory within the extracted files.
func findGDB(dir string) string {
	var found string
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, _ error) error {
		if info != nil && info.IsDir() && strings.HasSuffix(strings.ToLower(info.Name()), ".gdb") {
			found = path
			return filepath.SkipAll
		}
		return nil
	})
	return found
}

// strValFromMap extracts a string from a row using a column index map.
func strValFromMap(row []any, cols map[string]int, key string) string {
	idx, ok := cols[key]
	if !ok || idx >= len(row) {
		return ""
	}
	if row[idx] == nil {
		return ""
	}
	s, ok := row[idx].(string)
	if !ok {
		return fmt.Sprintf("%v", row[idx])
	}
	return s
}
