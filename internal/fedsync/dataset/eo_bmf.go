package dataset

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
)

const (
	eoBMFBatchSize = 10000
	eoBMFBaseURL   = "https://www.irs.gov/pub/irs-soi"
)

// eoBMFRegions lists the 4 regional CSV files published by IRS SOI.
var eoBMFRegions = []string{"eo1.csv", "eo2.csv", "eo3.csv", "eo4.csv"}

// eoBMFColumns defines the target DB columns in upsert order.
var eoBMFColumns = []string{
	"ein", "name", "ico", "street", "city", "state", "zip",
	"group_exemption", "subsection", "affiliation", "classification",
	"ruling", "deductibility", "foundation", "activity",
	"organization", "status", "tax_period",
	"asset_cd", "income_cd", "filing_req_cd", "pf_filing_req_cd",
	"acct_pd", "asset_amt", "income_amt", "revenue_amt",
	"ntee_cd", "sort_name",
}

// EOBMF implements the IRS Exempt Organizations Business Master File dataset.
// Data source: IRS SOI — 4 regional CSVs (~400MB total, ~1.94M rows).
// Contains EIN, name, address, subsection, assets, income, NTEE code for every
// tax-exempt organization recognized by the IRS.
type EOBMF struct{}

// Name implements Dataset.
func (d *EOBMF) Name() string { return "eo_bmf" }

// Table implements Dataset.
func (d *EOBMF) Table() string { return "fed_data.eo_bmf" }

// Phase implements Dataset.
func (d *EOBMF) Phase() Phase { return Phase1 }

// Cadence implements Dataset.
func (d *EOBMF) Cadence() Cadence { return Monthly }

// ShouldRun implements Dataset.
func (d *EOBMF) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return MonthlySchedule(now, lastSync)
}

// Sync downloads and loads all 4 regional EO BMF CSVs.
func (d *EOBMF) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "eo_bmf"))
	var totalRows atomic.Int64

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(2) // limit concurrent downloads

	for _, region := range eoBMFRegions {
		g.Go(func() error {
			rows, err := d.syncRegion(gctx, pool, f, tempDir, region, log)
			if err != nil {
				return err
			}
			totalRows.Add(rows)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return &SyncResult{
		RowsSynced: totalRows.Load(),
		Metadata:   map[string]any{"regions": len(eoBMFRegions)},
	}, nil
}

// syncRegion downloads and processes a single regional CSV.
func (d *EOBMF) syncRegion(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir, region string, log *zap.Logger) (int64, error) {
	url := fmt.Sprintf("%s/%s", eoBMFBaseURL, region)
	csvPath := filepath.Join(tempDir, region)

	log.Info("downloading EO BMF region", zap.String("region", region), zap.String("url", url))

	if _, err := f.DownloadToFile(ctx, url, csvPath); err != nil {
		return 0, eris.Wrapf(err, "eo_bmf: download %s", region)
	}
	defer os.Remove(csvPath) //nolint:errcheck

	file, err := os.Open(csvPath) // #nosec G304 -- path constructed from downloaded IRS data in trusted temp directory
	if err != nil {
		return 0, eris.Wrapf(err, "eo_bmf: open %s", region)
	}
	defer file.Close() //nolint:errcheck

	rows, err := d.parseCSV(ctx, pool, file)
	if err != nil {
		return 0, eris.Wrapf(err, "eo_bmf: parse %s", region)
	}

	log.Info("processed EO BMF region", zap.String("region", region), zap.Int64("rows", rows))
	return rows, nil
}

// parseCSV reads an EO BMF CSV and upserts rows into fed_data.eo_bmf.
func (d *EOBMF) parseCSV(ctx context.Context, pool db.Pool, r io.Reader) (int64, error) {
	reader := csv.NewReader(r)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		return 0, eris.Wrap(err, "eo_bmf: read CSV header")
	}

	colIdx := mapColumnsNormalized(header)

	// Verify EIN column exists.
	if _, ok := colIdx["ein"]; !ok {
		return 0, eris.New("eo_bmf: EIN column not found in header")
	}

	var batch [][]any
	var totalRows int64

	for {
		record, readErr := reader.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			continue // skip malformed rows
		}

		ein := strings.TrimSpace(getColN(record, colIdx, "ein"))
		if ein == "" {
			continue
		}

		row := d.mapRow(record, colIdx)
		batch = append(batch, row)

		if len(batch) >= eoBMFBatchSize {
			n, upsertErr := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table:        "fed_data.eo_bmf",
				Columns:      eoBMFColumns,
				ConflictKeys: []string{"ein"},
			}, batch)
			if upsertErr != nil {
				return totalRows, eris.Wrap(upsertErr, "eo_bmf: bulk upsert")
			}
			totalRows += n
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		n, upsertErr := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        "fed_data.eo_bmf",
			Columns:      eoBMFColumns,
			ConflictKeys: []string{"ein"},
		}, batch)
		if upsertErr != nil {
			return totalRows, eris.Wrap(upsertErr, "eo_bmf: bulk upsert final batch")
		}
		totalRows += n
	}

	return totalRows, nil
}

// mapRow converts a CSV record to a row of values matching eoBMFColumns.
func (d *EOBMF) mapRow(record []string, colIdx map[string]int) []any {
	get := func(name string) string { return trimQuotes(getColN(record, colIdx, name)) }
	getInt16 := func(name string) any {
		v := get(name)
		if v == "" {
			return nil
		}
		n, err := strconv.ParseInt(v, 10, 16)
		if err != nil {
			return nil
		}
		return int16(n)
	}
	getInt64 := func(name string) any {
		v := get(name)
		if v == "" {
			return nil
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil
		}
		return n
	}
	getText := func(name string) any {
		v := get(name)
		if v == "" {
			return nil
		}
		return sanitizeUTF8(v)
	}

	return []any{
		sanitizeUTF8(get("ein")),     // ein (PK, not null)
		sanitizeUTF8(get("name")),    // name (not null)
		getText("ico"),               // ico
		getText("street"),            // street
		getText("city"),              // city
		getText("state"),             // state
		getText("zip"),               // zip
		getText("group"),             // group_exemption
		getInt16("subsection"),       // subsection
		getInt16("affiliation"),      // affiliation
		getText("classification"),    // classification
		getText("ruling"),            // ruling
		getInt16("deductibility"),    // deductibility
		getInt16("foundation"),       // foundation
		getText("activity"),          // activity
		getInt16("organization"),     // organization
		getInt16("status"),           // status
		getText("tax_period"),        // tax_period
		getInt16("asset_cd"),         // asset_cd
		getInt16("income_cd"),        // income_cd
		getInt16("filing_req_cd"),    // filing_req_cd
		getInt16("pf_filing_req_cd"), // pf_filing_req_cd
		getInt16("acct_pd"),          // acct_pd
		getInt64("asset_amt"),        // asset_amt
		getInt64("income_amt"),       // income_amt
		getInt64("revenue_amt"),      // revenue_amt
		getText("ntee_cd"),           // ntee_cd
		getText("sort_name"),         // sort_name
	}
}
