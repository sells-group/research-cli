package dataset

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	pppDatasetID = "8aa276e2-6cab-4f86-aca4-a7dde42adf24"
	pppBatchSize = 10000
)

// PPP implements the SBA Paycheck Protection Program loan dataset.
// PPP is a one-time load — the program ended in 2021 and data is static.
type PPP struct{}

func (d *PPP) Name() string     { return "ppp" }
func (d *PPP) Table() string    { return "fed_data.ppp_loans" }
func (d *PPP) Phase() Phase     { return Phase1 }
func (d *PPP) Cadence() Cadence { return Annual }

func (d *PPP) ShouldRun(_ time.Time, lastSync *time.Time) bool {
	return lastSync == nil // one-time load
}

func (d *PPP) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "ppp"))

	resources, err := d.discoverResources(ctx, f)
	if err != nil {
		return nil, err
	}

	log.Info("discovered PPP CSV resources", zap.Int("count", len(resources)))

	var totalRows atomic.Int64

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(3)

	for i, res := range resources {
		res := res
		idx := i
		g.Go(func() error {
			csvPath := filepath.Join(tempDir, fmt.Sprintf("ppp_%d.csv", idx))
			log.Info("downloading PPP file", zap.String("name", res.Name), zap.String("url", res.URL))

			if _, err := f.DownloadToFile(gctx, res.URL, csvPath); err != nil {
				return eris.Wrapf(err, "ppp: download %s", res.Name)
			}

			rows, err := d.processCSV(gctx, pool, csvPath)
			if err != nil {
				return eris.Wrapf(err, "ppp: process %s", res.Name)
			}

			totalRows.Add(rows)
			log.Info("processed PPP file", zap.String("name", res.Name), zap.Int64("rows", rows))

			_ = os.Remove(csvPath)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return &SyncResult{
		RowsSynced: totalRows.Load(),
		Metadata:   map[string]any{"files": len(resources)},
	}, nil
}

// pppResource describes one downloadable CSV from SBA FOIA.
type pppResource struct {
	Name string
	URL  string
}

// discoverResources fetches the SBA CKAN dataset metadata and returns CSV download URLs.
func (d *PPP) discoverResources(ctx context.Context, f fetcher.Fetcher) ([]pppResource, error) {
	apiURL := fmt.Sprintf("https://data.sba.gov/api/3/action/package_show?id=%s", pppDatasetID)
	body, err := f.Download(ctx, apiURL)
	if err != nil {
		return nil, eris.Wrap(err, "ppp: fetch CKAN metadata")
	}
	defer body.Close()

	var resp struct {
		Result struct {
			Resources []struct {
				Name   string `json:"name"`
				URL    string `json:"url"`
				Format string `json:"format"`
			} `json:"resources"`
		} `json:"result"`
	}
	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		return nil, eris.Wrap(err, "ppp: decode CKAN metadata")
	}

	var resources []pppResource
	for _, r := range resp.Result.Resources {
		if strings.EqualFold(r.Format, "csv") {
			resources = append(resources, pppResource{Name: r.Name, URL: r.URL})
		}
	}

	if len(resources) == 0 {
		return nil, eris.New("ppp: no CSV resources found in CKAN metadata")
	}

	return resources, nil
}

func (d *PPP) processCSV(ctx context.Context, pool db.Pool, csvPath string) (int64, error) {
	file, err := os.Open(csvPath)
	if err != nil {
		return 0, eris.Wrap(err, "ppp: open CSV")
	}
	defer file.Close()

	return d.parseCSV(ctx, pool, file)
}

func (d *PPP) parseCSV(ctx context.Context, pool db.Pool, r io.Reader) (int64, error) {
	reader := csv.NewReader(r)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		return 0, eris.Wrap(err, "ppp: read CSV header")
	}

	colIdx := mapColumns(header)

	columns := []string{
		"loannumber", "borrowername", "borroweraddress", "borrowercity",
		"borrowerstate", "borrowerzip", "currentapprovalamount", "forgivenessamount",
		"jobsreported", "dateapproved", "loanstatus", "businesstype",
		"naicscode", "businessagedescription",
	}
	conflictKeys := []string{"loannumber"}

	var batch [][]any
	var totalRows int64

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // skip malformed rows
		}

		loanNum := parseInt64Or(trimQuotes(getCol(record, colIdx, "loannumber")), 0)
		if loanNum == 0 {
			continue // skip rows without valid loan number
		}

		// Parse date (SBA uses MM/DD/YYYY or YYYY-MM-DD).
		dateStr := trimQuotes(getCol(record, colIdx, "dateapproved"))
		var dateApproved *time.Time
		if dateStr != "" {
			for _, layout := range []string{"01/02/2006", "2006-01-02"} {
				if t, err := time.Parse(layout, dateStr); err == nil {
					dateApproved = &t
					break
				}
			}
		}

		row := []any{
			loanNum,
			sanitizeUTF8(trimQuotes(getCol(record, colIdx, "borrowername"))),
			sanitizeUTF8(trimQuotes(getCol(record, colIdx, "borroweraddress"))),
			sanitizeUTF8(trimQuotes(getCol(record, colIdx, "borrowercity"))),
			trimQuotes(getCol(record, colIdx, "borrowerstate")),
			trimQuotes(getCol(record, colIdx, "borrowerzip")),
			parseFloat64Or(trimQuotes(getCol(record, colIdx, "currentapprovalamount")), 0),
			parseFloat64Or(trimQuotes(getCol(record, colIdx, "forgivenessamount")), 0),
			parseIntOr(trimQuotes(getCol(record, colIdx, "jobsreported")), 0),
			dateApproved,
			sanitizeUTF8(trimQuotes(getCol(record, colIdx, "loanstatus"))),
			sanitizeUTF8(trimQuotes(getCol(record, colIdx, "businesstype"))),
			trimQuotes(getCol(record, colIdx, "naicscode")),
			sanitizeUTF8(trimQuotes(getCol(record, colIdx, "businessagedescription"))),
		}

		batch = append(batch, row)

		if len(batch) >= pppBatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table:        "fed_data.ppp_loans",
				Columns:      columns,
				ConflictKeys: conflictKeys,
			}, batch)
			if err != nil {
				return totalRows, eris.Wrap(err, "ppp: bulk upsert")
			}
			totalRows += n
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        "fed_data.ppp_loans",
			Columns:      columns,
			ConflictKeys: conflictKeys,
		}, batch)
		if err != nil {
			return totalRows, eris.Wrap(err, "ppp: bulk upsert final batch")
		}
		totalRows += n
	}

	return totalRows, nil
}
