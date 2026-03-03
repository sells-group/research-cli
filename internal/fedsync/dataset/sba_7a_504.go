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
	sbaDatasetID = "0ff8e8e9-b967-4f4e-987c-6ac78c575087"
	sbaBatchSize = 10000
)

// SBA7a504 implements the SBA 7(a) and 504 loan dataset.
// Data is published quarterly via FOIA bulk download from data.sba.gov.
type SBA7a504 struct{}

// Name implements Dataset.
func (d *SBA7a504) Name() string { return "sba_7a_504" }

// Table implements Dataset.
func (d *SBA7a504) Table() string { return "fed_data.sba_loans" }

// Phase implements Dataset.
func (d *SBA7a504) Phase() Phase { return Phase1 }

// Cadence implements Dataset.
func (d *SBA7a504) Cadence() Cadence { return Quarterly }

// ShouldRun implements Dataset.
func (d *SBA7a504) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return QuarterlyWithLag(now, lastSync, 1) // available ~1 month after quarter end
}

// Sync fetches and loads SBA 7(a) and 504 loan data.
func (d *SBA7a504) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "sba_7a_504"))

	resources, err := d.discoverResources(ctx, f)
	if err != nil {
		return nil, err
	}

	log.Info("discovered SBA CSV resources", zap.Int("count", len(resources)))

	var totalRows atomic.Int64
	var count7a, count504 atomic.Int64

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(1) // sequential to avoid deadlocks on overlapping (program, l2locid) keys

	for i, res := range resources {
		idx := i
		g.Go(func() error {
			csvPath := filepath.Join(tempDir, fmt.Sprintf("sba_%d.csv", idx))
			log.Info("downloading SBA file", zap.String("name", res.Name), zap.String("url", res.URL))

			if _, err := f.DownloadToFile(gctx, res.URL, csvPath); err != nil {
				return eris.Wrapf(err, "sba: download %s", res.Name)
			}

			rows, n7a, n504, err := d.processCSV(gctx, pool, csvPath, res.Program)
			if err != nil {
				return eris.Wrapf(err, "sba: process %s", res.Name)
			}

			totalRows.Add(rows)
			count7a.Add(n7a)
			count504.Add(n504)
			log.Info("processed SBA file", zap.String("name", res.Name), zap.Int64("rows", rows))

			_ = os.Remove(csvPath)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return &SyncResult{
		RowsSynced: totalRows.Load(),
		Metadata: map[string]any{
			"files":    len(resources),
			"rows_7a":  count7a.Load(),
			"rows_504": count504.Load(),
		},
	}, nil
}

// sbaResource describes one downloadable CSV from SBA FOIA.
type sbaResource struct {
	Name    string
	URL     string
	Program string // "7A" or "504" detected from resource name
}

// discoverResources fetches the SBA CKAN dataset metadata and returns CSV download URLs.
func (d *SBA7a504) discoverResources(ctx context.Context, f fetcher.Fetcher) ([]sbaResource, error) {
	apiURL := fmt.Sprintf("https://data.sba.gov/api/3/action/package_show?id=%s", sbaDatasetID)
	body, err := f.Download(ctx, apiURL)
	if err != nil {
		return nil, eris.Wrap(err, "sba: fetch CKAN metadata")
	}
	defer body.Close() //nolint:errcheck

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
		return nil, eris.Wrap(err, "sba: decode CKAN metadata")
	}

	var resources []sbaResource
	for _, r := range resp.Result.Resources {
		if !strings.EqualFold(r.Format, "csv") {
			continue
		}
		prog := detectProgram(r.Name)
		if prog == "" {
			continue // skip non-loan CSVs (e.g., data dictionary)
		}
		resources = append(resources, sbaResource{Name: r.Name, URL: r.URL, Program: prog})
	}

	if len(resources) == 0 {
		return nil, eris.New("sba: no CSV resources found in CKAN metadata")
	}

	return resources, nil
}

// detectProgram determines the loan program from a CKAN resource name.
// Returns "7A", "504", or "" if the file is not a loan CSV.
func detectProgram(name string) string {
	upper := strings.ToUpper(name)
	if strings.Contains(upper, "7(A)") || strings.Contains(upper, "7A") {
		return "7A"
	}
	if strings.Contains(upper, "504") {
		return "504"
	}
	return ""
}

func (d *SBA7a504) processCSV(ctx context.Context, pool db.Pool, csvPath, defaultProgram string) (int64, int64, int64, error) {
	file, err := os.Open(csvPath) // #nosec G304 -- path constructed from downloaded SBA data in trusted temp directory
	if err != nil {
		return 0, 0, 0, eris.Wrap(err, "sba: open CSV")
	}
	defer file.Close() //nolint:errcheck

	return d.parseCSV(ctx, pool, file, defaultProgram)
}

func (d *SBA7a504) parseCSV(ctx context.Context, pool db.Pool, r io.Reader, defaultProgram string) (int64, int64, int64, error) {
	reader := csv.NewReader(r)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		return 0, 0, 0, eris.Wrap(err, "sba: read CSV header")
	}

	colIdx := mapColumns(header)

	columns := sbaColumns()
	conflictKeys := []string{"program", "l2locid"}

	var batch [][]any
	var totalRows, rows7a, rows504 int64

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // skip malformed rows
		}

		l2locid := parseInt64Or(trimQuotes(getCol(record, colIdx, "l2locid")), 0)
		if l2locid == 0 {
			continue // skip rows without valid loan ID
		}

		// Detect program from row or fall back to filename-based detection.
		program := strings.ToUpper(strings.TrimSpace(trimQuotes(getCol(record, colIdx, "program"))))
		if program == "" {
			program = defaultProgram
		}
		// Normalize "7A()" variants to "7A".
		if strings.Contains(program, "7") {
			program = "7A"
		} else if strings.Contains(program, "504") {
			program = "504"
		}
		if program != "7A" && program != "504" {
			continue
		}

		approvalDate := parseSBADate(getCol(record, colIdx, "approvaldate"))
		firstDisbDate := parseSBADate(getCol(record, colIdx, "firstdisbursementdate"))
		pifDate := parseSBADate(getCol(record, colIdx, "paidinfulldate"))
		chgoffDate := parseSBADate(getCol(record, colIdx, "chargeoffdate"))

		row := []any{
			program,
			l2locid,
			sanitizeUTF8(trimQuotes(getCol(record, colIdx, "borrname"))),
			nilIfEmpty(sanitizeUTF8(trimQuotes(getCol(record, colIdx, "borrstreet")))),
			nilIfEmpty(sanitizeUTF8(trimQuotes(getCol(record, colIdx, "borrcity")))),
			nilIfEmpty(trimQuotes(getCol(record, colIdx, "borrstate"))),
			nilIfEmpty(trimQuotes(getCol(record, colIdx, "borrzip"))),
			parseNullFloat(trimQuotes(getCol(record, colIdx, "grossapproval"))),
			parseNullFloat(trimQuotes(getCol(record, colIdx, "sbaguaranteedapproval"))),
			approvalDate,
			parseNullInt(trimQuotes(getCol(record, colIdx, "approvalfiscalyear"))),
			firstDisbDate,
			parseNullInt(trimQuotes(getCol(record, colIdx, "terminmonths"))),
			parseNullFloat(trimQuotes(getCol(record, colIdx, "initialinterestrate"))),
			nilIfEmpty(trimQuotes(getCol(record, colIdx, "fixedorvariableinterestind"))),
			nilIfEmpty(trimQuotes(getCol(record, colIdx, "naicscode"))),
			nilIfEmpty(sanitizeUTF8(trimQuotes(getCol(record, colIdx, "naicsdescription")))),
			nilIfEmpty(sanitizeUTF8(trimQuotes(getCol(record, colIdx, "loanstatus")))),
			pifDate,
			chgoffDate,
			parseNullFloat(trimQuotes(getCol(record, colIdx, "grosschargeoffamount"))),
			parseNullInt(trimQuotes(getCol(record, colIdx, "jobssupported"))),
			nilIfEmpty(sanitizeUTF8(trimQuotes(getCol(record, colIdx, "businesstype")))),
			nilIfEmpty(sanitizeUTF8(trimQuotes(getCol(record, colIdx, "businessage")))),
			nilIfEmpty(sanitizeUTF8(trimQuotes(getCol(record, colIdx, "franchisecode")))),
			nilIfEmpty(sanitizeUTF8(trimQuotes(getCol(record, colIdx, "franchisename")))),
			nilIfEmpty(sanitizeUTF8(trimQuotes(getCol(record, colIdx, "processingmethod")))),
			nilIfEmpty(sanitizeUTF8(trimQuotes(getCol(record, colIdx, "subprogram")))),
			nilIfEmpty(sanitizeUTF8(trimQuotes(getCol(record, colIdx, "projectcounty")))),
			nilIfEmpty(trimQuotes(getCol(record, colIdx, "projectstate"))),
			nilIfEmpty(sanitizeUTF8(trimQuotes(getCol(record, colIdx, "sbadistrictoffice")))),
			nilIfEmpty(trimQuotes(getCol(record, colIdx, "congressionaldistrict"))),
			// 7(a)-specific
			nilIfEmpty(sanitizeUTF8(trimQuotes(getCol(record, colIdx, "bankname")))),
			nilIfEmpty(trimQuotes(getCol(record, colIdx, "bankfdicnumber"))),
			nilIfEmpty(trimQuotes(getCol(record, colIdx, "bankncuanumber"))),
			nilIfEmpty(sanitizeUTF8(trimQuotes(getCol(record, colIdx, "bankstreet")))),
			nilIfEmpty(sanitizeUTF8(trimQuotes(getCol(record, colIdx, "bankcity")))),
			nilIfEmpty(trimQuotes(getCol(record, colIdx, "bankstate"))),
			nilIfEmpty(trimQuotes(getCol(record, colIdx, "bankzip"))),
			parseNullInt(trimQuotes(getCol(record, colIdx, "revolverstatus"))),
			nilIfEmpty(trimQuotes(getCol(record, colIdx, "collateralind"))),
			nilIfEmpty(trimQuotes(getCol(record, colIdx, "soldsecmrktind"))),
			// 504-specific
			nilIfEmpty(sanitizeUTF8(trimQuotes(getCol(record, colIdx, "cdc_name")))),
			nilIfEmpty(sanitizeUTF8(trimQuotes(getCol(record, colIdx, "cdc_street")))),
			nilIfEmpty(sanitizeUTF8(trimQuotes(getCol(record, colIdx, "cdc_city")))),
			nilIfEmpty(trimQuotes(getCol(record, colIdx, "cdc_state"))),
			nilIfEmpty(trimQuotes(getCol(record, colIdx, "cdc_zip"))),
			nilIfEmpty(sanitizeUTF8(trimQuotes(getCol(record, colIdx, "thirdpartylender_name")))),
			nilIfEmpty(sanitizeUTF8(trimQuotes(getCol(record, colIdx, "thirdpartylender_city")))),
			nilIfEmpty(trimQuotes(getCol(record, colIdx, "thirdpartylender_state"))),
			parseNullFloat(trimQuotes(getCol(record, colIdx, "thirdpartydollars"))),
			nilIfEmpty(sanitizeUTF8(trimQuotes(getCol(record, colIdx, "deliverymethod")))),
		}

		batch = append(batch, row)

		if program == "7A" {
			rows7a++
		} else {
			rows504++
		}

		if len(batch) >= sbaBatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table:        "fed_data.sba_loans",
				Columns:      columns,
				ConflictKeys: conflictKeys,
			}, batch)
			if err != nil {
				return totalRows, rows7a, rows504, eris.Wrap(err, "sba: bulk upsert")
			}
			totalRows += n
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        "fed_data.sba_loans",
			Columns:      columns,
			ConflictKeys: conflictKeys,
		}, batch)
		if err != nil {
			return totalRows, rows7a, rows504, eris.Wrap(err, "sba: bulk upsert final batch")
		}
		totalRows += n
	}

	return totalRows, rows7a, rows504, nil
}

// sbaColumns returns the column list for the sba_loans table, matching the
// order of values produced by parseCSV.
func sbaColumns() []string {
	return []string{
		"program", "l2locid",
		"borrname", "borrstreet", "borrcity", "borrstate", "borrzip",
		"grossapproval", "sbaguaranteedapproval",
		"approvaldate", "approvalfiscalyear", "firstdisbursementdate",
		"terminmonths", "initialinterestrate", "fixedorvariableinterestind",
		"naicscode", "naicsdescription",
		"loanstatus", "paidinfulldate", "chargeoffdate", "grosschargeoffamount",
		"jobssupported",
		"businesstype", "businessage",
		"franchisecode", "franchisename",
		"processingmethod", "subprogram",
		"projectcounty", "projectstate",
		"sbadistrictoffice", "congressionaldistrict",
		// 7(a)-specific
		"bankname", "bankfdicnumber", "bankncuanumber",
		"bankstreet", "bankcity", "bankstate", "bankzip",
		"revolverstatus", "collateralind", "soldsecmrktind",
		// 504-specific
		"cdc_name", "cdc_street", "cdc_city", "cdc_state", "cdc_zip",
		"thirdpartylender_name", "thirdpartylender_city", "thirdpartylender_state",
		"thirdpartydollars", "deliverymethod",
	}
}

// parseSBADate parses dates in MM/DD/YYYY or YYYY-MM-DD format, returning nil if empty or invalid.
func parseSBADate(s string) *time.Time {
	s = trimQuotes(strings.TrimSpace(s))
	if s == "" {
		return nil
	}
	for _, layout := range []string{"01/02/2006", "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return &t
		}
	}
	return nil
}

// parseNullFloat parses a float64, returning nil for empty or invalid input.
func parseNullFloat(s string) *float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	v := parseFloat64Or(s, 0)
	if v == 0 && s != "0" && s != "0.00" && s != "0.0" {
		return nil
	}
	return &v
}

// parseNullInt parses an integer, returning nil for empty or invalid input.
func parseNullInt(s string) *int {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	v := parseIntOr(s, 0)
	if v == 0 && s != "0" {
		return nil
	}
	return &v
}
