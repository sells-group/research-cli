package dataset

import (
	"context"
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/db"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/fetcher"
)

const (
	advDataURL   = "https://www.sec.gov/files/data/investment-adviser-data/adv-data-current.csv"
	advBatchSize = 5000
)

// ADVPart1 implements the SEC ADV Part 1A bulk CSV dataset.
// Downloads the full IARD registration dataset and upserts firms, AUM history,
// private fund data, and ownership records.
type ADVPart1 struct{}

func (d *ADVPart1) Name() string     { return "adv_part1" }
func (d *ADVPart1) Table() string    { return "fed_data.adv_firms" }
func (d *ADVPart1) Phase() Phase     { return Phase1B }
func (d *ADVPart1) Cadence() Cadence { return Monthly }

func (d *ADVPart1) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return MonthlySchedule(now, lastSync)
}

func (d *ADVPart1) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "adv_part1"))

	csvPath := filepath.Join(tempDir, "adv-data-current.csv")
	log.Info("downloading ADV bulk CSV", zap.String("url", advDataURL))

	if _, err := f.DownloadToFile(ctx, advDataURL, csvPath); err != nil {
		return nil, eris.Wrap(err, "adv_part1: download CSV")
	}
	defer os.Remove(csvPath)

	file, err := os.Open(csvPath)
	if err != nil {
		return nil, eris.Wrap(err, "adv_part1: open CSV")
	}
	defer file.Close()

	return d.parseAndLoad(ctx, pool, file, log)
}

func (d *ADVPart1) parseAndLoad(ctx context.Context, pool db.Pool, r io.Reader, log *zap.Logger) (*SyncResult, error) {
	reader := csv.NewReader(r)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		return nil, eris.Wrap(err, "adv_part1: read CSV header")
	}

	colIdx := mapColumns(header)

	var (
		firmBatch   [][]any
		aumBatch    [][]any
		fundBatch   [][]any
		ownerBatch  [][]any
		totalFirms  int64
		totalAUM    int64
		totalFunds  int64
		totalOwners int64
	)

	firmCols := []string{"crd_number", "firm_name", "sec_number", "city", "state", "country", "website", "aum", "num_accounts", "num_employees", "filing_date"}
	firmConflict := []string{"crd_number"}

	aumCols := []string{"crd_number", "report_date", "aum", "raum", "num_accounts"}
	aumConflict := []string{"crd_number", "report_date"}

	fundCols := []string{"crd_number", "fund_id", "fund_name", "fund_type", "gross_asset_value", "net_asset_value"}
	fundConflict := []string{"crd_number", "fund_id"}

	ownerCols := []string{"crd_number", "owner_name", "owner_type", "ownership_pct", "is_control"}
	ownerConflict := []string{"crd_number", "owner_name"}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		crd := parseIntOr(trimQuotes(getCol(record, colIdx, "crd_number")), 0)
		if crd == 0 {
			continue
		}

		// Firm record
		filingDate := parseDate(trimQuotes(getCol(record, colIdx, "filing_date")))
		firmRow := []any{
			crd,
			trimQuotes(getCol(record, colIdx, "firm_name")),
			trimQuotes(getCol(record, colIdx, "sec_number")),
			trimQuotes(getCol(record, colIdx, "city")),
			trimQuotes(getCol(record, colIdx, "state")),
			trimQuotes(getCol(record, colIdx, "country")),
			trimQuotes(getCol(record, colIdx, "website")),
			parseInt64Or(trimQuotes(getCol(record, colIdx, "aum")), 0),
			parseIntOr(trimQuotes(getCol(record, colIdx, "num_accounts")), 0),
			parseIntOr(trimQuotes(getCol(record, colIdx, "num_employees")), 0),
			filingDate,
		}
		firmBatch = append(firmBatch, firmRow)

		// AUM snapshot (if report_date present)
		reportDateStr := trimQuotes(getCol(record, colIdx, "report_date"))
		if reportDateStr != "" {
			reportDate := parseDate(reportDateStr)
			if reportDate != nil {
				aumRow := []any{
					crd,
					reportDate,
					parseInt64Or(trimQuotes(getCol(record, colIdx, "aum")), 0),
					parseInt64Or(trimQuotes(getCol(record, colIdx, "raum")), 0),
					parseIntOr(trimQuotes(getCol(record, colIdx, "num_accounts")), 0),
				}
				aumBatch = append(aumBatch, aumRow)
			}
		}

		// Private fund (if fund_id present)
		fundID := trimQuotes(getCol(record, colIdx, "fund_id"))
		if fundID != "" {
			fundRow := []any{
				crd,
				fundID,
				trimQuotes(getCol(record, colIdx, "fund_name")),
				trimQuotes(getCol(record, colIdx, "fund_type")),
				parseInt64Or(trimQuotes(getCol(record, colIdx, "gross_asset_value")), 0),
				parseInt64Or(trimQuotes(getCol(record, colIdx, "net_asset_value")), 0),
			}
			fundBatch = append(fundBatch, fundRow)
		}

		// Owner (if owner_name present)
		ownerName := trimQuotes(getCol(record, colIdx, "owner_name"))
		if ownerName != "" {
			ownerRow := []any{
				crd,
				ownerName,
				trimQuotes(getCol(record, colIdx, "owner_type")),
				parseFloat64Or(trimQuotes(getCol(record, colIdx, "ownership_pct")), 0),
				strings.EqualFold(trimQuotes(getCol(record, colIdx, "is_control")), "Y"),
			}
			ownerBatch = append(ownerBatch, ownerRow)
		}

		// Flush firm batch
		if len(firmBatch) >= advBatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table: "fed_data.adv_firms", Columns: firmCols, ConflictKeys: firmConflict,
			}, firmBatch)
			if err != nil {
				return nil, eris.Wrap(err, "adv_part1: upsert firms")
			}
			totalFirms += n
			firmBatch = firmBatch[:0]
		}

		// Flush AUM batch
		if len(aumBatch) >= advBatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table: "fed_data.adv_aum", Columns: aumCols, ConflictKeys: aumConflict,
			}, aumBatch)
			if err != nil {
				return nil, eris.Wrap(err, "adv_part1: upsert aum")
			}
			totalAUM += n
			aumBatch = aumBatch[:0]
		}

		// Flush fund batch
		if len(fundBatch) >= advBatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table: "fed_data.adv_private_funds", Columns: fundCols, ConflictKeys: fundConflict,
			}, fundBatch)
			if err != nil {
				return nil, eris.Wrap(err, "adv_part1: upsert funds")
			}
			totalFunds += n
			fundBatch = fundBatch[:0]
		}

		// Flush owner batch
		if len(ownerBatch) >= advBatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table: "fed_data.adv_owners", Columns: ownerCols, ConflictKeys: ownerConflict,
			}, ownerBatch)
			if err != nil {
				return nil, eris.Wrap(err, "adv_part1: upsert owners")
			}
			totalOwners += n
			ownerBatch = ownerBatch[:0]
		}
	}

	// Flush remaining batches
	if len(firmBatch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table: "fed_data.adv_firms", Columns: firmCols, ConflictKeys: firmConflict,
		}, firmBatch)
		if err != nil {
			return nil, eris.Wrap(err, "adv_part1: upsert firms final")
		}
		totalFirms += n
	}
	if len(aumBatch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table: "fed_data.adv_aum", Columns: aumCols, ConflictKeys: aumConflict,
		}, aumBatch)
		if err != nil {
			return nil, eris.Wrap(err, "adv_part1: upsert aum final")
		}
		totalAUM += n
	}
	if len(fundBatch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table: "fed_data.adv_private_funds", Columns: fundCols, ConflictKeys: fundConflict,
		}, fundBatch)
		if err != nil {
			return nil, eris.Wrap(err, "adv_part1: upsert funds final")
		}
		totalFunds += n
	}
	if len(ownerBatch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table: "fed_data.adv_owners", Columns: ownerCols, ConflictKeys: ownerConflict,
		}, ownerBatch)
		if err != nil {
			return nil, eris.Wrap(err, "adv_part1: upsert owners final")
		}
		totalOwners += n
	}

	log.Info("adv_part1 sync complete",
		zap.Int64("firms", totalFirms),
		zap.Int64("aum_records", totalAUM),
		zap.Int64("funds", totalFunds),
		zap.Int64("owners", totalOwners),
	)

	return &SyncResult{
		RowsSynced: totalFirms,
		Metadata: map[string]any{
			"firms":       totalFirms,
			"aum_records": totalAUM,
			"funds":       totalFunds,
			"owners":      totalOwners,
		},
	}, nil
}

// parseDate attempts to parse a date string in common SEC formats.
func parseDate(s string) *time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	formats := []string{
		"2006-01-02",
		"01/02/2006",
		"1/2/2006",
		"2006-01-02T15:04:05",
		"01-02-2006",
	}
	for _, fmt := range formats {
		if t, err := time.Parse(fmt, s); err == nil {
			return &t
		}
	}
	return nil
}
