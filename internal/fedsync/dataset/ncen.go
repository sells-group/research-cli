package dataset

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
)

const (
	ncenBaseURL   = "https://www.sec.gov/files/dera/data/form-n-cen-data-sets"
	ncenBatchSize = 5000

	// ncenBackfillQuarters is how many quarters to load on the first run.
	ncenBackfillQuarters = 8
)

// ncenRegistrantCols defines the target DB columns for ncen_registrants.
var ncenRegistrantCols = []string{
	"accession_number", "cik", "registrant_name", "file_num", "lei",
	"address1", "address2", "city", "state", "country", "zip", "phone",
	"investment_company_type", "total_series",
	"filing_date", "report_ending_period",
	"is_first_filing", "is_last_filing",
	"family_investment_company_name", "updated_at",
}

// ncenFundCols defines the target DB columns for ncen_funds.
var ncenFundCols = []string{
	"fund_id", "accession_number", "fund_name", "series_id", "lei",
	"is_etf", "is_index", "is_money_market", "is_target_date", "is_fund_of_fund",
	"monthly_avg_net_assets", "daily_avg_net_assets",
	"nav_per_share", "management_fee", "net_operating_expenses",
	"updated_at",
}

// ncenAdviserCols defines the target DB columns for ncen_advisers.
var ncenAdviserCols = []string{
	"fund_id", "adviser_name", "adviser_crd", "adviser_lei", "file_num",
	"adviser_type", "state", "country", "is_affiliated",
	"updated_at",
}

// NCEN implements the SEC Form N-CEN dataset for registered investment companies.
// Data source: SEC DERA quarterly ZIP files containing tab-delimited TSV.
type NCEN struct {
	cfg *config.Config
}

// Name implements Dataset.
func (d *NCEN) Name() string { return "ncen" }

// Table implements Dataset.
func (d *NCEN) Table() string { return "fed_data.ncen_registrants" }

// Phase implements Dataset.
func (d *NCEN) Phase() Phase { return Phase2 }

// Cadence implements Dataset.
func (d *NCEN) Cadence() Cadence { return Quarterly }

// ShouldRun implements Dataset.
func (d *NCEN) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return QuarterlyWithLag(now, lastSync, 1)
}

// Sync downloads N-CEN quarterly ZIP files from SEC EDGAR, parses the TSV files,
// and upserts registrant, fund, and adviser data into Postgres.
func (d *NCEN) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))

	quarters := d.quartersToSync()
	log.Info("ncen: syncing quarters", zap.Int("count", len(quarters)))

	var totalRegistrants, totalFunds, totalAdvisers int64
	var syncedQuarters []string

	for _, q := range quarters {
		reg, funds, adv, err := d.syncQuarter(ctx, pool, f, tempDir, q, log)
		if err != nil {
			// Skip quarters that are not yet available (404).
			if isNotFoundErr(err) {
				log.Info("ncen: quarter not available, skipping", zap.String("quarter", q.String()))
				continue
			}
			return nil, eris.Wrapf(err, "ncen: sync quarter %s", q.String())
		}
		totalRegistrants += reg
		totalFunds += funds
		totalAdvisers += adv
		syncedQuarters = append(syncedQuarters, q.String())
	}

	log.Info("ncen sync complete",
		zap.Int64("registrants", totalRegistrants),
		zap.Int64("funds", totalFunds),
		zap.Int64("advisers", totalAdvisers),
		zap.Strings("quarters", syncedQuarters),
	)

	return &SyncResult{
		RowsSynced: totalRegistrants + totalFunds + totalAdvisers,
		Metadata: map[string]any{
			"registrants": totalRegistrants,
			"funds":       totalFunds,
			"advisers":    totalAdvisers,
			"quarters":    syncedQuarters,
		},
	}, nil
}

// ncenQuarter represents a year/quarter pair for N-CEN data.
type ncenQuarter struct {
	Year    int
	Quarter int
}

// String returns a human-readable representation like "2025q3".
func (q ncenQuarter) String() string {
	return fmt.Sprintf("%dq%d", q.Year, q.Quarter)
}

// quartersToSync returns the quarters to download. On first run, backfill
// the last ncenBackfillQuarters quarters. On subsequent runs, only the latest.
func (d *NCEN) quartersToSync() []ncenQuarter {
	now := time.Now()
	// Latest available quarter: current quarter minus 1.
	q := currentQuarter(now)
	q = prevQuarter(q)

	var result []ncenQuarter
	for i := 0; i < ncenBackfillQuarters; i++ {
		result = append(result, q)
		q = prevQuarter(q)
	}

	// Reverse so we process oldest first.
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result
}

func currentQuarter(t time.Time) ncenQuarter {
	return ncenQuarter{Year: t.Year(), Quarter: (int(t.Month())-1)/3 + 1}
}

func prevQuarter(q ncenQuarter) ncenQuarter {
	if q.Quarter == 1 {
		return ncenQuarter{Year: q.Year - 1, Quarter: 4}
	}
	return ncenQuarter{Year: q.Year, Quarter: q.Quarter - 1}
}

// syncQuarter downloads and processes a single quarter's ZIP.
func (d *NCEN) syncQuarter(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string, q ncenQuarter, log *zap.Logger) (int64, int64, int64, error) {
	zipPath := filepath.Join(tempDir, fmt.Sprintf("%s_ncen.zip", q.String()))

	log.Info("ncen: downloading quarter", zap.String("quarter", q.String()))
	if err := d.downloadQuarter(ctx, f, q, zipPath); err != nil {
		return 0, 0, 0, err
	}
	defer os.Remove(zipPath) //nolint:errcheck

	extracted, err := fetcher.ExtractZIP(zipPath, tempDir)
	if err != nil {
		return 0, 0, 0, eris.Wrapf(err, "ncen: extract ZIP %s", q.String())
	}
	defer func() {
		for _, p := range extracted {
			_ = os.Remove(p)
		}
	}()

	// Build a map of base filename → full path for easy lookup.
	fileMap := make(map[string]string, len(extracted))
	for _, p := range extracted {
		base := strings.ToUpper(filepath.Base(p))
		fileMap[base] = p
	}

	now := time.Now()

	// Parse registrants (SUBMISSION.tsv + REGISTRANT.tsv joined on accession_number).
	submissionData, err := d.parseTSVFile(fileMap, "SUBMISSION.TSV")
	if err != nil {
		return 0, 0, 0, eris.Wrap(err, "ncen: parse SUBMISSION.tsv")
	}
	registrantData, err := d.parseTSVFile(fileMap, "REGISTRANT.TSV")
	if err != nil {
		return 0, 0, 0, eris.Wrap(err, "ncen: parse REGISTRANT.tsv")
	}

	regRows := d.buildRegistrantRows(submissionData, registrantData, now)

	// Parse funds (FUND_REPORTED_INFO.tsv).
	fundData, err := d.parseTSVFile(fileMap, "FUND_REPORTED_INFO.TSV")
	if err != nil {
		return 0, 0, 0, eris.Wrap(err, "ncen: parse FUND_REPORTED_INFO.tsv")
	}
	fundRows := d.buildFundRows(fundData, now)

	// Parse advisers (ADVISER.tsv).
	advData, err := d.parseTSVFile(fileMap, "ADVISER.TSV")
	if err != nil {
		return 0, 0, 0, eris.Wrap(err, "ncen: parse ADVISER.tsv")
	}
	advRows := d.buildAdviserRows(advData, now)

	// Upsert all three tables in a single transaction.
	results, err := db.BulkUpsertMulti(ctx, pool, []db.MultiUpsertEntry{
		{Config: ncenRegistrantUpsertCfg(), Rows: regRows},
		{Config: ncenFundUpsertCfg(), Rows: fundRows},
		{Config: ncenAdviserUpsertCfg(), Rows: advRows},
	})
	if err != nil {
		return 0, 0, 0, eris.Wrapf(err, "ncen: bulk upsert %s", q.String())
	}

	nReg := results["fed_data.ncen_registrants"]
	nFund := results["fed_data.ncen_funds"]
	nAdv := results["fed_data.ncen_advisers"]

	log.Info("ncen: quarter synced",
		zap.String("quarter", q.String()),
		zap.Int64("registrants", nReg),
		zap.Int64("funds", nFund),
		zap.Int64("advisers", nAdv),
	)

	return nReg, nFund, nAdv, nil
}

// downloadQuarter tries the primary URL, then falls back to the _0 suffix variant.
func (d *NCEN) downloadQuarter(ctx context.Context, f fetcher.Fetcher, q ncenQuarter, zipPath string) error {
	primaryURL := fmt.Sprintf("%s/%s_ncen.zip", ncenBaseURL, q.String())
	_, err := f.DownloadToFile(ctx, primaryURL, zipPath)
	if err == nil {
		return nil
	}

	// Retry with _0 suffix variant.
	altURL := fmt.Sprintf("%s/%s_ncen_0.zip", ncenBaseURL, q.String())
	_, err2 := f.DownloadToFile(ctx, altURL, zipPath)
	if err2 == nil {
		return nil
	}

	// Return the primary error for better diagnostics.
	return err
}

// parsedTSV holds the column index map and all records from a TSV file.
type parsedTSV struct {
	colIdx  map[string]int
	records [][]string
}

// parseTSVFile opens and parses a TSV file from the extracted file map.
func (d *NCEN) parseTSVFile(fileMap map[string]string, name string) (*parsedTSV, error) {
	path, ok := fileMap[name]
	if !ok {
		return &parsedTSV{colIdx: map[string]int{}}, nil
	}

	file, err := os.Open(path) // #nosec G304 -- path from ExtractZIP in trusted temp dir
	if err != nil {
		return nil, eris.Wrapf(err, "open %s", name)
	}
	defer file.Close() //nolint:errcheck

	return parseTSV(file)
}

// parseTSV reads a tab-delimited file into a column index map and records.
func parseTSV(r io.Reader) (*parsedTSV, error) {
	reader := csv.NewReader(r)
	reader.Comma = '\t'
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1 // variable field count

	header, err := reader.Read()
	if err != nil {
		return nil, eris.Wrap(err, "read TSV header")
	}

	colIdx := mapColumnsNormalized(header)
	var records [][]string

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // skip malformed rows
		}
		records = append(records, record)
	}

	return &parsedTSV{colIdx: colIdx, records: records}, nil
}

// buildRegistrantRows joins SUBMISSION and REGISTRANT data on accession_number.
func (d *NCEN) buildRegistrantRows(sub, reg *parsedTSV, now time.Time) [][]any {
	// Build REGISTRANT lookup by accession_number.
	regByAccession := make(map[string][]string, len(reg.records))
	for _, rec := range reg.records {
		acc := sanitizeUTF8(trimQuotes(getColN(rec, reg.colIdx, "ACCESSION_NUMBER")))
		if acc != "" {
			regByAccession[acc] = rec
		}
	}

	var rows [][]any
	for _, rec := range sub.records {
		acc := sanitizeUTF8(trimQuotes(getColN(rec, sub.colIdx, "ACCESSION_NUMBER")))
		if acc == "" {
			continue
		}

		cik := sanitizeUTF8(trimQuotes(getColN(rec, sub.colIdx, "CIK")))
		filingDate := parseNCENDate(getColN(rec, sub.colIdx, "FILING_DATE"))
		reportPeriod := parseNCENDate(getColN(rec, sub.colIdx, "REPORT_ENDING_PERIOD"))

		// Look up registrant data for this accession.
		regRec, hasReg := regByAccession[acc]
		var regName, fileNum, lei, addr1, addr2, city, state, country, zip, phone string
		var companyType, familyName string
		var totalSeries any
		var isFirst, isLast any

		if hasReg {
			regName = sanitizeUTF8(trimQuotes(getColN(regRec, reg.colIdx, "REGISTRANT_NAME")))
			fileNum = sanitizeUTF8(trimQuotes(getColN(regRec, reg.colIdx, "FILE_NUM")))
			lei = sanitizeUTF8(trimQuotes(getColN(regRec, reg.colIdx, "LEI")))
			addr1 = sanitizeUTF8(trimQuotes(getColN(regRec, reg.colIdx, "ADDRESS1")))
			addr2 = sanitizeUTF8(trimQuotes(getColN(regRec, reg.colIdx, "ADDRESS2")))
			city = sanitizeUTF8(trimQuotes(getColN(regRec, reg.colIdx, "CITY")))
			state = sanitizeUTF8(trimQuotes(getColN(regRec, reg.colIdx, "STATE")))
			country = sanitizeUTF8(trimQuotes(getColN(regRec, reg.colIdx, "COUNTRY")))
			zip = sanitizeUTF8(trimQuotes(getColN(regRec, reg.colIdx, "ZIP")))
			phone = sanitizeUTF8(trimQuotes(getColN(regRec, reg.colIdx, "PHONE")))
			companyType = sanitizeUTF8(trimQuotes(getColN(regRec, reg.colIdx, "INVESTMENT_COMPANY_TYPE")))
			familyName = sanitizeUTF8(trimQuotes(getColN(regRec, reg.colIdx, "FAMILY_INVESTMENT_COMPANY_NAME")))
			totalSeries = parseInt64OrNil(getColN(regRec, reg.colIdx, "TOTAL_SERIES"))
			isFirst = parseBoolYNOrNil(getColN(regRec, reg.colIdx, "IS_FIRST_FILING"))
			isLast = parseBoolYNOrNil(getColN(regRec, reg.colIdx, "IS_LAST_FILING"))
		}

		rows = append(rows, []any{
			acc,                     // accession_number
			cik,                     // cik
			nilIfEmpty(regName),     // registrant_name
			nilIfEmpty(fileNum),     // file_num
			nilIfEmpty(lei),         // lei
			nilIfEmpty(addr1),       // address1
			nilIfEmpty(addr2),       // address2
			nilIfEmpty(city),        // city
			nilIfEmpty(state),       // state
			nilIfEmpty(country),     // country
			nilIfEmpty(zip),         // zip
			nilIfEmpty(phone),       // phone
			nilIfEmpty(companyType), // investment_company_type
			totalSeries,             // total_series
			filingDate,              // filing_date
			reportPeriod,            // report_ending_period
			isFirst,                 // is_first_filing
			isLast,                  // is_last_filing
			nilIfEmpty(familyName),  // family_investment_company_name
			now,                     // updated_at
		})
	}
	return rows
}

// buildFundRows parses FUND_REPORTED_INFO records into DB rows.
func (d *NCEN) buildFundRows(data *parsedTSV, now time.Time) [][]any {
	var rows [][]any
	for _, rec := range data.records {
		fundID := sanitizeUTF8(trimQuotes(getColN(rec, data.colIdx, "FUND_ID")))
		if fundID == "" {
			continue
		}

		rows = append(rows, []any{
			fundID, // fund_id
			nilIfEmpty(sanitizeUTF8(trimQuotes(getColN(rec, data.colIdx, "ACCESSION_NUMBER")))), // accession_number
			nilIfEmpty(sanitizeUTF8(trimQuotes(getColN(rec, data.colIdx, "FUND_NAME")))),        // fund_name
			nilIfEmpty(sanitizeUTF8(trimQuotes(getColN(rec, data.colIdx, "SERIES_ID")))),        // series_id
			nilIfEmpty(sanitizeUTF8(trimQuotes(getColN(rec, data.colIdx, "LEI")))),              // lei
			parseBoolYNOrNil(getColN(rec, data.colIdx, "IS_ETF")),                               // is_etf
			parseBoolYNOrNil(getColN(rec, data.colIdx, "IS_INDEX")),                             // is_index
			parseBoolYNOrNil(getColN(rec, data.colIdx, "IS_MONEY_MARKET")),                      // is_money_market
			parseBoolYNOrNil(getColN(rec, data.colIdx, "IS_TARGET_DATE")),                       // is_target_date
			parseBoolYNOrNil(getColN(rec, data.colIdx, "IS_FUND_OF_FUND")),                      // is_fund_of_fund
			parseFloat64OrNil(getColN(rec, data.colIdx, "MONTHLY_AVG_NET_ASSETS")),              // monthly_avg_net_assets
			parseFloat64OrNil(getColN(rec, data.colIdx, "DAILY_AVG_NET_ASSETS")),                // daily_avg_net_assets
			parseFloat64OrNil(getColN(rec, data.colIdx, "NAV_PER_SHARE")),                       // nav_per_share
			parseFloat64OrNil(getColN(rec, data.colIdx, "MANAGEMENT_FEE")),                      // management_fee
			parseFloat64OrNil(getColN(rec, data.colIdx, "NET_OPERATING_EXPENSES")),              // net_operating_expenses
			now, // updated_at
		})
	}
	return rows
}

// buildAdviserRows parses ADVISER records into DB rows.
func (d *NCEN) buildAdviserRows(data *parsedTSV, now time.Time) [][]any {
	var rows [][]any
	for _, rec := range data.records {
		fundID := sanitizeUTF8(trimQuotes(getColN(rec, data.colIdx, "FUND_ID")))
		if fundID == "" {
			continue
		}

		crd := sanitizeUTF8(trimQuotes(getColN(rec, data.colIdx, "CRD_NUM")))

		adviserName := sanitizeUTF8(trimQuotes(getColN(rec, data.colIdx, "ADVISER_NAME")))
		adviserType := sanitizeUTF8(trimQuotes(getColN(rec, data.colIdx, "ADVISER_TYPE")))

		rows = append(rows, []any{
			fundID,          // fund_id
			adviserName,     // adviser_name (NOT NULL — part of PK)
			nilIfEmpty(crd), // adviser_crd
			nilIfEmpty(sanitizeUTF8(trimQuotes(getColN(rec, data.colIdx, "ADVISER_LEI")))), // adviser_lei
			nilIfEmpty(sanitizeUTF8(trimQuotes(getColN(rec, data.colIdx, "FILE_NUM")))),    // file_num
			adviserType, // adviser_type (NOT NULL — part of PK)
			nilIfEmpty(sanitizeUTF8(trimQuotes(getColN(rec, data.colIdx, "STATE")))),   // state
			nilIfEmpty(sanitizeUTF8(trimQuotes(getColN(rec, data.colIdx, "COUNTRY")))), // country
			parseBoolYNOrNil(getColN(rec, data.colIdx, "IS_AFFILIATED")),               // is_affiliated
			now, // updated_at
		})
	}
	return rows
}

// parseNCENDate parses SEC date formats: "DD-MON-YYYY" (e.g., "13-AUG-2025")
// and "YYYY-MM-DD". Returns nil on failure.
func parseNCENDate(s string) any {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}

	// Try DD-MON-YYYY first (SEC EDGAR format).
	t, err := time.Parse("02-Jan-2006", s)
	if err == nil {
		return t
	}

	// Try ISO format.
	t, err = time.Parse("2006-01-02", s)
	if err == nil {
		return t
	}

	return nil
}

// parseBoolYNOrNil returns true for "Y", false for "N", nil for empty/other.
func parseBoolYNOrNil(s string) any {
	s = strings.TrimSpace(s)
	switch strings.ToUpper(s) {
	case "Y":
		return true
	case "N":
		return false
	default:
		return nil
	}
}

// ncenRegistrantUpsertCfg returns the upsert configuration for ncen_registrants.
func ncenRegistrantUpsertCfg() db.UpsertConfig {
	return db.UpsertConfig{
		Table:        "fed_data.ncen_registrants",
		Columns:      ncenRegistrantCols,
		ConflictKeys: []string{"accession_number"},
	}
}

// ncenFundUpsertCfg returns the upsert configuration for ncen_funds.
func ncenFundUpsertCfg() db.UpsertConfig {
	return db.UpsertConfig{
		Table:        "fed_data.ncen_funds",
		Columns:      ncenFundCols,
		ConflictKeys: []string{"fund_id"},
	}
}

// ncenAdviserUpsertCfg returns the upsert configuration for ncen_advisers.
func ncenAdviserUpsertCfg() db.UpsertConfig {
	return db.UpsertConfig{
		Table:        "fed_data.ncen_advisers",
		Columns:      ncenAdviserCols,
		ConflictKeys: []string{"fund_id", "adviser_name", "adviser_type"},
	}
}
