package dataset

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
)

const (
	// ncuaCallReportBaseURL is the per-quarter ZIP URL pattern.
	// Format: call-report-data-YYYY-MM.zip where MM is 03, 06, 09, or 12.
	ncuaCallReportBaseURL = "https://ncua.gov/files/publications/analysis/call-report-data-%d-%02d.zip"
	ncuaBatchSize         = 5000
	// ncuaHistoryYears controls how many years back to look for quarters on initial sync.
	ncuaHistoryYears = 3
)

// NCUACallReports syncs NCUA 5300 Call Report data for credit unions.
type NCUACallReports struct{}

// Name implements Dataset.
func (d *NCUACallReports) Name() string { return "ncua_call_reports" }

// Table implements Dataset.
func (d *NCUACallReports) Table() string { return "fed_data.ncua_call_reports" }

// Phase implements Dataset.
func (d *NCUACallReports) Phase() Phase { return Phase2 }

// Cadence implements Dataset.
func (d *NCUACallReports) Cadence() Cadence { return Quarterly }

// ShouldRun implements Dataset.
func (d *NCUACallReports) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return QuarterlyWithLag(now, lastSync, 2)
}

// ncuaQuarter represents a year-quarter pair.
type ncuaQuarter struct {
	Year  int
	Month int // 3, 6, 9, or 12
}

func (q ncuaQuarter) String() string {
	return fmt.Sprintf("%d-%02d", q.Year, q.Month)
}

func (q ncuaQuarter) url() string {
	return fmt.Sprintf(ncuaCallReportBaseURL, q.Year, q.Month)
}

// ncuaMissingQuarters returns quarters not yet in the DB, going back ncuaHistoryYears.
func ncuaMissingQuarters(ctx context.Context, pool db.Pool, now time.Time) ([]ncuaQuarter, error) {
	rows, err := pool.Query(ctx, `SELECT DISTINCT cycle_date FROM fed_data.ncua_call_reports`)
	if err != nil {
		return nil, eris.Wrap(err, "ncua: query existing quarters")
	}
	defer rows.Close()

	existing := make(map[string]bool)
	for rows.Next() {
		var d time.Time
		if err := rows.Scan(&d); err != nil {
			return nil, eris.Wrap(err, "ncua: scan cycle_date")
		}
		existing[fmt.Sprintf("%d-%02d", d.Year(), int(d.Month()))] = true
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "ncua: iterate cycle_dates")
	}

	months := []int{3, 6, 9, 12}
	startYear := now.Year() - ncuaHistoryYears
	var missing []ncuaQuarter
	for y := startYear; y <= now.Year(); y++ {
		for _, m := range months {
			qEnd := time.Date(y, time.Month(m), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, -1)
			if qEnd.After(now) {
				continue
			}
			key := fmt.Sprintf("%d-%02d", y, m)
			if !existing[key] {
				missing = append(missing, ncuaQuarter{Year: y, Month: m})
			}
		}
	}
	return missing, nil
}

// Sync downloads, parses, and upserts NCUA 5300 Call Report data.
// It's incremental — only downloads quarters not already in the DB.
func (d *NCUACallReports) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))

	now := time.Now().UTC()
	missing, err := ncuaMissingQuarters(ctx, pool, now)
	if err != nil {
		return nil, err
	}

	if len(missing) == 0 {
		log.Info("ncua_call_reports: all quarters up to date")
		return &SyncResult{}, nil
	}

	log.Info("ncua_call_reports: downloading missing quarters", zap.Int("missing", len(missing)))

	var totalRows int64
	var quartersLoaded int

	for _, q := range missing {
		if ctx.Err() != nil {
			return nil, eris.Wrap(ctx.Err(), "ncua: context cancelled")
		}

		rows, dlErr := d.syncQuarter(ctx, pool, f, tempDir, q, log)
		if dlErr != nil {
			if strings.Contains(dlErr.Error(), "404") || strings.Contains(dlErr.Error(), "403") {
				log.Info("ncua: quarter not available yet", zap.String("quarter", q.String()))
				continue
			}
			return nil, eris.Wrapf(dlErr, "ncua: sync quarter %s", q.String())
		}
		totalRows += rows
		quartersLoaded++
	}

	log.Info("ncua_call_reports sync complete",
		zap.Int64("rows", totalRows),
		zap.Int("quarters_loaded", quartersLoaded),
	)
	return &SyncResult{
		RowsSynced: totalRows,
		Metadata: map[string]any{
			"quarters_loaded":  quartersLoaded,
			"quarters_checked": len(missing),
		},
	}, nil
}

// syncQuarter downloads and upserts a single quarter's data.
// NCUA ZIPs contain .txt files: FOICU.txt (basic info) and FS220.txt (financials).
func (d *NCUACallReports) syncQuarter(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string, q ncuaQuarter, log *zap.Logger) (int64, error) {
	log.Info("downloading quarter", zap.String("quarter", q.String()))

	zipPath := filepath.Join(tempDir, fmt.Sprintf("ncua_%s.zip", q.String()))
	if _, err := f.DownloadToFile(ctx, q.url(), zipPath); err != nil {
		return 0, err
	}
	defer os.Remove(zipPath) //nolint:errcheck

	extractDir := filepath.Join(tempDir, fmt.Sprintf("ncua_%s", q.String()))
	files, err := fetcher.ExtractZIP(zipPath, extractDir)
	if err != nil {
		return 0, eris.Wrap(err, "extract ZIP")
	}
	defer os.RemoveAll(extractDir) //nolint:errcheck

	// Find FOICU.txt and FS220.txt in extracted files.
	var foicuPath, fs220Path string
	for _, fp := range files {
		base := strings.ToUpper(filepath.Base(fp))
		switch base {
		case "FOICU.TXT":
			foicuPath = fp
		case "FS220.TXT":
			fs220Path = fp
		}
	}
	if foicuPath == "" {
		return 0, eris.Errorf("FOICU.txt not found in %s ZIP", q.String())
	}
	if fs220Path == "" {
		return 0, eris.Errorf("FS220.txt not found in %s ZIP", q.String())
	}

	// 1. Load financials from FS220.txt into a map keyed by CU_NUMBER.
	financials, err := loadFS220(ctx, fs220Path)
	if err != nil {
		return 0, eris.Wrap(err, "load FS220")
	}
	log.Info("loaded FS220 financials", zap.Int("credit_unions", len(financials)))

	// 2. Stream FOICU.txt, join with financials, and upsert.
	totalRows, err := d.processFOICU(ctx, pool, foicuPath, financials, log)
	if err != nil {
		return 0, eris.Wrap(err, "process FOICU")
	}

	log.Info("quarter loaded",
		zap.String("quarter", q.String()),
		zap.Int64("rows", totalRows),
	)
	return totalRows, nil
}

// ncuaFinancials holds the key financial fields from FS220.txt for one CU.
type ncuaFinancials struct {
	TotalAssets         *int64
	TotalLoans          *int64
	TotalShares         *int64
	TotalBorrowings     *int64
	NetWorth            *int64
	NetIncome           *int64
	GrossIncome         *int64
	TotalExpenses       *int64
	ProvisionLosses     *int64
	Members             *int
	MemberBusinessLoans *int64
	Investments         *int64
}

// loadFS220 reads FS220.txt and returns financials keyed by CU_NUMBER.
func loadFS220(ctx context.Context, path string) (map[int]*ncuaFinancials, error) {
	f, err := openFileForRead(path)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck

	rowCh, errCh := fetcher.StreamCSV(ctx, f, fetcher.CSVOptions{HasHeader: false, LazyQuotes: true})

	colIdx := make(map[string]int)
	first := true
	result := make(map[int]*ncuaFinancials)

	for record := range rowCh {
		if first {
			for i, col := range record {
				colIdx[strings.ToUpper(strings.TrimSpace(col))] = i
			}
			first = false
			continue
		}

		get := func(name string) string {
			if i, ok := colIdx[name]; ok && i < len(record) {
				return strings.TrimSpace(record[i])
			}
			return ""
		}

		cuNumStr := get("CU_NUMBER")
		if cuNumStr == "" {
			continue
		}
		cuNum, parseErr := strconv.Atoi(cuNumStr)
		if parseErr != nil {
			continue
		}

		fin := &ncuaFinancials{
			TotalAssets:         parseOptInt64(get("ACCT_010")),
			TotalLoans:          parseOptInt64(get("ACCT_025B")),
			TotalShares:         parseOptInt64(get("ACCT_018")),
			TotalBorrowings:     parseOptInt64(get("ACCT_860C")),
			NetWorth:            parseOptInt64(get("ACCT_940")),
			NetIncome:           parseOptInt64(get("ACCT_602")),
			GrossIncome:         parseOptInt64(get("ACCT_550")),
			TotalExpenses:       parseOptInt64(get("ACCT_671")),
			ProvisionLosses:     parseOptInt64(get("ACCT_300")),
			Members:             parseOptInt(get("ACCT_083")),
			MemberBusinessLoans: parseOptInt64(get("ACCT_042")),
			Investments:         parseOptInt64(get("ACCT_008")),
		}
		result[cuNum] = fin
	}

	if err := <-errCh; err != nil {
		return nil, err
	}
	return result, nil
}

// processFOICU reads FOICU.txt, joins with financials, and upserts to the DB.
func (d *NCUACallReports) processFOICU(ctx context.Context, pool db.Pool, foicuPath string, financials map[int]*ncuaFinancials, _ *zap.Logger) (int64, error) {
	f, err := openFileForRead(foicuPath)
	if err != nil {
		return 0, err
	}
	defer f.Close() //nolint:errcheck

	rowCh, errCh := fetcher.StreamCSV(ctx, f, fetcher.CSVOptions{HasHeader: false, LazyQuotes: true})

	colIdx := make(map[string]int)
	first := true
	var batch [][]any
	var totalRows int64

	for record := range rowCh {
		if first {
			for i, col := range record {
				colIdx[strings.ToUpper(strings.TrimSpace(col))] = i
			}
			first = false
			continue
		}

		get := func(name string) string {
			if i, ok := colIdx[name]; ok && i < len(record) {
				return strings.TrimSpace(record[i])
			}
			return ""
		}

		cuNumStr := get("CU_NUMBER")
		if cuNumStr == "" {
			continue
		}
		cuNum, parseErr := strconv.Atoi(cuNumStr)
		if parseErr != nil {
			continue
		}

		cycleDateStr := get("CYCLE_DATE")
		cycleDate := parseNCUADate(cycleDateStr)
		if cycleDate.IsZero() {
			continue
		}

		cuName := get("CU_NAME")
		if cuName == "" {
			continue
		}

		fin := financials[cuNum]
		if fin == nil {
			fin = &ncuaFinancials{}
		}

		// Compute net_worth_ratio = net_worth / total_assets.
		var netWorthRatio *float64
		if fin.NetWorth != nil && fin.TotalAssets != nil && *fin.TotalAssets > 0 {
			r := float64(*fin.NetWorth) / float64(*fin.TotalAssets) * 100
			netWorthRatio = &r
		}

		row := []any{
			cuNum,
			cycleDate,
			cuName,
			nilIfEmpty(get("STREET")),
			nilIfEmpty(get("CITY")),
			nilIfEmpty(get("STATE")),
			nilIfEmpty(get("ZIP_CODE")),
			nilIfEmpty(get("COUNTY_CODE")),
			nilInt16(get("CU_TYPE")),
			nilInt16(get("REGION")),
			nilInt16(get("Peer_Group")),
			anyOrNil(fin.TotalAssets),
			anyOrNil(fin.TotalLoans),
			anyOrNil(fin.TotalShares),
			anyOrNil(fin.TotalBorrowings),
			anyOrNil(fin.NetWorth),
			anyOrNil(fin.NetIncome),
			anyOrNil(fin.GrossIncome),
			anyOrNil(fin.TotalExpenses),
			anyOrNil(fin.ProvisionLosses),
			anyOrNilInt(fin.Members),
			anyOrNilFloat(netWorthRatio),
			anyOrNil(fin.MemberBusinessLoans),
			anyOrNil(fin.Investments),
			nil, // num_employees — not available in NCUA data
		}

		batch = append(batch, row)

		if len(batch) >= ncuaBatchSize {
			n, upsertErr := db.BulkUpsert(ctx, pool, ncuaUpsertCfg(), batch)
			if upsertErr != nil {
				return totalRows, eris.Wrap(upsertErr, "upsert batch")
			}
			totalRows += n
			batch = batch[:0]
		}
	}

	if err := <-errCh; err != nil {
		return totalRows, eris.Wrap(err, "stream FOICU")
	}

	if len(batch) > 0 {
		n, upsertErr := db.BulkUpsert(ctx, pool, ncuaUpsertCfg(), batch)
		if upsertErr != nil {
			return totalRows, eris.Wrap(upsertErr, "upsert final batch")
		}
		totalRows += n
	}

	return totalRows, nil
}

var ncuaCols = []string{
	"cu_number", "cycle_date", "cu_name",
	"street", "city", "state", "zip_code", "county",
	"cu_type", "region", "peer_group",
	"total_assets", "total_loans", "total_shares", "total_borrowings",
	"net_worth", "net_income", "gross_income", "total_expenses",
	"provision_losses", "members", "net_worth_ratio",
	"member_business_loans", "investments", "num_employees",
}

func ncuaUpsertCfg() db.UpsertConfig {
	return db.UpsertConfig{
		Table:        "fed_data.ncua_call_reports",
		Columns:      ncuaCols,
		ConflictKeys: []string{"cu_number", "cycle_date"},
	}
}

// parseNCUADate handles both MM/DD/YYYY HH:MM:SS, MM/DD/YYYY, and YYYY-MM-DD date formats.
func parseNCUADate(s string) time.Time {
	// Strip time component if present (e.g., "6/30/2025 0:00:00").
	if idx := strings.Index(s, " "); idx > 0 {
		s = s[:idx]
	}
	for _, layout := range []string{"01/02/2006", "1/2/2006", "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

// parseOptInt64 parses s as int64, returning nil if empty or unparseable.
func parseOptInt64(s string) *int64 {
	if s == "" {
		return nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil
	}
	return &v
}

// parseOptInt parses s as int, returning nil if empty or unparseable.
func parseOptInt(s string) *int {
	if s == "" {
		return nil
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return nil
	}
	return &v
}

// anyOrNil converts a *int64 to any, returning nil if the pointer is nil.
func anyOrNil(v *int64) any {
	if v == nil {
		return nil
	}
	return *v
}

// anyOrNilInt converts a *int to any, returning nil if the pointer is nil.
func anyOrNilInt(v *int) any {
	if v == nil {
		return nil
	}
	return *v
}

// anyOrNilFloat converts a *float64 to any, returning nil if the pointer is nil.
func anyOrNilFloat(v *float64) any {
	if v == nil {
		return nil
	}
	return *v
}

// nilInt16 parses s as an int16, returning nil if empty or unparseable.
func nilInt16(s string) any {
	if s == "" {
		return nil
	}
	v, err := strconv.ParseInt(s, 10, 16)
	if err != nil {
		return nil
	}
	return int16(v)
}
