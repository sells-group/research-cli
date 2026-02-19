package dataset

import (
	"archive/zip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
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
	advBatchSize = 10000

	// SEC IAPD reports API — effective January 2025.
	// The SEC migrated FOIA data from sec.gov to reports.adviserinfo.sec.gov.
	// Metadata endpoint lists all available FOIA data files (monthly cadence).
	foiaMetadataURL = "https://reports.adviserinfo.sec.gov/reports/foia/reports_metadata.json"
	foiaBaseURL     = "https://reports.adviserinfo.sec.gov/reports/foia"

	// Historical ADV filing data ZIPs (pre-2025 archive on sec.gov).
	// These may be blocked by SEC's WAF; kept as best-effort for --full historical loads.
	advHistPart1URL = "https://www.sec.gov/files/adv-filing-data-20111105-20241231-part1.zip"
	advHistPart2URL = "https://www.sec.gov/files/adv-filing-data-20111105-20241231-part2.zip"
)

// advFilingCols defines the full set of columns for adv_filings upserts.
// Shared by streamBaseFile to extract all available columns from any CSV format.
var advFilingCols = []string{
	"crd_number", "filing_date", "aum", "raum", "num_accounts", "num_employees",
	"legal_name", "form_of_org", "num_other_offices",
	"total_employees", "num_adviser_reps", "client_types",
	// 5E: Compensation (7)
	"comp_pct_aum", "comp_hourly", "comp_subscription", "comp_fixed",
	"comp_commissions", "comp_performance", "comp_other",
	// 5F: AUM breakdown (3)
	"aum_discretionary", "aum_non_discretionary", "aum_total",
	// 5G: Advisory services (12)
	"svc_financial_planning", "svc_portfolio_individuals", "svc_portfolio_inv_cos",
	"svc_portfolio_pooled", "svc_portfolio_institutional", "svc_pension_consulting",
	"svc_adviser_selection", "svc_periodicals", "svc_security_ratings",
	"svc_market_timing", "svc_seminars", "svc_other",
	// 5I/5J: Wrap fee + financial planning
	"wrap_fee_program", "wrap_fee_raum", "financial_planning_clients",
	// 6A: Other business (14)
	"biz_broker_dealer", "biz_registered_rep", "biz_cpo_cta",
	"biz_futures_commission", "biz_real_estate", "biz_insurance",
	"biz_bank", "biz_trust_company", "biz_municipal_advisor",
	"biz_swap_dealer", "biz_major_swap", "biz_accountant",
	"biz_lawyer", "biz_other_financial",
	// 7A: Affiliations (16)
	"aff_broker_dealer", "aff_other_adviser", "aff_municipal_advisor",
	"aff_swap_dealer", "aff_major_swap", "aff_cpo_cta",
	"aff_futures_commission", "aff_bank", "aff_trust_company",
	"aff_accountant", "aff_lawyer", "aff_insurance",
	"aff_pension_consultant", "aff_real_estate", "aff_lp_sponsor",
	"aff_pooled_vehicle",
	// Item 2: Registration
	"sec_registered", "exempt_reporting", "state_registered",
	// 5L: Discretionary
	"discretionary_authority",
	// Item 8: Transactions (10)
	"txn_proprietary_interest", "txn_sells_own_securities",
	"txn_buys_from_clients", "txn_recommends_own",
	"txn_recommends_broker", "txn_agency_cross",
	"txn_principal", "txn_referral_compensation",
	"txn_other_research", "txn_revenue_sharing",
	// Item 9: Custody (5)
	"custody_client_cash", "custody_client_securities",
	"custody_related_person", "custody_qualified_custodian",
	"custody_surprise_exam",
	// Item 11: DRP (14)
	"drp_criminal_firm", "drp_criminal_affiliate",
	"drp_regulatory_firm", "drp_regulatory_affiliate",
	"drp_civil_firm", "drp_civil_affiliate",
	"drp_complaint_firm", "drp_complaint_affiliate",
	"drp_termination_firm", "drp_termination_affiliate",
	"drp_judgment", "drp_financial_firm", "drp_financial_affiliate",
	"has_any_drp",
}
var advFilingConflict = []string{"crd_number", "filing_date"}

// ADVPart1 implements the SEC ADV Part 1A dataset.
// Weekly FOIA roster → adv_firms (identity) + adv_filings (metrics/details).
// Historical --full → adv_firms + adv_owners + adv_private_funds.
type ADVPart1 struct{}

func (d *ADVPart1) Name() string     { return "adv_part1" }
func (d *ADVPart1) Table() string    { return "fed_data.adv_firms" }
func (d *ADVPart1) Phase() Phase     { return Phase1B }
func (d *ADVPart1) Cadence() Cadence { return Monthly }

func (d *ADVPart1) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return MonthlySchedule(now, lastSync)
}

// Sync downloads the monthly FOIA filing data and upserts adv_firms.
// Uses the IAPD reports metadata API to discover the latest available data file.
// The monthly FOIA ZIP contains ERA_ADV_Base_*.csv with filing-level data (columns: 1E1, 1A, 1D, etc.).
func (d *ADVPart1) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "adv_part1"))

	// Fetch IAPD reports metadata to find the latest ADV filing data URL.
	meta, err := fetchFOIAMetadata(ctx, f)
	if err != nil {
		return nil, eris.Wrap(err, "adv_part1: fetch FOIA metadata")
	}

	url, err := latestFileURL(meta.ADVFilingData, "advFilingData")
	if err != nil {
		return nil, eris.Wrap(err, "adv_part1: resolve FOIA URL")
	}

	firms, filings, err := syncFOIAMonth(ctx, pool, f, tempDir, url, log)
	if err != nil {
		return nil, err
	}

	return &SyncResult{
		RowsSynced: firms,
		Metadata:   map[string]any{"firms": firms, "filings": filings},
	}, nil
}

// syncFOIAMonth downloads one FOIA filing data ZIP, extracts base CSVs, and upserts
// firms + filings. Returns (firms, filings, error).
func syncFOIAMonth(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir, url string, log *zap.Logger) (int64, int64, error) {
	log.Info("downloading ADV filing data", zap.String("url", url))

	zipPath := filepath.Join(tempDir, "adv_filing_data.zip")
	if _, err := f.DownloadToFile(ctx, url, zipPath); err != nil {
		return 0, 0, eris.Wrapf(err, "adv_part1: download %s", url)
	}
	defer os.Remove(zipPath)

	// Extract ZIP to temp dir.
	extractDir := filepath.Join(tempDir, "adv_filing_extract")
	if err := os.MkdirAll(extractDir, 0o755); err != nil {
		return 0, 0, eris.Wrap(err, "adv_part1: create extract dir")
	}
	defer os.RemoveAll(extractDir)

	extractedFiles, err := fetcher.ExtractZIP(zipPath, extractDir)
	if err != nil {
		return 0, 0, eris.Wrapf(err, "adv_part1: extract %s", url)
	}

	// Find base files with firm-level data (ERA_ADV_Base, IA_ADV_Base_A).
	// Exclude IA_ADV_Base_B which only has Item 2 data and is handled separately.
	var baseFiles []string
	for _, fp := range extractedFiles {
		base := strings.ToLower(filepath.Base(fp))
		if strings.Contains(base, "base_b") {
			continue // Item 2 file — handled separately via buildBaseBMap
		}
		if strings.Contains(base, "adv_base") || strings.Contains(base, "era_adv_base") {
			baseFiles = append(baseFiles, fp)
		}
	}
	if len(baseFiles) == 0 {
		return 0, 0, eris.Errorf("adv_part1: no ADV base CSV found in %s", url)
	}

	// Monthly files don't need latest-filing filtering (all rows are current month).
	// Build an all-pass latestFiling map from the base file.
	latestFiling := make(map[string]int64)
	for _, path := range baseFiles {
		if err := buildLatestFilingMap(path, latestFiling); err != nil {
			return 0, 0, eris.Wrapf(err, "adv_part1: build filing map from %s", filepath.Base(path))
		}
	}
	log.Info("monthly filing data", zap.Int("firms", len(latestFiling)))

	// Also look for Schedule D 1I (website data) if present.
	websiteMap := make(map[int64]string)
	for _, fp := range extractedFiles {
		base := strings.ToLower(filepath.Base(fp))
		if strings.Contains(base, "schedule_d_1i") {
			if err := buildWebsiteMap(fp, websiteMap, latestFiling); err != nil {
				log.Warn("failed to build website map", zap.Error(err))
			}
		}
	}

	// Load IA_ADV_Base_B (Item 2 registration data) keyed by FilingID.
	var baseBMap map[int64]map[string]string
	for _, fp := range extractedFiles {
		base := strings.ToLower(filepath.Base(fp))
		if strings.Contains(base, "ia_adv_base_b") || strings.Contains(base, "adv_base_b") {
			m, err := buildBaseBMap(fp)
			if err != nil {
				log.Warn("failed to build Base_B map", zap.Error(err))
			} else {
				baseBMap = m
				log.Info("loaded Base_B registration data", zap.Int("filings", len(m)))
			}
		}
	}

	var totalFirms, totalFilings int64
	for _, path := range baseFiles {
		firms, filings, err := streamBaseFile(ctx, pool, path, latestFiling, websiteMap, baseBMap, log)
		if err != nil {
			return 0, 0, eris.Wrapf(err, "adv_part1: load firms from %s", filepath.Base(path))
		}
		totalFirms += firms
		totalFilings += filings
	}

	log.Info("adv_part1 FOIA month complete", zap.Int64("firms", totalFirms), zap.Int64("filings", totalFilings))
	return totalFirms, totalFilings, nil
}

// clientTypeEntry represents one client type category from Item 5D.
type clientTypeEntry struct {
	Type    string `json:"type"`
	Count   int    `json:"count"`
	PctRAUM string `json:"pct_raum"`
	RAUM    int64  `json:"raum"`
}

// clientTypeCategory maps a label to the FOIA column suffix letter for Item 5D.
var clientTypeCategories = []struct {
	Label  string
	Suffix string
}{
	{"Individuals (other than high net worth)", "a"},
	{"High net worth individuals", "b"},
	{"Banking or thrift institutions", "c"},
	{"Investment companies", "d"},
	{"Business development companies", "e"},
	{"Pooled investment vehicles", "f"},
	{"Pension and profit sharing plans", "g"},
	{"Charitable organizations", "h"},
	{"State or municipal government entities", "i"},
	{"Other investment advisers", "j"},
	{"Insurance companies", "k"},
	{"Sovereign wealth funds", "l"},
	{"Corporations or other businesses", "m"},
	{"Other", "n"},
}

// buildClientTypesJSON builds a JSONB array from the 14 Item 5D client type categories.
// Each category has sub-columns: 5D(1)(x) (count), 5D(2)(x) (% RAUM), 5D(3)(x) ($ RAUM).
// Only non-zero entries are included. Returns nil if no entries.
// colIdx must be built with mapColumnsNormalized.
func buildClientTypesJSON(record []string, colIdx map[string]int) json.RawMessage {
	var entries []clientTypeEntry

	for _, cat := range clientTypeCategories {
		countStr := trimQuotes(getColN(record, colIdx, fmt.Sprintf("5d(1)(%s)", cat.Suffix)))
		pctStr := trimQuotes(getColN(record, colIdx, fmt.Sprintf("5d(2)(%s)", cat.Suffix)))
		raumStr := trimQuotes(getColN(record, colIdx, fmt.Sprintf("5d(3)(%s)", cat.Suffix)))

		count := parseIntOr(countStr, 0)
		raum := parseAUM(raumStr)

		if count == 0 && raum == 0 && pctStr == "" {
			continue
		}

		entries = append(entries, clientTypeEntry{
			Type:    cat.Label,
			Count:   count,
			PctRAUM: pctStr,
			RAUM:    raum,
		})
	}

	if len(entries) == 0 {
		return nil
	}

	data, err := json.Marshal(entries)
	if err != nil {
		return nil
	}
	return data
}

// buildFilingRow extracts all filing detail columns from a CSV record using normalized column lookup.
// Works for both FOIA roster (parenthesized headers) and base file (no-paren headers) formats.
// extra is optional supplementary column data (e.g., Item 2 from IA_ADV_Base_B), nil if not available.
func buildFilingRow(record []string, colIdx map[string]int, crd int, filingDate *time.Time, extra map[string]string) []any {
	aumVal := parseAUM(trimQuotes(getColN(record, colIdx, "5f(2)(c)")))
	numAccounts := parseIntOr(trimQuotes(getColN(record, colIdx, "5f(2)(f)")), 0)
	// Try 5H (range code like "11-25") first, then 5A (raw count) as fallback.
	numEmployees := parseEmployeeRange(trimQuotes(getColN(record, colIdx, "5h")))
	if numEmployees == 0 {
		numEmployees = parseIntOr(trimQuotes(getColN(record, colIdx, "5a")), 0)
	}

	// DRP flags — parse individually then aggregate.
	drpCrimFirm := anyBoolYN(record, colIdx, "11a(1)")
	drpCrimAff := anyBoolYN(record, colIdx, "11a(2)")
	drpRegFirm := anyBoolYN(record, colIdx, "11b(1)")
	drpRegAff := anyBoolYN(record, colIdx, "11b(2)")
	drpCivilFirm := anyBoolYN(record, colIdx, "11c(1)")
	drpCivilAff := anyBoolYN(record, colIdx, "11c(2)")
	drpComplFirm := anyBoolYN(record, colIdx, "11d(1)")
	drpComplAff := anyBoolYN(record, colIdx, "11d(2)")
	drpTermFirm := anyBoolYN(record, colIdx, "11e(1)")
	drpTermAff := anyBoolYN(record, colIdx, "11e(2)")
	drpJudgment := anyBoolYN(record, colIdx, "11f")
	drpFinFirm := anyBoolYN(record, colIdx, "11h(1)")
	drpFinAff := anyBoolYN(record, colIdx, "11h(2)")
	hasAnyDRP := drpCrimFirm || drpCrimAff || drpRegFirm || drpRegAff ||
		drpCivilFirm || drpCivilAff || drpComplFirm || drpComplAff ||
		drpTermFirm || drpTermAff || drpJudgment || drpFinFirm || drpFinAff

	clientTypes := buildClientTypesJSON(record, colIdx)

	return []any{
		crd, filingDate, aumVal, aumVal, numAccounts, numEmployees,
		// Legal name: FOIA="Legal Name", ERA/IA="1A", historical="1C-Legal"
		sanitizeUTF8(firstNonEmpty(record, colIdx, "legal name", "1a", "1c-legal")),
		firstNonEmpty(record, colIdx, "form of organization", "1c-formorg"),
		parseIntOr(trimQuotes(getColN(record, colIdx, "1i")), 0),
		// 5A: Total employees + adviser reps
		numEmployees,
		parseIntOr(trimQuotes(getColN(record, colIdx, "5a(2)")), 0),
		clientTypes,
		// 5E: Compensation (7)
		parseBoolYN(getColN(record, colIdx, "5e(1)")),
		parseBoolYN(getColN(record, colIdx, "5e(2)")),
		parseBoolYN(getColN(record, colIdx, "5e(3)")),
		parseBoolYN(getColN(record, colIdx, "5e(4)")),
		parseBoolYN(getColN(record, colIdx, "5e(5)")),
		parseBoolYN(getColN(record, colIdx, "5e(6)")),
		parseBoolYN(getColN(record, colIdx, "5e(7)")),
		// 5F: AUM breakdown (3)
		parseAUM(trimQuotes(getColN(record, colIdx, "5f(2)(a)"))),
		parseAUM(trimQuotes(getColN(record, colIdx, "5f(2)(b)"))),
		parseAUM(trimQuotes(getColN(record, colIdx, "5f(2)(c)"))),
		// 5G: Advisory services (12)
		parseBoolYN(getColN(record, colIdx, "5g(1)")),
		parseBoolYN(getColN(record, colIdx, "5g(2)")),
		parseBoolYN(getColN(record, colIdx, "5g(3)")),
		parseBoolYN(getColN(record, colIdx, "5g(4)")),
		parseBoolYN(getColN(record, colIdx, "5g(5)")),
		parseBoolYN(getColN(record, colIdx, "5g(6)")),
		parseBoolYN(getColN(record, colIdx, "5g(7)")),
		parseBoolYN(getColN(record, colIdx, "5g(8)")),
		parseBoolYN(getColN(record, colIdx, "5g(9)")),
		parseBoolYN(getColN(record, colIdx, "5g(10)")),
		parseBoolYN(getColN(record, colIdx, "5g(11)")),
		parseBoolYN(getColN(record, colIdx, "5g(12)")),
		// 5I/5J: Wrap fee + financial planning (3)
		parseBoolYN(getColN(record, colIdx, "5i(1)")),
		parseAUM(trimQuotes(getColN(record, colIdx, "5i(2)"))),
		parseIntOr(trimQuotes(getColN(record, colIdx, "5j")), 0),
		// 6A: Other business (14)
		parseBoolYN(getColN(record, colIdx, "6a(1)")),
		parseBoolYN(getColN(record, colIdx, "6a(2)")),
		parseBoolYN(getColN(record, colIdx, "6a(3)")),
		parseBoolYN(getColN(record, colIdx, "6a(4)")),
		parseBoolYN(getColN(record, colIdx, "6a(5)")),
		parseBoolYN(getColN(record, colIdx, "6a(6)")),
		parseBoolYN(getColN(record, colIdx, "6a(7)")),
		parseBoolYN(getColN(record, colIdx, "6a(8)")),
		parseBoolYN(getColN(record, colIdx, "6a(9)")),
		parseBoolYN(getColN(record, colIdx, "6a(10)")),
		parseBoolYN(getColN(record, colIdx, "6a(11)")),
		parseBoolYN(getColN(record, colIdx, "6a(12)")),
		parseBoolYN(getColN(record, colIdx, "6a(13)")),
		parseBoolYN(getColN(record, colIdx, "6a(14)")),
		// 7A: Affiliations (16)
		parseBoolYN(getColN(record, colIdx, "7a(1)")),
		parseBoolYN(getColN(record, colIdx, "7a(2)")),
		parseBoolYN(getColN(record, colIdx, "7a(3)")),
		parseBoolYN(getColN(record, colIdx, "7a(4)")),
		parseBoolYN(getColN(record, colIdx, "7a(5)")),
		parseBoolYN(getColN(record, colIdx, "7a(6)")),
		parseBoolYN(getColN(record, colIdx, "7a(7)")),
		parseBoolYN(getColN(record, colIdx, "7a(8)")),
		parseBoolYN(getColN(record, colIdx, "7a(9)")),
		parseBoolYN(getColN(record, colIdx, "7a(10)")),
		parseBoolYN(getColN(record, colIdx, "7a(11)")),
		parseBoolYN(getColN(record, colIdx, "7a(12)")),
		parseBoolYN(getColN(record, colIdx, "7a(13)")),
		parseBoolYN(getColN(record, colIdx, "7a(14)")),
		parseBoolYN(getColN(record, colIdx, "7a(15)")),
		parseBoolYN(getColN(record, colIdx, "7a(16)")),
		// Item 2: Registration — may come from main record (FOIA roster) or extra (IA_ADV_Base_B).
		parseBoolYN(getColNOrExtra(record, colIdx, extra, "2a(1)")),
		parseBoolYN(getColNOrExtra(record, colIdx, extra, "2a(2)")),
		parseBoolYN(getColNOrExtra(record, colIdx, extra, "2a(12)")),
		// 5L: Discretionary — FOIA has single "5L"; base file has sub-items 5L1a-5L1e
		anyBoolYN(record, colIdx, "5l", "5l1a", "5l1b", "5l1c", "5l1d", "5l1e"),
		// Item 8: Transactions — use anyBoolYN for base file sub-items (10)
		anyBoolYN(record, colIdx, "8a", "8a(1)", "8a(2)", "8a(3)"),
		anyBoolYN(record, colIdx, "8b(1)"),
		anyBoolYN(record, colIdx, "8b(2)"),
		anyBoolYN(record, colIdx, "8b(3)"),
		anyBoolYN(record, colIdx, "8b(4)"),
		anyBoolYN(record, colIdx, "8c", "8c1", "8c2", "8c3", "8c4"),
		anyBoolYN(record, colIdx, "8d"),
		anyBoolYN(record, colIdx, "8e"),
		anyBoolYN(record, colIdx, "8f"),
		anyBoolYN(record, colIdx, "8h", "8h1", "8h2"),
		// Item 9: Custody — FOIA has "9A(1)"; base file has sub-items 9A1a, 9A1b (5)
		anyBoolYN(record, colIdx, "9a(1)", "9a1a", "9a1b"),
		anyBoolYN(record, colIdx, "9a(2)", "9a2a", "9a2b"),
		anyBoolYN(record, colIdx, "9b(1)", "9b1a", "9b1b"),
		anyBoolYN(record, colIdx, "9b(2)", "9b2a", "9b2b"),
		anyBoolYN(record, colIdx, "9c", "9c1", "9c2", "9c3", "9c4"),
		// Item 11: DRP (14)
		drpCrimFirm, drpCrimAff, drpRegFirm, drpRegAff,
		drpCivilFirm, drpCivilAff, drpComplFirm, drpComplAff,
		drpTermFirm, drpTermAff, drpJudgment, drpFinFirm, drpFinAff, hasAnyDRP,
	}
}

// SyncFull performs a full historical load:
//  1. Tries the Part 1 + Part 2 bulk ZIPs from sec.gov (pre-2025 archive).
//  2. Processes ALL available FOIA monthly filing data ZIPs (2025+).
//
// The sec.gov historical ZIPs may be blocked by WAF — failures are logged and skipped.
func (d *ADVPart1) SyncFull(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "adv_part1"), zap.String("mode", "full"))

	var totalFirms, totalOwners, totalFunds, totalFilings int64

	// --- Phase A: Historical sec.gov ZIPs (best-effort) ---
	histFirms, histOwners, histFunds, err := d.syncHistorical(ctx, pool, f, tempDir, log)
	if err != nil {
		log.Warn("historical sec.gov ZIPs failed (may be WAF-blocked), continuing with FOIA months", zap.Error(err))
	} else {
		totalFirms += histFirms
		totalOwners += histOwners
		totalFunds += histFunds
	}

	// --- Phase B: All available FOIA monthly filing data ZIPs ---
	meta, err := fetchFOIAMetadata(ctx, f)
	if err != nil {
		return nil, eris.Wrap(err, "adv_part1: fetch FOIA metadata")
	}

	if len(meta.ADVFilingData) == 0 {
		log.Warn("no FOIA filing data entries found in metadata")
	} else {
		log.Info("processing all FOIA filing data months", zap.Int("months", len(meta.ADVFilingData)))

		for i, entry := range meta.ADVFilingData {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}

			url := fileURL(entry, "advFilingData")
			monthLog := log.With(
				zap.String("month", entry.DisplayName),
				zap.Int("index", i+1),
				zap.Int("total", len(meta.ADVFilingData)),
			)

			// Use a per-month temp subdirectory to avoid file collisions.
			monthDir := filepath.Join(tempDir, fmt.Sprintf("foia_month_%d", i))
			if err := os.MkdirAll(monthDir, 0o755); err != nil {
				return nil, eris.Wrapf(err, "adv_part1: create month dir %d", i)
			}

			firms, filings, err := syncFOIAMonth(ctx, pool, f, monthDir, url, monthLog)
			os.RemoveAll(monthDir)
			if err != nil {
				monthLog.Error("failed to process FOIA month, continuing", zap.Error(err))
				continue
			}
			totalFirms += firms
			totalFilings += filings
		}
	}

	log.Info("adv_part1 full sync complete",
		zap.Int64("firms", totalFirms),
		zap.Int64("filings", totalFilings),
		zap.Int64("owners", totalOwners),
		zap.Int64("funds", totalFunds),
	)

	return &SyncResult{
		RowsSynced: totalFirms,
		Metadata: map[string]any{
			"firms":   totalFirms,
			"filings": totalFilings,
			"owners":  totalOwners,
			"funds":   totalFunds,
		},
	}, nil
}

// syncHistorical downloads and processes the pre-2025 bulk ZIPs from sec.gov.
func (d *ADVPart1) syncHistorical(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string, log *zap.Logger) (firms, owners, funds int64, err error) {
	part1Dir := filepath.Join(tempDir, "adv_part1")
	part2Dir := filepath.Join(tempDir, "adv_part2")
	if err := os.MkdirAll(part1Dir, 0o755); err != nil {
		return 0, 0, 0, eris.Wrap(err, "adv_part1: create part1 dir")
	}
	if err := os.MkdirAll(part2Dir, 0o755); err != nil {
		return 0, 0, 0, eris.Wrap(err, "adv_part1: create part2 dir")
	}

	// Download Part 1 ZIP.
	log.Info("downloading historical Part 1 ZIP")
	part1Zip := filepath.Join(tempDir, "adv_hist_part1.zip")
	if _, err := f.DownloadToFile(ctx, advHistPart1URL, part1Zip); err != nil {
		return 0, 0, 0, eris.Wrap(err, "adv_part1: download historical part1 ZIP")
	}
	defer os.Remove(part1Zip)

	// Download Part 2 ZIP.
	log.Info("downloading historical Part 2 ZIP")
	part2Zip := filepath.Join(tempDir, "adv_hist_part2.zip")
	if _, err := f.DownloadToFile(ctx, advHistPart2URL, part2Zip); err != nil {
		return 0, 0, 0, eris.Wrap(err, "adv_part1: download historical part2 ZIP")
	}
	defer os.Remove(part2Zip)

	// Extract Part 1 ZIP.
	log.Info("extracting Part 1 ZIP")
	part1Files, err2 := fetcher.ExtractZIP(part1Zip, part1Dir)
	if err2 != nil {
		return 0, 0, 0, eris.Wrap(err2, "adv_part1: extract part1 ZIP")
	}
	defer os.RemoveAll(part1Dir)

	// Extract Part 2 ZIP.
	log.Info("extracting Part 2 ZIP")
	part2Files, err2 := fetcher.ExtractZIP(part2Zip, part2Dir)
	if err2 != nil {
		return 0, 0, 0, eris.Wrap(err2, "adv_part1: extract part2 ZIP")
	}
	defer os.RemoveAll(part2Dir)

	// Pass 1: Build latestFiling map (CRD → max FilingID) from base files.
	log.Info("pass 1: building latest filing map")
	latestFiling := make(map[string]int64)
	for _, path := range part1Files {
		base := strings.ToLower(filepath.Base(path))
		if strings.Contains(base, "base_b") {
			continue
		}
		if strings.Contains(base, "adv_base") || strings.Contains(base, "era_adv_base") {
			if err := buildLatestFilingMap(path, latestFiling); err != nil {
				return 0, 0, 0, eris.Wrapf(err, "adv_part1: build latest filing from %s", filepath.Base(path))
			}
		}
	}
	log.Info("latest filing map built", zap.Int("firms", len(latestFiling)))

	// Pass 1b: Build website map from Schedule D 1I.
	websiteMap := make(map[int64]string)
	for _, path := range part1Files {
		base := strings.ToLower(filepath.Base(path))
		if strings.Contains(base, "schedule_d_1i") {
			if err := buildWebsiteMap(path, websiteMap, latestFiling); err != nil {
				log.Warn("failed to build website map", zap.Error(err))
			}
		}
	}

	// Pass 1c: Load Base_B (Item 2 registration data) keyed by FilingID.
	var baseBMap map[int64]map[string]string
	for _, path := range part1Files {
		base := strings.ToLower(filepath.Base(path))
		if strings.Contains(base, "ia_adv_base_b") || strings.Contains(base, "adv_base_b") {
			m, err := buildBaseBMap(path)
			if err != nil {
				log.Warn("failed to build Base_B map", zap.Error(err))
			} else {
				baseBMap = m
				log.Info("loaded Base_B registration data", zap.Int("filings", len(m)))
			}
		}
	}

	// Pass 2: Emit firm rows from base files using latestFiling filter.
	log.Info("pass 2: loading firms")
	firms, err2 = d.loadFirmsFromBase(ctx, pool, part1Files, latestFiling, websiteMap, baseBMap, log)
	if err2 != nil {
		return 0, 0, 0, err2
	}

	// Pass 3: Load owners from Schedule A/B files.
	log.Info("pass 3: loading owners")
	owners, err2 = d.loadOwners(ctx, pool, part1Files, latestFiling, log)
	if err2 != nil {
		return 0, 0, 0, err2
	}

	// Pass 4: Load private funds from Schedule D 7B1 (Part 1 + Part 2).
	log.Info("pass 4: loading private funds")
	allFiles := append(part1Files, part2Files...)
	funds, err2 = d.loadPrivateFunds(ctx, pool, allFiles, latestFiling, log)
	if err2 != nil {
		return 0, 0, 0, err2
	}

	return firms, owners, funds, nil
}

// loadFirmsFromBase streams base CSV files, filters to latest filing per CRD, and upserts.
func (d *ADVPart1) loadFirmsFromBase(ctx context.Context, pool db.Pool, files []string, latestFiling map[string]int64, websiteMap map[int64]string, baseBMap map[int64]map[string]string, log *zap.Logger) (int64, error) {
	var totalFirms int64

	for _, path := range files {
		base := strings.ToLower(filepath.Base(path))
		if strings.Contains(base, "base_b") {
			continue
		}
		if !strings.Contains(base, "adv_base") && !strings.Contains(base, "era_adv_base") {
			continue
		}

		firms, _, err := streamBaseFile(ctx, pool, path, latestFiling, websiteMap, baseBMap, log)
		if err != nil {
			return totalFirms, eris.Wrapf(err, "adv_part1: load firms from %s", filepath.Base(path))
		}
		totalFirms += firms
	}

	return totalFirms, nil
}

// loadOwners streams Schedule A/B files, filters to latest filing, upserts owners.
func (d *ADVPart1) loadOwners(ctx context.Context, pool db.Pool, files []string, latestFiling map[string]int64, log *zap.Logger) (int64, error) {
	ownerCols := []string{"crd_number", "owner_name", "owner_type", "ownership_pct", "is_control"}
	ownerConflict := []string{"crd_number", "owner_name"}

	var total int64
	for _, path := range files {
		base := strings.ToLower(filepath.Base(path))
		if !strings.Contains(base, "schedule_a_b") {
			continue
		}

		n, err := streamOwnerFile(ctx, pool, path, latestFiling, ownerCols, ownerConflict, log)
		if err != nil {
			return total, eris.Wrapf(err, "adv_part1: load owners from %s", filepath.Base(path))
		}
		total += n
	}

	return total, nil
}

// loadPrivateFunds streams Schedule D 7B1 files, filters to latest filing, upserts funds.
func (d *ADVPart1) loadPrivateFunds(ctx context.Context, pool db.Pool, files []string, latestFiling map[string]int64, log *zap.Logger) (int64, error) {
	fundCols := []string{"crd_number", "fund_id", "fund_name", "fund_type", "gross_asset_value", "net_asset_value"}
	fundConflict := []string{"crd_number", "fund_id"}

	var total int64
	for _, path := range files {
		base := strings.ToLower(filepath.Base(path))
		if !strings.Contains(base, "schedule_d_7b1") {
			continue
		}

		n, err := streamFundFile(ctx, pool, path, latestFiling, fundCols, fundConflict, log)
		if err != nil {
			return total, eris.Wrapf(err, "adv_part1: load funds from %s", filepath.Base(path))
		}
		total += n
	}

	return total, nil
}

// --- FOIA metadata helpers ---

// foiaReportsMetadata represents the reports_metadata.json structure from the SEC IAPD system.
// The raw JSON nests files under year keys (e.g. "2025": {"files": [...]}), so we
// flatten all years into a single slice per section.
type foiaReportsMetadata struct {
	ADVFilingData  []foiaFileEntry
	ADVBrochures   []foiaFileEntry
	ADVFirmCRSDocs []foiaFileEntry
	ADVFirmCRS     []foiaFileEntry
}

// foiaFileEntry represents a single file entry in the reports metadata.
type foiaFileEntry struct {
	DisplayName string `json:"displayName"`
	FileName    string `json:"fileName"`
	Year        string `json:"year"`
	FileType    string `json:"fileType"`
	UploadedOn  string `json:"uploadedOn"`
}

// foiaYearFiles is the container for files within a year sub-key.
type foiaYearFiles struct {
	Files []foiaFileEntry `json:"files"`
}

// extractFOIAFiles flattens a section's year-keyed file lists into a single slice.
// The raw structure is: {"sectionDisplayName": "...", "2025": {"files": [...]}, "2026": {"files": [...]}}.
func extractFOIAFiles(raw json.RawMessage) []foiaFileEntry {
	var section map[string]json.RawMessage
	if err := json.Unmarshal(raw, &section); err != nil {
		return nil
	}

	var result []foiaFileEntry
	for key, val := range section {
		// Year keys are numeric (e.g. "2024", "2025", "2026").
		if len(key) != 4 || key[0] < '0' || key[0] > '9' {
			continue
		}
		var yf foiaYearFiles
		if err := json.Unmarshal(val, &yf); err != nil {
			continue
		}
		result = append(result, yf.Files...)
	}
	return result
}

// fetchFOIAMetadata downloads and parses the IAPD reports metadata JSON.
func fetchFOIAMetadata(ctx context.Context, f fetcher.Fetcher) (*foiaReportsMetadata, error) {
	rc, err := f.Download(ctx, foiaMetadataURL)
	if err != nil {
		return nil, eris.Wrap(err, "fetch FOIA metadata")
	}
	defer rc.Close()

	var raw map[string]json.RawMessage
	if err := json.NewDecoder(rc).Decode(&raw); err != nil {
		return nil, eris.Wrap(err, "parse FOIA metadata JSON")
	}

	meta := &foiaReportsMetadata{
		ADVFilingData:  extractFOIAFiles(raw["advFilingData"]),
		ADVBrochures:   extractFOIAFiles(raw["advBrochures"]),
		ADVFirmCRSDocs: extractFOIAFiles(raw["advFirmCRSDocs"]),
		ADVFirmCRS:     extractFOIAFiles(raw["advFirmCRS"]),
	}
	return meta, nil
}

// fileURL builds the download URL for a single FOIA entry.
func fileURL(e foiaFileEntry, fileType string) string {
	return fmt.Sprintf("%s/%s/%s/%s", foiaBaseURL, fileType, e.Year, e.FileName)
}

// latestFileURL returns the download URL for the most recently uploaded entry of a given file type.
func latestFileURL(entries []foiaFileEntry, fileType string) (string, error) {
	if len(entries) == 0 {
		return "", eris.Errorf("no %s entries in FOIA metadata", fileType)
	}

	latest := entries[0]
	for _, e := range entries[1:] {
		if e.UploadedOn > latest.UploadedOn {
			latest = e
		}
	}

	return fileURL(latest, fileType), nil
}

// --- Helper functions ---

// findCSVInZip extracts a ZIP and returns the path to the first CSV/TXT file found.
func findCSVInZip(zipPath, destDir string) (string, error) {
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return "", eris.Wrap(err, "open zip")
	}
	defer zr.Close()

	for _, zf := range zr.File {
		name := strings.ToLower(zf.Name)
		if strings.HasSuffix(name, ".csv") || strings.HasSuffix(name, ".txt") {
			rc, err := zf.Open()
			if err != nil {
				return "", eris.Wrapf(err, "open %s in zip", zf.Name)
			}

			outPath := filepath.Join(destDir, filepath.Base(zf.Name))
			out, err := os.Create(outPath)
			if err != nil {
				rc.Close()
				return "", eris.Wrap(err, "create output file")
			}

			_, err = io.Copy(out, rc)
			rc.Close()
			out.Close()
			if err != nil {
				return "", eris.Wrap(err, "extract file from zip")
			}
			return outPath, nil
		}
	}

	return "", eris.New("no CSV/TXT file found in ZIP")
}

// parseAUM strips commas and trailing ".00" then parses as int64.
func parseAUM(s string) int64 {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, ",", "")
	// Handle scientific notation (e.g., "1.30565E+12") — parse as float first.
	if strings.ContainsAny(s, "eE") {
		return int64(parseFloat64Or(s, 0))
	}
	if idx := strings.Index(s, "."); idx >= 0 {
		s = s[:idx]
	}
	return parseInt64Or(s, 0)
}

// parseEmployeeRange converts the 5H employee range code to a midpoint value.
func parseEmployeeRange(s string) int {
	s = strings.TrimSpace(s)
	switch s {
	case "", "0":
		return 0
	case "1-10":
		return 5
	case "11-25":
		return 18
	case "26-50":
		return 38
	case "51-100":
		return 75
	case "101-250":
		return 175
	case "251-500":
		return 375
	}
	if strings.Contains(strings.ToLower(s), "more than") {
		return 750
	}
	return parseIntOr(s, 0)
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
		"01/02/2006 03:04:05 PM", // IA_ADV_Base datetime with AM/PM (Sep 2025+)
		"1/2/2006 3:04:05 PM",    // single-digit month/day AM/PM variant
		"01/02/2006 15:04",       // 24-hour HH:MM without seconds
		"1/2/2006 15:04",         // single-digit month/day 24-hour variant (Jan-Aug 2025)
	}
	for _, fmt := range formats {
		if t, err := time.Parse(fmt, s); err == nil {
			return &t
		}
	}
	return nil
}

// buildLatestFilingMap scans a base CSV to find the max FilingID per CRD.
// Expects columns: FilingID (or Filing_ID), 1E1 (CRD number).
func buildLatestFilingMap(path string, out map[string]int64) error {
	f, err := os.Open(path)
	if err != nil {
		return eris.Wrap(err, "open base file")
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		return eris.Wrap(err, "read header")
	}
	colIdx := mapColumns(header)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		filingIDStr := trimQuotes(getCol(record, colIdx, "filingid"))
		if filingIDStr == "" {
			filingIDStr = trimQuotes(getCol(record, colIdx, "filing_id"))
		}
		filingID := parseInt64Or(filingIDStr, 0)
		if filingID == 0 {
			continue
		}

		crd := trimQuotes(getCol(record, colIdx, "1e1"))
		if crd == "" {
			continue
		}

		if filingID > out[crd] {
			out[crd] = filingID
		}
	}

	return nil
}

// buildWebsiteMap scans Schedule D 1I to find the first website per FilingID.
func buildWebsiteMap(path string, out map[int64]string, latestFiling map[string]int64) error {
	f, err := os.Open(path)
	if err != nil {
		return eris.Wrap(err, "open schedule_d_1i")
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		return eris.Wrap(err, "read header")
	}
	colIdx := mapColumns(header)

	latestFilingSet := make(map[int64]bool, len(latestFiling))
	for _, fid := range latestFiling {
		latestFilingSet[fid] = true
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		filingIDStr := trimQuotes(getCol(record, colIdx, "filingid"))
		if filingIDStr == "" {
			filingIDStr = trimQuotes(getCol(record, colIdx, "filing_id"))
		}
		filingID := parseInt64Or(filingIDStr, 0)
		if filingID == 0 || !latestFilingSet[filingID] {
			continue
		}

		if _, exists := out[filingID]; exists {
			continue // keep first
		}

		url := trimQuotes(getCol(record, colIdx, "website_address"))
		if url == "" {
			url = trimQuotes(getCol(record, colIdx, "website"))
		}
		if url != "" {
			out[filingID] = url
		}
	}

	return nil
}

// buildBaseBMap reads IA_ADV_Base_B files and returns a map of FilingID → {normalized_col → value}.
// Base_B contains Item 2 data (SEC registration, exempt reporting, state registration)
// keyed only by FilingID (no CRD column). This data is merged into filing rows at upsert time.
func buildBaseBMap(path string) (map[int64]map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, eris.Wrap(err, "open base_b file")
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		return nil, eris.Wrap(err, "read base_b header")
	}
	colIdx := mapColumnsNormalized(header)

	result := make(map[int64]map[string]string)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		filingIDStr := getColN(record, colIdx, "filingid")
		filingID := parseInt64Or(filingIDStr, 0)
		if filingID == 0 {
			continue
		}

		row := make(map[string]string, len(header))
		for col, idx := range colIdx {
			if idx < len(record) {
				row[col] = record[idx]
			}
		}
		result[filingID] = row
	}
	return result, nil
}

// streamBaseFile streams a base CSV, filters to latest filings, upserts adv_firms (identity) + adv_filings (full detail).
// baseBMap is optional Item 2 data from IA_ADV_Base_B (FilingID → normalized columns), nil if not available.
func streamBaseFile(ctx context.Context, pool db.Pool, path string, latestFiling map[string]int64, websiteMap map[int64]string, baseBMap map[int64]map[string]string, log *zap.Logger) (firmCount, filingCount int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, eris.Wrap(err, "open base file")
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		return 0, 0, eris.Wrap(err, "read header")
	}
	colIdx := mapColumnsNormalized(header)

	firmCols := []string{"crd_number", "firm_name", "sec_number", "city", "state", "country", "website"}
	firmConflict := []string{"crd_number"}

	var firmBatch, filingBatch [][]any

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		filingIDStr := trimQuotes(getColN(record, colIdx, "filingid"))
		if filingIDStr == "" {
			filingIDStr = trimQuotes(getColN(record, colIdx, "filing_id"))
		}
		filingID := parseInt64Or(filingIDStr, 0)

		crd := trimQuotes(getColN(record, colIdx, "1e1"))
		if crd == "" || filingID == 0 {
			continue
		}

		// Only emit rows for the latest filing per CRD.
		if latestFiling[crd] != filingID {
			continue
		}

		website := ""
		if w, ok := websiteMap[filingID]; ok {
			website = w
		}

		firmName := trimQuotes(getColN(record, colIdx, "1a"))
		secNumber := trimQuotes(getColN(record, colIdx, "1d"))
		city := firstNonEmpty(record, colIdx, "1f1-city", "1f1_city")
		state := firstNonEmpty(record, colIdx, "1f1-state", "1f1_state")
		country := firstNonEmpty(record, colIdx, "1f1-country", "1f1_country")

		firmRow := []any{
			parseIntOr(crd, 0),
			sanitizeUTF8(firmName),
			sanitizeUTF8(secNumber),
			sanitizeUTF8(city),
			sanitizeUTF8(state),
			sanitizeUTF8(country),
			sanitizeUTF8(website),
		}
		firmBatch = append(firmBatch, firmRow)

		filingDate := parseDate(trimQuotes(getColN(record, colIdx, "datesubmitted")))
		if filingDate != nil {
			// Look up supplementary Item 2 data from Base_B (if available).
			var extra map[string]string
			if baseBMap != nil {
				extra = baseBMap[filingID]
			}

			// Derive registration type from file source when Base_B has no data.
			// ERA_ADV_Base → Exempt Reporting Advisers; IA_ADV_Base → SEC-registered IAs.
			if extra == nil {
				extra = make(map[string]string)
			}
			isERA := strings.Contains(strings.ToLower(filepath.Base(path)), "era_")
			if isERA {
				if extra[normalizeCol("2a(2)")] == "" {
					extra[normalizeCol("2a(2)")] = "Y" // exempt_reporting
				}
			} else {
				if extra[normalizeCol("2a(1)")] == "" {
					extra[normalizeCol("2a(1)")] = "Y" // sec_registered
				}
			}

			filingBatch = append(filingBatch, buildFilingRow(record, colIdx, parseIntOr(crd, 0), filingDate, extra))
		}

		if len(firmBatch) >= advBatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table: "fed_data.adv_firms", Columns: firmCols, ConflictKeys: firmConflict,
			}, firmBatch)
			if err != nil {
				return firmCount, filingCount, eris.Wrap(err, "adv_part1: upsert firms")
			}
			firmCount += n
			firmBatch = firmBatch[:0]
		}

		if len(filingBatch) >= advBatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table: "fed_data.adv_filings", Columns: advFilingCols, ConflictKeys: advFilingConflict,
			}, filingBatch)
			if err != nil {
				return firmCount, filingCount, eris.Wrap(err, "adv_part1: upsert filings")
			}
			filingCount += n
			filingBatch = filingBatch[:0]
		}
	}

	if len(firmBatch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table: "fed_data.adv_firms", Columns: firmCols, ConflictKeys: firmConflict,
		}, firmBatch)
		if err != nil {
			return firmCount, filingCount, eris.Wrap(err, "adv_part1: upsert firms final")
		}
		firmCount += n
	}

	if len(filingBatch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table: "fed_data.adv_filings", Columns: advFilingCols, ConflictKeys: advFilingConflict,
		}, filingBatch)
		if err != nil {
			return firmCount, filingCount, eris.Wrap(err, "adv_part1: upsert filings final")
		}
		filingCount += n
	}

	log.Info("loaded firms from base file", zap.String("file", filepath.Base(path)), zap.Int64("firms", firmCount), zap.Int64("filings", filingCount))
	return firmCount, filingCount, nil
}

// streamOwnerFile streams a Schedule A/B CSV, filters to latest filings, upserts owners.
func streamOwnerFile(ctx context.Context, pool db.Pool, path string, latestFiling map[string]int64, cols, conflictKeys []string, log *zap.Logger) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, eris.Wrap(err, "open owner file")
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		return 0, eris.Wrap(err, "read header")
	}
	colIdx := mapColumns(header)

	// Build reverse map: filingID → CRD
	filingToCRD := make(map[int64]string, len(latestFiling))
	for crd, fid := range latestFiling {
		filingToCRD[fid] = crd
	}

	var batch [][]any
	var total int64

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		filingIDStr := trimQuotes(getCol(record, colIdx, "filingid"))
		if filingIDStr == "" {
			filingIDStr = trimQuotes(getCol(record, colIdx, "filing_id"))
		}
		filingID := parseInt64Or(filingIDStr, 0)
		if filingID == 0 {
			continue
		}

		crd, ok := filingToCRD[filingID]
		if !ok {
			continue
		}

		ownerName := sanitizeUTF8(trimQuotes(getCol(record, colIdx, "full legal name")))
		if ownerName == "" {
			continue
		}

		ownerType := trimQuotes(getCol(record, colIdx, "de/fe/i"))
		ownershipPct := parseOwnershipCode(trimQuotes(getCol(record, colIdx, "ownership code")))
		isControl := strings.EqualFold(trimQuotes(getCol(record, colIdx, "control person")), "Y")

		row := []any{
			parseIntOr(crd, 0),
			ownerName,
			ownerType,
			ownershipPct,
			isControl,
		}
		batch = append(batch, row)

		if len(batch) >= advBatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table: "fed_data.adv_owners", Columns: cols, ConflictKeys: conflictKeys,
			}, batch)
			if err != nil {
				return total, eris.Wrap(err, "adv_part1: upsert owners")
			}
			total += n
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table: "fed_data.adv_owners", Columns: cols, ConflictKeys: conflictKeys,
		}, batch)
		if err != nil {
			return total, eris.Wrap(err, "adv_part1: upsert owners final")
		}
		total += n
	}

	log.Info("loaded owners from file", zap.String("file", filepath.Base(path)), zap.Int64("rows", total))
	return total, nil
}

// streamFundFile streams a Schedule D 7B1 CSV, filters to latest filings, upserts funds.
func streamFundFile(ctx context.Context, pool db.Pool, path string, latestFiling map[string]int64, cols, conflictKeys []string, log *zap.Logger) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, eris.Wrap(err, "open fund file")
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		return 0, eris.Wrap(err, "read header")
	}
	colIdx := mapColumns(header)

	// Build reverse map: filingID → CRD
	filingToCRD := make(map[int64]string, len(latestFiling))
	for crd, fid := range latestFiling {
		filingToCRD[fid] = crd
	}

	var batch [][]any
	var total int64

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		filingIDStr := trimQuotes(getCol(record, colIdx, "filingid"))
		if filingIDStr == "" {
			filingIDStr = trimQuotes(getCol(record, colIdx, "filing_id"))
		}
		filingID := parseInt64Or(filingIDStr, 0)
		if filingID == 0 {
			continue
		}

		crd, ok := filingToCRD[filingID]
		if !ok {
			continue
		}

		fundID := trimQuotes(getCol(record, colIdx, "fund id"))
		if fundID == "" {
			fundID = trimQuotes(getCol(record, colIdx, "fund_id"))
		}
		if fundID == "" {
			continue
		}

		fundName := sanitizeUTF8(trimQuotes(getCol(record, colIdx, "fund name")))
		if fundName == "" {
			fundName = sanitizeUTF8(trimQuotes(getCol(record, colIdx, "fund_name")))
		}
		fundType := trimQuotes(getCol(record, colIdx, "fund type"))
		if fundType == "" {
			fundType = trimQuotes(getCol(record, colIdx, "fund_type"))
		}

		grossAV := parseInt64Or(trimQuotes(getCol(record, colIdx, "gross asset value")), 0)
		if grossAV == 0 {
			grossAV = parseInt64Or(trimQuotes(getCol(record, colIdx, "gross_asset_value")), 0)
		}

		row := []any{
			parseIntOr(crd, 0),
			fundID,
			fundName,
			fundType,
			grossAV,
			nil, // net_asset_value (not in source)
		}
		batch = append(batch, row)

		if len(batch) >= advBatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table: "fed_data.adv_private_funds", Columns: cols, ConflictKeys: conflictKeys,
			}, batch)
			if err != nil {
				return total, eris.Wrap(err, "adv_part1: upsert funds")
			}
			total += n
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table: "fed_data.adv_private_funds", Columns: cols, ConflictKeys: conflictKeys,
		}, batch)
		if err != nil {
			return total, eris.Wrap(err, "adv_part1: upsert funds final")
		}
		total += n
	}

	log.Info("loaded funds from file", zap.String("file", filepath.Base(path)), zap.Int64("rows", total))
	return total, nil
}

// parseOwnershipCode converts SEC ownership code to a numeric percentage.
func parseOwnershipCode(code string) *float64 {
	code = strings.TrimSpace(strings.ToUpper(code))
	var val float64
	switch code {
	case "A":
		val = 12.5
	case "B":
		val = 37.5
	case "C":
		val = 62.5
	case "D":
		val = 87.5
	case "E":
		val = 25.0
	default:
		return nil
	}
	return &val
}
