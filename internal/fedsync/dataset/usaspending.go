package dataset

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
	usaspendingBatchSize       = 5000
	usaspendingBulkDownloadURL = "https://api.usaspending.gov/api/v2/bulk_download/awards/"
	usaspendingStatusURL       = "https://api.usaspending.gov/api/v2/download/status/"
	usaspendingPollInterval    = 10 * time.Second
	usaspendingMaxPollAttempts = 180
)

// usaspendingPrimeAwardTypes lists all prime award type codes for the bulk download request.
// Contracts: A-D; IDV subtypes: IDV_A through IDV_E; Assistance: 02-11.
var usaspendingPrimeAwardTypes = []string{
	"A", "B", "C", "D",
	"IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E",
	"02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
}

// usaspendingColumns defines the target DB columns in upsert order.
var usaspendingColumns = []string{
	"award_id", "award_type", "award_type_code",
	"piid", "fain", "uri",
	"awarding_agency_code", "awarding_agency_name",
	"awarding_sub_agency_code", "awarding_sub_agency_name",
	"funding_agency_code", "funding_agency_name",
	"recipient_uei", "recipient_duns", "recipient_name",
	"recipient_parent_uei", "recipient_parent_name",
	"recipient_address_line_1", "recipient_city", "recipient_state",
	"recipient_zip", "recipient_country",
	"total_obligated_amount", "total_outlayed_amount",
	"naics_code", "naics_description", "psc_code",
	"cfda_number", "cfda_title",
	"award_base_action_date", "award_latest_action_date",
	"period_of_perf_start", "period_of_perf_end",
	"last_modified_date",
	"pop_city", "pop_state", "pop_zip", "pop_country",
	"award_description", "usaspending_permalink",
}

// bulkDownloadRequest is the POST body for the USAspending awards endpoint.
type bulkDownloadRequest struct {
	Filters    bulkDownloadFilters `json:"filters"`
	Columns    []string            `json:"columns"`
	FileFormat string              `json:"file_format"`
}

// bulkDownloadFilters defines the filter criteria for a bulk download request.
type bulkDownloadFilters struct {
	PrimeAwardTypes []string              `json:"prime_award_types"`
	DateType        string                `json:"date_type"`
	DateRange       bulkDownloadDateRange `json:"date_range"`
}

// bulkDownloadDateRange defines the date range for a bulk download filter.
type bulkDownloadDateRange struct {
	StartDate string `json:"start_date"`
	EndDate   string `json:"end_date"`
}

// bulkDownloadResponse is the response from the POST endpoint.
type bulkDownloadResponse struct {
	Status   string `json:"status"`
	FileURL  string `json:"file_url"`
	FileName string `json:"file_name"`
	Message  string `json:"message"`
}

// bulkDownloadStatus is the response from the status polling endpoint.
type bulkDownloadStatus struct {
	Status         string  `json:"status"`
	TotalRows      int     `json:"total_rows"`
	TotalSize      float64 `json:"total_size"`
	FileURL        string  `json:"file_url"`
	Message        string  `json:"message"`
	SecondsElapsed float64 `json:"seconds_elapsed"`
}

// USAspending syncs federal awards from USAspending.gov bulk download API.
// Data includes contracts, grants, direct payments, and loans across all
// federal agencies. Monthly incremental sync uses a 35-day lookback window;
// full sync downloads all fiscal years from FY2017 onwards.
type USAspending struct {
	cfg *config.Config

	// httpClient is used for POST requests to the bulk download API.
	// Defaults to http.DefaultClient; overridden in tests.
	httpClient *http.Client

	// pollInterval controls delay between status polls. Defaults to usaspendingPollInterval.
	pollInterval time.Duration

	// maxPollAttempts limits polling iterations. Defaults to usaspendingMaxPollAttempts.
	maxPollAttempts int

	// bulkDownloadURL is the POST endpoint. Defaults to usaspendingBulkDownloadURL.
	bulkDownloadURL string

	// statusURL is the GET status endpoint. Defaults to usaspendingStatusURL.
	statusURL string
}

// Name implements Dataset.
func (d *USAspending) Name() string { return "usaspending" }

// Table implements Dataset.
func (d *USAspending) Table() string { return "fed_data.usaspending_awards" }

// Phase implements Dataset.
func (d *USAspending) Phase() Phase { return Phase1 }

// Cadence implements Dataset.
func (d *USAspending) Cadence() Cadence { return Monthly }

// ShouldRun implements Dataset.
func (d *USAspending) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return MonthlySchedule(now, lastSync)
}

// client returns the HTTP client, defaulting to http.DefaultClient.
func (d *USAspending) client() *http.Client {
	if d.httpClient != nil {
		return d.httpClient
	}
	return http.DefaultClient
}

// getPollInterval returns the poll interval, defaulting to usaspendingPollInterval.
func (d *USAspending) getPollInterval() time.Duration {
	if d.pollInterval > 0 {
		return d.pollInterval
	}
	return usaspendingPollInterval
}

// getMaxPollAttempts returns the max poll attempts, defaulting to usaspendingMaxPollAttempts.
func (d *USAspending) getMaxPollAttempts() int {
	if d.maxPollAttempts > 0 {
		return d.maxPollAttempts
	}
	return usaspendingMaxPollAttempts
}

// getBulkDownloadURL returns the bulk download URL, defaulting to usaspendingBulkDownloadURL.
func (d *USAspending) getBulkDownloadURL() string {
	if d.bulkDownloadURL != "" {
		return d.bulkDownloadURL
	}
	return usaspendingBulkDownloadURL
}

// getStatusURL returns the status URL, defaulting to usaspendingStatusURL.
func (d *USAspending) getStatusURL() string {
	if d.statusURL != "" {
		return d.statusURL
	}
	return usaspendingStatusURL
}

// Sync downloads and loads USAspending awards modified in the last 35 days.
func (d *USAspending) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "usaspending"))

	endDate := time.Now().Format("2006-01-02")
	startDate := time.Now().AddDate(0, 0, -35).Format("2006-01-02")

	log.Info("requesting bulk download", zap.String("start_date", startDate), zap.String("end_date", endDate))

	dlResp, err := d.requestBulkDownload(ctx, startDate, endDate)
	if err != nil {
		return nil, err
	}

	log.Info("polling for download completion", zap.String("file_name", dlResp.FileName))

	status, err := d.pollStatus(ctx, dlResp.FileName)
	if err != nil {
		return nil, err
	}

	totalRows, err := d.downloadAndProcess(ctx, pool, f, tempDir, status.FileURL, "usaspending")
	if err != nil {
		return nil, err
	}

	return &SyncResult{
		RowsSynced: totalRows,
		Metadata:   map[string]any{"date_range": fmt.Sprintf("%s to %s", startDate, endDate)},
	}, nil
}

// SyncFull downloads all fiscal years from FY2017 to present.
func (d *USAspending) SyncFull(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "usaspending"))
	var totalRows int64

	currentYear := time.Now().Year()
	for fy := 2017; fy <= currentYear; fy++ {
		startDate := fmt.Sprintf("%d-10-01", fy-1) // FY starts Oct 1
		endDate := fmt.Sprintf("%d-09-30", fy)
		if fy == currentYear {
			endDate = time.Now().Format("2006-01-02")
		}

		log.Info("requesting FY bulk download", zap.Int("fy", fy))

		dlResp, err := d.requestBulkDownload(ctx, startDate, endDate)
		if err != nil {
			return nil, eris.Wrapf(err, "usaspending: FY%d request", fy)
		}

		status, err := d.pollStatus(ctx, dlResp.FileName)
		if err != nil {
			return nil, eris.Wrapf(err, "usaspending: FY%d poll", fy)
		}

		label := fmt.Sprintf("usaspending_fy%d", fy)
		rows, err := d.downloadAndProcess(ctx, pool, f, tempDir, status.FileURL, label)
		if err != nil {
			return nil, eris.Wrapf(err, "usaspending: FY%d process", fy)
		}
		totalRows += rows

		log.Info("completed FY", zap.Int("fy", fy), zap.Int64("total_rows", totalRows))
	}

	return &SyncResult{
		RowsSynced: totalRows,
		Metadata:   map[string]any{"method": "full", "fy_range": fmt.Sprintf("2017-%d", currentYear)},
	}, nil
}

// downloadAndProcess downloads a ZIP, extracts CSVs, and upserts rows.
func (d *USAspending) downloadAndProcess(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir, fileURL, label string) (int64, error) {
	log := zap.L().With(zap.String("dataset", "usaspending"))

	zipPath := filepath.Join(tempDir, label+".zip")
	if _, err := f.DownloadToFile(ctx, fileURL, zipPath); err != nil {
		return 0, eris.Wrap(err, "usaspending: download ZIP")
	}
	defer os.Remove(zipPath) //nolint:errcheck

	extractDir := filepath.Join(tempDir, label)
	csvPaths, err := fetcher.ExtractZIP(zipPath, extractDir)
	if err != nil {
		return 0, eris.Wrap(err, "usaspending: extract ZIP")
	}
	defer os.RemoveAll(extractDir) //nolint:errcheck

	var totalRows int64
	for _, csvPath := range csvPaths {
		if !strings.HasSuffix(csvPath, ".csv") {
			continue
		}
		file, err := os.Open(csvPath) // #nosec G304 -- path from trusted ZIP extraction in temp directory
		if err != nil {
			return totalRows, eris.Wrapf(err, "usaspending: open %s", filepath.Base(csvPath))
		}

		rows, parseErr := d.parseCSV(ctx, pool, file)
		_ = file.Close()
		if parseErr != nil {
			return totalRows, eris.Wrapf(parseErr, "usaspending: parse %s", filepath.Base(csvPath))
		}
		totalRows += rows
		log.Info("processed CSV", zap.String("file", filepath.Base(csvPath)), zap.Int64("rows", rows))
	}

	return totalRows, nil
}

// requestBulkDownload POSTs a bulk download request to the USAspending API.
func (d *USAspending) requestBulkDownload(ctx context.Context, startDate, endDate string) (*bulkDownloadResponse, error) {
	reqBody := bulkDownloadRequest{
		Filters: bulkDownloadFilters{
			PrimeAwardTypes: usaspendingPrimeAwardTypes,
			DateType:        "last_modified_date",
			DateRange:       bulkDownloadDateRange{StartDate: startDate, EndDate: endDate},
		},
		FileFormat: "csv",
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, eris.Wrap(err, "usaspending: marshal request")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.getBulkDownloadURL(), bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, eris.Wrap(err, "usaspending: create request")
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client().Do(req)
	if err != nil {
		return nil, eris.Wrap(err, "usaspending: POST bulk download")
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return nil, eris.Errorf("usaspending: POST returned status %d", resp.StatusCode)
	}

	var dlResp bulkDownloadResponse
	if err := json.NewDecoder(resp.Body).Decode(&dlResp); err != nil {
		return nil, eris.Wrap(err, "usaspending: decode response")
	}
	return &dlResp, nil
}

// pollStatus polls the status endpoint until the download is finished or fails.
func (d *USAspending) pollStatus(ctx context.Context, fileName string) (*bulkDownloadStatus, error) {
	for i := 0; i < d.getMaxPollAttempts(); i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(d.getPollInterval()):
		}

		url := fmt.Sprintf("%s?file_name=%s", d.getStatusURL(), fileName)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, eris.Wrap(err, "usaspending: create status request")
		}

		resp, err := d.client().Do(req)
		if err != nil {
			return nil, eris.Wrap(err, "usaspending: GET status")
		}

		var status bulkDownloadStatus
		err = json.NewDecoder(resp.Body).Decode(&status)
		_ = resp.Body.Close()
		if err != nil {
			return nil, eris.Wrap(err, "usaspending: decode status")
		}

		if status.Status == "finished" {
			return &status, nil
		}
		if status.Status == "failed" {
			return nil, eris.Errorf("usaspending: bulk download failed: %s", status.Message)
		}
	}
	return nil, eris.New("usaspending: polling timed out")
}

// parseCSV reads a USAspending CSV and upserts rows into fed_data.usaspending_awards.
func (d *USAspending) parseCSV(ctx context.Context, pool db.Pool, r io.Reader) (int64, error) {
	reader := csv.NewReader(r)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		return 0, eris.Wrap(err, "usaspending: read CSV header")
	}

	colIdx := mapColumnsNormalized(header)
	if _, ok := colIdx["contract_award_unique_key"]; !ok {
		if _, ok := colIdx["assistance_award_unique_key"]; !ok {
			return 0, eris.New("usaspending: no award key column found in header")
		}
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

		row := d.mapRow(record, colIdx)
		if row == nil {
			continue
		}
		batch = append(batch, row)

		if len(batch) >= usaspendingBatchSize {
			n, upsertErr := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table:        "fed_data.usaspending_awards",
				Columns:      usaspendingColumns,
				ConflictKeys: []string{"award_id"},
			}, batch)
			if upsertErr != nil {
				return totalRows, eris.Wrap(upsertErr, "usaspending: bulk upsert")
			}
			totalRows += n
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		n, upsertErr := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        "fed_data.usaspending_awards",
			Columns:      usaspendingColumns,
			ConflictKeys: []string{"award_id"},
		}, batch)
		if upsertErr != nil {
			return totalRows, eris.Wrap(upsertErr, "usaspending: bulk upsert final batch")
		}
		totalRows += n
	}

	return totalRows, nil
}

// mapRow converts a CSV record to a row of values matching usaspendingColumns.
// Returns nil if the row has no award ID.
func (d *USAspending) mapRow(record []string, colIdx map[string]int) []any {
	get := func(name string) string { return trimQuotes(getColN(record, colIdx, name)) }

	// Derive award_id: use contract key for contracts, assistance key for assistance.
	awardID := get("contract_award_unique_key")
	if awardID == "" {
		awardID = get("assistance_award_unique_key")
	}
	if awardID == "" {
		return nil
	}

	awardTypeCode := get("award_type_code")
	awardType := classifyAwardType(awardTypeCode)

	getText := func(name string) any {
		v := get(name)
		if v == "" {
			return nil
		}
		return sanitizeUTF8(v)
	}
	getNumeric := func(name string) any {
		v := get(name)
		if v == "" {
			return nil
		}
		f := parseFloat64Or(v, 0)
		if f == 0 && v != "0" && v != "0.0" && v != "0.00" {
			return nil
		}
		return f
	}
	getDate := func(name string) any {
		v := get(name)
		if v == "" {
			return nil
		}
		t, err := time.Parse("2006-01-02", v)
		if err != nil {
			return nil
		}
		return t
	}
	getStateCode := func(name string) any {
		v := get(name)
		if v == "" {
			return nil
		}
		if len(v) > 2 {
			return v[:2]
		}
		return v
	}

	return []any{
		awardID,
		awardType,
		getText("award_type_code"),
		getText("piid"),
		getText("fain"),
		getText("uri"),
		getText("awarding_agency_code"),
		getText("awarding_agency_name"),
		getText("awarding_sub_agency_code"),
		getText("awarding_sub_agency_name"),
		getText("funding_agency_code"),
		getText("funding_agency_name"),
		getText("recipient_uei"),
		getText("recipient_duns"),
		getText("recipient_name"),
		getText("parent_uei"),
		getText("parent_recipient_name"),
		getText("recipient_address_line_1"),
		getText("recipient_city_name"),
		getStateCode("recipient_state_code"),
		getText("recipient_zip_4_code"),
		getText("recipient_country_code"),
		getNumeric("total_obligated_amount"),
		getNumeric("total_outlayed_amount"),
		getText("naics_code"),
		getText("naics_description"),
		getText("product_or_service_code"),
		getText("cfda_number"),
		getText("cfda_title"),
		getDate("award_base_action_date"),
		getDate("award_latest_action_date"),
		getDate("period_of_performance_start_date"),
		getDate("period_of_performance_current_end_date"),
		getDate("last_modified_date"),
		getText("primary_place_of_performance_city_name"),
		getStateCode("primary_place_of_performance_state_code"),
		getText("primary_place_of_performance_zip_4"),
		getText("primary_place_of_performance_country_code"),
		getText("award_description"),
		getText("usaspending_permalink"),
	}
}

// classifyAwardType maps award_type_code to a category.
func classifyAwardType(code string) string {
	code = strings.TrimSpace(strings.ToUpper(code))
	switch {
	case code == "A" || code == "B" || code == "C" || code == "D" || strings.HasPrefix(code, "IDV"):
		return "contract"
	case code == "02" || code == "03" || code == "04" || code == "05":
		return "grant"
	case code == "06" || code == "10":
		return "direct_payment"
	case code == "07" || code == "08":
		return "loan"
	case code == "09" || code == "11":
		return "other"
	default:
		return "other"
	}
}
