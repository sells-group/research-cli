package dataset

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
)

const (
	eftsSearchURL     = "https://efts.sec.gov/LATEST/search-index"
	holdingsBatchSize = 5000
)

// Holdings13F implements the SEC 13F Holdings dataset.
// Downloads 13F XML filings from EDGAR full-text search, parses holdings, and upserts.
type Holdings13F struct {
	cfg *config.Config
}

func (d *Holdings13F) Name() string     { return "holdings_13f" }
func (d *Holdings13F) Table() string    { return "fed_data.f13_holdings" }
func (d *Holdings13F) Phase() Phase     { return Phase1B }
func (d *Holdings13F) Cadence() Cadence { return Quarterly }

func (d *Holdings13F) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return QuarterlyAfterDelay(now, lastSync, 45)
}

// f13Filing represents a 13F filing from the EFTS search results.
type f13Filing struct {
	CIK            string `json:"cik"`
	CompanyName    string `json:"company_name"`
	FormType       string `json:"form_type"`
	FilingDate     string `json:"filing_date"`
	AccessionNumber string `json:"accession_number"`
}

// f13InfoTable represents the root of a 13F XML holdings document.
type f13InfoTable struct {
	XMLName  xml.Name      `xml:"informationTable"`
	Holdings []f13Holding  `xml:"infoTable"`
}

// f13Holding represents a single holding in a 13F filing.
type f13Holding struct {
	IssuerName string `xml:"nameOfIssuer"`
	ClassTitle string `xml:"titleOfClass"`
	CUSIP      string `xml:"cusip"`
	Value      int64  `xml:"value"`
	Shares     int64  `xml:"shrsOrPrnAmt>sshPrnamt"`
	ShPrnType  string `xml:"shrsOrPrnAmt>sshPrnamtType"`
	PutCall    string `xml:"putCall"`
}

// eftsSearchResult is the response from the EDGAR full-text search API.
type eftsSearchResult struct {
	Hits struct {
		Total int `json:"total"`
		Hits  []struct {
			Source struct {
				CIK             string `json:"entity_cik"`
				CompanyName     string `json:"entity_name"`
				FormType        string `json:"form_type"`
				FilingDate      string `json:"file_date"`
				AccessionNumber string `json:"accession_no"`
				PeriodOfReport  string `json:"period_of_report"`
			} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

func (d *Holdings13F) Sync(ctx context.Context, pool *pgxpool.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "holdings_13f"))

	// Determine the most recent quarter-end for which data should be available.
	now := time.Now().UTC()
	qEnd := mostRecentQuarterEnd(now.AddDate(0, 0, -45))
	period := qEnd.Format("2006-01-02")
	startDate := qEnd.AddDate(0, 0, 1).Format("2006-01-02")
	endDate := now.Format("2006-01-02")

	log.Info("searching for 13F filings",
		zap.String("period", period),
		zap.String("start_date", startDate),
		zap.String("end_date", endDate),
	)

	// Search for 13F-HR filings via EFTS.
	searchURL := fmt.Sprintf(
		"%s?q=*&dateRange=custom&startdt=%s&enddt=%s&forms=13F-HR&from=0&size=200",
		eftsSearchURL, startDate, endDate,
	)

	body, err := f.Download(ctx, searchURL)
	if err != nil {
		return nil, eris.Wrap(err, "holdings_13f: search EFTS")
	}

	searchResult, err := fetcher.DecodeJSONObject[eftsSearchResult](body)
	body.Close()
	if err != nil {
		return nil, eris.Wrap(err, "holdings_13f: decode search results")
	}

	log.Info("found 13F filings", zap.Int("total", searchResult.Hits.Total))

	var totalRows int64

	for _, hit := range searchResult.Hits.Hits {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		src := hit.Source
		cik := strings.TrimLeft(src.CIK, "0")
		accession := strings.ReplaceAll(src.AccessionNumber, "-", "")

		periodDate := parseDate(src.PeriodOfReport)
		filingDate := parseDate(src.FilingDate)

		// Upsert filer record
		filerCols := []string{"cik", "company_name", "form_type", "filing_date", "period_of_report", "total_value"}
		filerRow := []any{cik, src.CompanyName, src.FormType, filingDate, periodDate, int64(0)}
		if _, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table: "fed_data.f13_filers", Columns: filerCols, ConflictKeys: []string{"cik"},
		}, [][]any{filerRow}); err != nil {
			log.Warn("holdings_13f: upsert filer failed", zap.String("cik", cik), zap.Error(err))
			continue
		}

		// Download the 13F holdings XML.
		holdingsURL := fmt.Sprintf(
			"https://www.sec.gov/Archives/edgar/data/%s/%s/primary_doc.xml",
			cik, accession,
		)

		rows, err := d.downloadAndParseHoldings(ctx, f, pool, holdingsURL, cik, periodDate, tempDir, log)
		if err != nil {
			log.Warn("holdings_13f: parse holdings failed",
				zap.String("cik", cik),
				zap.String("accession", src.AccessionNumber),
				zap.Error(err),
			)
			continue
		}

		// Update filer total_value
		totalValue := d.sumHoldingsValue(rows)
		if _, err := pool.Exec(ctx,
			"UPDATE fed_data.f13_filers SET total_value = $1 WHERE cik = $2",
			totalValue, cik,
		); err != nil {
			log.Warn("holdings_13f: update filer total_value", zap.Error(err))
		}

		totalRows += int64(len(rows))
	}

	log.Info("holdings_13f sync complete", zap.Int64("holdings", totalRows))

	return &SyncResult{
		RowsSynced: totalRows,
		Metadata: map[string]any{
			"period":       period,
			"filings_found": searchResult.Hits.Total,
		},
	}, nil
}

func (d *Holdings13F) downloadAndParseHoldings(
	ctx context.Context,
	f fetcher.Fetcher,
	pool *pgxpool.Pool,
	url string,
	cik string,
	period *time.Time,
	tempDir string,
	log *zap.Logger,
) ([][]any, error) {
	xmlPath := filepath.Join(tempDir, fmt.Sprintf("13f_%s.xml", cik))
	if _, err := f.DownloadToFile(ctx, url, xmlPath); err != nil {
		return nil, eris.Wrapf(err, "download 13F holdings for %s", cik)
	}
	defer os.Remove(xmlPath)

	file, err := os.Open(xmlPath)
	if err != nil {
		return nil, eris.Wrap(err, "open 13F XML")
	}
	defer file.Close()

	return d.parseHoldingsXML(ctx, pool, file, cik, period, log)
}

func (d *Holdings13F) parseHoldingsXML(
	ctx context.Context,
	pool *pgxpool.Pool,
	r io.Reader,
	cik string,
	period *time.Time,
	log *zap.Logger,
) ([][]any, error) {
	holdingCh, errCh := fetcher.StreamXML[f13Holding](ctx, r, "infoTable")

	columns := []string{"cik", "period", "cusip", "issuer_name", "class_title", "value", "shares", "sh_prn_type", "put_call"}
	conflictKeys := []string{"cik", "period", "cusip"}

	var batch [][]any
	var allRows [][]any

	for h := range holdingCh {
		cusip := strings.TrimSpace(h.CUSIP)
		if len(cusip) < 9 {
			continue
		}

		row := []any{
			cik,
			period,
			cusip[:9],
			strings.TrimSpace(h.IssuerName),
			strings.TrimSpace(h.ClassTitle),
			h.Value * 1000, // 13F values are in thousands
			h.Shares,
			strings.TrimSpace(h.ShPrnType),
			strings.TrimSpace(h.PutCall),
		}
		batch = append(batch, row)
		allRows = append(allRows, row)

		if len(batch) >= holdingsBatchSize {
			if _, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table: "fed_data.f13_holdings", Columns: columns, ConflictKeys: conflictKeys,
			}, batch); err != nil {
				return allRows, eris.Wrap(err, "holdings_13f: upsert holdings")
			}
			batch = batch[:0]
		}
	}

	if err := <-errCh; err != nil {
		return allRows, eris.Wrap(err, "holdings_13f: parse XML stream")
	}

	if len(batch) > 0 {
		if _, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table: "fed_data.f13_holdings", Columns: columns, ConflictKeys: conflictKeys,
		}, batch); err != nil {
			return allRows, eris.Wrap(err, "holdings_13f: upsert holdings final")
		}
	}

	return allRows, nil
}

func (d *Holdings13F) sumHoldingsValue(rows [][]any) int64 {
	var total int64
	for _, row := range rows {
		if len(row) > 5 {
			if v, ok := row[5].(int64); ok {
				total += v
			}
		}
	}
	return total
}
