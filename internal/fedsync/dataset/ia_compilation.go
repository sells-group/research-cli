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
	iaCompilationBaseURL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=ADV&dateb=&owner=include&count=40&search_text=&action=getcompany&output=atom"
	iaDailyURL           = "https://www.sec.gov/files/data/investment-adviser-data/ia-daily-compilation.xml"
	iaBatchSize          = 2000
)

// IACompilation implements the IARD IA Compilation daily XML delta dataset.
// Downloads daily delta XML from SEC and applies changes to adv_firms.
type IACompilation struct {
	cfg *config.Config
}

func (d *IACompilation) Name() string     { return "ia_compilation" }
func (d *IACompilation) Table() string    { return "fed_data.adv_firms" }
func (d *IACompilation) Phase() Phase     { return Phase1B }
func (d *IACompilation) Cadence() Cadence { return Daily }

func (d *IACompilation) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return DailySchedule(now, lastSync)
}

// iaFirm represents a single firm record from the daily compilation XML.
type iaFirm struct {
	XMLName     xml.Name `xml:"Firm"`
	CRDNumber   int      `xml:"CrdNb"`
	FirmName    string   `xml:"FirmName"`
	SECNumber   string   `xml:"SecNb"`
	City        string   `xml:"MainAddr>City"`
	State       string   `xml:"MainAddr>StateOrCountry"`
	Country     string   `xml:"MainAddr>Country"`
	Website     string   `xml:"WebAddr"`
	AUM         int64    `xml:"TotalGrossAssetAmt"`
	NumAccounts int      `xml:"TotalNumberOfAccounts"`
	FilingDate  string   `xml:"MostRecentFilingDate"`
}

func (d *IACompilation) Sync(ctx context.Context, pool *pgxpool.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "ia_compilation"))

	xmlPath := filepath.Join(tempDir, "ia-daily-compilation.xml")
	log.Info("downloading IA daily compilation XML", zap.String("url", iaDailyURL))

	if _, err := f.DownloadToFile(ctx, iaDailyURL, xmlPath); err != nil {
		return nil, eris.Wrap(err, "ia_compilation: download XML")
	}
	defer os.Remove(xmlPath)

	file, err := os.Open(xmlPath)
	if err != nil {
		return nil, eris.Wrap(err, "ia_compilation: open XML")
	}
	defer file.Close()

	return d.parseAndLoad(ctx, pool, file, log)
}

func (d *IACompilation) parseAndLoad(ctx context.Context, pool *pgxpool.Pool, r io.Reader, log *zap.Logger) (*SyncResult, error) {
	firmCh, errCh := fetcher.StreamXML[iaFirm](ctx, r, "Firm")

	columns := []string{"crd_number", "firm_name", "sec_number", "city", "state", "country", "website", "aum", "num_accounts", "filing_date"}
	conflictKeys := []string{"crd_number"}

	var batch [][]any
	var totalRows int64

	for firm := range firmCh {
		if firm.CRDNumber == 0 {
			continue
		}

		state := strings.TrimSpace(firm.State)
		if len(state) > 2 {
			state = state[:2]
		}

		row := []any{
			firm.CRDNumber,
			strings.TrimSpace(firm.FirmName),
			strings.TrimSpace(firm.SECNumber),
			strings.TrimSpace(firm.City),
			state,
			strings.TrimSpace(firm.Country),
			strings.TrimSpace(firm.Website),
			firm.AUM,
			firm.NumAccounts,
			parseDate(firm.FilingDate),
		}
		batch = append(batch, row)

		if len(batch) >= iaBatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table: "fed_data.adv_firms", Columns: columns, ConflictKeys: conflictKeys,
			}, batch)
			if err != nil {
				return nil, eris.Wrap(err, "ia_compilation: upsert firms")
			}
			totalRows += n
			batch = batch[:0]
		}
	}

	if err := <-errCh; err != nil {
		return nil, eris.Wrap(err, "ia_compilation: parse XML")
	}

	if len(batch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table: "fed_data.adv_firms", Columns: columns, ConflictKeys: conflictKeys,
		}, batch)
		if err != nil {
			return nil, eris.Wrap(err, "ia_compilation: upsert firms final")
		}
		totalRows += n
	}

	log.Info("ia_compilation sync complete", zap.Int64("rows", totalRows))

	return &SyncResult{
		RowsSynced: totalRows,
	}, nil
}

// userAgent returns the configured EDGAR User-Agent.
func (d *IACompilation) userAgent() string {
	if d.cfg != nil && d.cfg.Fedsync.EDGARUserAgent != "" {
		return d.cfg.Fedsync.EDGARUserAgent
	}
	return fmt.Sprintf("research-cli/1.0 (%s)", "fedsync")
}
