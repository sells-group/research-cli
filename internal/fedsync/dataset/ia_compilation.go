package dataset

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/db"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/fetcher"
)

const (
	// SEC IAPD compilation reports — daily full firm roster.
	// Manifest lists today's available feeds (SEC, STATE, INDVL).
	iaCompManifestURL = "https://reports.adviserinfo.sec.gov/reports/CompilationReports/CompilationReports.manifest.json"
	iaCompBaseURL     = "https://reports.adviserinfo.sec.gov/reports/CompilationReports"
	iaBatchSize       = 2000
)

// IACompilation implements the IARD IA Compilation daily XML dataset.
// Downloads the daily IA_FIRM_SEC_Feed XML.gz from the SEC IAPD system
// and upserts into adv_firms (identity) + adv_filings (metrics).
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

// iaCompManifest represents the CompilationReports.manifest.json structure.
type iaCompManifest struct {
	Files []iaCompManifestFile `json:"files"`
}

type iaCompManifestFile struct {
	Name string `json:"name"`
	Size string `json:"size"`
	Date string `json:"date"`
}

// iaFirm represents a single <Firm> from the IA_FIRM_SEC_Feed XML.
// The SEC IAPD compilation uses attribute-heavy nested elements:
//
//	<Firm>
//	  <Info FirmCrdNb="123" SECNb="801-..." BusNm="Acme LLC"/>
//	  <MainAddr City="NYC" State="NY" Cntry="United States"/>
//	  <Filing Dt="2025-04-03"/>
//	  <FormInfo><Part1A>
//	    <Item1><WebAddrs><WebAddr>https://...</WebAddr></WebAddrs></Item1>
//	    <Item5A TtlEmp="19"/>
//	    <Item5F Q5F2C="1000000" Q5F2F="7"/>
//	  </Part1A></FormInfo>
//	</Firm>
type iaFirm struct {
	XMLName  xml.Name   `xml:"Firm"`
	Info     iaInfo     `xml:"Info"`
	MainAddr iaMainAddr `xml:"MainAddr"`
	Filing   iaFiling   `xml:"Filing"`
	FormInfo iaFormInfo `xml:"FormInfo"`
}

type iaInfo struct {
	CRDNumber int    `xml:"FirmCrdNb,attr"`
	FirmName  string `xml:"BusNm,attr"`
	SECNumber string `xml:"SECNb,attr"`
}

type iaMainAddr struct {
	City    string `xml:"City,attr"`
	State   string `xml:"State,attr"`
	Country string `xml:"Cntry,attr"`
}

type iaFiling struct {
	Date string `xml:"Dt,attr"`
}

type iaFormInfo struct {
	Part1A iaPart1A `xml:"Part1A"`
}

type iaPart1A struct {
	Item1  iaItem1  `xml:"Item1"`
	Item5A iaItem5A `xml:"Item5A"`
	Item5F iaItem5F `xml:"Item5F"`
}

type iaItem1 struct {
	WebAddrs []string `xml:"WebAddrs>WebAddr"`
}

type iaItem5A struct {
	TotalEmployees int `xml:"TtlEmp,attr"`
}

type iaItem5F struct {
	AUM         int64 `xml:"Q5F2C,attr"`
	NumAccounts int   `xml:"Q5F2F,attr"`
}

func (d *IACompilation) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "ia_compilation"))

	// Fetch the compilation reports manifest to find today's SEC firm feed.
	rc, err := f.Download(ctx, iaCompManifestURL)
	if err != nil {
		log.Warn("ia_compilation: failed to fetch compilation manifest", zap.Error(err))
		return &SyncResult{
			RowsSynced: 0,
			Metadata:   map[string]any{"status": "manifest_unavailable", "error": err.Error()},
		}, nil
	}

	var manifest iaCompManifest
	if err := json.NewDecoder(rc).Decode(&manifest); err != nil {
		_ = rc.Close()
		return nil, eris.Wrap(err, "ia_compilation: parse manifest JSON")
	}
	_ = rc.Close()

	// Find the IA_FIRM_SEC_Feed file (SEC-registered firms, gzipped XML).
	var feedURL string
	for _, file := range manifest.Files {
		if strings.Contains(file.Name, "IA_FIRM_SEC_Feed") {
			feedURL = iaCompBaseURL + "/" + file.Name
			break
		}
	}
	if feedURL == "" {
		return &SyncResult{
			RowsSynced: 0,
			Metadata:   map[string]any{"status": "no_sec_feed_found"},
		}, nil
	}

	log.Info("downloading IA firm compilation feed", zap.String("url", feedURL))

	// Download the gzipped XML.
	gzPath := filepath.Join(tempDir, "ia_firm_sec_feed.xml.gz")
	if _, err := f.DownloadToFile(ctx, feedURL, gzPath); err != nil {
		return nil, eris.Wrap(err, "ia_compilation: download feed")
	}
	defer os.Remove(gzPath) //nolint:errcheck

	// Decompress gzip → parse XML.
	gzFile, err := os.Open(gzPath)
	if err != nil {
		return nil, eris.Wrap(err, "ia_compilation: open gzip file")
	}
	defer gzFile.Close() //nolint:errcheck

	gzReader, err := gzip.NewReader(gzFile)
	if err != nil {
		return nil, eris.Wrap(err, "ia_compilation: create gzip reader")
	}
	defer gzReader.Close() //nolint:errcheck

	result, err := d.parseAndLoad(ctx, pool, gzReader, log)
	if err != nil {
		return nil, err
	}

	// If 0 rows parsed, the XML format may have changed — warn but don't fail.
	if result.RowsSynced == 0 {
		log.Warn("ia_compilation: 0 rows parsed from feed; XML element names may have changed",
			zap.String("expected_element", "Firm"),
			zap.String("url", feedURL))
	}

	return result, nil
}

func (d *IACompilation) parseAndLoad(ctx context.Context, pool db.Pool, r io.Reader, log *zap.Logger) (*SyncResult, error) {
	firmCh, errCh := fetcher.StreamXML[iaFirm](ctx, r, "Firm")

	firmCols := []string{"crd_number", "firm_name", "sec_number", "city", "state", "country", "website"}
	firmConflict := []string{"crd_number"}
	filingCols := []string{"crd_number", "filing_date", "aum", "num_accounts", "legal_name", "num_employees", "total_employees", "sec_registered"}
	filingConflict := []string{"crd_number", "filing_date"}

	var firmBatch, filingBatch [][]any
	var totalFirms, totalFilings int64

	for firm := range firmCh {
		if firm.Info.CRDNumber == 0 {
			continue
		}

		state := strings.TrimSpace(firm.MainAddr.State)
		if len(state) > 2 {
			state = state[:2]
		}

		var website string
		if len(firm.FormInfo.Part1A.Item1.WebAddrs) > 0 {
			website = strings.TrimSpace(firm.FormInfo.Part1A.Item1.WebAddrs[0])
		}

		firmRow := []any{
			firm.Info.CRDNumber,
			strings.TrimSpace(firm.Info.FirmName),
			strings.TrimSpace(firm.Info.SECNumber),
			strings.TrimSpace(firm.MainAddr.City),
			state,
			strings.TrimSpace(firm.MainAddr.Country),
			website,
		}
		firmBatch = append(firmBatch, firmRow)

		filingDate := parseDate(firm.Filing.Date)
		if filingDate != nil {
			filingRow := []any{
				firm.Info.CRDNumber,
				filingDate,
				firm.FormInfo.Part1A.Item5F.AUM,
				firm.FormInfo.Part1A.Item5F.NumAccounts,
				sanitizeUTF8(strings.TrimSpace(firm.Info.FirmName)),
				firm.FormInfo.Part1A.Item5A.TotalEmployees,
				firm.FormInfo.Part1A.Item5A.TotalEmployees,
				true, // all firms in IA_FIRM_SEC_Feed are SEC-registered
			}
			filingBatch = append(filingBatch, filingRow)
		}

		if len(firmBatch) >= iaBatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table: "fed_data.adv_firms", Columns: firmCols, ConflictKeys: firmConflict,
			}, firmBatch)
			if err != nil {
				return nil, eris.Wrap(err, "ia_compilation: upsert firms")
			}
			totalFirms += n
			firmBatch = firmBatch[:0]
		}

		if len(filingBatch) >= iaBatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table:        "fed_data.adv_filings",
				Columns:      filingCols,
				ConflictKeys: filingConflict,
				UpdateCols:   []string{"aum", "num_accounts", "legal_name", "num_employees", "total_employees", "sec_registered"},
			}, filingBatch)
			if err != nil {
				return nil, eris.Wrap(err, "ia_compilation: upsert filings")
			}
			totalFilings += n
			filingBatch = filingBatch[:0]
		}
	}

	if err := <-errCh; err != nil {
		return nil, eris.Wrap(err, "ia_compilation: parse XML")
	}

	if len(firmBatch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table: "fed_data.adv_firms", Columns: firmCols, ConflictKeys: firmConflict,
		}, firmBatch)
		if err != nil {
			return nil, eris.Wrap(err, "ia_compilation: upsert firms final")
		}
		totalFirms += n
	}

	if len(filingBatch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        "fed_data.adv_filings",
			Columns:      filingCols,
			ConflictKeys: filingConflict,
			UpdateCols:   []string{"aum", "num_accounts", "legal_name", "num_employees", "total_employees", "sec_registered"},
		}, filingBatch)
		if err != nil {
			return nil, eris.Wrap(err, "ia_compilation: upsert filings final")
		}
		totalFilings += n
	}

	log.Info("ia_compilation sync complete", zap.Int64("firms", totalFirms), zap.Int64("filings", totalFilings))

	return &SyncResult{
		RowsSynced: totalFirms,
	}, nil
}

// userAgent returns the configured EDGAR User-Agent.
func (d *IACompilation) userAgent() string {
	if d.cfg != nil && d.cfg.Fedsync.EDGARUserAgent != "" {
		return d.cfg.Fedsync.EDGARUserAgent
	}
	return fmt.Sprintf("research-cli/1.0 (%s)", "fedsync")
}
