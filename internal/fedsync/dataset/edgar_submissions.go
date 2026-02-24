package dataset

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/db"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/fetcher"
)

const (
	submissionsZipURL    = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
	submissionsBatchSize = 10000
)

// EDGARSubmissions implements the EDGAR Submissions bulk JSON dataset.
// Downloads the bulk submissions ZIP from SEC, parses company data and recent filings,
// and upserts into edgar_entities and edgar_filings tables.
type EDGARSubmissions struct {
	cfg *config.Config
}

func (d *EDGARSubmissions) Name() string     { return "edgar_submissions" }
func (d *EDGARSubmissions) Table() string    { return "fed_data.edgar_entities" }
func (d *EDGARSubmissions) Phase() Phase     { return Phase1B }
func (d *EDGARSubmissions) Cadence() Cadence { return Weekly }

func (d *EDGARSubmissions) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return WeeklySchedule(now, lastSync)
}

// submissionJSON represents a single company submission JSON file from the bulk download.
type submissionJSON struct {
	CIK            string           `json:"cik"`
	EntityType     string           `json:"entityType"`
	SIC            string           `json:"sic"`
	SICDescription string           `json:"sicDescription"`
	Name           string           `json:"name"`
	StateOfInc     string           `json:"stateOfIncorporation"`
	Addresses      submissionAddrs  `json:"addresses"`
	EIN            string           `json:"ein"`
	Tickers        []string         `json:"tickers"`
	Exchanges      []string         `json:"exchanges"`
	RecentFilings  recentFilings    `json:"filings"`
}

type submissionAddrs struct {
	Business submissionAddr `json:"business"`
}

type submissionAddr struct {
	StateOrCountry string `json:"stateOrCountry"`
}

type recentFilings struct {
	Recent filingList `json:"recent"`
}

type filingList struct {
	AccessionNumber []string `json:"accessionNumber"`
	FilingDate      []string `json:"filingDate"`
	Form            []string `json:"form"`
	PrimaryDoc      []string `json:"primaryDocument"`
	PrimaryDocDesc  []string `json:"primaryDocDescription"`
	Items           []string `json:"items"`
	Size            []int    `json:"size"`
	IsXBRL          []int    `json:"isXBRL"`
	IsInlineXBRL    []int    `json:"isInlineXBRL"`
}

func (d *EDGARSubmissions) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "edgar_submissions"))

	zipPath := filepath.Join(tempDir, "submissions.zip")
	log.Info("downloading EDGAR submissions bulk ZIP", zap.String("url", submissionsZipURL))

	if _, err := f.DownloadToFile(ctx, submissionsZipURL, zipPath); err != nil {
		return nil, eris.Wrap(err, "edgar_submissions: download ZIP")
	}
	defer os.Remove(zipPath) //nolint:errcheck

	// Extract all JSON files from the ZIP.
	extractDir := filepath.Join(tempDir, "submissions")
	if err := os.MkdirAll(extractDir, 0o755); err != nil {
		return nil, eris.Wrap(err, "edgar_submissions: create extract dir")
	}
	defer os.RemoveAll(extractDir) //nolint:errcheck

	files, err := fetcher.ExtractZIP(zipPath, extractDir)
	if err != nil {
		return nil, eris.Wrap(err, "edgar_submissions: extract ZIP")
	}

	log.Info("extracted submission files", zap.Int("count", len(files)))

	entityCols := []string{"cik", "entity_name", "entity_type", "sic", "sic_description", "state_of_inc", "state_of_business", "ein", "tickers", "exchanges"}
	entityConflict := []string{"cik"}

	filingCols := []string{"accession_number", "cik", "form_type", "filing_date", "primary_doc", "primary_doc_desc", "items", "size", "is_xbrl", "is_inline_xbrl"}
	filingConflict := []string{"accession_number"}

	// Parallel decode: collect parsed rows from worker pool.
	var mu sync.Mutex
	var entityBatch [][]any
	var filingBatch [][]any
	var totalEntities, totalFilings int64

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(5)

	for _, fp := range files {
		fp := fp
		if !strings.HasSuffix(fp, ".json") {
			continue
		}
		base := filepath.Base(fp)
		if strings.HasPrefix(base, "filings-") {
			continue
		}

		g.Go(func() error {
			select {
			case <-gctx.Done():
				return gctx.Err()
			default:
			}

			sub, err := d.parseSubmissionFile(fp)
			if err != nil {
				log.Debug("skip submission file", zap.String("file", base), zap.Error(err))
				return nil
			}

			cik := strings.TrimLeft(sub.CIK, "0")
			if cik == "" || sub.Name == "" {
				return nil
			}

			cik = fmt.Sprintf("%010s", cik)
			if len(cik) > 10 {
				cik = cik[:10]
			}

			entityRow := []any{
				cik, sub.Name, sub.EntityType, sub.SIC, sub.SICDescription,
				sub.StateOfInc, sub.Addresses.Business.StateOrCountry, sub.EIN, sub.Tickers, sub.Exchanges,
			}

			var filingRows [][]any
			recent := sub.RecentFilings.Recent
			numFilings := len(recent.AccessionNumber)
			for i := range numFilings {
				accession := recent.AccessionNumber[i]
				if accession == "" {
					continue
				}

				filingRows = append(filingRows, []any{
					accession, cik,
					safeIndex(recent.Form, i),
					parseDate(safeIndex(recent.FilingDate, i)),
					safeIndex(recent.PrimaryDoc, i),
					safeIndex(recent.PrimaryDocDesc, i),
					safeIndex(recent.Items, i),
					safeIntIndex(recent.Size, i),
					safeIntIndex(recent.IsXBRL, i) == 1,
					safeIntIndex(recent.IsInlineXBRL, i) == 1,
				})
			}

			mu.Lock()
			entityBatch = append(entityBatch, entityRow)
			filingBatch = append(filingBatch, filingRows...)
			mu.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Upsert all collected entities in batches.
	for i := 0; i < len(entityBatch); i += submissionsBatchSize {
		end := i + submissionsBatchSize
		if end > len(entityBatch) {
			end = len(entityBatch)
		}
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table: "fed_data.edgar_entities", Columns: entityCols, ConflictKeys: entityConflict,
		}, entityBatch[i:end])
		if err != nil {
			return nil, eris.Wrap(err, "edgar_submissions: upsert entities")
		}
		totalEntities += n
	}

	// Deduplicate filings by accession number (multiple companies can reference the same filing).
	seen := make(map[string]struct{}, len(filingBatch))
	deduped := make([][]any, 0, len(filingBatch))
	for _, row := range filingBatch {
		acc := row[0].(string)
		if _, ok := seen[acc]; ok {
			continue
		}
		seen[acc] = struct{}{}
		deduped = append(deduped, row)
	}
	filingBatch = deduped

	// Upsert all collected filings in batches.
	for i := 0; i < len(filingBatch); i += submissionsBatchSize {
		end := i + submissionsBatchSize
		if end > len(filingBatch) {
			end = len(filingBatch)
		}
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table: "fed_data.edgar_filings", Columns: filingCols, ConflictKeys: filingConflict,
		}, filingBatch[i:end])
		if err != nil {
			return nil, eris.Wrap(err, "edgar_submissions: upsert filings")
		}
		totalFilings += n
	}

	log.Info("edgar_submissions sync complete",
		zap.Int64("entities", totalEntities),
		zap.Int64("filings", totalFilings),
	)

	return &SyncResult{
		RowsSynced: totalEntities,
		Metadata: map[string]any{
			"entities": totalEntities,
			"filings":  totalFilings,
			"files":    len(files),
		},
	}, nil
}

func (d *EDGARSubmissions) parseSubmissionFile(path string) (*submissionJSON, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, eris.Wrap(err, "open submission file")
	}
	defer file.Close() //nolint:errcheck

	return d.decodeSubmission(file)
}

func (d *EDGARSubmissions) decodeSubmission(r io.Reader) (*submissionJSON, error) {
	var sub submissionJSON
	if err := json.NewDecoder(r).Decode(&sub); err != nil {
		return nil, eris.Wrap(err, "decode submission JSON")
	}
	return &sub, nil
}

// safeIndex returns the string at index i, or empty string if out of bounds.
func safeIndex(s []string, i int) string {
	if i < len(s) {
		return s[i]
	}
	return ""
}

// safeIntIndex returns the int at index i, or 0 if out of bounds.
func safeIntIndex(s []int, i int) int {
	if i < len(s) {
		return s[i]
	}
	return 0
}
