package dataset

import (
	"context"
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

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/fetcher"
)

const (
	submissionsZipURL    = "https://data.sec.gov/submissions/submissions.zip"
	submissionsBatchSize = 5000
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
	CIK             json.Number   `json:"cik"`
	EntityType      string        `json:"entityType"`
	SIC             string        `json:"sic"`
	SICDescription  string        `json:"sicDescription"`
	Name            string        `json:"name"`
	StateOfInc      string        `json:"stateOfIncorporation"`
	StateOfBusiness string        `json:"addresses,omitempty"`
	EIN             string        `json:"ein"`
	Tickers         []string      `json:"tickers"`
	Exchanges       []string      `json:"exchanges"`
	RecentFilings   recentFilings `json:"filings"`
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
	defer os.Remove(zipPath)

	// Extract all JSON files from the ZIP.
	extractDir := filepath.Join(tempDir, "submissions")
	if err := os.MkdirAll(extractDir, 0o755); err != nil {
		return nil, eris.Wrap(err, "edgar_submissions: create extract dir")
	}
	defer os.RemoveAll(extractDir)

	files, err := fetcher.ExtractZIP(zipPath, extractDir)
	if err != nil {
		return nil, eris.Wrap(err, "edgar_submissions: extract ZIP")
	}

	log.Info("extracted submission files", zap.Int("count", len(files)))

	var totalEntities, totalFilings int64

	entityCols := []string{"cik", "entity_name", "entity_type", "sic", "sic_description", "state_of_inc", "state_of_business", "ein", "tickers", "exchanges"}
	entityConflict := []string{"cik"}

	filingCols := []string{"accession_number", "cik", "form_type", "filing_date", "primary_doc", "primary_doc_desc", "items", "size", "is_xbrl", "is_inline_xbrl"}
	filingConflict := []string{"accession_number"}

	var entityBatch [][]any
	var filingBatch [][]any

	for _, filePath := range files {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if !strings.HasSuffix(filePath, ".json") {
			continue
		}

		// Skip supplemental files (filings-*.json).
		base := filepath.Base(filePath)
		if strings.HasPrefix(base, "filings-") {
			continue
		}

		sub, err := d.parseSubmissionFile(filePath)
		if err != nil {
			log.Debug("skip submission file", zap.String("file", base), zap.Error(err))
			continue
		}

		cik := strings.TrimLeft(sub.CIK.String(), "0")
		if cik == "" || sub.Name == "" {
			continue
		}

		// Pad CIK to 10 chars for consistency.
		cik = fmt.Sprintf("%010s", cik)
		if len(cik) > 10 {
			cik = cik[:10]
		}

		// Parse business state from addresses if available.
		stateOfBusiness := ""

		entityRow := []any{
			cik,
			sub.Name,
			sub.EntityType,
			sub.SIC,
			sub.SICDescription,
			sub.StateOfInc,
			stateOfBusiness,
			sub.EIN,
			sub.Tickers,
			sub.Exchanges,
		}
		entityBatch = append(entityBatch, entityRow)

		// Process recent filings.
		recent := sub.RecentFilings.Recent
		numFilings := len(recent.AccessionNumber)
		for i := range numFilings {
			accession := recent.AccessionNumber[i]
			if accession == "" {
				continue
			}

			formType := safeIndex(recent.Form, i)
			fileDateStr := safeIndex(recent.FilingDate, i)
			filingDate := parseDate(fileDateStr)
			primaryDoc := safeIndex(recent.PrimaryDoc, i)
			primaryDocDesc := safeIndex(recent.PrimaryDocDesc, i)
			items := safeIndex(recent.Items, i)
			size := safeIntIndex(recent.Size, i)
			isXBRL := safeIntIndex(recent.IsXBRL, i) == 1
			isInlineXBRL := safeIntIndex(recent.IsInlineXBRL, i) == 1

			filingRow := []any{
				accession,
				cik,
				formType,
				filingDate,
				primaryDoc,
				primaryDocDesc,
				items,
				size,
				isXBRL,
				isInlineXBRL,
			}
			filingBatch = append(filingBatch, filingRow)
		}

		// Flush entity batch.
		if len(entityBatch) >= submissionsBatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table: "fed_data.edgar_entities", Columns: entityCols, ConflictKeys: entityConflict,
			}, entityBatch)
			if err != nil {
				return nil, eris.Wrap(err, "edgar_submissions: upsert entities")
			}
			totalEntities += n
			entityBatch = entityBatch[:0]
		}

		// Flush filing batch.
		if len(filingBatch) >= submissionsBatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table: "fed_data.edgar_filings", Columns: filingCols, ConflictKeys: filingConflict,
			}, filingBatch)
			if err != nil {
				return nil, eris.Wrap(err, "edgar_submissions: upsert filings")
			}
			totalFilings += n
			filingBatch = filingBatch[:0]
		}
	}

	// Flush remaining entity batch.
	if len(entityBatch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table: "fed_data.edgar_entities", Columns: entityCols, ConflictKeys: entityConflict,
		}, entityBatch)
		if err != nil {
			return nil, eris.Wrap(err, "edgar_submissions: upsert entities final")
		}
		totalEntities += n
	}

	// Flush remaining filing batch.
	if len(filingBatch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table: "fed_data.edgar_filings", Columns: filingCols, ConflictKeys: filingConflict,
		}, filingBatch)
		if err != nil {
			return nil, eris.Wrap(err, "edgar_submissions: upsert filings final")
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
	defer file.Close()

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
