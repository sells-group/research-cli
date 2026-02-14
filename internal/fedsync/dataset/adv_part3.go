package dataset

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/ocr"
)

const advPart3BatchSize = 100

// ADVPart3 syncs CRS (Client Relationship Summary) PDFs → OCR → text.
type ADVPart3 struct {
	cfg *config.Config
}

func (d *ADVPart3) Name() string    { return "adv_part3" }
func (d *ADVPart3) Table() string   { return "fed_data.adv_crs" }
func (d *ADVPart3) Phase() Phase    { return Phase3 }
func (d *ADVPart3) Cadence() Cadence { return Monthly }

func (d *ADVPart3) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return MonthlySchedule(now, lastSync)
}

func (d *ADVPart3) Sync(ctx context.Context, pool *pgxpool.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))

	ext, err := ocr.NewExtractor(d.cfg.Fedsync.OCR, d.cfg.Fedsync.MistralKey)
	if err != nil {
		return nil, eris.Wrap(err, "adv_part3: create OCR extractor")
	}

	// Query adv_firms for CRD numbers that need CRS updates.
	rows, err := pool.Query(ctx,
		`SELECT crd_number FROM fed_data.adv_firms
		 WHERE crd_number NOT IN (SELECT crd_number FROM fed_data.adv_crs)
		 ORDER BY crd_number LIMIT 500`)
	if err != nil {
		return nil, eris.Wrap(err, "adv_part3: query adv_firms")
	}
	defer rows.Close()

	var crdNumbers []int
	for rows.Next() {
		var crd int
		if err := rows.Scan(&crd); err != nil {
			return nil, eris.Wrap(err, "adv_part3: scan crd_number")
		}
		crdNumbers = append(crdNumbers, crd)
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "adv_part3: iterate crd_numbers")
	}

	log.Info("processing CRS documents", zap.Int("firms", len(crdNumbers)))

	columns := []string{"crd_number", "crs_id", "filing_date", "text_content", "extracted_at"}
	conflictKeys := []string{"crd_number", "crs_id"}

	var batch [][]any
	var totalRows int64

	for _, crd := range crdNumbers {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// SEC CRS document URL pattern from IAPD system.
		crsURL := fmt.Sprintf("https://advfm.sec.gov/IAPD/Content/Common/crd_ia_doc.aspx?ESSION=crs&ESSION_CRD=%d", crd)
		crsID := fmt.Sprintf("crs_%d", crd)

		pdfPath := filepath.Join(tempDir, fmt.Sprintf("adv_crs_%d.pdf", crd))
		if _, err := f.DownloadToFile(ctx, crsURL, pdfPath); err != nil {
			log.Debug("skipping CRS download", zap.Int("crd", crd), zap.Error(err))
			continue
		}

		text, err := ext.ExtractText(ctx, pdfPath)
		if err != nil {
			log.Debug("skipping CRS OCR", zap.Int("crd", crd), zap.Error(err))
			_ = os.Remove(pdfPath)
			continue
		}
		_ = os.Remove(pdfPath)

		now := time.Now()
		batch = append(batch, []any{crd, crsID, now.Format("2006-01-02"), text, now})

		if len(batch) >= advPart3BatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table:        d.Table(),
				Columns:      columns,
				ConflictKeys: conflictKeys,
			}, batch)
			if err != nil {
				return nil, eris.Wrap(err, "adv_part3: bulk upsert")
			}
			totalRows += n
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        d.Table(),
			Columns:      columns,
			ConflictKeys: conflictKeys,
		}, batch)
		if err != nil {
			return nil, eris.Wrap(err, "adv_part3: bulk upsert final batch")
		}
		totalRows += n
	}

	return &SyncResult{
		RowsSynced: totalRows,
		Metadata:   map[string]any{"firms_processed": len(crdNumbers)},
	}, nil
}
