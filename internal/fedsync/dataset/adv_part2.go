package dataset

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/db"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/ocr"
)

const advPart2BatchSize = 100

// ADVPart2 syncs SEC ADV Part 2 brochure PDFs → OCR → text.
type ADVPart2 struct {
	cfg *config.Config
}

func (d *ADVPart2) Name() string     { return "adv_part2" }
func (d *ADVPart2) Table() string    { return "fed_data.adv_brochures" }
func (d *ADVPart2) Phase() Phase     { return Phase2 }
func (d *ADVPart2) Cadence() Cadence { return Monthly }

func (d *ADVPart2) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return MonthlySchedule(now, lastSync)
}

func (d *ADVPart2) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))

	ext, err := ocr.NewExtractor(d.cfg.Fedsync.OCR, d.cfg.Fedsync.MistralKey)
	if err != nil {
		return nil, eris.Wrap(err, "adv_part2: create OCR extractor")
	}

	// Query adv_firms for CRD numbers that need brochure updates.
	rows, err := pool.Query(ctx,
		`SELECT crd_number FROM fed_data.adv_firms
		 WHERE crd_number NOT IN (SELECT crd_number FROM fed_data.adv_brochures)
		 ORDER BY crd_number LIMIT 500`)
	if err != nil {
		return nil, eris.Wrap(err, "adv_part2: query adv_firms")
	}
	defer rows.Close()

	var crdNumbers []int
	for rows.Next() {
		var crd int
		if err := rows.Scan(&crd); err != nil {
			return nil, eris.Wrap(err, "adv_part2: scan crd_number")
		}
		crdNumbers = append(crdNumbers, crd)
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "adv_part2: iterate crd_numbers")
	}

	log.Info("processing ADV Part 2 brochures", zap.Int("firms", len(crdNumbers)))

	columns := []string{"crd_number", "brochure_id", "filing_date", "text_content", "extracted_at"}
	conflictKeys := []string{"crd_number", "brochure_id"}

	var batch [][]any
	var totalRows int64

	for _, crd := range crdNumbers {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// SEC ADV brochure URL pattern from IAPD system.
		brochureURL := fmt.Sprintf("https://advfm.sec.gov/IAPD/Content/Common/crd_ia_doc.aspx?ESSION=brochure&ESSION_CRD=%d", crd)
		brochureID := fmt.Sprintf("brochure_%d", crd)

		pdfPath := filepath.Join(tempDir, fmt.Sprintf("adv_part2_%d.pdf", crd))
		if _, err := f.DownloadToFile(ctx, brochureURL, pdfPath); err != nil {
			log.Debug("skipping brochure download", zap.Int("crd", crd), zap.Error(err))
			continue
		}

		text, err := ext.ExtractText(ctx, pdfPath)
		if err != nil {
			log.Debug("skipping brochure OCR", zap.Int("crd", crd), zap.Error(err))
			_ = os.Remove(pdfPath)
			continue
		}
		_ = os.Remove(pdfPath)

		now := time.Now()
		batch = append(batch, []any{crd, brochureID, now.Format("2006-01-02"), text, now})

		if len(batch) >= advPart2BatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table:        d.Table(),
				Columns:      columns,
				ConflictKeys: conflictKeys,
			}, batch)
			if err != nil {
				return nil, eris.Wrap(err, "adv_part2: bulk upsert")
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
			return nil, eris.Wrap(err, "adv_part2: bulk upsert final batch")
		}
		totalRows += n
	}

	return &SyncResult{
		RowsSynced: totalRows,
		Metadata:   map[string]any{"firms_processed": len(crdNumbers)},
	}, nil
}
