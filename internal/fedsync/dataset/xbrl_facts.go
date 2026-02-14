package dataset

import (
	"context"
	"fmt"
	"time"

	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/db"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/fedsync/xbrl"
	"github.com/sells-group/research-cli/internal/fetcher"
)

// XBRLFacts syncs EDGAR Company Facts JSON-LD → XBRL financial data.
type XBRLFacts struct {
	cfg *config.Config
}

func (d *XBRLFacts) Name() string     { return "xbrl_facts" }
func (d *XBRLFacts) Table() string    { return "fed_data.xbrl_facts" }
func (d *XBRLFacts) Phase() Phase     { return Phase3 }
func (d *XBRLFacts) Cadence() Cadence { return Daily }

func (d *XBRLFacts) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return DailySchedule(now, lastSync)
}

func (d *XBRLFacts) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))
	log.Info("syncing XBRL company facts")

	// Get CIKs from entity_xref that have linked EDGAR entities.
	cikRows, err := pool.Query(ctx,
		"SELECT DISTINCT cik FROM fed_data.entity_xref WHERE cik IS NOT NULL AND cik != '' LIMIT 1000")
	if err != nil {
		return nil, eris.Wrap(err, "xbrl_facts: query CIKs")
	}
	defer cikRows.Close()

	var ciks []string
	for cikRows.Next() {
		var cik string
		if err := cikRows.Scan(&cik); err != nil {
			return nil, eris.Wrap(err, "xbrl_facts: scan CIK")
		}
		ciks = append(ciks, cik)
	}

	log.Info("fetching company facts", zap.Int("cik_count", len(ciks)))

	var totalRows int64
	const batchSize = 500

	var rows [][]any

	for _, cik := range ciks {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Pad CIK to 10 digits for URL.
		paddedCIK := fmt.Sprintf("%010s", cik)
		url := fmt.Sprintf("https://data.sec.gov/api/xbrl/companyfacts/CIK%s.json", paddedCIK)

		body, err := f.Download(ctx, url)
		if err != nil {
			log.Debug("skip CIK", zap.String("cik", cik), zap.Error(err))
			continue
		}

		facts, err := xbrl.ParseCompanyFacts(body)
		body.Close()
		if err != nil {
			log.Debug("skip malformed facts", zap.String("cik", cik), zap.Error(err))
			continue
		}

		extracted := xbrl.ExtractTargetFacts(facts, xbrl.TargetFacts)
		for _, ef := range extracted {
			rows = append(rows, []any{
				cik,
				ef.FactName,
				ef.Period,
				ef.Value,
				ef.Unit,
				ef.Form,
				int16(ef.FY),
				ef.Filed,
			})
		}

		if len(rows) >= batchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table:        d.Table(),
				Columns:      []string{"cik", "fact_name", "period_end", "value", "unit", "form", "fy", "accession"},
				ConflictKeys: []string{"cik", "fact_name", "period_end"},
			}, rows)
			if err != nil {
				return nil, eris.Wrap(err, "xbrl_facts: upsert")
			}
			totalRows += n
			rows = rows[:0]
		}
	}

	if len(rows) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        d.Table(),
			Columns:      []string{"cik", "fact_name", "period_end", "value", "unit", "form", "fy", "accession"},
			ConflictKeys: []string{"cik", "fact_name", "period_end"},
		}, rows)
		if err != nil {
			return nil, eris.Wrap(err, "xbrl_facts: upsert final")
		}
		totalRows += n
	}

	return &SyncResult{RowsSynced: totalRows}, nil
}
