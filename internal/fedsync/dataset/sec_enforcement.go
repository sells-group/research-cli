package dataset

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
)

const (
	// SEC EDGAR litigation releases API endpoint.
	secEnforcementURL = "https://efts.sec.gov/LATEST/search-index?q=%%22enforcement+action%%22&dateRange=custom&startdt=%s&enddt=%s&forms=LIT_REL"
)

// SECEnforcement implements the Dataset interface for SEC enforcement actions.
type SECEnforcement struct{}

// Name implements Dataset.
func (d *SECEnforcement) Name() string { return "sec_enforcement" }

// Table implements Dataset.
func (d *SECEnforcement) Table() string { return "fed_data.sec_enforcement_actions" }

// Phase implements Dataset.
func (d *SECEnforcement) Phase() Phase { return Phase2 }

// Cadence implements Dataset.
func (d *SECEnforcement) Cadence() Cadence { return Monthly }

// ShouldRun implements Dataset.
func (d *SECEnforcement) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return MonthlySchedule(now, lastSync)
}

// enforcementSearchResult represents the EFTS search API response.
type enforcementSearchResult struct {
	Hits struct {
		Hits []struct {
			ID     string `json:"_id"`
			Source struct {
				DisplayNames []string `json:"display_names"`
				FileDate     string   `json:"file_date"`
				FormType     string   `json:"form_type"`
				DisplayDesc  string   `json:"display_description"`
				FileNum      string   `json:"file_num"`
			} `json:"_source"`
		} `json:"hits"`
		Total struct {
			Value int `json:"value"`
		} `json:"total"`
	} `json:"hits"`
}

// Sync fetches recent SEC enforcement actions and upserts into the database.
func (d *SECEnforcement) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "sec_enforcement"))

	// Fetch enforcement actions from the last 90 days.
	endDate := time.Now()
	startDate := endDate.AddDate(0, -3, 0)

	url := fmt.Sprintf(secEnforcementURL,
		startDate.Format("2006-01-02"),
		endDate.Format("2006-01-02"))

	log.Info("fetching SEC enforcement actions",
		zap.String("start", startDate.Format("2006-01-02")),
		zap.String("end", endDate.Format("2006-01-02")))

	rc, err := f.Download(ctx, url)
	if err != nil {
		return nil, eris.Wrap(err, "sec_enforcement: fetch EFTS search")
	}
	defer rc.Close() //nolint:errcheck

	var result enforcementSearchResult
	if err := json.NewDecoder(rc).Decode(&result); err != nil {
		return nil, eris.Wrap(err, "sec_enforcement: parse EFTS response")
	}

	log.Info("parsed enforcement results", zap.Int("hits", result.Hits.Total.Value))

	cols := []string{
		"action_id", "action_type", "respondent_name",
		"crd_number", "cik", "action_date", "description",
		"outcome", "penalty_amount", "url",
	}
	conflictKeys := []string{"action_id"}

	var rows [][]any
	for _, hit := range result.Hits.Hits {
		src := hit.Source

		actionDate := parseDate(src.FileDate)
		respondent := ""
		if len(src.DisplayNames) > 0 {
			respondent = strings.Join(src.DisplayNames, "; ")
		}

		row := []any{
			hit.ID,
			src.FormType,
			respondent,
			nil, // crd_number — resolved via entity_xref later
			nil, // cik
			actionDate,
			src.DisplayDesc,
			nil, // outcome — not in search results
			nil, // penalty_amount — not in search results
			fmt.Sprintf("https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&filenum=%s", src.FileNum),
		}
		rows = append(rows, row)
	}

	if len(rows) == 0 {
		log.Info("no new enforcement actions found")
		return &SyncResult{RowsSynced: 0}, nil
	}

	n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
		Table:        "fed_data.sec_enforcement_actions",
		Columns:      cols,
		ConflictKeys: conflictKeys,
	}, rows)
	if err != nil {
		return nil, eris.Wrap(err, "sec_enforcement: upsert actions")
	}

	log.Info("sec_enforcement sync complete", zap.Int64("rows", n))

	return &SyncResult{
		RowsSynced: n,
		Metadata:   map[string]any{"total_hits": result.Hits.Total.Value, "synced": n},
	}, nil
}
