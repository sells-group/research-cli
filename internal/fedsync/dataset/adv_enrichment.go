package dataset

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/pkg/anthropic"
)

const (
	enrichBatchSize       = 50
	enrichBrochureLimit   = 500
	enrichCRSLimit        = 500
	enrichBrochureMaxChar = 15000
	enrichCRSMaxChar      = 8000
	enrichHaikuModel      = "claude-haiku-4-5-20251001"
	enrichMaxTokens       = 1024
)

// ADVEnrichment extracts structured data from ADV brochures and CRS documents
// using Haiku. Reads text_content from adv_brochures and adv_crs, produces
// enrichment rows in adv_brochure_enrichment and adv_crs_enrichment.
type ADVEnrichment struct {
	cfg    *config.Config
	client anthropic.Client // nil in production → created from cfg; set directly in tests
}

// Name implements Dataset.
func (d *ADVEnrichment) Name() string { return "adv_enrichment" }

// Table implements Dataset.
func (d *ADVEnrichment) Table() string { return "fed_data.adv_brochure_enrichment" }

// Phase implements Dataset.
func (d *ADVEnrichment) Phase() Phase { return Phase3 }

// Cadence implements Dataset.
func (d *ADVEnrichment) Cadence() Cadence { return Monthly }

// ShouldRun implements Dataset.
func (d *ADVEnrichment) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return MonthlySchedule(now, lastSync)
}

// Sync fetches and loads ADV brochure and CRS enrichment data.
func (d *ADVEnrichment) Sync(ctx context.Context, pool db.Pool, _ fetcher.Fetcher, _ string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))

	client := d.client
	if client == nil {
		if d.cfg == nil || d.cfg.Anthropic.Key == "" {
			return nil, eris.New("adv_enrichment: anthropic API key is required")
		}
		client = anthropic.NewClient(d.cfg.Anthropic.Key)
	}

	brochureRows, err := d.enrichBrochures(ctx, pool, client, log)
	if err != nil {
		return nil, eris.Wrap(err, "adv_enrichment: enrich brochures")
	}

	crsRows, err := d.enrichCRS(ctx, pool, client, log)
	if err != nil {
		return nil, eris.Wrap(err, "adv_enrichment: enrich CRS")
	}

	total := brochureRows + crsRows
	log.Info("adv_enrichment sync complete",
		zap.Int64("brochures", brochureRows),
		zap.Int64("crs", crsRows),
	)

	return &SyncResult{
		RowsSynced: total,
		Metadata: map[string]any{
			"brochures_enriched": brochureRows,
			"crs_enriched":       crsRows,
		},
	}, nil
}

// pendingDoc represents a document pending enrichment.
type pendingDoc struct {
	CRDNumber int
	DocID     string
	Text      string
}

// enrichBrochures queries un-enriched brochures and extracts structured data via Haiku.
func (d *ADVEnrichment) enrichBrochures(ctx context.Context, pool db.Pool, client anthropic.Client, log *zap.Logger) (int64, error) {
	query := `SELECT b.crd_number, b.brochure_id, b.text_content
		FROM fed_data.adv_brochures b
		LEFT JOIN fed_data.adv_brochure_enrichment e
			ON b.crd_number = e.crd_number AND b.brochure_id = e.brochure_id
		WHERE b.text_content IS NOT NULL AND e.crd_number IS NULL
		LIMIT $1`

	rows, err := pool.Query(ctx, query, enrichBrochureLimit)
	if err != nil {
		return 0, eris.Wrap(err, "query pending brochures")
	}
	defer rows.Close()

	var docs []pendingDoc
	for rows.Next() {
		var doc pendingDoc
		if err := rows.Scan(&doc.CRDNumber, &doc.DocID, &doc.Text); err != nil {
			return 0, eris.Wrap(err, "scan brochure row")
		}
		docs = append(docs, doc)
	}
	if err := rows.Err(); err != nil {
		return 0, eris.Wrap(err, "iterate brochure rows")
	}

	if len(docs) == 0 {
		log.Info("no pending brochures to enrich")
		return 0, nil
	}

	cols := []string{"crd_number", "brochure_id", "investment_strategies", "industry_specializations",
		"min_account_size", "fee_schedule", "target_clients", "model", "input_tokens", "output_tokens", "enriched_at"}
	conflictKeys := []string{"crd_number", "brochure_id"}

	var batch [][]any
	var total int64

	for _, doc := range docs {
		select {
		case <-ctx.Done():
			return total, ctx.Err()
		default:
		}

		text := doc.Text
		if len(text) > enrichBrochureMaxChar {
			text = text[:enrichBrochureMaxChar]
		}

		prompt := fmt.Sprintf(`Extract structured data from this SEC ADV Part 2 brochure for CRD %d. Return ONLY valid JSON:
{
  "investment_strategies": ["strategy1", ...],
  "industry_specializations": ["industry1", ...],
  "min_account_size": 500000,
  "fee_schedule": "1%% on first $1M, 0.75%% thereafter",
  "target_clients": "High net worth individuals and family offices"
}
Rules:
- investment_strategies: Array of investment strategy descriptions (e.g., "equity growth", "fixed income", "alternatives")
- industry_specializations: Array of industry focus areas (e.g., "healthcare", "technology", "real estate")
- min_account_size: Minimum account size in dollars as integer, or 0 if not stated
- fee_schedule: Brief description of the fee structure
- target_clients: Brief description of target client types

Brochure text:
%s`, doc.CRDNumber, text)

		resp, err := client.CreateMessage(ctx, anthropic.MessageRequest{
			Model:     enrichHaikuModel,
			MaxTokens: enrichMaxTokens,
			Messages:  []anthropic.Message{{Role: "user", Content: prompt}},
		})
		if err != nil {
			log.Debug("skipping brochure enrichment", zap.Int("crd", doc.CRDNumber), zap.Error(err))
			continue
		}

		respText := extractResponseText(resp)
		var result brochureEnrichResult
		cleaned := cleanJSONFromText(respText)
		if err := json.Unmarshal([]byte(cleaned), &result); err != nil {
			log.Debug("failed to parse brochure enrichment JSON",
				zap.Int("crd", doc.CRDNumber), zap.Error(err))
			continue
		}

		strategies, _ := json.Marshal(result.InvestmentStrategies)
		industries, _ := json.Marshal(result.IndustrySpecializations)

		row := []any{
			doc.CRDNumber,
			doc.DocID,
			json.RawMessage(strategies),
			json.RawMessage(industries),
			result.MinAccountSize,
			result.FeeSchedule,
			result.TargetClients,
			resp.Model,
			resp.Usage.InputTokens,
			resp.Usage.OutputTokens,
			time.Now(),
		}
		batch = append(batch, row)

		if len(batch) >= enrichBatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table: "fed_data.adv_brochure_enrichment", Columns: cols, ConflictKeys: conflictKeys,
			}, batch)
			if err != nil {
				return total, eris.Wrap(err, "upsert brochure enrichment")
			}
			total += n
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table: "fed_data.adv_brochure_enrichment", Columns: cols, ConflictKeys: conflictKeys,
		}, batch)
		if err != nil {
			return total, eris.Wrap(err, "upsert brochure enrichment final")
		}
		total += n
	}

	return total, nil
}

// enrichCRS queries un-enriched CRS documents and extracts structured data via Haiku.
func (d *ADVEnrichment) enrichCRS(ctx context.Context, pool db.Pool, client anthropic.Client, log *zap.Logger) (int64, error) {
	query := `SELECT c.crd_number, c.crs_id, c.text_content
		FROM fed_data.adv_crs c
		LEFT JOIN fed_data.adv_crs_enrichment e
			ON c.crd_number = e.crd_number AND c.crs_id = e.crs_id
		WHERE c.text_content IS NOT NULL AND e.crd_number IS NULL
		LIMIT $1`

	rows, err := pool.Query(ctx, query, enrichCRSLimit)
	if err != nil {
		return 0, eris.Wrap(err, "query pending CRS")
	}
	defer rows.Close()

	var docs []pendingDoc
	for rows.Next() {
		var doc pendingDoc
		if err := rows.Scan(&doc.CRDNumber, &doc.DocID, &doc.Text); err != nil {
			return 0, eris.Wrap(err, "scan CRS row")
		}
		docs = append(docs, doc)
	}
	if err := rows.Err(); err != nil {
		return 0, eris.Wrap(err, "iterate CRS rows")
	}

	if len(docs) == 0 {
		log.Info("no pending CRS to enrich")
		return 0, nil
	}

	cols := []string{"crd_number", "crs_id", "firm_type", "key_services", "fee_types",
		"has_disciplinary_history", "conflicts_of_interest", "model", "input_tokens", "output_tokens", "enriched_at"}
	conflictKeys := []string{"crd_number", "crs_id"}

	var batch [][]any
	var total int64

	for _, doc := range docs {
		select {
		case <-ctx.Done():
			return total, ctx.Err()
		default:
		}

		text := doc.Text
		if len(text) > enrichCRSMaxChar {
			text = text[:enrichCRSMaxChar]
		}

		prompt := fmt.Sprintf(`Extract structured data from this SEC Form CRS for CRD %d. Return ONLY valid JSON:
{
  "firm_type": "investment adviser",
  "key_services": "Portfolio management, financial planning",
  "fee_types": ["asset-based", "hourly"],
  "has_disciplinary_history": false,
  "conflicts_of_interest": "Revenue sharing with affiliates"
}
Rules:
- firm_type: One of "investment adviser", "broker-dealer", "dual registrant", or other description
- key_services: Comma-separated list of key services offered
- fee_types: Array of fee type descriptions (e.g., "asset-based", "hourly", "fixed", "commission")
- has_disciplinary_history: true if disciplinary history is disclosed, false otherwise
- conflicts_of_interest: Brief description of disclosed conflicts

CRS text:
%s`, doc.CRDNumber, text)

		resp, err := client.CreateMessage(ctx, anthropic.MessageRequest{
			Model:     enrichHaikuModel,
			MaxTokens: enrichMaxTokens,
			Messages:  []anthropic.Message{{Role: "user", Content: prompt}},
		})
		if err != nil {
			log.Debug("skipping CRS enrichment", zap.Int("crd", doc.CRDNumber), zap.Error(err))
			continue
		}

		respText := extractResponseText(resp)
		var result crsEnrichResult
		cleaned := cleanJSONFromText(respText)
		if err := json.Unmarshal([]byte(cleaned), &result); err != nil {
			log.Debug("failed to parse CRS enrichment JSON",
				zap.Int("crd", doc.CRDNumber), zap.Error(err))
			continue
		}

		feeTypes, _ := json.Marshal(result.FeeTypes)

		row := []any{
			doc.CRDNumber,
			doc.DocID,
			result.FirmType,
			result.KeyServices,
			json.RawMessage(feeTypes),
			result.HasDisciplinaryHistory,
			result.ConflictsOfInterest,
			resp.Model,
			resp.Usage.InputTokens,
			resp.Usage.OutputTokens,
			time.Now(),
		}
		batch = append(batch, row)

		if len(batch) >= enrichBatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table: "fed_data.adv_crs_enrichment", Columns: cols, ConflictKeys: conflictKeys,
			}, batch)
			if err != nil {
				return total, eris.Wrap(err, "upsert CRS enrichment")
			}
			total += n
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table: "fed_data.adv_crs_enrichment", Columns: cols, ConflictKeys: conflictKeys,
		}, batch)
		if err != nil {
			return total, eris.Wrap(err, "upsert CRS enrichment final")
		}
		total += n
	}

	return total, nil
}

// --- Response types ---

type brochureEnrichResult struct {
	InvestmentStrategies    []string `json:"investment_strategies"`
	IndustrySpecializations []string `json:"industry_specializations"`
	MinAccountSize          int64    `json:"min_account_size"`
	FeeSchedule             string   `json:"fee_schedule"`
	TargetClients           string   `json:"target_clients"`
}

type crsEnrichResult struct {
	FirmType               string   `json:"firm_type"`
	KeyServices            string   `json:"key_services"`
	FeeTypes               []string `json:"fee_types"`
	HasDisciplinaryHistory bool     `json:"has_disciplinary_history"`
	ConflictsOfInterest    string   `json:"conflicts_of_interest"`
}

// --- Helpers ---

// extractResponseText returns the concatenated text from a message response.
func extractResponseText(resp *anthropic.MessageResponse) string {
	if resp == nil {
		return ""
	}
	var sb strings.Builder
	for _, b := range resp.Content {
		if b.Type == "" || b.Type == "text" {
			sb.WriteString(b.Text)
		}
	}
	return sb.String()
}

// cleanJSONFromText attempts to extract a JSON object from text that may contain
// markdown code fences or other wrapping. Localized from internal/pipeline/linkedin.go
// to avoid cross-package dependency.
func cleanJSONFromText(text string) string {
	text = strings.TrimSpace(text)

	// Strip markdown code fences.
	if strings.HasPrefix(text, "```json") {
		text = strings.TrimPrefix(text, "```json")
		if idx := strings.LastIndex(text, "```"); idx >= 0 {
			text = text[:idx]
		}
	} else if strings.HasPrefix(text, "```") {
		text = strings.TrimPrefix(text, "```")
		if idx := strings.LastIndex(text, "```"); idx >= 0 {
			text = text[:idx]
		}
	}

	// Find first { and last }.
	start := strings.Index(text, "{")
	end := strings.LastIndex(text, "}")
	if start >= 0 && end > start {
		text = text[start : end+1]
	}

	return strings.TrimSpace(text)
}
