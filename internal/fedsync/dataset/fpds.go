package dataset

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync/transform"
	"github.com/sells-group/research-cli/internal/fetcher"
)

const (
	fpdsBatchSize = 1000
	fpdsPageSize  = 100
	fpdsBaseURL   = "https://api.sam.gov/opportunities/v2/search"
)

// FPDS implements the Federal Procurement Data System dataset via SAM.gov API.
type FPDS struct {
	cfg *config.Config
}

func (d *FPDS) Name() string    { return "fpds" }
func (d *FPDS) Table() string   { return "fed_data.fpds_contracts" }
func (d *FPDS) Phase() Phase    { return Phase1 }
func (d *FPDS) Cadence() Cadence { return Daily }

func (d *FPDS) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return DailySchedule(now, lastSync)
}

func (d *FPDS) Sync(ctx context.Context, pool *pgxpool.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "fpds"))

	apiKey := ""
	if d.cfg != nil {
		apiKey = d.cfg.Fedsync.SAMKey
	}
	if apiKey == "" {
		return nil, eris.New("fpds: SAM API key not configured (fedsync.sam_api_key)")
	}

	var totalRows int64
	offset := 0

	// Build NAICS filter for financial services
	naicsFilter := strings.Join(transform.NAICSPrefixes, ",")

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		url := fmt.Sprintf("%s?api_key=%s&naics=%s&limit=%d&offset=%d&postedFrom=%s&postedTo=%s",
			fpdsBaseURL,
			apiKey,
			naicsFilter,
			fpdsPageSize,
			offset,
			time.Now().AddDate(0, 0, -30).Format("01/02/2006"),
			time.Now().Format("01/02/2006"),
		)

		log.Debug("fetching FPDS page", zap.Int("offset", offset))

		body, err := f.Download(ctx, url)
		if err != nil {
			return nil, eris.Wrapf(err, "fpds: fetch page at offset %d", offset)
		}

		data, err := io.ReadAll(body)
		body.Close()
		if err != nil {
			return nil, eris.Wrap(err, "fpds: read response body")
		}

		contracts, hasMore, err := d.parseResponse(data)
		if err != nil {
			return nil, eris.Wrapf(err, "fpds: parse response at offset %d", offset)
		}

		if len(contracts) == 0 {
			break
		}

		n, err := d.upsertContracts(ctx, pool, contracts)
		if err != nil {
			return nil, eris.Wrapf(err, "fpds: upsert at offset %d", offset)
		}
		totalRows += n
		log.Debug("upserted FPDS contracts", zap.Int("count", len(contracts)), zap.Int64("total", totalRows))

		if !hasMore {
			break
		}
		offset += fpdsPageSize
	}

	return &SyncResult{
		RowsSynced: totalRows,
		Metadata:   map[string]any{"api": "sam.gov"},
	}, nil
}

// samResponse represents the SAM.gov API response structure.
type samResponse struct {
	OpportunitiesData []samOpportunity `json:"opportunitiesData"`
	TotalRecords      int              `json:"totalRecords"`
}

type samOpportunity struct {
	NoticeID     string       `json:"noticeId"`
	PIID         string       `json:"solicitationNumber"`
	Agency       string       `json:"fullParentPathName"`
	AgencyCode   string       `json:"fullParentPathCode"`
	Title        string       `json:"title"`
	Description  string       `json:"description"`
	NAICS        string       `json:"naicsCode"`
	PSC          string       `json:"classificationCode"`
	PostedDate   string       `json:"postedDate"`
	Award        *samAward    `json:"award"`
	PointOfContact []samContact `json:"pointOfContact"`
}

type samAward struct {
	Amount float64 `json:"amount"`
	Awardee *samAwardee `json:"awardee"`
	Date    string      `json:"date"`
}

type samAwardee struct {
	Name    string     `json:"name"`
	UEI     string     `json:"ueiSAM"`
	DUNS    string     `json:"duns"`
	Location *samLocation `json:"location"`
}

type samLocation struct {
	City    string `json:"city"`
	State   string `json:"state"`
	Zip     string `json:"zip"`
}

type samContact struct {
	Type  string `json:"type"`
	Email string `json:"email"`
}

func (d *FPDS) parseResponse(data []byte) ([][]any, bool, error) {
	var resp samResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, false, eris.Wrap(err, "fpds: unmarshal JSON")
	}

	var rows [][]any
	for _, opp := range resp.OpportunitiesData {
		contractID := opp.NoticeID
		if contractID == "" {
			continue
		}

		var vendorName, vendorDUNS, vendorUEI, vendorCity, vendorState, vendorZip string
		var dollarsObligated float64
		var dateSigned *time.Time

		if opp.Award != nil {
			dollarsObligated = opp.Award.Amount
			if opp.Award.Date != "" {
				if t, err := time.Parse("2006-01-02", opp.Award.Date); err == nil {
					dateSigned = &t
				}
			}
			if opp.Award.Awardee != nil {
				vendorName = opp.Award.Awardee.Name
				vendorDUNS = opp.Award.Awardee.DUNS
				vendorUEI = opp.Award.Awardee.UEI
				if opp.Award.Awardee.Location != nil {
					vendorCity = opp.Award.Awardee.Location.City
					vendorState = opp.Award.Awardee.Location.State
					vendorZip = opp.Award.Awardee.Location.Zip
				}
			}
		}

		// Parse agency from path
		agencyName := opp.Agency
		agencyID := opp.AgencyCode
		if parts := strings.SplitN(agencyID, ".", 2); len(parts) > 0 {
			agencyID = parts[0]
		}
		if len(agencyID) > 4 {
			agencyID = agencyID[:4]
		}

		naics := transform.NormalizeNAICS(opp.NAICS)
		psc := opp.PSC
		if len(psc) > 4 {
			psc = psc[:4]
		}

		description := opp.Title
		if opp.Description != "" {
			description = opp.Description
		}

		row := []any{
			contractID,
			opp.PIID,
			agencyID,
			agencyName,
			vendorName,
			vendorDUNS,
			vendorUEI,
			vendorCity,
			vendorState,
			vendorZip,
			naics,
			psc,
			dateSigned,
			dollarsObligated,
			description,
		}

		rows = append(rows, row)
	}

	hasMore := len(resp.OpportunitiesData) >= fpdsPageSize
	return rows, hasMore, nil
}

func (d *FPDS) upsertContracts(ctx context.Context, pool *pgxpool.Pool, rows [][]any) (int64, error) {
	columns := []string{
		"contract_id", "piid", "agency_id", "agency_name",
		"vendor_name", "vendor_duns", "vendor_uei",
		"vendor_city", "vendor_state", "vendor_zip",
		"naics", "psc", "date_signed", "dollars_obligated", "description",
	}
	conflictKeys := []string{"contract_id"}

	var totalRows int64
	for i := 0; i < len(rows); i += fpdsBatchSize {
		end := i + fpdsBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        "fed_data.fpds_contracts",
			Columns:      columns,
			ConflictKeys: conflictKeys,
		}, rows[i:end])
		if err != nil {
			return totalRows, eris.Wrap(err, "fpds: bulk upsert")
		}
		totalRows += n
	}

	return totalRows, nil
}
