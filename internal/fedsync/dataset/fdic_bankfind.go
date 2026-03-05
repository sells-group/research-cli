package dataset

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
)

const (
	fdicInstitutionsURL = "https://api.fdic.gov/banks/institutions"
	fdicLocationsURL    = "https://api.fdic.gov/banks/locations"
	fdicPageSize        = 10000
	fdicBatchSize       = 5000
)

// fdicResponse is the top-level FDIC API response envelope.
type fdicResponse struct {
	Meta struct {
		Total int `json:"total"`
	} `json:"meta"`
	Data []fdicRecord `json:"data"`
}

// fdicRecord is a single record from the FDIC API.
type fdicRecord struct {
	Data  map[string]any `json:"data"`
	Score float64        `json:"score"`
}

// FDICBankFind syncs FDIC BankFind institution and branch data.
type FDICBankFind struct{}

// Name implements Dataset.
func (d *FDICBankFind) Name() string { return "fdic_bankfind" }

// Table implements Dataset.
func (d *FDICBankFind) Table() string { return "fed_data.fdic_institutions" }

// Phase implements Dataset.
func (d *FDICBankFind) Phase() Phase { return Phase2 }

// Cadence implements Dataset.
func (d *FDICBankFind) Cadence() Cadence { return Weekly }

// ShouldRun implements Dataset.
func (d *FDICBankFind) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return WeeklySchedule(now, lastSync)
}

// Sync fetches and loads FDIC institution and branch data.
func (d *FDICBankFind) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))

	// Stage 1: Institutions
	log.Info("syncing FDIC institutions")
	instRows, err := d.syncInstitutions(ctx, f, pool, log)
	if err != nil {
		return nil, eris.Wrap(err, "fdic_bankfind: sync institutions")
	}

	// Stage 2: Branches
	log.Info("syncing FDIC branches")
	branchRows, err := d.syncBranches(ctx, f, pool, log)
	if err != nil {
		return nil, eris.Wrap(err, "fdic_bankfind: sync branches")
	}

	total := instRows + branchRows
	log.Info("fdic_bankfind sync complete",
		zap.Int64("institutions", instRows),
		zap.Int64("branches", branchRows),
		zap.Int64("total", total),
	)
	return &SyncResult{
		RowsSynced: total,
		Metadata: map[string]any{
			"institutions": instRows,
			"branches":     branchRows,
		},
	}, nil
}

func (d *FDICBankFind) syncInstitutions(ctx context.Context, f fetcher.Fetcher, pool db.Pool, log *zap.Logger) (int64, error) {
	var totalRows int64
	offset := 0

	for {
		if err := ctx.Err(); err != nil {
			return totalRows, eris.Wrap(err, "fdic_bankfind: context cancelled")
		}

		url := fmt.Sprintf("%s?limit=%d&offset=%d&sort_by=CERT&sort_order=ASC", fdicInstitutionsURL, fdicPageSize, offset)
		resp, err := d.fetchPage(ctx, f, url)
		if err != nil {
			return totalRows, eris.Wrapf(err, "fdic_bankfind: fetch institutions page offset=%d", offset)
		}

		if len(resp.Data) == 0 {
			break
		}

		rows := make([][]any, 0, len(resp.Data))
		for _, rec := range resp.Data {
			rows = append(rows, parseInstitution(rec.Data))
		}

		// Batch upsert
		for i := 0; i < len(rows); i += fdicBatchSize {
			end := i + fdicBatchSize
			if end > len(rows) {
				end = len(rows)
			}
			n, err := db.BulkUpsert(ctx, pool, fdicInstitutionUpsertCfg(), rows[i:end])
			if err != nil {
				return totalRows, eris.Wrap(err, "fdic_bankfind: upsert institutions")
			}
			totalRows += n
		}

		log.Info("institutions page synced",
			zap.Int("offset", offset),
			zap.Int("page_records", len(resp.Data)),
			zap.Int("total_available", resp.Meta.Total),
		)

		offset += len(resp.Data)
		if offset >= resp.Meta.Total || len(resp.Data) < fdicPageSize {
			break
		}
	}

	return totalRows, nil
}

func (d *FDICBankFind) syncBranches(ctx context.Context, f fetcher.Fetcher, pool db.Pool, log *zap.Logger) (int64, error) {
	var totalRows int64
	offset := 0

	for {
		if err := ctx.Err(); err != nil {
			return totalRows, eris.Wrap(err, "fdic_bankfind: context cancelled")
		}

		url := fmt.Sprintf("%s?limit=%d&offset=%d&sort_by=UNINUM&sort_order=ASC", fdicLocationsURL, fdicPageSize, offset)
		resp, err := d.fetchPage(ctx, f, url)
		if err != nil {
			return totalRows, eris.Wrapf(err, "fdic_bankfind: fetch branches page offset=%d", offset)
		}

		if len(resp.Data) == 0 {
			break
		}

		rows := make([][]any, 0, len(resp.Data))
		for _, rec := range resp.Data {
			rows = append(rows, parseBranch(rec.Data))
		}

		for i := 0; i < len(rows); i += fdicBatchSize {
			end := i + fdicBatchSize
			if end > len(rows) {
				end = len(rows)
			}
			n, err := db.BulkUpsert(ctx, pool, fdicBranchUpsertCfg(), rows[i:end])
			if err != nil {
				return totalRows, eris.Wrap(err, "fdic_bankfind: upsert branches")
			}
			totalRows += n
		}

		log.Info("branches page synced",
			zap.Int("offset", offset),
			zap.Int("page_records", len(resp.Data)),
			zap.Int("total_available", resp.Meta.Total),
		)

		offset += len(resp.Data)
		if offset >= resp.Meta.Total || len(resp.Data) < fdicPageSize {
			break
		}
	}

	return totalRows, nil
}

func (d *FDICBankFind) fetchPage(ctx context.Context, f fetcher.Fetcher, url string) (*fdicResponse, error) {
	body, err := f.Download(ctx, url)
	if err != nil {
		return nil, eris.Wrap(err, "fdic_bankfind: download")
	}
	defer body.Close() //nolint:errcheck

	data, err := io.ReadAll(body)
	if err != nil {
		return nil, eris.Wrap(err, "fdic_bankfind: read response")
	}

	var resp fdicResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, eris.Wrap(err, "fdic_bankfind: unmarshal JSON")
	}
	return &resp, nil
}

// Institution columns (97 fields from the migration).
var institutionCols = []string{
	"cert", "name", "active", "inactive",
	"address", "address2", "city", "stalp", "stname", "zip", "county",
	"stnum", "stcnty", "latitude", "longitude",
	"cbsa", "cbsa_no", "cbsa_div", "cbsa_div_no", "cbsa_div_flg",
	"cbsa_metro", "cbsa_metro_flg", "cbsa_metro_name", "cbsa_micro_flg",
	"csa", "csa_no", "csa_flg",
	"bkclass", "clcode", "specgrp", "instcat", "charter_class", "cb",
	"regagnt", "regagent2", "chrtagnt", "charter", "stchrtr", "fedchrtr",
	"fed", "fed_rssd", "fdicdbs", "fdicregn", "fdicsupv", "suprv_fd",
	"occdist", "docket", "cfpbflag", "cfpbeffdte", "cfpbenddte",
	"insagnt1", "insagnt2", "insbif", "inscoml", "insdate", "insdif",
	"insfdic", "inssaif", "inssave",
	"asset", "dep", "depdom", "eq", "netinc", "roa", "roe",
	"offices", "offdom", "offfor", "offoa", "webaddr", "trust",
	"estymd", "endefymd", "effdate", "procdate", "dateupdt", "repdte", "risdate", "rundate",
	"changec1", "newcert", "ultcert", "priorname1",
	"hctmult", "namehcr", "parcert", "rssdhcr", "cityhcr", "stalphcr",
	"conserve", "mdi_status_code", "mdi_status_desc", "mutual", "subchaps",
	"oakar", "sasser", "law_sasser_flg", "iba", "qbprcoml", "denovo", "form31",
	"te01n528", "te02n528", "te03n528", "te04n528", "te05n528",
	"te06n528", "te07n528", "te08n528", "te09n528", "te10n528",
	"te01n529", "te02n529", "te03n529", "te04n529", "te05n529", "te06n529",
	"uninum", "oi",
}

// Branch columns (38 fields from the migration).
var branchCols = []string{
	"uni_num", "cert", "name", "off_name", "off_num", "fi_uninum",
	"address", "address2", "city", "stalp", "stname", "zip", "county", "stcnty",
	"latitude", "longitude",
	"main_off", "bk_class", "serv_type", "serv_type_desc",
	"cbsa", "cbsa_no", "cbsa_div", "cbsa_div_no", "cbsa_div_flg",
	"cbsa_metro", "cbsa_metro_flg", "cbsa_metro_name", "cbsa_micro_flg",
	"csa", "csa_no", "csa_flg",
	"mdi_status_code", "mdi_status_desc", "run_date", "estymd", "acqdate",
}

func fdicInstitutionUpsertCfg() db.UpsertConfig {
	return db.UpsertConfig{
		Table:        "fed_data.fdic_institutions",
		Columns:      institutionCols,
		ConflictKeys: []string{"cert"},
	}
}

func fdicBranchUpsertCfg() db.UpsertConfig {
	return db.UpsertConfig{
		Table:        "fed_data.fdic_branches",
		Columns:      branchCols,
		ConflictKeys: []string{"uni_num"},
	}
}

// parseInstitution extracts all institution fields from the FDIC API JSON map.
// FDIC returns field names in UPPERCASE; we normalize to lowercase column names.
func parseInstitution(m map[string]any) []any {
	return []any{
		fdicInt(m, "CERT"),
		fdicStr(m, "NAME"),
		fdicInt(m, "ACTIVE"),
		fdicInt(m, "INACTIVE"),

		fdicStr(m, "ADDRESS"),
		fdicStr(m, "ADDRESS2"),
		fdicStr(m, "CITY"),
		fdicStr(m, "STALP"),
		fdicStr(m, "STNAME"),
		fdicStr(m, "ZIP"),
		fdicStr(m, "COUNTY"),
		fdicStr(m, "STNUM"),
		fdicStr(m, "STCNTY"),
		fdicFloat(m, "LATITUDE"),
		fdicFloat(m, "LONGITUDE"),

		fdicStr(m, "CBSA"),
		fdicStr(m, "CBSA_NO"),
		fdicStr(m, "CBSA_DIV"),
		fdicStr(m, "CBSA_DIV_NO"),
		fdicStr(m, "CBSA_DIV_FLG"),
		fdicStr(m, "CBSA_METRO"),
		fdicStr(m, "CBSA_METRO_FLG"),
		fdicStr(m, "CBSA_METRO_NAME"),
		fdicStr(m, "CBSA_MICRO_FLG"),
		fdicStr(m, "CSA"),
		fdicStr(m, "CSA_NO"),
		fdicStr(m, "CSA_FLG"),

		fdicStr(m, "BKCLASS"),
		fdicInt(m, "CLCODE"),
		fdicInt(m, "SPECGRP"),
		fdicInt(m, "INSTCAT"),
		fdicStr(m, "CHARTER_CLASS"),
		fdicStr(m, "CB"),

		fdicStr(m, "REGAGNT"),
		fdicStr(m, "REGAGENT2"),
		fdicStr(m, "CHRTAGNT"),
		fdicStr(m, "CHARTER"),
		fdicStr(m, "STCHRTR"),
		fdicStr(m, "FEDCHRTR"),
		fdicStr(m, "FED"),
		fdicStr(m, "FED_RSSD"),
		fdicStr(m, "FDICDBS"),
		fdicStr(m, "FDICREGN"),
		fdicStr(m, "FDICSUPV"),
		fdicStr(m, "SUPRV_FD"),
		fdicStr(m, "OCCDIST"),
		fdicStr(m, "DOCKET"),
		fdicStr(m, "CFPBFLAG"),
		fdicStr(m, "CFPBEFFDTE"),
		fdicStr(m, "CFPBENDDTE"),

		fdicStr(m, "INSAGNT1"),
		fdicStr(m, "INSAGNT2"),
		fdicStr(m, "INSBIF"),
		fdicStr(m, "INSCOML"),
		fdicStr(m, "INSDATE"),
		fdicStr(m, "INSDIF"),
		fdicInt(m, "INSFDIC"),
		fdicStr(m, "INSSAIF"),
		fdicStr(m, "INSSAVE"),

		fdicBigInt(m, "ASSET"),
		fdicBigInt(m, "DEP"),
		fdicBigInt(m, "DEPDOM"),
		fdicStr(m, "EQ"),
		fdicBigInt(m, "NETINC"),
		fdicFloat(m, "ROA"),
		fdicFloat(m, "ROE"),

		fdicInt(m, "OFFICES"),
		fdicInt(m, "OFFDOM"),
		fdicInt(m, "OFFFOR"),
		fdicInt(m, "OFFOA"),
		fdicStr(m, "WEBADDR"),
		fdicStr(m, "TRUST"),

		fdicStr(m, "ESTYMD"),
		fdicStr(m, "ENDEFYMD"),
		fdicStr(m, "EFFDATE"),
		fdicStr(m, "PROCDATE"),
		fdicStr(m, "DATEUPDT"),
		fdicStr(m, "REPDTE"),
		fdicStr(m, "RISDATE"),
		fdicStr(m, "RUNDATE"),

		fdicStr(m, "CHANGEC1"),
		fdicStr(m, "NEWCERT"),
		fdicStr(m, "ULTCERT"),
		fdicStr(m, "PRIORNAME1"),

		fdicStr(m, "HCTMULT"),
		fdicStr(m, "NAMEHCR"),
		fdicStr(m, "PARCERT"),
		fdicStr(m, "RSSDHCR"),
		fdicStr(m, "CITYHCR"),
		fdicStr(m, "STALPHCR"),

		fdicStr(m, "CONSERVE"),
		fdicStr(m, "MDI_STATUS_CODE"),
		fdicStr(m, "MDI_STATUS_DESC"),
		fdicStr(m, "MUTUAL"),
		fdicStr(m, "SUBCHAPS"),
		fdicStr(m, "OAKAR"),
		fdicStr(m, "SASSER"),
		fdicStr(m, "LAW_SASSER_FLG"),
		fdicStr(m, "IBA"),
		fdicStr(m, "QBPRCOML"),
		fdicStr(m, "DENOVO"),
		fdicStr(m, "FORM31"),

		fdicStr(m, "TE01N528"),
		fdicStr(m, "TE02N528"),
		fdicStr(m, "TE03N528"),
		fdicStr(m, "TE04N528"),
		fdicStr(m, "TE05N528"),
		fdicStr(m, "TE06N528"),
		fdicStr(m, "TE07N528"),
		fdicStr(m, "TE08N528"),
		fdicStr(m, "TE09N528"),
		fdicStr(m, "TE10N528"),
		fdicStr(m, "TE01N529"),
		fdicStr(m, "TE02N529"),
		fdicStr(m, "TE03N529"),
		fdicStr(m, "TE04N529"),
		fdicStr(m, "TE05N529"),
		fdicStr(m, "TE06N529"),

		fdicStr(m, "UNINUM"),
		fdicStr(m, "OI"),
	}
}

// parseBranch extracts all branch fields from the FDIC API JSON map.
func parseBranch(m map[string]any) []any {
	return []any{
		fdicInt(m, "UNINUM"),
		fdicInt(m, "CERT"),
		fdicStr(m, "NAME"),
		fdicStr(m, "OFFNAME"),
		fdicStr(m, "OFFNUM"),
		fdicStr(m, "FI_UNINUM"),

		fdicStr(m, "ADDRESS"),
		fdicStr(m, "ADDRESS2"),
		fdicStr(m, "CITY"),
		fdicStr(m, "STALP"),
		fdicStr(m, "STNAME"),
		fdicStr(m, "ZIP"),
		fdicStr(m, "COUNTY"),
		fdicStr(m, "STCNTY"),

		fdicFloat(m, "LATITUDE"),
		fdicFloat(m, "LONGITUDE"),

		fdicInt(m, "MAINOFF"),
		fdicStr(m, "BKCLASS"),
		fdicInt(m, "SERVTYPE"),
		fdicStr(m, "SERVTYPE_DESC"),

		fdicStr(m, "CBSA"),
		fdicStr(m, "CBSA_NO"),
		fdicStr(m, "CBSA_DIV"),
		fdicStr(m, "CBSA_DIV_NO"),
		fdicStr(m, "CBSA_DIV_FLG"),
		fdicStr(m, "CBSA_METRO"),
		fdicStr(m, "CBSA_METRO_FLG"),
		fdicStr(m, "CBSA_METRO_NAME"),
		fdicStr(m, "CBSA_MICRO_FLG"),
		fdicStr(m, "CSA"),
		fdicStr(m, "CSA_NO"),
		fdicStr(m, "CSA_FLG"),

		fdicStr(m, "MDI_STATUS_CODE"),
		fdicStr(m, "MDI_STATUS_DESC"),
		fdicStr(m, "RUNDATE"),
		fdicStr(m, "ESTYMD"),
		fdicStr(m, "ACQDATE"),
	}
}

// fdicStr safely extracts a string value from the FDIC JSON map.
func fdicStr(m map[string]any, key string) any {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	switch val := v.(type) {
	case string:
		return val
	case float64:
		return fmt.Sprintf("%g", val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// fdicInt safely extracts an integer value from the FDIC JSON map.
func fdicInt(m map[string]any, key string) any {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	switch val := v.(type) {
	case float64:
		return int(val)
	case string:
		return parseIntOr(val, 0)
	default:
		return nil
	}
}

// fdicBigInt safely extracts a int64 value from the FDIC JSON map.
func fdicBigInt(m map[string]any, key string) any {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	switch val := v.(type) {
	case float64:
		return int64(val)
	case string:
		return parseInt64Or(val, 0)
	default:
		return nil
	}
}

// fdicFloat safely extracts a float64 value from the FDIC JSON map.
func fdicFloat(m map[string]any, key string) any {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	switch val := v.(type) {
	case float64:
		return val
	case string:
		return parseFloat64Or(val, 0)
	default:
		return nil
	}
}
