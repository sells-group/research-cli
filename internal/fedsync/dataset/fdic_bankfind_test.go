package dataset

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	fetchermocks "github.com/sells-group/research-cli/internal/fetcher/mocks"
)

func init() {
	// Suppress zap output during tests.
	zap.ReplaceGlobals(zap.NewNop())
}

func TestFDICBankFind_Metadata(t *testing.T) {
	d := &FDICBankFind{}
	assert.Equal(t, "fdic_bankfind", d.Name())
	assert.Equal(t, "fed_data.fdic_institutions", d.Table())
	assert.Equal(t, Phase2, d.Phase())
	assert.Equal(t, Weekly, d.Cadence())
}

func TestFDICBankFind_ShouldRun(t *testing.T) {
	d := &FDICBankFind{}

	t.Run("nil lastSync", func(t *testing.T) {
		assert.True(t, d.ShouldRun(time.Now(), nil))
	})

	t.Run("synced same week", func(t *testing.T) {
		// Wednesday June 11, 2025 — week starts Monday June 9
		now := time.Date(2025, 6, 11, 0, 0, 0, 0, time.UTC)
		last := time.Date(2025, 6, 9, 12, 0, 0, 0, time.UTC) // Monday same week
		assert.False(t, d.ShouldRun(now, &last))
	})

	t.Run("synced different week", func(t *testing.T) {
		// Wednesday June 11, 2025 — week starts Monday June 9
		now := time.Date(2025, 6, 11, 0, 0, 0, 0, time.UTC)
		last := time.Date(2025, 6, 2, 0, 0, 0, 0, time.UTC) // previous week
		assert.True(t, d.ShouldRun(now, &last))
	})
}

// fdicInstitutionResponse builds a fdicResponse with institution records.
func fdicInstitutionResponse(records ...map[string]any) fdicResponse {
	resp := fdicResponse{}
	resp.Meta.Total = len(records)
	for _, r := range records {
		resp.Data = append(resp.Data, fdicRecord{Data: r, Score: 1.0})
	}
	return resp
}

// fdicBranchResponse builds a fdicResponse with branch records.
func fdicBranchResponse(records ...map[string]any) fdicResponse {
	return fdicInstitutionResponse(records...) // same structure
}

// emptyFDICResponse returns a response with zero records and the given total.
func emptyFDICResponse() fdicResponse {
	resp := fdicResponse{}
	resp.Meta.Total = 0
	return resp
}

func TestFDICBankFind_Sync_Institutions(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	instResp := fdicInstitutionResponse(
		map[string]any{"CERT": float64(12345), "NAME": "First National Bank", "ACTIVE": float64(1), "ASSET": float64(500000000)},
		map[string]any{"CERT": float64(67890), "NAME": "Community Trust", "ACTIVE": float64(1), "ASSET": float64(200000000)},
	)

	// Institutions page
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "institutions")
	})).Return(jsonBody(t, instResp), nil).Once()

	// Branches page (empty)
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "locations")
	})).Return(jsonBody(t, emptyFDICResponse()), nil).Once()

	expectBulkUpsert(pool, "fed_data.fdic_institutions", institutionCols, 2)

	d := &FDICBankFind{}
	result, err := d.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	assert.Equal(t, int64(2), result.Metadata["institutions"])
	assert.Equal(t, int64(0), result.Metadata["branches"])
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestFDICBankFind_Sync_Branches(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Empty institutions
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "institutions")
	})).Return(jsonBody(t, emptyFDICResponse()), nil).Once()

	branchResp := fdicBranchResponse(
		map[string]any{"UNINUM": float64(1), "CERT": float64(12345), "NAME": "First National", "OFFNAME": "Main Office", "LATITUDE": float64(30.267), "LONGITUDE": float64(-97.743)},
		map[string]any{"UNINUM": float64(2), "CERT": float64(12345), "NAME": "First National", "OFFNAME": "Downtown Branch", "LATITUDE": float64(30.271), "LONGITUDE": float64(-97.740)},
		map[string]any{"UNINUM": float64(3), "CERT": float64(67890), "NAME": "Community Trust", "OFFNAME": "HQ", "LATITUDE": float64(32.776), "LONGITUDE": float64(-96.797)},
	)

	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "locations")
	})).Return(jsonBody(t, branchResp), nil).Once()

	expectBulkUpsert(pool, "fed_data.fdic_branches", branchCols, 3)

	d := &FDICBankFind{}
	result, err := d.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.RowsSynced)
	assert.Equal(t, int64(0), result.Metadata["institutions"])
	assert.Equal(t, int64(3), result.Metadata["branches"])
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestFDICBankFind_Sync_Pagination(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Build a full page of institutions (fdicPageSize = 10000).
	page1Records := make([]map[string]any, fdicPageSize)
	for i := range page1Records {
		page1Records[i] = map[string]any{"CERT": float64(i + 1), "NAME": "Bank"}
	}
	page1 := fdicResponse{}
	page1.Meta.Total = fdicPageSize + 50
	for _, r := range page1Records {
		page1.Data = append(page1.Data, fdicRecord{Data: r, Score: 1.0})
	}

	page2Records := make([]map[string]any, 50)
	for i := range page2Records {
		page2Records[i] = map[string]any{"CERT": float64(fdicPageSize + i + 1), "NAME": "Bank"}
	}
	page2 := fdicResponse{}
	page2.Meta.Total = fdicPageSize + 50
	for _, r := range page2Records {
		page2.Data = append(page2.Data, fdicRecord{Data: r, Score: 1.0})
	}

	// Page 1: contains "offset=0"
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "institutions") && strings.Contains(url, "offset=0")
	})).Return(jsonBody(t, page1), nil).Once()

	// Page 2: contains "offset=10000"
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "institutions") && strings.Contains(url, "offset=10000")
	})).Return(jsonBody(t, page2), nil).Once()

	// Empty branches
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "locations")
	})).Return(jsonBody(t, emptyFDICResponse()), nil).Once()

	// Page 1: 10000 rows in 2 batches (5000 each)
	expectBulkUpsert(pool, "fed_data.fdic_institutions", institutionCols, 5000)
	expectBulkUpsert(pool, "fed_data.fdic_institutions", institutionCols, 5000)
	// Page 2: 50 rows in 1 batch
	expectBulkUpsert(pool, "fed_data.fdic_institutions", institutionCols, 50)

	d := &FDICBankFind{}
	result, err := d.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(10050), result.RowsSynced)
	assert.Equal(t, int64(10050), result.Metadata["institutions"])
	assert.Equal(t, int64(0), result.Metadata["branches"])
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestFDICBankFind_Sync_EmptyResponse(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Both institutions and branches return empty
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "institutions")
	})).Return(jsonBody(t, emptyFDICResponse()), nil).Once()

	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "locations")
	})).Return(jsonBody(t, emptyFDICResponse()), nil).Once()

	d := &FDICBankFind{}
	result, err := d.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	assert.Equal(t, int64(0), result.Metadata["institutions"])
	assert.Equal(t, int64(0), result.Metadata["branches"])
}

func TestFDICBankFind_Sync_DownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "institutions")
	})).Return(nil, errors.New("network timeout")).Once()

	d := &FDICBankFind{}
	_, err = d.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestFDICBankFind_Sync_ContextCancellation(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	// The context check happens before Download is called, so no Download mock needed.
	// But the mock may or may not be called depending on timing, so allow it.
	f.EXPECT().Download(mock.Anything, mock.Anything).
		Return(nil, context.Canceled).Maybe()

	d := &FDICBankFind{}
	_, err = d.Sync(ctx, pool, f, t.TempDir())
	assert.Error(t, err)
}

func TestFDICBankFind_Sync_MalformedJSON(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "institutions")
	})).Return(io.NopCloser(strings.NewReader(`{invalid json`)), nil).Once()

	d := &FDICBankFind{}
	_, err = d.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestFDICBankFind_ParseInstitutions(t *testing.T) {
	m := map[string]any{
		"CERT":            float64(12345),
		"NAME":            "First National Bank",
		"ACTIVE":          float64(1),
		"INACTIVE":        float64(0),
		"ADDRESS":         "123 Main St",
		"ADDRESS2":        "Suite 100",
		"CITY":            "Austin",
		"STALP":           "TX",
		"STNAME":          "Texas",
		"ZIP":             "78701",
		"COUNTY":          "Travis",
		"STNUM":           "48",
		"STCNTY":          "48453",
		"LATITUDE":        float64(30.267),
		"LONGITUDE":       float64(-97.743),
		"CBSA":            "Austin-Round Rock",
		"CBSA_NO":         "12420",
		"CBSA_DIV":        "",
		"CBSA_DIV_NO":     "",
		"CBSA_DIV_FLG":    "N",
		"CBSA_METRO":      "Austin-Round Rock",
		"CBSA_METRO_FLG":  "Y",
		"CBSA_METRO_NAME": "Austin-Round Rock-San Marcos, TX",
		"CBSA_MICRO_FLG":  "N",
		"CSA":             "Austin-Round Rock",
		"CSA_NO":          "206",
		"CSA_FLG":         "Y",
		"BKCLASS":         "NM",
		"CLCODE":          float64(21),
		"SPECGRP":         float64(4),
		"INSTCAT":         float64(2),
		"CHARTER_CLASS":   "N",
		"CB":              "N",
		"REGAGNT":         "FDIC",
		"REGAGENT2":       "",
		"CHRTAGNT":        "STATE",
		"CHARTER":         "0",
		"STCHRTR":         "1",
		"FEDCHRTR":        "0",
		"FED":             "11",
		"FED_RSSD":        "987654",
		"FDICDBS":         "05",
		"FDICREGN":        "06",
		"FDICSUPV":        "Dallas",
		"SUPRV_FD":        "06",
		"OCCDIST":         "",
		"DOCKET":          "00123",
		"CFPBFLAG":        "N",
		"CFPBEFFDTE":      "",
		"CFPBENDDTE":      "",
		"INSAGNT1":        "DIF",
		"INSAGNT2":        "",
		"INSBIF":          "Y",
		"INSCOML":         "Y",
		"INSDATE":         "01/01/1950",
		"INSDIF":          "Y",
		"INSFDIC":         float64(1),
		"INSSAIF":         "N",
		"INSSAVE":         "N",
		"ASSET":           float64(500000000),
		"DEP":             float64(400000000),
		"DEPDOM":          float64(400000000),
		"EQ":              "50000000",
		"NETINC":          float64(5000000),
		"ROA":             float64(1.05),
		"ROE":             float64(10.5),
		"OFFICES":         float64(5),
		"OFFDOM":          float64(5),
		"OFFFOR":          float64(0),
		"OFFOA":           float64(0),
		"WEBADDR":         "www.firstnational.com",
		"TRUST":           "Y",
		"ESTYMD":          "19500101",
		"ENDEFYMD":        "",
		"EFFDATE":         "20240630",
		"PROCDATE":        "20240701",
		"DATEUPDT":        "20240701",
		"REPDTE":          "20240630",
		"RISDATE":         "20240630",
		"RUNDATE":         "20240701",
		"CHANGEC1":        "",
		"NEWCERT":         "0",
		"ULTCERT":         "12345",
		"PRIORNAME1":      "",
		"HCTMULT":         "Y",
		"NAMEHCR":         "First National Bancshares",
		"PARCERT":         "0",
		"RSSDHCR":         "1234567",
		"CITYHCR":         "Austin",
		"STALPHCR":        "TX",
		"CONSERVE":        "N",
		"MDI_STATUS_CODE": "",
		"MDI_STATUS_DESC": "",
		"MUTUAL":          "N",
		"SUBCHAPS":        "N",
		"OAKAR":           "",
		"SASSER":          "",
		"LAW_SASSER_FLG":  "N",
		"IBA":             "N",
		"QBPRCOML":        "N",
		"DENOVO":          "N",
		"FORM31":          "N",
		"TE01N528":        "",
		"TE02N528":        "",
		"TE03N528":        "",
		"TE04N528":        "",
		"TE05N528":        "",
		"TE06N528":        "",
		"TE07N528":        "",
		"TE08N528":        "",
		"TE09N528":        "",
		"TE10N528":        "",
		"TE01N529":        "",
		"TE02N529":        "",
		"TE03N529":        "",
		"TE04N529":        "",
		"TE05N529":        "",
		"TE06N529":        "",
		"UNINUM":          "12345",
		"OI":              "",
	}

	row := parseInstitution(m)
	require.Len(t, row, len(institutionCols))

	// Spot-check key fields by column index (see institutionCols)
	assert.Equal(t, 12345, row[0])                    // cert
	assert.Equal(t, "First National Bank", row[1])    // name (REPNM)
	assert.Equal(t, 1, row[2])                        // active
	assert.Equal(t, 0, row[3])                        // inactive
	assert.Equal(t, "123 Main St", row[4])            // address
	assert.Equal(t, "Austin", row[6])                 // city
	assert.Equal(t, "TX", row[7])                     // stalp
	assert.Equal(t, float64(30.267), row[13])         // latitude
	assert.Equal(t, float64(-97.743), row[14])        // longitude
	assert.Equal(t, int64(500000000), row[59])        // asset
	assert.Equal(t, int64(400000000), row[60])        // dep
	assert.Equal(t, "50000000", row[62])              // eq (string via fdicStr)
	assert.Equal(t, float64(1.05), row[64])           // roa
	assert.Equal(t, float64(10.5), row[65])           // roe
	assert.Equal(t, 5, row[66])                       // offices
	assert.Equal(t, "www.firstnational.com", row[70]) // webaddr
}

func TestFDICBankFind_ParseBranches(t *testing.T) {
	m := map[string]any{
		"UNINUM":          float64(99001),
		"CERT":            float64(12345),
		"NAME":            "First National Bank",
		"OFFNAME":         "Downtown Branch",
		"OFFNUM":          "002",
		"FI_UNINUM":       "12345",
		"ADDRESS":         "456 Elm St",
		"ADDRESS2":        "",
		"CITY":            "Austin",
		"STALP":           "TX",
		"STNAME":          "Texas",
		"ZIP":             "78702",
		"COUNTY":          "Travis",
		"STCNTY":          "48453",
		"LATITUDE":        float64(30.271),
		"LONGITUDE":       float64(-97.740),
		"MAINOFF":         float64(0),
		"BKCLASS":         "NM",
		"SERVTYPE":        float64(11),
		"SERVTYPE_DESC":   "Full Service Brick and Mortar Office",
		"CBSA":            "Austin-Round Rock",
		"CBSA_NO":         "12420",
		"CBSA_DIV":        "",
		"CBSA_DIV_NO":     "",
		"CBSA_DIV_FLG":    "N",
		"CBSA_METRO":      "Austin-Round Rock",
		"CBSA_METRO_FLG":  "Y",
		"CBSA_METRO_NAME": "Austin-Round Rock-San Marcos, TX",
		"CBSA_MICRO_FLG":  "N",
		"CSA":             "Austin-Round Rock",
		"CSA_NO":          "206",
		"CSA_FLG":         "Y",
		"MDI_STATUS_CODE": "",
		"MDI_STATUS_DESC": "",
		"RUNDATE":         "07/01/2024",
		"ESTYMD":          "01/15/1985",
		"ACQDATE":         "",
	}

	row := parseBranch(m)
	require.Len(t, row, len(branchCols))

	// Spot-check key fields
	assert.Equal(t, 99001, row[0])                                   // uni_num
	assert.Equal(t, 12345, row[1])                                   // cert
	assert.Equal(t, "First National Bank", row[2])                   // name
	assert.Equal(t, "Downtown Branch", row[3])                       // off_name
	assert.Equal(t, "002", row[4])                                   // off_num
	assert.Equal(t, "456 Elm St", row[6])                            // address
	assert.Equal(t, "Austin", row[8])                                // city
	assert.Equal(t, float64(30.271), row[14])                        // latitude
	assert.Equal(t, float64(-97.740), row[15])                       // longitude
	assert.Equal(t, 0, row[16])                                      // main_off
	assert.Equal(t, 11, row[18])                                     // serv_type
	assert.Equal(t, "Full Service Brick and Mortar Office", row[19]) // serv_type_desc
}

func TestFDICBankFind_ParseInstitution_NilFields(t *testing.T) {
	// Completely empty map — all fields should be nil or zero-value.
	m := map[string]any{}

	row := parseInstitution(m)
	require.Len(t, row, len(institutionCols))

	// All values should be nil when not present in the map.
	for i, val := range row {
		assert.Nil(t, val, "expected nil at index %d (col %s)", i, institutionCols[i])
	}
}

func TestFDICBankFind_ParseBranch_NilFields(t *testing.T) {
	// Map with explicit nil values.
	m := map[string]any{
		"UNINUM":    nil,
		"CERT":      nil,
		"NAME":      nil,
		"LATITUDE":  nil,
		"LONGITUDE": nil,
	}

	row := parseBranch(m)
	require.Len(t, row, len(branchCols))

	// Explicit nil values should also produce nil.
	assert.Nil(t, row[0], "uni_num should be nil")
	assert.Nil(t, row[1], "cert should be nil")
	assert.Nil(t, row[2], "name should be nil")
	assert.Nil(t, row[14], "latitude should be nil")
	assert.Nil(t, row[15], "longitude should be nil")
}

// --- Helper function coverage ---

func TestFDICStr(t *testing.T) {
	t.Run("string value", func(t *testing.T) {
		m := map[string]any{"K": "hello"}
		assert.Equal(t, "hello", fdicStr(m, "K"))
	})
	t.Run("float64 value", func(t *testing.T) {
		m := map[string]any{"K": float64(42)}
		assert.Equal(t, "42", fdicStr(m, "K"))
	})
	t.Run("other type", func(t *testing.T) {
		m := map[string]any{"K": true}
		assert.Equal(t, "true", fdicStr(m, "K"))
	})
	t.Run("nil value", func(t *testing.T) {
		m := map[string]any{"K": nil}
		assert.Nil(t, fdicStr(m, "K"))
	})
	t.Run("missing key", func(t *testing.T) {
		m := map[string]any{}
		assert.Nil(t, fdicStr(m, "K"))
	})
}

func TestFDICInt(t *testing.T) {
	t.Run("float64 value", func(t *testing.T) {
		m := map[string]any{"K": float64(42)}
		assert.Equal(t, 42, fdicInt(m, "K"))
	})
	t.Run("string value", func(t *testing.T) {
		m := map[string]any{"K": "99"}
		assert.Equal(t, 99, fdicInt(m, "K"))
	})
	t.Run("other type", func(t *testing.T) {
		m := map[string]any{"K": true}
		assert.Nil(t, fdicInt(m, "K"))
	})
	t.Run("nil value", func(t *testing.T) {
		m := map[string]any{"K": nil}
		assert.Nil(t, fdicInt(m, "K"))
	})
	t.Run("missing key", func(t *testing.T) {
		m := map[string]any{}
		assert.Nil(t, fdicInt(m, "K"))
	})
}

func TestFDICBigInt(t *testing.T) {
	t.Run("float64 value", func(t *testing.T) {
		m := map[string]any{"K": float64(1e9)}
		assert.Equal(t, int64(1e9), fdicBigInt(m, "K"))
	})
	t.Run("string value", func(t *testing.T) {
		m := map[string]any{"K": "500000"}
		assert.Equal(t, int64(500000), fdicBigInt(m, "K"))
	})
	t.Run("other type", func(t *testing.T) {
		m := map[string]any{"K": true}
		assert.Nil(t, fdicBigInt(m, "K"))
	})
	t.Run("nil value", func(t *testing.T) {
		m := map[string]any{"K": nil}
		assert.Nil(t, fdicBigInt(m, "K"))
	})
	t.Run("missing key", func(t *testing.T) {
		m := map[string]any{}
		assert.Nil(t, fdicBigInt(m, "K"))
	})
}

func TestFDICFloat(t *testing.T) {
	t.Run("float64 value", func(t *testing.T) {
		m := map[string]any{"K": float64(3.14)}
		assert.Equal(t, 3.14, fdicFloat(m, "K"))
	})
	t.Run("string value", func(t *testing.T) {
		m := map[string]any{"K": "2.71"}
		assert.Equal(t, 2.71, fdicFloat(m, "K"))
	})
	t.Run("other type", func(t *testing.T) {
		m := map[string]any{"K": true}
		assert.Nil(t, fdicFloat(m, "K"))
	})
	t.Run("nil value", func(t *testing.T) {
		m := map[string]any{"K": nil}
		assert.Nil(t, fdicFloat(m, "K"))
	})
	t.Run("missing key", func(t *testing.T) {
		m := map[string]any{}
		assert.Nil(t, fdicFloat(m, "K"))
	})
}

func TestFDICBankFind_Sync_BranchDownloadError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Institutions succeed (empty)
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "institutions")
	})).Return(jsonBody(t, emptyFDICResponse()), nil).Once()

	// Branches fail
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "locations")
	})).Return(nil, errors.New("branch download failed")).Once()

	d := &FDICBankFind{}
	_, err = d.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sync branches")
}

func TestFDICBankFind_Sync_InstitutionUpsertError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	instResp := fdicInstitutionResponse(
		map[string]any{"CERT": float64(1), "NAME": "Bank"},
	)
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "institutions")
	})).Return(jsonBody(t, instResp), nil).Once()

	// BulkUpsert will fail at Begin
	pool.ExpectBegin().WillReturnError(errors.New("db connection error"))

	d := &FDICBankFind{}
	_, err = d.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "upsert institutions")
}

func TestFDICBankFind_Sync_BranchUpsertError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Empty institutions
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "institutions")
	})).Return(jsonBody(t, emptyFDICResponse()), nil).Once()

	// Branches with data
	branchResp := fdicBranchResponse(
		map[string]any{"UNINUM": float64(1), "CERT": float64(1), "NAME": "Bank"},
	)
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "locations")
	})).Return(jsonBody(t, branchResp), nil).Once()

	// BulkUpsert will fail
	pool.ExpectBegin().WillReturnError(errors.New("db error"))

	d := &FDICBankFind{}
	_, err = d.Sync(context.Background(), pool, f, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "upsert branches")
}

func TestFDICBankFind_Sync_BranchPagination(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Empty institutions
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "institutions")
	})).Return(jsonBody(t, emptyFDICResponse()), nil).Once()

	// Branch page 1 (full page)
	page1Records := make([]map[string]any, fdicPageSize)
	for i := range page1Records {
		page1Records[i] = map[string]any{"UNINUM": float64(i + 1), "CERT": float64(1), "NAME": "Bank"}
	}
	page1 := fdicResponse{}
	page1.Meta.Total = fdicPageSize + 10
	for _, r := range page1Records {
		page1.Data = append(page1.Data, fdicRecord{Data: r, Score: 1.0})
	}

	// Branch page 2 (partial)
	page2Records := make([]map[string]any, 10)
	for i := range page2Records {
		page2Records[i] = map[string]any{"UNINUM": float64(fdicPageSize + i + 1), "CERT": float64(1), "NAME": "Bank"}
	}
	page2 := fdicResponse{}
	page2.Meta.Total = fdicPageSize + 10
	for _, r := range page2Records {
		page2.Data = append(page2.Data, fdicRecord{Data: r, Score: 1.0})
	}

	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "locations") && strings.Contains(url, "offset=0")
	})).Return(jsonBody(t, page1), nil).Once()

	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "locations") && strings.Contains(url, "offset=10000")
	})).Return(jsonBody(t, page2), nil).Once()

	// Page 1: 10000 rows in 2 batches (5000 each)
	expectBulkUpsert(pool, "fed_data.fdic_branches", branchCols, 5000)
	expectBulkUpsert(pool, "fed_data.fdic_branches", branchCols, 5000)
	// Page 2: 10 rows
	expectBulkUpsert(pool, "fed_data.fdic_branches", branchCols, 10)

	d := &FDICBankFind{}
	result, err := d.Sync(context.Background(), pool, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(10010), result.RowsSynced)
	assert.Equal(t, int64(10010), result.Metadata["branches"])
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestFDICBankFind_Sync_BranchContextCancellation(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	f := fetchermocks.NewMockFetcher(t)

	// Institutions succeed (empty)
	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "institutions")
	})).Return(jsonBody(t, emptyFDICResponse()), nil).Once()

	// Cancel context after institutions but branches will see it
	ctx, cancel := context.WithCancel(context.Background())

	f.EXPECT().Download(mock.Anything, mock.MatchedBy(func(url string) bool {
		return strings.Contains(url, "locations")
	})).RunAndReturn(func(_ context.Context, _ string) (io.ReadCloser, error) {
		cancel()
		return jsonBody(t, emptyFDICResponse()), nil
	}).Maybe()

	d := &FDICBankFind{}
	// May or may not error depending on timing; just verify it doesn't panic.
	_, _ = d.Sync(ctx, pool, f, t.TempDir())
}

func TestFDICBankFind_FetchPage_ReadError(t *testing.T) {
	f := fetchermocks.NewMockFetcher(t)

	// Return a reader that errors on Read.
	f.EXPECT().Download(mock.Anything, mock.Anything).Return(
		io.NopCloser(&errReader{}), nil,
	).Once()

	d := &FDICBankFind{}
	_, err := d.fetchPage(context.Background(), f, "https://example.com")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read response")
}

// errReader always returns an error on Read.
type errReader struct{}

func (r *errReader) Read(_ []byte) (int, error) {
	return 0, errors.New("read error")
}
