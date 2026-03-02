package company

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func init() {
	zap.ReplaceGlobals(zap.NewNop())
}

// errorMockStore wraps mockStore to inject errors on specific calls.
type errorMockStore struct {
	mockStore
	identifiersErr error
	companyErr     error
}

func (m *errorMockStore) GetIdentifiers(_ context.Context, _ int64) ([]Identifier, error) {
	if m.identifiersErr != nil {
		return nil, m.identifiersErr
	}
	return m.mockStore.GetIdentifiers(context.Background(), 0)
}

func (m *errorMockStore) GetCompany(_ context.Context, id int64) (*CompanyRecord, error) {
	if m.companyErr != nil {
		return nil, m.companyErr
	}
	return m.mockStore.GetCompany(context.Background(), id)
}

// errMatchStore wraps mockStore but returns an error on UpsertMatch.
type errMatchStore struct {
	mockStore
	upsertErr error
}

func (m *errMatchStore) UpsertMatch(_ context.Context, _ *Match) error {
	return m.upsertErr
}

// --- FDIC tests ---

func TestLinker_MatchFDIC_Found(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := newMockStore()
	ms.companies[1] = &CompanyRecord{ID: 1, Name: "Test Bank", State: "FL"}
	ms.identifiers[1] = []Identifier{{CompanyID: 1, System: SystemFDIC, Identifier: "12345"}}

	pool.ExpectQuery(`SELECT name FROM fed_data\.fdic_institutions WHERE cert = \$1`).
		WithArgs("12345").
		WillReturnRows(pgxmock.NewRows([]string{"name"}).AddRow("Test Bank"))

	l := NewLinker(pool, ms)
	matched, err := l.matchFDIC(context.Background(), 1, "12345")
	require.NoError(t, err)
	assert.Equal(t, 1, matched)
	assert.Len(t, ms.matches[1], 1)
	assert.Equal(t, "fdic_institutions", ms.matches[1][0].MatchedSource)
	assert.Equal(t, "12345", ms.matches[1][0].MatchedKey)
	assert.Equal(t, "direct_fdic_cert", ms.matches[1][0].MatchType)
	assert.Equal(t, 1.0, *ms.matches[1][0].Confidence)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchFDIC_NotFound(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := newMockStore()

	pool.ExpectQuery(`SELECT name FROM fed_data\.fdic_institutions WHERE cert = \$1`).
		WithArgs("99999").
		WillReturnRows(pgxmock.NewRows([]string{"name"}))

	l := NewLinker(pool, ms)
	matched, err := l.matchFDIC(context.Background(), 1, "99999")
	require.NoError(t, err)
	assert.Equal(t, 0, matched)
	assert.Empty(t, ms.matches[1])
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchFDIC_QueryError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := newMockStore()

	pool.ExpectQuery(`SELECT name FROM fed_data\.fdic_institutions WHERE cert = \$1`).
		WithArgs("bad").
		WillReturnError(assert.AnError)

	l := NewLinker(pool, ms)
	_, err = l.matchFDIC(context.Background(), 1, "bad")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "query fdic_institutions")
	require.NoError(t, pool.ExpectationsWereMet())
}

// --- CRD tests ---

func TestLinker_MatchCRD_Found(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := newMockStore()

	pool.ExpectQuery(`SELECT firm_name FROM fed_data\.adv_firms WHERE crd_number = \$1`).
		WithArgs("100").
		WillReturnRows(pgxmock.NewRows([]string{"firm_name"}).AddRow("Acme Advisors"))

	l := NewLinker(pool, ms)
	matched, err := l.matchCRD(context.Background(), 1, "100")
	require.NoError(t, err)
	assert.Equal(t, 1, matched)
	assert.Len(t, ms.matches[1], 1)
	assert.Equal(t, "adv_firms", ms.matches[1][0].MatchedSource)
	assert.Equal(t, "direct_crd", ms.matches[1][0].MatchType)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchCRD_NotFound(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := newMockStore()

	pool.ExpectQuery(`SELECT firm_name FROM fed_data\.adv_firms WHERE crd_number = \$1`).
		WithArgs("999").
		WillReturnRows(pgxmock.NewRows([]string{"firm_name"}))

	l := NewLinker(pool, ms)
	matched, err := l.matchCRD(context.Background(), 1, "999")
	require.NoError(t, err)
	assert.Equal(t, 0, matched)
	assert.Empty(t, ms.matches[1])
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchCRD_QueryError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := newMockStore()

	pool.ExpectQuery(`SELECT firm_name FROM fed_data\.adv_firms WHERE crd_number = \$1`).
		WithArgs("bad").
		WillReturnError(assert.AnError)

	l := NewLinker(pool, ms)
	_, err = l.matchCRD(context.Background(), 1, "bad")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "query adv_firms")
	require.NoError(t, pool.ExpectationsWereMet())
}

// --- CIK tests ---

func TestLinker_MatchCIK_Found(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := newMockStore()

	pool.ExpectQuery(`SELECT entity_name FROM fed_data\.edgar_entities WHERE cik = \$1`).
		WithArgs("0001234567").
		WillReturnRows(pgxmock.NewRows([]string{"entity_name"}).AddRow("Widget Corp"))

	l := NewLinker(pool, ms)
	matched, err := l.matchCIK(context.Background(), 2, "0001234567")
	require.NoError(t, err)
	assert.Equal(t, 1, matched)
	assert.Len(t, ms.matches[2], 1)
	assert.Equal(t, "edgar_entities", ms.matches[2][0].MatchedSource)
	assert.Equal(t, "direct_cik", ms.matches[2][0].MatchType)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchCIK_NotFound(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := newMockStore()

	pool.ExpectQuery(`SELECT entity_name FROM fed_data\.edgar_entities WHERE cik = \$1`).
		WithArgs("0009999999").
		WillReturnRows(pgxmock.NewRows([]string{"entity_name"}))

	l := NewLinker(pool, ms)
	matched, err := l.matchCIK(context.Background(), 2, "0009999999")
	require.NoError(t, err)
	assert.Equal(t, 0, matched)
	assert.Empty(t, ms.matches[2])
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchCIK_QueryError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := newMockStore()

	pool.ExpectQuery(`SELECT entity_name FROM fed_data\.edgar_entities WHERE cik = \$1`).
		WithArgs("bad").
		WillReturnError(assert.AnError)

	l := NewLinker(pool, ms)
	_, err = l.matchCIK(context.Background(), 2, "bad")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "query edgar_entities")
	require.NoError(t, pool.ExpectationsWereMet())
}

// --- EIN tests ---

func TestLinker_MatchEIN(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	store := newMockStore()
	linker := NewLinker(pool, store)

	rows := pgxmock.NewRows([]string{"sponsor_dfe_name"}).
		AddRow("ACME Corp 401(k) Plan")
	pool.ExpectQuery("SELECT sponsor_dfe_name FROM fed_data.form_5500").
		WithArgs("123456789").
		WillReturnRows(rows)

	n, err := linker.matchEIN(context.Background(), 42, "123456789")
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	// Verify the match was upserted with correct fields.
	require.Len(t, store.matches[42], 1)
	m := store.matches[42][0]
	assert.Equal(t, int64(42), m.CompanyID)
	assert.Equal(t, "form_5500", m.MatchedSource)
	assert.Equal(t, "123456789", m.MatchedKey)
	assert.Equal(t, "direct_ein", m.MatchType)
	require.NotNil(t, m.Confidence)
	assert.Equal(t, 1.0, *m.Confidence)

	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchEIN_QueryError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	store := newMockStore()
	linker := NewLinker(pool, store)

	pool.ExpectQuery("SELECT sponsor_dfe_name FROM fed_data.form_5500").
		WithArgs("999999999").
		WillReturnError(errors.New("connection refused"))

	n, err := linker.matchEIN(context.Background(), 42, "999999999")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "link: query form_5500")
	assert.Equal(t, 0, n)

	assert.Empty(t, store.matches[42])
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchEIN_NoRows(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	store := newMockStore()
	linker := NewLinker(pool, store)

	pool.ExpectQuery("SELECT sponsor_dfe_name FROM fed_data.form_5500").
		WithArgs("000000000").
		WillReturnError(pgx.ErrNoRows)

	n, err := linker.matchEIN(context.Background(), 42, "000000000")
	require.NoError(t, err)
	assert.Equal(t, 0, n)

	assert.Empty(t, store.matches[42])
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchEIN_UpsertError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	store := &errMatchStore{
		mockStore: *newMockStore(),
		upsertErr: errors.New("upsert failed"),
	}
	linker := NewLinker(pool, store)

	rows := pgxmock.NewRows([]string{"sponsor_dfe_name"}).
		AddRow("Test Plan")
	pool.ExpectQuery("SELECT sponsor_dfe_name FROM fed_data.form_5500").
		WithArgs("111222333").
		WillReturnRows(rows)

	n, err := linker.matchEIN(context.Background(), 42, "111222333")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert failed")
	assert.Equal(t, 0, n)
	assert.NoError(t, pool.ExpectationsWereMet())
}

// --- Name+State tests ---

func TestLinker_MatchNameState(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := newMockStore()

	pool.ExpectQuery(`SELECT cik, entity_name FROM fed_data\.edgar_entities`).
		WithArgs("Acme Corp", "DE").
		WillReturnRows(pgxmock.NewRows([]string{"cik", "entity_name"}).
			AddRow("0001111111", "Acme Corp"))

	l := NewLinker(pool, ms)
	matched, err := l.matchNameState(context.Background(), 1, "Acme Corp", "DE")
	require.NoError(t, err)
	assert.Equal(t, 1, matched)
	assert.Len(t, ms.matches[1], 1)
	assert.Equal(t, "edgar_entities", ms.matches[1][0].MatchedSource)
	assert.Equal(t, "exact_name_state", ms.matches[1][0].MatchType)
	assert.Equal(t, 0.95, *ms.matches[1][0].Confidence)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchNameState_NoMatch(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := newMockStore()

	pool.ExpectQuery(`SELECT cik, entity_name FROM fed_data\.edgar_entities`).
		WithArgs("Unknown Corp", "ZZ").
		WillReturnRows(pgxmock.NewRows([]string{"cik", "entity_name"}))

	l := NewLinker(pool, ms)
	matched, err := l.matchNameState(context.Background(), 1, "Unknown Corp", "ZZ")
	require.NoError(t, err)
	assert.Equal(t, 0, matched)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchNameState_QueryError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := newMockStore()

	pool.ExpectQuery(`SELECT cik, entity_name FROM fed_data\.edgar_entities`).
		WithArgs("Bad Corp", "XX").
		WillReturnError(assert.AnError)

	l := NewLinker(pool, ms)
	_, err = l.matchNameState(context.Background(), 1, "Bad Corp", "XX")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "name+state query")
	require.NoError(t, pool.ExpectationsWereMet())
}

// --- Fuzzy name tests ---

func TestLinker_MatchFuzzyName(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := newMockStore()

	pool.ExpectQuery(`SELECT cik, entity_name, similarity`).
		WithArgs("Acme Corporation", "TX").
		WillReturnRows(pgxmock.NewRows([]string{"cik", "entity_name", "sim"}).
			AddRow("0002222222", "ACME CORP", 0.75))

	l := NewLinker(pool, ms)
	matched, err := l.matchFuzzyName(context.Background(), 1, "Acme Corporation", "TX")
	require.NoError(t, err)
	assert.Equal(t, 1, matched)
	assert.Len(t, ms.matches[1], 1)
	assert.Equal(t, "fuzzy_name", ms.matches[1][0].MatchType)
	assert.Equal(t, 0.75, *ms.matches[1][0].Confidence)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchFuzzyName_LowSimilarity(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := newMockStore()

	// When state is empty, matchFuzzyName only passes name (1 arg).
	pool.ExpectQuery(`SELECT cik, entity_name, similarity`).
		WithArgs("Banana Inc").
		WillReturnRows(pgxmock.NewRows([]string{"cik", "entity_name", "sim"}).
			AddRow("0003333333", "Something Else", 0.3))

	l := NewLinker(pool, ms)
	matched, err := l.matchFuzzyName(context.Background(), 1, "Banana Inc", "")
	require.NoError(t, err)
	assert.Equal(t, 0, matched)
	assert.Empty(t, ms.matches[1])
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchFuzzyName_NoMatch(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := newMockStore()

	// With state="CA", 2 args are passed.
	pool.ExpectQuery(`SELECT cik, entity_name, similarity`).
		WithArgs("XYZ LLC", "CA").
		WillReturnRows(pgxmock.NewRows([]string{"cik", "entity_name", "sim"}))

	l := NewLinker(pool, ms)
	matched, err := l.matchFuzzyName(context.Background(), 1, "XYZ LLC", "CA")
	require.NoError(t, err)
	assert.Equal(t, 0, matched)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchFuzzyName_SkipsAlreadyMatched(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := newMockStore()
	// Pre-populate an edgar_entities match.
	ms.matches[1] = []Match{{CompanyID: 1, MatchedSource: "edgar_entities", MatchedKey: "111"}}

	l := NewLinker(pool, ms)
	matched, err := l.matchFuzzyName(context.Background(), 1, "Already Matched Corp", "CA")
	require.NoError(t, err)
	assert.Equal(t, 0, matched)
	// Pool should have no expectations since we skip early.
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchFuzzyName_QueryError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := newMockStore()

	pool.ExpectQuery(`SELECT cik, entity_name, similarity`).
		WithArgs("Error Corp", "FL").
		WillReturnError(assert.AnError)

	l := NewLinker(pool, ms)
	_, err = l.matchFuzzyName(context.Background(), 1, "Error Corp", "FL")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fuzzy query")
	require.NoError(t, pool.ExpectationsWereMet())
}

// --- LinkFedData integration tests ---

func TestLinker_LinkFedData_FullCascade(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := newMockStore()
	ms.companies[10] = &CompanyRecord{ID: 10, Name: "Test Advisors", State: "TX"}
	ms.identifiers[10] = []Identifier{
		{CompanyID: 10, System: SystemCRD, Identifier: "100"},
		{CompanyID: 10, System: SystemFDIC, Identifier: "55555"},
	}

	// Pass 1: CRD match
	pool.ExpectQuery(`SELECT firm_name FROM fed_data\.adv_firms WHERE crd_number = \$1`).
		WithArgs("100").
		WillReturnRows(pgxmock.NewRows([]string{"firm_name"}).AddRow("Test Advisors LLC"))

	// Pass 1: FDIC match
	pool.ExpectQuery(`SELECT name FROM fed_data\.fdic_institutions WHERE cert = \$1`).
		WithArgs("55555").
		WillReturnRows(pgxmock.NewRows([]string{"name"}).AddRow("Test National Bank"))

	// Pass 2: Name+state match
	pool.ExpectQuery(`SELECT cik, entity_name FROM fed_data\.edgar_entities`).
		WithArgs("Test Advisors", "TX").
		WillReturnRows(pgxmock.NewRows([]string{"cik", "entity_name"}))

	// Pass 3: Fuzzy match — already matched edgar, but let's check it skips
	// (matchFuzzyName checks existing matches; after pass 2 returns 0, no edgar match exists
	// unless pass 1 added one, which it doesn't for edgar — only adv_firms + fdic_institutions)
	pool.ExpectQuery(`SELECT cik, entity_name, similarity`).
		WithArgs("Test Advisors", "TX").
		WillReturnRows(pgxmock.NewRows([]string{"cik", "entity_name", "sim"}))

	l := NewLinker(pool, ms)
	matched, err := l.LinkFedData(context.Background(), 10)
	require.NoError(t, err)
	assert.Equal(t, 2, matched) // CRD + FDIC
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_LinkFedData_MatchErrorsContinue(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := newMockStore()
	ms.companies[40] = &CompanyRecord{ID: 40, Name: "Flaky Corp", State: "FL"}
	ms.identifiers[40] = []Identifier{
		{CompanyID: 40, System: SystemCRD, Identifier: "err1"},
		{CompanyID: 40, System: SystemCIK, Identifier: "err2"},
		{CompanyID: 40, System: SystemFDIC, Identifier: "err3"},
	}

	// All three match queries error — LinkFedData should warn and continue.
	pool.ExpectQuery(`SELECT firm_name FROM fed_data\.adv_firms`).
		WithArgs("err1").
		WillReturnError(assert.AnError)
	pool.ExpectQuery(`SELECT entity_name FROM fed_data\.edgar_entities WHERE cik`).
		WithArgs("err2").
		WillReturnError(assert.AnError)
	pool.ExpectQuery(`SELECT name FROM fed_data\.fdic_institutions`).
		WithArgs("err3").
		WillReturnError(assert.AnError)

	// Pass 2: name+state query
	pool.ExpectQuery(`SELECT cik, entity_name FROM fed_data\.edgar_entities`).
		WithArgs("Flaky Corp", "FL").
		WillReturnError(assert.AnError)

	// Pass 3: fuzzy name query
	pool.ExpectQuery(`SELECT cik, entity_name, similarity`).
		WithArgs("Flaky Corp", "FL").
		WillReturnError(assert.AnError)

	l := NewLinker(pool, ms)
	matched, err := l.LinkFedData(context.Background(), 40)
	require.NoError(t, err) // errors are logged, not returned
	assert.Equal(t, 0, matched)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_LinkFedData_NameOnlyNoState(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := newMockStore()
	// Has name but no state — skips pass 2 (name+state), goes to pass 3 (fuzzy).
	ms.companies[50] = &CompanyRecord{ID: 50, Name: "Stateless Inc", State: ""}
	ms.identifiers[50] = []Identifier{}

	// Pass 3: fuzzy name (no state, so 1 arg).
	pool.ExpectQuery(`SELECT cik, entity_name, similarity`).
		WithArgs("Stateless Inc").
		WillReturnRows(pgxmock.NewRows([]string{"cik", "entity_name", "sim"}).
			AddRow("0006666666", "STATELESS INC", 0.85))

	l := NewLinker(pool, ms)
	matched, err := l.LinkFedData(context.Background(), 50)
	require.NoError(t, err)
	assert.Equal(t, 1, matched)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_LinkFedData_NoIdentifiers(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := newMockStore()
	ms.companies[20] = &CompanyRecord{ID: 20, Name: "Solo Corp", State: "NY"}
	ms.identifiers[20] = []Identifier{} // no identifiers

	// Pass 2: Name+state
	pool.ExpectQuery(`SELECT cik, entity_name FROM fed_data\.edgar_entities`).
		WithArgs("Solo Corp", "NY").
		WillReturnRows(pgxmock.NewRows([]string{"cik", "entity_name"}).
			AddRow("0005555555", "Solo Corp"))

	// Pass 3: Fuzzy — already matched in pass 2 so skips
	// (matchFuzzyName sees edgar_entities in existing matches)

	l := NewLinker(pool, ms)
	matched, err := l.LinkFedData(context.Background(), 20)
	require.NoError(t, err)
	assert.Equal(t, 1, matched)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_LinkFedData_GetIdentifiersError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := &errorMockStore{mockStore: *newMockStore(), identifiersErr: assert.AnError}
	ms.companies[1] = &CompanyRecord{ID: 1, Name: "Corp", State: "CA"}

	l := NewLinker(pool, ms)
	_, err = l.LinkFedData(context.Background(), 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get identifiers")
}

func TestLinker_LinkFedData_GetCompanyError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := &errorMockStore{mockStore: *newMockStore(), companyErr: assert.AnError}
	ms.identifiers[1] = []Identifier{}

	l := NewLinker(pool, ms)
	_, err = l.LinkFedData(context.Background(), 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get company")
}

func TestLinker_LinkFedData_EmptyNameState(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ms := newMockStore()
	ms.companies[30] = &CompanyRecord{ID: 30, Name: "", State: ""}
	ms.identifiers[30] = []Identifier{}

	l := NewLinker(pool, ms)
	matched, err := l.LinkFedData(context.Background(), 30)
	require.NoError(t, err)
	assert.Equal(t, 0, matched)
	require.NoError(t, pool.ExpectationsWereMet())
}

// --- Utility tests ---

func TestPtrFloat(t *testing.T) {
	p := ptrFloat(0.95)
	require.NotNil(t, p)
	assert.Equal(t, 0.95, *p)
}
