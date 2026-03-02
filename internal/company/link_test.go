package company

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockStore implements CompanyStore for link tests.
// Only UpsertMatch is exercised; all other methods are stubs.
type mockStore struct {
	upsertedMatches []Match
}

func (m *mockStore) UpsertMatch(_ context.Context, match *Match) error {
	match.ID = int64(len(m.upsertedMatches) + 1)
	m.upsertedMatches = append(m.upsertedMatches, *match)
	return nil
}

func (m *mockStore) CreateCompany(_ context.Context, _ *CompanyRecord) error       { return nil }
func (m *mockStore) UpdateCompany(_ context.Context, _ *CompanyRecord) error       { return nil }
func (m *mockStore) GetCompany(_ context.Context, _ int64) (*CompanyRecord, error) { return nil, nil }
func (m *mockStore) GetCompanyByDomain(_ context.Context, _ string) (*CompanyRecord, error) {
	return nil, nil
}
func (m *mockStore) SearchCompaniesByName(_ context.Context, _ string, _ int) ([]CompanyRecord, error) {
	return nil, nil
}
func (m *mockStore) UpsertIdentifier(_ context.Context, _ *Identifier) error { return nil }
func (m *mockStore) GetIdentifiers(_ context.Context, _ int64) ([]Identifier, error) {
	return nil, nil
}
func (m *mockStore) FindByIdentifier(_ context.Context, _, _ string) (*CompanyRecord, error) {
	return nil, nil
}
func (m *mockStore) UpsertAddress(_ context.Context, _ *Address) error          { return nil }
func (m *mockStore) GetAddresses(_ context.Context, _ int64) ([]Address, error) { return nil, nil }
func (m *mockStore) UpsertContact(_ context.Context, _ *Contact) error          { return nil }
func (m *mockStore) GetContacts(_ context.Context, _ int64) ([]Contact, error)  { return nil, nil }
func (m *mockStore) GetContactsByRole(_ context.Context, _ int64, _ string) ([]Contact, error) {
	return nil, nil
}
func (m *mockStore) UpsertLicense(_ context.Context, _ *License) error         { return nil }
func (m *mockStore) GetLicenses(_ context.Context, _ int64) ([]License, error) { return nil, nil }
func (m *mockStore) UpsertSource(_ context.Context, _ *Source) error           { return nil }
func (m *mockStore) GetSources(_ context.Context, _ int64) ([]Source, error)   { return nil, nil }
func (m *mockStore) GetSource(_ context.Context, _ int64, _, _ string) (*Source, error) {
	return nil, nil
}
func (m *mockStore) UpsertFinancial(_ context.Context, _ *Financial) error { return nil }
func (m *mockStore) GetFinancials(_ context.Context, _ int64, _ string) ([]Financial, error) {
	return nil, nil
}
func (m *mockStore) SetTags(_ context.Context, _ int64, _ string, _ []string) error { return nil }
func (m *mockStore) GetTags(_ context.Context, _ int64) ([]Tag, error)              { return nil, nil }
func (m *mockStore) GetMatches(_ context.Context, _ int64) ([]Match, error)         { return nil, nil }
func (m *mockStore) FindByMatch(_ context.Context, _, _ string) (*CompanyRecord, error) {
	return nil, nil
}
func (m *mockStore) GetUngeocodedAddresses(_ context.Context, _ int) ([]Address, error) {
	return nil, nil
}
func (m *mockStore) UpdateAddressGeocode(_ context.Context, _ int64, _, _ float64, _, _, _ string) error {
	return nil
}
func (m *mockStore) UpsertAddressMSA(_ context.Context, _ *AddressMSA) error { return nil }
func (m *mockStore) GetAddressMSAs(_ context.Context, _ int64) ([]AddressMSA, error) {
	return nil, nil
}
func (m *mockStore) GetCompanyMSAs(_ context.Context, _ int64) ([]AddressMSA, error) {
	return nil, nil
}

// errStore returns an error on UpsertMatch; other methods are stubs.
type errStore struct {
	mockStore
	upsertErr error
}

func (m *errStore) UpsertMatch(_ context.Context, _ *Match) error {
	return m.upsertErr
}

func TestLinker_MatchEIN(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	store := &mockStore{}
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
	require.Len(t, store.upsertedMatches, 1)
	m := store.upsertedMatches[0]
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

	store := &mockStore{}
	linker := NewLinker(pool, store)

	pool.ExpectQuery("SELECT sponsor_dfe_name FROM fed_data.form_5500").
		WithArgs("999999999").
		WillReturnError(errors.New("connection refused"))

	n, err := linker.matchEIN(context.Background(), 42, "999999999")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "link: query form_5500")
	assert.Equal(t, 0, n)

	// No match should be upserted.
	assert.Empty(t, store.upsertedMatches)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchEIN_NoRows(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	store := &mockStore{}
	linker := NewLinker(pool, store)

	pool.ExpectQuery("SELECT sponsor_dfe_name FROM fed_data.form_5500").
		WithArgs("000000000").
		WillReturnError(pgx.ErrNoRows)

	n, err := linker.matchEIN(context.Background(), 42, "000000000")
	require.NoError(t, err)
	assert.Equal(t, 0, n)

	// No match should be upserted.
	assert.Empty(t, store.upsertedMatches)

	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestLinker_MatchEIN_UpsertError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	store := &errStore{upsertErr: errors.New("upsert failed")}
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

func TestPtrFloat(t *testing.T) {
	p := ptrFloat(0.95)
	require.NotNil(t, p)
	assert.Equal(t, 0.95, *p)
}
