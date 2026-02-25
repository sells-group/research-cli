package geo

import (
	"context"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/company"
)

// mockCompanyStore implements the minimum CompanyStore interface needed for tests.
type mockCompanyStore struct {
	upserted []company.AddressMSA
}

func (m *mockCompanyStore) UpsertAddressMSA(_ context.Context, am *company.AddressMSA) error {
	am.ID = int64(len(m.upserted) + 1)
	am.ComputedAt = time.Now()
	m.upserted = append(m.upserted, *am)
	return nil
}

// Stubs for the full CompanyStore interface — only UpsertAddressMSA is used.
func (m *mockCompanyStore) CreateCompany(_ context.Context, _ *company.CompanyRecord) error {
	return nil
}
func (m *mockCompanyStore) UpdateCompany(_ context.Context, _ *company.CompanyRecord) error {
	return nil
}
func (m *mockCompanyStore) GetCompany(_ context.Context, _ int64) (*company.CompanyRecord, error) {
	return nil, nil
}
func (m *mockCompanyStore) GetCompanyByDomain(_ context.Context, _ string) (*company.CompanyRecord, error) {
	return nil, nil
}
func (m *mockCompanyStore) SearchCompaniesByName(_ context.Context, _ string, _ int) ([]company.CompanyRecord, error) {
	return nil, nil
}
func (m *mockCompanyStore) UpsertIdentifier(_ context.Context, _ *company.Identifier) error {
	return nil
}
func (m *mockCompanyStore) GetIdentifiers(_ context.Context, _ int64) ([]company.Identifier, error) {
	return nil, nil
}
func (m *mockCompanyStore) FindByIdentifier(_ context.Context, _, _ string) (*company.CompanyRecord, error) {
	return nil, nil
}
func (m *mockCompanyStore) UpsertAddress(_ context.Context, _ *company.Address) error { return nil }
func (m *mockCompanyStore) GetAddresses(_ context.Context, _ int64) ([]company.Address, error) {
	return nil, nil
}
func (m *mockCompanyStore) UpsertContact(_ context.Context, _ *company.Contact) error { return nil }
func (m *mockCompanyStore) GetContacts(_ context.Context, _ int64) ([]company.Contact, error) {
	return nil, nil
}
func (m *mockCompanyStore) GetContactsByRole(_ context.Context, _ int64, _ string) ([]company.Contact, error) {
	return nil, nil
}
func (m *mockCompanyStore) UpsertLicense(_ context.Context, _ *company.License) error { return nil }
func (m *mockCompanyStore) GetLicenses(_ context.Context, _ int64) ([]company.License, error) {
	return nil, nil
}
func (m *mockCompanyStore) UpsertSource(_ context.Context, _ *company.Source) error { return nil }
func (m *mockCompanyStore) GetSources(_ context.Context, _ int64) ([]company.Source, error) {
	return nil, nil
}
func (m *mockCompanyStore) GetSource(_ context.Context, _ int64, _, _ string) (*company.Source, error) {
	return nil, nil
}
func (m *mockCompanyStore) UpsertFinancial(_ context.Context, _ *company.Financial) error { return nil }
func (m *mockCompanyStore) GetFinancials(_ context.Context, _ int64, _ string) ([]company.Financial, error) {
	return nil, nil
}
func (m *mockCompanyStore) SetTags(_ context.Context, _ int64, _ string, _ []string) error {
	return nil
}
func (m *mockCompanyStore) GetTags(_ context.Context, _ int64) ([]company.Tag, error) {
	return nil, nil
}
func (m *mockCompanyStore) UpsertMatch(_ context.Context, _ *company.Match) error { return nil }
func (m *mockCompanyStore) GetMatches(_ context.Context, _ int64) ([]company.Match, error) {
	return nil, nil
}
func (m *mockCompanyStore) FindByMatch(_ context.Context, _, _ string) (*company.CompanyRecord, error) {
	return nil, nil
}
func (m *mockCompanyStore) GetUngeocodedAddresses(_ context.Context, _ int) ([]company.Address, error) {
	return nil, nil
}
func (m *mockCompanyStore) UpdateAddressGeocode(_ context.Context, _ int64, _, _ float64, _, _, _ string) error {
	return nil
}
func (m *mockCompanyStore) GetAddressMSAs(_ context.Context, _ int64) ([]company.AddressMSA, error) {
	return nil, nil
}
func (m *mockCompanyStore) GetCompanyMSAs(_ context.Context, _ int64) ([]company.AddressMSA, error) {
	return nil, nil
}

func TestAssociateAddress_QueryStructure(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	cs := &mockCompanyStore{}
	assoc := NewAssociator(mock, cs)

	// Expect the PostGIS query with specific parameters.
	mock.ExpectQuery("SELECT").
		WithArgs(-77.0365, 38.8977, 3). // lon, lat, topN
		WillReturnRows(
			pgxmock.NewRows([]string{"cbsa_code", "name", "is_within", "distance_km", "centroid_km", "edge_km"}).
				AddRow("47900", "Washington-Arlington-Alexandria, DC-VA-MD-WV", true, 0.0, 5.2, 0.0).
				AddRow("12580", "Baltimore-Columbia-Towson, MD", false, 45.0, 62.3, 45.0),
		)

	relations, err := assoc.AssociateAddress(context.Background(), 42, 38.8977, -77.0365, 3)
	require.NoError(t, err)
	require.Len(t, relations, 2)

	// First relation: within DC MSA.
	assert.Equal(t, "47900", relations[0].CBSACode)
	assert.True(t, relations[0].IsWithin)
	assert.Equal(t, ClassUrbanCore, relations[0].Classification)

	// Second relation: outside Baltimore MSA, > 40km edge.
	assert.Equal(t, "12580", relations[1].CBSACode)
	assert.False(t, relations[1].IsWithin)
	assert.Equal(t, ClassRural, relations[1].Classification)

	// Verify associations were upserted.
	require.Len(t, cs.upserted, 2)
	assert.Equal(t, int64(42), cs.upserted[0].AddressID)
	assert.Equal(t, "47900", cs.upserted[0].CBSACode)
	assert.Equal(t, ClassUrbanCore, cs.upserted[0].Classification)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAssociateAddress_NoResults(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	cs := &mockCompanyStore{}
	assoc := NewAssociator(mock, cs)

	mock.ExpectQuery("SELECT").
		WithArgs(-170.0, 60.0, 3).
		WillReturnRows(
			pgxmock.NewRows([]string{"cbsa_code", "name", "is_within", "distance_km", "centroid_km", "edge_km"}),
		)

	relations, err := assoc.AssociateAddress(context.Background(), 99, 60.0, -170.0, 3)
	require.NoError(t, err)
	assert.Empty(t, relations)
	assert.Empty(t, cs.upserted)

	assert.NoError(t, mock.ExpectationsWereMet())
}
