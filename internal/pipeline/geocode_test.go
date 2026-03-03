package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sells-group/research-cli/internal/company"
	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/store"
	storemocks "github.com/sells-group/research-cli/internal/store/mocks"
	"github.com/sells-group/research-cli/pkg/geocode"
)

func TestExtractDomain_Geocode(t *testing.T) {
	// extractDomain is defined in linkedin.go — verify it works for geocode use cases too.
	tests := []struct {
		url      string
		expected string
	}{
		{"https://www.example.com/about", "example.com"},
		{"http://test.com", "test.com"},
		{"https://acme.com/path/to/page", "acme.com"},
		{"not-a-url", "not-a-url"},       // no host → returns raw
		{"", ""},                         // empty → returns empty
		{"/just/a/path", "/just/a/path"}, // no host → returns raw
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, extractDomain(tt.url), "url=%s", tt.url)
	}
}

func TestSetGeocoder(t *testing.T) {
	p := &Pipeline{}
	assert.Nil(t, p.geocoder)
	p.SetGeocoder(nil)
	assert.Nil(t, p.geocoder)
}

func TestSetGeoAssociator(t *testing.T) {
	p := &Pipeline{}
	assert.Nil(t, p.geoAssoc)
	p.SetGeoAssociator(nil)
	assert.Nil(t, p.geoAssoc)
}

func TestPhase7DGeocode_NilGeocoder(t *testing.T) {
	p := &Pipeline{}

	result, err := p.Phase7DGeocode(context.Background(), model.Company{Name: "Test"}, "")
	assert.NoError(t, err)
	assert.Equal(t, model.PhaseStatusSkipped, result.Status)
	assert.Equal(t, "geocoder_not_configured", result.Metadata["reason"])
}

func TestCompanyStore_NoStore(t *testing.T) {
	p := &Pipeline{}
	cs, ok := p.companyStore()
	assert.Nil(t, cs)
	assert.False(t, ok)
}

func TestCollectGeoData_NoStore(t *testing.T) {
	p := &Pipeline{}
	gd := p.collectGeoData(context.Background(), model.Company{Name: "Test", URL: "https://test.com"})
	assert.Nil(t, gd)
}

// --- Mock company store + store wrapper for geocode tests ---

// mockCompanyStore implements company.CompanyStore for testing.
type mockCompanyStore struct {
	getByDomainFn    func(ctx context.Context, domain string) (*company.CompanyRecord, error)
	getAddressesFn   func(ctx context.Context, companyID int64) ([]company.Address, error)
	upsertAddressFn  func(ctx context.Context, addr *company.Address) error
	updateGeocodeFn  func(ctx context.Context, id int64, lat, lon float64, source, quality, countyFIPS string) error
	getCompanyMSAsFn func(ctx context.Context, companyID int64) ([]company.AddressMSA, error)
}

func (m *mockCompanyStore) CreateCompany(_ context.Context, _ *company.CompanyRecord) error {
	return nil
}
func (m *mockCompanyStore) UpdateCompany(_ context.Context, _ *company.CompanyRecord) error {
	return nil
}
func (m *mockCompanyStore) GetCompany(_ context.Context, _ int64) (*company.CompanyRecord, error) {
	return nil, nil
}
func (m *mockCompanyStore) GetCompanyByDomain(ctx context.Context, domain string) (*company.CompanyRecord, error) {
	if m.getByDomainFn != nil {
		return m.getByDomainFn(ctx, domain)
	}
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
func (m *mockCompanyStore) UpsertAddress(ctx context.Context, addr *company.Address) error {
	if m.upsertAddressFn != nil {
		return m.upsertAddressFn(ctx, addr)
	}
	return nil
}
func (m *mockCompanyStore) GetAddresses(ctx context.Context, companyID int64) ([]company.Address, error) {
	if m.getAddressesFn != nil {
		return m.getAddressesFn(ctx, companyID)
	}
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
func (m *mockCompanyStore) UpdateAddressGeocode(ctx context.Context, id int64, lat, lon float64, source, quality, countyFIPS string) error {
	if m.updateGeocodeFn != nil {
		return m.updateGeocodeFn(ctx, id, lat, lon, source, quality, countyFIPS)
	}
	return nil
}
func (m *mockCompanyStore) UpsertAddressMSA(_ context.Context, _ *company.AddressMSA) error {
	return nil
}
func (m *mockCompanyStore) GetAddressMSAs(_ context.Context, _ int64) ([]company.AddressMSA, error) {
	return nil, nil
}
func (m *mockCompanyStore) GetCompanyMSAs(ctx context.Context, companyID int64) ([]company.AddressMSA, error) {
	if m.getCompanyMSAsFn != nil {
		return m.getCompanyMSAsFn(ctx, companyID)
	}
	return nil, nil
}

// storeWithCompanyStore wraps store.Store and adds CompanyStore() method.
type storeWithCompanyStore struct {
	store.Store
	cs company.CompanyStore
}

func (s *storeWithCompanyStore) CompanyStore() company.CompanyStore {
	return s.cs
}

// mockGeocoder implements geocode.Client for testing.
type mockGeocoder struct {
	geocodeFn func(ctx context.Context, addr geocode.AddressInput) (*geocode.Result, error)
}

func (m *mockGeocoder) Geocode(ctx context.Context, addr geocode.AddressInput) (*geocode.Result, error) {
	if m.geocodeFn != nil {
		return m.geocodeFn(ctx, addr)
	}
	return &geocode.Result{Matched: false}, nil
}

func (m *mockGeocoder) BatchGeocode(_ context.Context, _ []geocode.AddressInput) ([]geocode.Result, error) {
	return nil, nil
}

func (m *mockGeocoder) ReverseGeocode(_ context.Context, _, _ float64) (*geocode.ReverseResult, error) {
	return nil, nil
}

func newGeoTestPipeline(t *testing.T, cs *mockCompanyStore, gc geocode.Client) *Pipeline {
	t.Helper()
	mockSt := storemocks.NewMockStore(t)
	wrapped := &storeWithCompanyStore{Store: mockSt, cs: cs}
	return &Pipeline{
		store:    wrapped,
		geocoder: gc,
		cfg:      &config.Config{},
	}
}

// --- Phase7DGeocode extended tests ---

func TestPhase7DGeocode_StoreNotAvailable(t *testing.T) {
	// Pipeline has a geocoder but store doesn't implement companyStoreProvider.
	p := &Pipeline{
		geocoder: &mockGeocoder{},
		cfg:      &config.Config{},
	}

	result, err := p.Phase7DGeocode(context.Background(), model.Company{Name: "Test", URL: "https://test.com"}, "")
	assert.NoError(t, err)
	assert.Equal(t, model.PhaseStatusSkipped, result.Status)
	assert.Equal(t, "company_store_not_available", result.Metadata["reason"])
}

func TestPhase7DGeocode_CompanyNotFound(t *testing.T) {
	cs := &mockCompanyStore{
		getByDomainFn: func(_ context.Context, _ string) (*company.CompanyRecord, error) {
			return nil, errors.New("not found")
		},
	}
	p := newGeoTestPipeline(t, cs, &mockGeocoder{})

	result, err := p.Phase7DGeocode(context.Background(), model.Company{Name: "Test", URL: "https://test.com"}, "")
	assert.NoError(t, err)
	assert.Equal(t, model.PhaseStatusSkipped, result.Status)
	assert.Equal(t, "company_not_found", result.Metadata["reason"])
}

func TestPhase7DGeocode_CompanyNilResult(t *testing.T) {
	cs := &mockCompanyStore{
		getByDomainFn: func(_ context.Context, _ string) (*company.CompanyRecord, error) {
			return nil, nil
		},
	}
	p := newGeoTestPipeline(t, cs, &mockGeocoder{})

	result, err := p.Phase7DGeocode(context.Background(), model.Company{Name: "Test", URL: "https://test.com"}, "")
	assert.NoError(t, err)
	assert.Equal(t, model.PhaseStatusSkipped, result.Status)
}

func TestPhase7DGeocode_GetAddressesError(t *testing.T) {
	cs := &mockCompanyStore{
		getByDomainFn: func(_ context.Context, _ string) (*company.CompanyRecord, error) {
			return &company.CompanyRecord{ID: 1}, nil
		},
		getAddressesFn: func(_ context.Context, _ int64) ([]company.Address, error) {
			return nil, errors.New("db error")
		},
	}
	p := newGeoTestPipeline(t, cs, &mockGeocoder{})

	result, err := p.Phase7DGeocode(context.Background(), model.Company{Name: "Test", URL: "https://test.com"}, "")
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "geocode: get addresses")
}

func TestPhase7DGeocode_NoAddressesAndNoModelAddress(t *testing.T) {
	cs := &mockCompanyStore{
		getByDomainFn: func(_ context.Context, _ string) (*company.CompanyRecord, error) {
			return &company.CompanyRecord{ID: 1}, nil
		},
		getAddressesFn: func(_ context.Context, _ int64) ([]company.Address, error) {
			return nil, nil // No addresses.
		},
	}
	p := newGeoTestPipeline(t, cs, &mockGeocoder{})

	// Company model has no address fields either.
	result, err := p.Phase7DGeocode(context.Background(), model.Company{Name: "Test", URL: "https://test.com"}, "")
	assert.NoError(t, err)
	assert.Equal(t, model.PhaseStatusSkipped, result.Status)
	assert.Equal(t, "no_addresses", result.Metadata["reason"])
}

func TestPhase7DGeocode_CreatesAddressFromModel(t *testing.T) {
	var upserted bool
	cs := &mockCompanyStore{
		getByDomainFn: func(_ context.Context, _ string) (*company.CompanyRecord, error) {
			return &company.CompanyRecord{ID: 1}, nil
		},
		getAddressesFn: func(_ context.Context, _ int64) ([]company.Address, error) {
			return nil, nil // No existing addresses.
		},
		upsertAddressFn: func(_ context.Context, addr *company.Address) error {
			upserted = true
			assert.Equal(t, "123 Main St", addr.Street)
			assert.Equal(t, "Austin", addr.City)
			assert.Equal(t, "TX", addr.State)
			return nil
		},
	}

	gc := &mockGeocoder{
		geocodeFn: func(_ context.Context, _ geocode.AddressInput) (*geocode.Result, error) {
			return &geocode.Result{
				Matched:    true,
				Latitude:   30.267,
				Longitude:  -97.743,
				Source:     "tiger",
				Quality:    "rooftop",
				CountyFIPS: "48453",
			}, nil
		},
	}
	p := newGeoTestPipeline(t, cs, gc)

	co := model.Company{
		Name:   "Test",
		URL:    "https://test.com",
		Street: "123 Main St",
		City:   "Austin",
		State:  "TX",
	}

	result, err := p.Phase7DGeocode(context.Background(), co, "")
	assert.NoError(t, err)
	assert.True(t, upserted)
	assert.Equal(t, 1, result.Metadata["geocoded_count"])
}

func TestPhase7DGeocode_CreatesAddressFromRecord(t *testing.T) {
	var upserted bool
	cs := &mockCompanyStore{
		getByDomainFn: func(_ context.Context, _ string) (*company.CompanyRecord, error) {
			return &company.CompanyRecord{
				ID:     1,
				Street: "456 Elm St",
				City:   "Dallas",
				State:  "TX",
			}, nil
		},
		getAddressesFn: func(_ context.Context, _ int64) ([]company.Address, error) {
			return nil, nil
		},
		upsertAddressFn: func(_ context.Context, addr *company.Address) error {
			upserted = true
			assert.Equal(t, "456 Elm St", addr.Street)
			assert.Equal(t, "Dallas", addr.City)
			return nil
		},
	}

	gc := &mockGeocoder{
		geocodeFn: func(_ context.Context, _ geocode.AddressInput) (*geocode.Result, error) {
			return &geocode.Result{Matched: true, Latitude: 32.776, Longitude: -96.797}, nil
		},
	}
	p := newGeoTestPipeline(t, cs, gc)

	// Company model has NO address fields, so it falls back to company record.
	co := model.Company{Name: "Test", URL: "https://test.com"}

	result, err := p.Phase7DGeocode(context.Background(), co, "")
	assert.NoError(t, err)
	assert.True(t, upserted)
	assert.Equal(t, 1, result.Metadata["geocoded_count"])
}

func TestPhase7DGeocode_SkipsIncompleteAddress(t *testing.T) {
	cs := &mockCompanyStore{
		getByDomainFn: func(_ context.Context, _ string) (*company.CompanyRecord, error) {
			return &company.CompanyRecord{ID: 1}, nil
		},
		getAddressesFn: func(_ context.Context, _ int64) ([]company.Address, error) {
			return []company.Address{
				{ID: 1, City: "Austin"}, // Missing street and state.
			}, nil
		},
	}
	p := newGeoTestPipeline(t, cs, &mockGeocoder{})

	result, err := p.Phase7DGeocode(context.Background(), model.Company{Name: "Test", URL: "https://test.com"}, "")
	assert.NoError(t, err)
	assert.Equal(t, 0, result.Metadata["geocoded_count"])
}

func TestPhase7DGeocode_SkipsAlreadyGeocoded(t *testing.T) {
	lat := 30.267
	lon := -97.743
	cs := &mockCompanyStore{
		getByDomainFn: func(_ context.Context, _ string) (*company.CompanyRecord, error) {
			return &company.CompanyRecord{ID: 1}, nil
		},
		getAddressesFn: func(_ context.Context, _ int64) ([]company.Address, error) {
			return []company.Address{
				{ID: 1, Street: "123 Main St", City: "Austin", State: "TX", Latitude: &lat, Longitude: &lon},
			}, nil
		},
	}
	p := newGeoTestPipeline(t, cs, &mockGeocoder{})

	result, err := p.Phase7DGeocode(context.Background(), model.Company{Name: "Test", URL: "https://test.com"}, "")
	assert.NoError(t, err)
	assert.Equal(t, 0, result.Metadata["geocoded_count"])
}

func TestPhase7DGeocode_GeocodeError(t *testing.T) {
	cs := &mockCompanyStore{
		getByDomainFn: func(_ context.Context, _ string) (*company.CompanyRecord, error) {
			return &company.CompanyRecord{ID: 1}, nil
		},
		getAddressesFn: func(_ context.Context, _ int64) ([]company.Address, error) {
			return []company.Address{
				{ID: 1, Street: "123 Main St", City: "Austin", State: "TX"},
			}, nil
		},
	}
	gc := &mockGeocoder{
		geocodeFn: func(_ context.Context, _ geocode.AddressInput) (*geocode.Result, error) {
			return nil, errors.New("geocode api down")
		},
	}
	p := newGeoTestPipeline(t, cs, gc)

	result, err := p.Phase7DGeocode(context.Background(), model.Company{Name: "Test", URL: "https://test.com"}, "")
	assert.NoError(t, err) // Geocode errors are non-fatal.
	assert.Equal(t, 0, result.Metadata["geocoded_count"])
}

func TestPhase7DGeocode_GeocodeNotMatched(t *testing.T) {
	cs := &mockCompanyStore{
		getByDomainFn: func(_ context.Context, _ string) (*company.CompanyRecord, error) {
			return &company.CompanyRecord{ID: 1}, nil
		},
		getAddressesFn: func(_ context.Context, _ int64) ([]company.Address, error) {
			return []company.Address{
				{ID: 1, Street: "123 Main St", City: "Austin", State: "TX"},
			}, nil
		},
	}
	gc := &mockGeocoder{
		geocodeFn: func(_ context.Context, _ geocode.AddressInput) (*geocode.Result, error) {
			return &geocode.Result{Matched: false}, nil
		},
	}
	p := newGeoTestPipeline(t, cs, gc)

	result, err := p.Phase7DGeocode(context.Background(), model.Company{Name: "Test", URL: "https://test.com"}, "")
	assert.NoError(t, err)
	assert.Equal(t, 0, result.Metadata["geocoded_count"])
}

func TestPhase7DGeocode_UpdateGeocodeError(t *testing.T) {
	cs := &mockCompanyStore{
		getByDomainFn: func(_ context.Context, _ string) (*company.CompanyRecord, error) {
			return &company.CompanyRecord{ID: 1}, nil
		},
		getAddressesFn: func(_ context.Context, _ int64) ([]company.Address, error) {
			return []company.Address{
				{ID: 1, Street: "123 Main St", City: "Austin", State: "TX"},
			}, nil
		},
		updateGeocodeFn: func(_ context.Context, _ int64, _, _ float64, _, _, _ string) error {
			return errors.New("update failed")
		},
	}
	gc := &mockGeocoder{
		geocodeFn: func(_ context.Context, _ geocode.AddressInput) (*geocode.Result, error) {
			return &geocode.Result{Matched: true, Latitude: 30.267, Longitude: -97.743}, nil
		},
	}
	p := newGeoTestPipeline(t, cs, gc)

	result, err := p.Phase7DGeocode(context.Background(), model.Company{Name: "Test", URL: "https://test.com"}, "")
	assert.NoError(t, err) // Update errors are non-fatal.
	assert.Equal(t, 0, result.Metadata["geocoded_count"])
}

// --- collectGeoData extended tests ---

func TestCollectGeoData_CompanyLookupFails(t *testing.T) {
	cs := &mockCompanyStore{
		getByDomainFn: func(_ context.Context, _ string) (*company.CompanyRecord, error) {
			return nil, errors.New("db error")
		},
	}
	p := newGeoTestPipeline(t, cs, nil)
	gd := p.collectGeoData(context.Background(), model.Company{URL: "https://test.com"})
	assert.Nil(t, gd)
}

func TestCollectGeoData_NoAddresses(t *testing.T) {
	cs := &mockCompanyStore{
		getByDomainFn: func(_ context.Context, _ string) (*company.CompanyRecord, error) {
			return &company.CompanyRecord{ID: 1}, nil
		},
		getAddressesFn: func(_ context.Context, _ int64) ([]company.Address, error) {
			return nil, nil
		},
	}
	p := newGeoTestPipeline(t, cs, nil)
	gd := p.collectGeoData(context.Background(), model.Company{URL: "https://test.com"})
	assert.Nil(t, gd)
}

func TestCollectGeoData_NoPrimaryGeocoded(t *testing.T) {
	cs := &mockCompanyStore{
		getByDomainFn: func(_ context.Context, _ string) (*company.CompanyRecord, error) {
			return &company.CompanyRecord{ID: 1}, nil
		},
		getAddressesFn: func(_ context.Context, _ int64) ([]company.Address, error) {
			return []company.Address{
				{ID: 1, Street: "123 Main St"}, // No lat/lon.
			}, nil
		},
	}
	p := newGeoTestPipeline(t, cs, nil)
	gd := p.collectGeoData(context.Background(), model.Company{URL: "https://test.com"})
	assert.Nil(t, gd)
}

func TestCollectGeoData_Success(t *testing.T) {
	lat := 30.267
	lon := -97.743
	cs := &mockCompanyStore{
		getByDomainFn: func(_ context.Context, _ string) (*company.CompanyRecord, error) {
			return &company.CompanyRecord{ID: 1}, nil
		},
		getAddressesFn: func(_ context.Context, _ int64) ([]company.Address, error) {
			return []company.Address{
				{ID: 1, Street: "123 Main St", Latitude: &lat, Longitude: &lon, IsPrimary: true, CountyFIPS: "48453"},
			}, nil
		},
		getCompanyMSAsFn: func(_ context.Context, _ int64) ([]company.AddressMSA, error) {
			return []company.AddressMSA{
				{MSAName: "Austin-Round Rock-Georgetown", CBSACode: "12420", Classification: "Metro", CentroidKM: 5.2, EdgeKM: 1.3},
			}, nil
		},
	}
	p := newGeoTestPipeline(t, cs, nil)
	gd := p.collectGeoData(context.Background(), model.Company{URL: "https://test.com"})
	assert.NotNil(t, gd)
	assert.Equal(t, 30.267, gd.Latitude)
	assert.Equal(t, -97.743, gd.Longitude)
	assert.Equal(t, "48453", gd.CountyFIPS)
	assert.Equal(t, "Austin-Round Rock-Georgetown", gd.MSAName)
	assert.Equal(t, "12420", gd.CBSACode)
}

func TestCollectGeoData_NoMSAs(t *testing.T) {
	lat := 30.267
	lon := -97.743
	cs := &mockCompanyStore{
		getByDomainFn: func(_ context.Context, _ string) (*company.CompanyRecord, error) {
			return &company.CompanyRecord{ID: 1}, nil
		},
		getAddressesFn: func(_ context.Context, _ int64) ([]company.Address, error) {
			return []company.Address{
				{ID: 1, Latitude: &lat, Longitude: &lon, IsPrimary: true},
			}, nil
		},
		getCompanyMSAsFn: func(_ context.Context, _ int64) ([]company.AddressMSA, error) {
			return nil, errors.New("no MSAs")
		},
	}
	p := newGeoTestPipeline(t, cs, nil)
	gd := p.collectGeoData(context.Background(), model.Company{URL: "https://test.com"})
	assert.NotNil(t, gd)
	assert.Equal(t, 30.267, gd.Latitude)
	assert.Equal(t, "", gd.MSAName) // No MSA data.
}

func TestCollectGeoData_PrefersIsPrimary(t *testing.T) {
	lat1, lon1 := 30.0, -97.0
	lat2, lon2 := 31.0, -98.0
	cs := &mockCompanyStore{
		getByDomainFn: func(_ context.Context, _ string) (*company.CompanyRecord, error) {
			return &company.CompanyRecord{ID: 1}, nil
		},
		getAddressesFn: func(_ context.Context, _ int64) ([]company.Address, error) {
			return []company.Address{
				{ID: 1, Latitude: &lat1, Longitude: &lon1, IsPrimary: false},
				{ID: 2, Latitude: &lat2, Longitude: &lon2, IsPrimary: true}, // This one should win.
			}, nil
		},
		getCompanyMSAsFn: func(_ context.Context, _ int64) ([]company.AddressMSA, error) {
			return nil, nil
		},
	}
	p := newGeoTestPipeline(t, cs, nil)
	gd := p.collectGeoData(context.Background(), model.Company{URL: "https://test.com"})
	assert.NotNil(t, gd)
	assert.Equal(t, 31.0, gd.Latitude)
}

// TestPhase7DGeocode_UpsertAddressFromModelError tests that an error when
// upserting the address created from the company model propagates as error.
func TestPhase7DGeocode_UpsertAddressFromModelError(t *testing.T) {
	cs := &mockCompanyStore{
		getByDomainFn: func(_ context.Context, _ string) (*company.CompanyRecord, error) {
			return &company.CompanyRecord{ID: 1}, nil
		},
		getAddressesFn: func(_ context.Context, _ int64) ([]company.Address, error) {
			return nil, nil // No existing addresses.
		},
		upsertAddressFn: func(_ context.Context, _ *company.Address) error {
			return errors.New("upsert failed")
		},
	}
	p := newGeoTestPipeline(t, cs, &mockGeocoder{})

	co := model.Company{
		Name:   "Test",
		URL:    "https://test.com",
		Street: "123 Main St",
		City:   "Austin",
		State:  "TX",
	}

	result, err := p.Phase7DGeocode(context.Background(), co, "")
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "geocode: create address from model")
}

// TestPhase7DGeocode_UpsertAddressFromRecordError tests that an error when
// upserting the address created from the company record propagates as error.
func TestPhase7DGeocode_UpsertAddressFromRecordError(t *testing.T) {
	cs := &mockCompanyStore{
		getByDomainFn: func(_ context.Context, _ string) (*company.CompanyRecord, error) {
			return &company.CompanyRecord{
				ID:     1,
				Street: "456 Elm St",
				City:   "Dallas",
				State:  "TX",
			}, nil
		},
		getAddressesFn: func(_ context.Context, _ int64) ([]company.Address, error) {
			return nil, nil
		},
		upsertAddressFn: func(_ context.Context, _ *company.Address) error {
			return errors.New("upsert from record failed")
		},
	}
	p := newGeoTestPipeline(t, cs, &mockGeocoder{})

	// Company model has no address fields, triggering fallback to company record.
	co := model.Company{Name: "Test", URL: "https://test.com"}

	result, err := p.Phase7DGeocode(context.Background(), co, "")
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "geocode: create address from record")
}

// TestCollectGeoData_GetAddressesError tests that collectGeoData returns nil
// when GetAddresses fails.
func TestCollectGeoData_GetAddressesError(t *testing.T) {
	cs := &mockCompanyStore{
		getByDomainFn: func(_ context.Context, _ string) (*company.CompanyRecord, error) {
			return &company.CompanyRecord{ID: 1}, nil
		},
		getAddressesFn: func(_ context.Context, _ int64) ([]company.Address, error) {
			return nil, errors.New("db error")
		},
	}
	p := newGeoTestPipeline(t, cs, nil)
	gd := p.collectGeoData(context.Background(), model.Company{URL: "https://test.com"})
	assert.Nil(t, gd)
}

// TestCollectGeoData_NonPrimaryGeocoded tests that collectGeoData picks a
// geocoded address even when IsPrimary is false (first geocoded wins).
func TestCollectGeoData_NonPrimaryGeocoded(t *testing.T) {
	lat := 33.0
	lon := -96.0
	cs := &mockCompanyStore{
		getByDomainFn: func(_ context.Context, _ string) (*company.CompanyRecord, error) {
			return &company.CompanyRecord{ID: 1}, nil
		},
		getAddressesFn: func(_ context.Context, _ int64) ([]company.Address, error) {
			return []company.Address{
				{ID: 1, Latitude: &lat, Longitude: &lon, IsPrimary: false},
			}, nil
		},
		getCompanyMSAsFn: func(_ context.Context, _ int64) ([]company.AddressMSA, error) {
			return nil, nil
		},
	}
	p := newGeoTestPipeline(t, cs, nil)
	gd := p.collectGeoData(context.Background(), model.Company{URL: "https://test.com"})
	assert.NotNil(t, gd)
	assert.Equal(t, 33.0, gd.Latitude)
}

// companyStore() with a proper store.
func TestCompanyStore_WithProvider(t *testing.T) {
	cs := &mockCompanyStore{}
	mockSt := storemocks.NewMockStore(t)
	wrapped := &storeWithCompanyStore{Store: mockSt, cs: cs}
	p := &Pipeline{store: wrapped}

	result, ok := p.companyStore()
	assert.True(t, ok)
	assert.Equal(t, cs, result)
}
