package geocode

import (
	"context"
	"database/sql"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockProvider implements Provider for testing cascade behavior.
type mockProvider struct {
	name      string
	available bool
	result    *Result
	err       error
}

func (m *mockProvider) Name() string    { return m.name }
func (m *mockProvider) Available() bool { return m.available }
func (m *mockProvider) Geocode(_ context.Context, _ AddressInput) (*Result, error) {
	return m.result, m.err
}

func TestTigerProvider_Match(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT\s+ST_Y`).
		WithArgs("100 Main St, Miami, FL, 33131").
		WillReturnRows(
			pgxmock.NewRows([]string{"lat", "lon", "rating", "matched_address", "county_fips"}).
				AddRow(25.77, -80.19, 5, "100 Main St, Miami, FL 33131", "12086"),
		)

	p := NewTigerProvider(mock, 100)
	result, err := p.Geocode(context.Background(), AddressInput{
		Street:  "100 Main St",
		City:    "Miami",
		State:   "FL",
		ZipCode: "33131",
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Matched)
	assert.Equal(t, "tiger", result.Source)
	assert.Equal(t, "rooftop", result.Quality)
	assert.InDelta(t, 25.77, result.Latitude, 0.01)
	assert.InDelta(t, -80.19, result.Longitude, 0.01)
	assert.Equal(t, "12086", result.CountyFIPS)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTigerProvider_NoMatch(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT\s+ST_Y`).
		WithArgs("999 Fake Ave, Nowhere, XX, 00000").
		WillReturnError(assert.AnError)

	p := NewTigerProvider(mock, 100)
	result, err := p.Geocode(context.Background(), AddressInput{
		Street:  "999 Fake Ave",
		City:    "Nowhere",
		State:   "XX",
		ZipCode: "00000",
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Matched)
	assert.Equal(t, "tiger", result.Source)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTigerProvider_ExceedsMaxRating(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT\s+ST_Y`).
		WithArgs("123 Main St, Anytown, FL, 33101").
		WillReturnRows(
			pgxmock.NewRows([]string{"lat", "lon", "rating", "matched_address", "county_fips"}).
				AddRow(25.0, -80.0, 60, "123 Main St, Anytown, FL 33101", "12086"),
		)

	p := NewTigerProvider(mock, 50)
	result, err := p.Geocode(context.Background(), AddressInput{
		Street:  "123 Main St",
		City:    "Anytown",
		State:   "FL",
		ZipCode: "33101",
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Matched)
	assert.Equal(t, 60, result.Rating)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTigerProvider_EmptyAddress(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	p := NewTigerProvider(mock, 100)
	result, err := p.Geocode(context.Background(), AddressInput{})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Matched)
	assert.Equal(t, "tiger", result.Source)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTigerProvider_Name(t *testing.T) {
	p := NewTigerProvider(nil, 100)
	assert.Equal(t, "tiger", p.Name())
}

func TestTigerProvider_Available(t *testing.T) {
	p := NewTigerProvider(nil, 100)
	assert.True(t, p.Available())
}

func TestCascadeClient_FirstProviderMatches(t *testing.T) {
	p1 := &mockProvider{
		name:      "provider1",
		available: true,
		result:    &Result{Matched: true, Source: "provider1", Latitude: 25.77, Longitude: -80.19, Quality: "rooftop"},
	}
	p2 := &mockProvider{
		name:      "provider2",
		available: true,
		result:    &Result{Matched: true, Source: "provider2", Latitude: 30.0, Longitude: -85.0, Quality: "rooftop"},
	}

	c := NewCascadeClient(nil, []Provider{p1, p2}, WithCascadeCacheEnabled(false))
	result, err := c.Geocode(context.Background(), AddressInput{
		Street: "100 Main St",
		City:   "Miami",
		State:  "FL",
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Matched)
	assert.Equal(t, "provider1", result.Source)
	assert.InDelta(t, 25.77, result.Latitude, 0.01)
}

func TestCascadeClient_FirstMissesSecondMatches(t *testing.T) {
	p1 := &mockProvider{
		name:      "tiger",
		available: true,
		result:    &Result{Matched: false, Source: "tiger"},
	}
	p2 := &mockProvider{
		name:      "census",
		available: true,
		result:    &Result{Matched: true, Source: "census", Latitude: 38.899, Longitude: -77.016, Quality: "rooftop"},
	}

	c := NewCascadeClient(nil, []Provider{p1, p2}, WithCascadeCacheEnabled(false))
	result, err := c.Geocode(context.Background(), AddressInput{
		Street: "1600 Pennsylvania Ave NW",
		City:   "Washington",
		State:  "DC",
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Matched)
	assert.Equal(t, "census", result.Source)
	assert.InDelta(t, 38.899, result.Latitude, 0.001)
}

func TestCascadeClient_AllProvidersMiss(t *testing.T) {
	p1 := &mockProvider{
		name:      "tiger",
		available: true,
		result:    &Result{Matched: false, Source: "tiger"},
	}
	p2 := &mockProvider{
		name:      "census",
		available: true,
		result:    &Result{Matched: false, Source: "census"},
	}

	c := NewCascadeClient(nil, []Provider{p1, p2}, WithCascadeCacheEnabled(false))
	result, err := c.Geocode(context.Background(), AddressInput{
		Street: "999 Fake Ave",
		City:   "Nowhere",
		State:  "XX",
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Matched)
}

func TestCascadeClient_ProviderErrorTriesNext(t *testing.T) {
	p1 := &mockProvider{
		name:      "failing",
		available: true,
		err:       assert.AnError,
	}
	p2 := &mockProvider{
		name:      "census",
		available: true,
		result:    &Result{Matched: true, Source: "census", Latitude: 38.899, Longitude: -77.016, Quality: "rooftop"},
	}

	c := NewCascadeClient(nil, []Provider{p1, p2}, WithCascadeCacheEnabled(false))
	result, err := c.Geocode(context.Background(), AddressInput{
		Street: "100 Main St",
		City:   "Miami",
		State:  "FL",
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Matched)
	assert.Equal(t, "census", result.Source)
}

func TestCascadeClient_UnavailableProviderSkipped(t *testing.T) {
	p1 := &mockProvider{
		name:      "unavailable",
		available: false,
		result:    &Result{Matched: true, Source: "unavailable", Latitude: 1.0, Longitude: 1.0},
	}
	p2 := &mockProvider{
		name:      "census",
		available: true,
		result:    &Result{Matched: true, Source: "census", Latitude: 38.899, Longitude: -77.016, Quality: "rooftop"},
	}

	c := NewCascadeClient(nil, []Provider{p1, p2}, WithCascadeCacheEnabled(false))
	result, err := c.Geocode(context.Background(), AddressInput{
		Street: "100 Main St",
		City:   "Miami",
		State:  "FL",
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Matched)
	assert.Equal(t, "census", result.Source)
}

func TestCascadeClient_CacheHitSkipsProviders(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rating := 3
	countyFIPS := "12086"
	source := "tiger"
	mock.ExpectQuery(`SELECT latitude, longitude, quality, rating, matched, county_fips, source FROM public.geocode_cache`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnRows(
			pgxmock.NewRows([]string{"latitude", "longitude", "quality", "rating", "matched", "county_fips", "source"}).
				AddRow(25.77, -80.19, "rooftop", &rating, true, &countyFIPS, &source),
		)

	// Provider should NOT be called — cache hit short-circuits.
	p := &mockProvider{
		name:      "should-not-call",
		available: true,
		err:       assert.AnError,
	}

	c := NewCascadeClient(mock, []Provider{p}, WithCascadeCacheEnabled(true))
	result, err := c.Geocode(context.Background(), AddressInput{
		Street:  "100 Main St",
		City:    "Miami",
		State:   "FL",
		ZipCode: "33131",
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Matched)
	assert.InDelta(t, 25.77, result.Latitude, 0.01)
	assert.Equal(t, "tiger", result.Source)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCascadeClient_BatchGeocodeEmpty(t *testing.T) {
	c := NewCascadeClient(nil, nil, WithCascadeCacheEnabled(false))
	results, err := c.BatchGeocode(context.Background(), nil)

	require.NoError(t, err)
	assert.Nil(t, results)
}

func TestCascadeClient_BatchGeocodeMultiple(t *testing.T) {
	p := &mockProvider{
		name:      "mock",
		available: true,
		result:    &Result{Matched: true, Source: "mock", Latitude: 25.77, Longitude: -80.19, Quality: "rooftop"},
	}

	c := NewCascadeClient(nil, []Provider{p},
		WithCascadeCacheEnabled(false),
		WithCascadeBatchConcurrency(1),
	)
	results, err := c.BatchGeocode(context.Background(), []AddressInput{
		{Street: "100 Main St", City: "Miami", State: "FL"},
		{Street: "200 Main St", City: "Miami", State: "FL"},
	})

	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.True(t, results[0].Matched)
	assert.True(t, results[1].Matched)
}

func TestCascadeClient_CacheTableOption(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Expect query against custom table.
	mock.ExpectQuery(`SELECT latitude, longitude, quality, rating, matched, county_fips, source FROM geo.geocode_cache`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnError(assert.AnError) // cache miss

	p := &mockProvider{
		name:      "census",
		available: true,
		result:    &Result{Matched: false, Source: "census"},
	}

	// Expect negative cache store to custom table.
	mock.ExpectExec(`INSERT INTO geo.geocode_cache`).
		WithArgs(pgxmock.AnyArg(), 0.0, 0.0, "", 0, false, nil, "census").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	c := NewCascadeClient(mock, []Provider{p},
		WithCascadeCacheEnabled(true),
		WithCascadeCacheTable("geo.geocode_cache"),
	)
	result, err := c.Geocode(context.Background(), AddressInput{
		Street: "100 Main St",
		City:   "Miami",
		State:  "FL",
	})

	require.NoError(t, err)
	assert.False(t, result.Matched)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithCacheTable_LegacyClient(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	g := NewClient(mock, WithCacheTable("geo.geocode_cache")).(*geocoder)
	assert.Equal(t, "geo.geocode_cache", g.cacheTable)
}

func TestCascadeClient_CacheTTLDaysOption(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Expect cache query with TTL clause.
	mock.ExpectQuery(`SELECT latitude, longitude, quality, rating, matched, county_fips, source FROM public.geocode_cache WHERE address_hash = .+ AND cached_at > now\(\) - interval '30 days'`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnError(assert.AnError) // cache miss

	p := &mockProvider{
		name:      "census",
		available: true,
		result:    &Result{Matched: true, Source: "census", Latitude: 38.899, Longitude: -77.016, Quality: "rooftop"},
	}

	// Expect cache store after provider match.
	mock.ExpectExec(`INSERT INTO public.geocode_cache`).
		WithArgs(pgxmock.AnyArg(), 38.899, -77.016, "rooftop", 0, true, nil, "census").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	c := NewCascadeClient(mock, []Provider{p},
		WithCascadeCacheEnabled(true),
		WithCascadeCacheTTLDays(30),
	)
	result, err := c.Geocode(context.Background(), AddressInput{
		Street: "1600 Pennsylvania Ave NW",
		City:   "Washington",
		State:  "DC",
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Matched)
	assert.Equal(t, "census", result.Source)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCascadeClient_ReverseGeocode(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	fullAddr := sql.NullString{String: "100 Main St, Miami, FL 33131", Valid: true}
	state := sql.NullString{String: "FL", Valid: true}
	zip := sql.NullString{String: "33131", Valid: true}
	countyFIPS := sql.NullString{String: "12086", Valid: true}

	mock.ExpectQuery(`SELECT\s+pprint_addy`).
		WithArgs(-80.19, 25.77).
		WillReturnRows(
			pgxmock.NewRows([]string{"pprint_addy", "stateusps", "zip", "county_fips", "rating"}).
				AddRow(fullAddr, state, zip, countyFIPS, 3),
		)

	c := NewCascadeClient(mock, nil)
	result, err := c.ReverseGeocode(context.Background(), 25.77, -80.19)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "100 Main St, Miami, FL 33131", result.Street)
	assert.Equal(t, "FL", result.State)
	assert.Equal(t, "33131", result.ZipCode)
	assert.Equal(t, "12086", result.CountyFIPS)
	assert.Equal(t, 3, result.Rating)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCascadeClient_StoreCacheError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Cache miss.
	mock.ExpectQuery(`SELECT latitude, longitude, quality, rating, matched, county_fips, source FROM public.geocode_cache`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnError(assert.AnError)

	p := &mockProvider{
		name:      "census",
		available: true,
		result:    &Result{Matched: true, Source: "census", Latitude: 38.899, Longitude: -77.016, Quality: "rooftop"},
	}

	// Cache store fails — should not crash, error is swallowed.
	mock.ExpectExec(`INSERT INTO public.geocode_cache`).
		WithArgs(pgxmock.AnyArg(), 38.899, -77.016, "rooftop", 0, true, nil, "census").
		WillReturnError(assert.AnError)

	c := NewCascadeClient(mock, []Provider{p}, WithCascadeCacheEnabled(true))
	result, err := c.Geocode(context.Background(), AddressInput{
		Street: "100 Main St",
		City:   "Miami",
		State:  "FL",
	})

	// Geocode should still succeed even though cache store failed.
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Matched)
	assert.Equal(t, "census", result.Source)

	require.NoError(t, mock.ExpectationsWereMet())
}
