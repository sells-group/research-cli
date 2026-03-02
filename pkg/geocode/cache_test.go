package geocode

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCacheKey_Deterministic(t *testing.T) {
	addr := AddressInput{
		Street:  "100 S Biscayne Blvd",
		City:    "Miami",
		State:   "FL",
		ZipCode: "33131",
	}

	key1 := cacheKey(addr)
	key2 := cacheKey(addr)
	assert.Equal(t, key1, key2)
	assert.Len(t, key1, 64) // SHA-256 hex is 64 chars
}

func TestCacheKey_CaseInsensitive(t *testing.T) {
	addr1 := AddressInput{Street: "100 Main St", City: "Miami", State: "FL", ZipCode: "33131"}
	addr2 := AddressInput{Street: "100 MAIN ST", City: "MIAMI", State: "fl", ZipCode: "33131"}

	assert.Equal(t, cacheKey(addr1), cacheKey(addr2))
}

func TestCacheKey_DifferentAddresses(t *testing.T) {
	addr1 := AddressInput{Street: "100 Main St", City: "Miami", State: "FL", ZipCode: "33131"}
	addr2 := AddressInput{Street: "200 Main St", City: "Miami", State: "FL", ZipCode: "33131"}

	assert.NotEqual(t, cacheKey(addr1), cacheKey(addr2))
}

func TestCheckCache_Hit(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rating := 5
	countyFIPS := "12086"
	mock.ExpectQuery(`SELECT latitude, longitude, quality, rating, matched, county_fips FROM public.geocode_cache`).
		WithArgs("abc123").
		WillReturnRows(
			pgxmock.NewRows([]string{"latitude", "longitude", "quality", "rating", "matched", "county_fips"}).
				AddRow(25.77, -80.19, "rooftop", &rating, true, &countyFIPS),
		)

	g := &geocoder{pool: mock, cacheEnabled: true}
	result, err := g.checkCache(context.Background(), "abc123")

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Matched)
	assert.Equal(t, "tiger", result.Source)
	assert.InDelta(t, 25.77, result.Latitude, 0.01)
	assert.InDelta(t, -80.19, result.Longitude, 0.01)
	assert.Equal(t, "rooftop", result.Quality)
	assert.Equal(t, 5, result.Rating)
	assert.Equal(t, "12086", result.CountyFIPS)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCheckCache_Miss(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT latitude, longitude, quality, rating, matched, county_fips FROM public.geocode_cache`).
		WithArgs("missing-key").
		WillReturnError(assert.AnError)

	g := &geocoder{pool: mock, cacheEnabled: true}
	result, err := g.checkCache(context.Background(), "missing-key")

	assert.Error(t, err)
	assert.Nil(t, result)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCheckCache_NegativeHit(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT latitude, longitude, quality, rating, matched, county_fips FROM public.geocode_cache`).
		WithArgs("neg-key").
		WillReturnRows(
			pgxmock.NewRows([]string{"latitude", "longitude", "quality", "rating", "matched", "county_fips"}).
				AddRow(0.0, 0.0, "", (*int)(nil), false, (*string)(nil)),
		)

	g := &geocoder{pool: mock, cacheEnabled: true}
	result, err := g.checkCache(context.Background(), "neg-key")

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Matched, "negative cache entry should return Matched=false")
	assert.Equal(t, "tiger", result.Source)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCheckCache_TTLFilter(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// With TTL configured, the query should include the TTL clause.
	mock.ExpectQuery(`SELECT latitude, longitude, quality, rating, matched, county_fips FROM public.geocode_cache WHERE address_hash = .+ AND cached_at > now\(\) - interval '90 days'`).
		WithArgs("ttl-key").
		WillReturnError(assert.AnError)

	g := &geocoder{pool: mock, cacheEnabled: true, cacheTTLDays: 90}
	result, err := g.checkCache(context.Background(), "ttl-key")

	assert.Error(t, err)
	assert.Nil(t, result)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStoreCache(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec(`INSERT INTO public.geocode_cache`).
		WithArgs("hashkey", 25.77, -80.19, "rooftop", 5, true, "12086").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	g := &geocoder{pool: mock, cacheEnabled: true}
	err = g.storeCache(context.Background(), "hashkey", &Result{
		Latitude:   25.77,
		Longitude:  -80.19,
		Quality:    "rooftop",
		Rating:     5,
		Matched:    true,
		CountyFIPS: "12086",
	})

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStoreCache_NegativeEntry(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec(`INSERT INTO public.geocode_cache`).
		WithArgs("neg-hashkey", 0.0, 0.0, "", 0, false, nil).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	g := &geocoder{pool: mock, cacheEnabled: true}
	err = g.storeCache(context.Background(), "neg-hashkey", &Result{
		Matched: false,
		Source:  "tiger",
	})

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCacheRoundTrip_CountyFIPS(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Store a result with CountyFIPS.
	mock.ExpectExec(`INSERT INTO public.geocode_cache`).
		WithArgs("fips-key", 25.77, -80.19, "rooftop", 5, true, "12086").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	g := &geocoder{pool: mock, cacheEnabled: true}
	err = g.storeCache(context.Background(), "fips-key", &Result{
		Latitude:   25.77,
		Longitude:  -80.19,
		Quality:    "rooftop",
		Rating:     5,
		Matched:    true,
		CountyFIPS: "12086",
	})
	require.NoError(t, err)

	// Retrieve from cache — county_fips should round-trip.
	rating := 5
	countyFIPS := "12086"
	mock.ExpectQuery(`SELECT latitude, longitude, quality, rating, matched, county_fips FROM public.geocode_cache`).
		WithArgs("fips-key").
		WillReturnRows(
			pgxmock.NewRows([]string{"latitude", "longitude", "quality", "rating", "matched", "county_fips"}).
				AddRow(25.77, -80.19, "rooftop", &rating, true, &countyFIPS),
		)

	result, err := g.checkCache(context.Background(), "fips-key")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "12086", result.CountyFIPS, "county_fips should round-trip through cache")
	assert.True(t, result.Matched)
	assert.Equal(t, 5, result.Rating)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGeocode_CacheHitSkipsPostGIS(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rating := 3
	countyFIPS := "12086"
	mock.ExpectQuery(`SELECT latitude, longitude, quality, rating, matched, county_fips FROM public.geocode_cache`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnRows(
			pgxmock.NewRows([]string{"latitude", "longitude", "quality", "rating", "matched", "county_fips"}).
				AddRow(25.77, -80.19, "rooftop", &rating, true, &countyFIPS),
		)
	// No geocode() call expected — cache hit should short-circuit.

	g := NewClient(mock, WithCacheEnabled(true))
	result, err := g.Geocode(context.Background(), AddressInput{
		Street:  "100 Main St",
		City:    "Miami",
		State:   "FL",
		ZipCode: "33131",
	})

	require.NoError(t, err)
	assert.True(t, result.Matched)
	assert.InDelta(t, 25.77, result.Latitude, 0.01)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGeocode_NegativeCacheHitSkipsPostGIS(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Negative cache entry: Matched=false.
	mock.ExpectQuery(`SELECT latitude, longitude, quality, rating, matched, county_fips FROM public.geocode_cache`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnRows(
			pgxmock.NewRows([]string{"latitude", "longitude", "quality", "rating", "matched", "county_fips"}).
				AddRow(0.0, 0.0, "", (*int)(nil), false, (*string)(nil)),
		)
	// No geocode() call expected — negative cache hit should short-circuit.

	g := NewClient(mock, WithCacheEnabled(true))
	result, err := g.Geocode(context.Background(), AddressInput{
		City:  "Nonexistent",
		State: "ZZ",
	})

	require.NoError(t, err)
	assert.False(t, result.Matched, "negative cache hit should return unmatched")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStoreCache_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec(`INSERT INTO public.geocode_cache`).
		WithArgs("hashkey", 25.77, -80.19, "rooftop", 5, true, "12086").
		WillReturnError(assert.AnError)

	g := &geocoder{pool: mock, cacheEnabled: true}
	err = g.storeCache(context.Background(), "hashkey", &Result{
		Latitude:   25.77,
		Longitude:  -80.19,
		Quality:    "rooftop",
		Rating:     5,
		Matched:    true,
		CountyFIPS: "12086",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "geocode: store cache")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGeocode_CacheDisabled(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// No cache query expected when cache is disabled.
	mock.ExpectQuery(`SELECT\s+ST_Y`).
		WithArgs("100 Main St, Miami, FL, 33131").
		WillReturnRows(
			pgxmock.NewRows([]string{"lat", "lon", "rating", "matched_address", "county_fips"}).
				AddRow(25.77, -80.19, 3, "100 Main St, Miami, FL 33131", "12086"),
		)
	// No cache store expected either.

	g := NewClient(mock, WithCacheEnabled(false))
	result, err := g.Geocode(context.Background(), AddressInput{
		Street:  "100 Main St",
		City:    "Miami",
		State:   "FL",
		ZipCode: "33131",
	})

	require.NoError(t, err)
	assert.True(t, result.Matched)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBatchGeocode_Empty(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	g := NewClient(mock, WithBatchConcurrency(5))
	results, err := g.BatchGeocode(context.Background(), nil)

	require.NoError(t, err)
	assert.Nil(t, results)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithBatchConcurrency(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	g := NewClient(mock, WithBatchConcurrency(20)).(*geocoder)
	assert.Equal(t, 20, g.batchConcurrency)

	// Zero should not change default.
	g2 := NewClient(mock, WithBatchConcurrency(0)).(*geocoder)
	assert.Equal(t, 10, g2.batchConcurrency) // default
}

func TestWithCacheTTLDays(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	g := NewClient(mock, WithCacheTTLDays(30)).(*geocoder)
	assert.Equal(t, 30, g.cacheTTLDays)
}
