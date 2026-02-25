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
	mock.ExpectQuery(`SELECT latitude, longitude, quality, rating FROM public.geocode_cache`).
		WithArgs("abc123").
		WillReturnRows(
			pgxmock.NewRows([]string{"latitude", "longitude", "quality", "rating"}).
				AddRow(25.77, -80.19, "rooftop", &rating),
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

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCheckCache_Miss(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT latitude, longitude, quality, rating FROM public.geocode_cache`).
		WithArgs("missing-key").
		WillReturnError(assert.AnError)

	g := &geocoder{pool: mock, cacheEnabled: true}
	result, err := g.checkCache(context.Background(), "missing-key")

	assert.Error(t, err)
	assert.Nil(t, result)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStoreCache(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec(`INSERT INTO public.geocode_cache`).
		WithArgs("hashkey", 25.77, -80.19, "rooftop", 5).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	g := &geocoder{pool: mock, cacheEnabled: true}
	err = g.storeCache(context.Background(), "hashkey", &Result{
		Latitude:  25.77,
		Longitude: -80.19,
		Quality:   "rooftop",
		Rating:    5,
	})

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGeocode_CacheHitSkipsPostGIS(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rating := 3
	mock.ExpectQuery(`SELECT latitude, longitude, quality, rating FROM public.geocode_cache`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnRows(
			pgxmock.NewRows([]string{"latitude", "longitude", "quality", "rating"}).
				AddRow(25.77, -80.19, "rooftop", &rating),
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

func TestGeocode_CacheDisabled(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// No cache query expected when cache is disabled.
	mock.ExpectQuery(`SELECT\s+ST_Y`).
		WithArgs("100 Main St, Miami, FL, 33131").
		WillReturnRows(
			pgxmock.NewRows([]string{"lat", "lon", "rating", "matched_address"}).
				AddRow(25.77, -80.19, 3, "100 Main St, Miami, FL 33131"),
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
