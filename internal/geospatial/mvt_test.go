package geospatial

import (
	"context"
	"fmt"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultLayers_ExpectedEntries(t *testing.T) {
	layers := DefaultLayers()
	expected := []string{"counties", "places", "cbsa", "poi", "infrastructure", "epa_sites", "flood_zones"}
	for _, name := range expected {
		_, ok := layers[name]
		assert.True(t, ok, "expected layer %q", name)
	}
	assert.Len(t, layers, len(expected))
}

func TestDefaultLayers_ValidTables(t *testing.T) {
	for name, lc := range DefaultLayers() {
		assert.True(t, validMVTTables[lc.Table], "layer %q uses invalid table %q", name, lc.Table)
		assert.NotEmpty(t, lc.GeomColumn, "layer %q has empty geom column", name)
		assert.NotEmpty(t, lc.Columns, "layer %q has empty columns", name)
	}
}

func TestGenerateMVT_SQLStructure(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	layer := LayerConfig{
		Table:      "geo.poi",
		GeomColumn: "geom",
		Columns:    "id, name, category, subcategory",
		IsPoint:    true,
		MinZoom:    8,
		MaxZoom:    16,
	}

	tileData := []byte("mock-mvt-bytes")
	mock.ExpectQuery(`SELECT ST_AsMVT\(q, 'default', 4096, 'geom'\) FROM`).
		WithArgs(10, 512, 256).
		WillReturnRows(pgxmock.NewRows([]string{"st_asmvt"}).AddRow(tileData))

	tile, err := GenerateMVT(context.Background(), mock, layer, 10, 512, 256)
	require.NoError(t, err)
	assert.Equal(t, tileData, tile)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGenerateMVT_EmptyTile(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	layer := LayerConfig{
		Table:      "geo.counties",
		GeomColumn: "geom",
		Columns:    "id, geoid, name, state_fips, county_fips",
	}

	mock.ExpectQuery(`SELECT ST_AsMVT`).
		WithArgs(5, 10, 10).
		WillReturnRows(pgxmock.NewRows([]string{"st_asmvt"}).AddRow([]byte{}))

	tile, err := GenerateMVT(context.Background(), mock, layer, 5, 10, 10)
	require.NoError(t, err)
	assert.Empty(t, tile)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGenerateMVT_DBError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	layer := LayerConfig{
		Table:      "geo.poi",
		GeomColumn: "geom",
		Columns:    "id, name",
	}

	mock.ExpectQuery(`SELECT ST_AsMVT`).
		WithArgs(10, 0, 0).
		WillReturnError(fmt.Errorf("connection refused"))

	_, err = GenerateMVT(context.Background(), mock, layer, 10, 0, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "generate MVT")
}

func TestGenerateMVT_InvalidTable(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	layer := LayerConfig{
		Table:      "public.evil; DROP TABLE",
		GeomColumn: "geom",
		Columns:    "id",
	}

	_, err = GenerateMVT(context.Background(), mock, layer, 10, 0, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid MVT table")
}
