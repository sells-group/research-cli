package tiger

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseGeoJSONSeq(t *testing.T) {
	data := `{"type":"Feature","properties":{"NAME":"Lake Travis","FTYPE":"Lake"},"geometry":{"type":"Point","coordinates":[-97.9,30.4]}}
{"type":"Feature","properties":{"NAME":"Colorado River","FTYPE":"Stream"},"geometry":{"type":"Point","coordinates":[-97.7,30.3]}}
`
	dir := t.TempDir()
	path := filepath.Join(dir, "test.geojsonl")
	require.NoError(t, os.WriteFile(path, []byte(data), 0o644))

	result, err := parseGeoJSONSeq(path)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)

	// Should have name, ftype, + the_geom columns.
	assert.Contains(t, result.Columns, "the_geom")
	assert.True(t, len(result.Columns) >= 3)
}

func TestParseGeoJSONSeq_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.geojsonl")
	require.NoError(t, os.WriteFile(path, []byte(""), 0o644))

	result, err := parseGeoJSONSeq(path)
	require.NoError(t, err)
	assert.Empty(t, result.Rows)
}

func TestParseGeoJSONSeq_NullGeometry(t *testing.T) {
	data := `{"type":"Feature","properties":{"NAME":"Bad"},"geometry":null}
`
	dir := t.TempDir()
	path := filepath.Join(dir, "null_geom.geojsonl")
	require.NoError(t, os.WriteFile(path, []byte(data), 0o644))

	result, err := parseGeoJSONSeq(path)
	require.NoError(t, err)
	assert.Empty(t, result.Rows) // Null geom should be skipped.
}

func TestParseGeoJSONSeq_InvalidJSON(t *testing.T) {
	data := `not valid json
{"type":"Feature","properties":{"NAME":"Good"},"geometry":{"type":"Point","coordinates":[-97.9,30.4]}}
`
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.geojsonl")
	require.NoError(t, os.WriteFile(path, []byte(data), 0o644))

	result, err := parseGeoJSONSeq(path)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1) // Invalid line skipped, good line kept.
}

func TestParseGeoJSONSeq_MultiPolygon(t *testing.T) {
	data := `{"type":"Feature","properties":{"NAME":"Park"},"geometry":{"type":"MultiPolygon","coordinates":[[[[-97.0,30.0],[-97.1,30.0],[-97.1,30.1],[-97.0,30.0]]]]}}
`
	dir := t.TempDir()
	path := filepath.Join(dir, "multipoly.geojsonl")
	require.NoError(t, os.WriteFile(path, []byte(data), 0o644))

	result, err := parseGeoJSONSeq(path)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	// The geometry column should be EWKB bytes.
	geomIdx := len(result.Columns) - 1
	wkb, ok := result.Rows[0][geomIdx].([]byte)
	assert.True(t, ok)
	assert.True(t, len(wkb) > 0)
}

func TestParseGeoJSONSeq_Polygon(t *testing.T) {
	data := `{"type":"Feature","properties":{"NAME":"Area"},"geometry":{"type":"Polygon","coordinates":[[[-97.0,30.0],[-97.1,30.0],[-97.1,30.1],[-97.0,30.0]]]}}
`
	dir := t.TempDir()
	path := filepath.Join(dir, "polygon.geojsonl")
	require.NoError(t, os.WriteFile(path, []byte(data), 0o644))

	result, err := parseGeoJSONSeq(path)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
}

func TestParseGeoJSONSeq_LineString(t *testing.T) {
	data := `{"type":"Feature","properties":{"NAME":"Road"},"geometry":{"type":"LineString","coordinates":[[-97.0,30.0],[-97.1,30.1]]}}
`
	dir := t.TempDir()
	path := filepath.Join(dir, "line.geojsonl")
	require.NoError(t, os.WriteFile(path, []byte(data), 0o644))

	result, err := parseGeoJSONSeq(path)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
}

func TestParseGeoJSONSeq_MultiLineString(t *testing.T) {
	data := `{"type":"Feature","properties":{"NAME":"River"},"geometry":{"type":"MultiLineString","coordinates":[[[-97.0,30.0],[-97.1,30.1]],[[-97.2,30.2],[-97.3,30.3]]]}}
`
	dir := t.TempDir()
	path := filepath.Join(dir, "multiline.geojsonl")
	require.NoError(t, os.WriteFile(path, []byte(data), 0o644))

	result, err := parseGeoJSONSeq(path)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
}

func TestParseGeoJSONSeq_FileNotFound(t *testing.T) {
	_, err := parseGeoJSONSeq("/nonexistent/file.geojsonl")
	require.Error(t, err)
}

func TestParseGeoJSONSeq_PropertyTypes(t *testing.T) {
	data := `{"type":"Feature","properties":{"NAME":"Test","COUNT":42.0,"TAGS":["a","b"],"EMPTY":"","NIL":null},"geometry":{"type":"Point","coordinates":[-97.9,30.4]}}
`
	dir := t.TempDir()
	path := filepath.Join(dir, "types.geojsonl")
	require.NoError(t, os.WriteFile(path, []byte(data), 0o644))

	result, err := parseGeoJSONSeq(path)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	// Find column indexes.
	colIdx := make(map[string]int)
	for i, c := range result.Columns {
		colIdx[c] = i
	}

	row := result.Rows[0]

	// String value preserved.
	assert.Equal(t, "Test", row[colIdx["name"]])
	// Float64 value preserved.
	assert.Equal(t, 42.0, row[colIdx["count"]])
	// Array serialized to JSON string.
	assert.Equal(t, `["a","b"]`, row[colIdx["tags"]])
	// Empty string becomes nil.
	assert.Nil(t, row[colIdx["empty"]])
	// Null becomes nil.
	assert.Nil(t, row[colIdx["nil"]])
}

func TestGeoJSONToEWKB_Point(t *testing.T) {
	raw := []byte(`{"type":"Point","coordinates":[-97.9,30.4]}`)
	wkb, err := geoJSONToEWKB(raw)
	require.NoError(t, err)
	assert.True(t, len(wkb) > 0)
	// First byte should be little-endian marker.
	assert.Equal(t, byte(0x01), wkb[0])
}

func TestGeoJSONToEWKB_InvalidJSON(t *testing.T) {
	raw := []byte(`not json`)
	_, err := geoJSONToEWKB(raw)
	require.Error(t, err)
}

func TestGDBLayerNames_MissingOgrinfo(t *testing.T) {
	// Cancelled context causes exec to fail immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := GDBLayerNames(ctx, "/nonexistent.gdb")
	require.Error(t, err)
}

func TestParseGDB_MissingOgr2ogr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := ParseGDB(ctx, "/nonexistent.gdb", "layer", t.TempDir())
	require.Error(t, err)
}

func TestParseGeoJSONSeq_Testdata(t *testing.T) {
	path := filepath.Join("testdata", "sample.geojsonl")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Skip("testdata/sample.geojsonl not found")
	}

	result, err := parseGeoJSONSeq(path)
	require.NoError(t, err)
	require.Len(t, result.Rows, 3)
	assert.Contains(t, result.Columns, "the_geom")
	assert.Contains(t, result.Columns, "name")
	assert.Contains(t, result.Columns, "ftype")
	assert.Contains(t, result.Columns, "gnis_id")
}
