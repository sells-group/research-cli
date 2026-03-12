package scraper

import (
	"encoding/binary"
	"testing"

	"github.com/jonas-p/go-shp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/tiger"
)

func TestRewriteEWKBSRID(t *testing.T) {
	// Build a real EWKB polygon via tiger.EncodeWKB (SRID 4269).
	points := []shp.Point{
		{X: -97.0, Y: 30.0},
		{X: -96.0, Y: 30.0},
		{X: -96.0, Y: 31.0},
		{X: -97.0, Y: 31.0},
		{X: -97.0, Y: 30.0},
	}
	poly := &shp.Polygon{
		Box:       shp.BBoxFromPoints(points),
		NumParts:  1,
		NumPoints: int32(len(points)),
		Parts:     []int32{0},
		Points:    points,
	}

	wkb, err := tiger.EncodeWKB(poly)
	require.NoError(t, err)
	require.NotNil(t, wkb)

	// Verify original SRID is 4269.
	require.Greater(t, len(wkb), 9)
	assert.Equal(t, byte(0x01), wkb[0], "byte order should be NDR (LE)")
	assert.Equal(t, byte(0x20), wkb[4]&0x20, "SRID flag should be set in byte[4]")
	origSRID := binary.LittleEndian.Uint32(wkb[5:9])
	assert.Equal(t, uint32(4269), origSRID)

	// Rewrite to 4326.
	rewritten := rewriteEWKBSRID(wkb, 4326)
	newSRID := binary.LittleEndian.Uint32(rewritten[5:9])
	assert.Equal(t, uint32(4326), newSRID)

	// Original should be unchanged.
	assert.Equal(t, uint32(4269), binary.LittleEndian.Uint32(wkb[5:9]))
}

func TestRewriteEWKBSRID_NonNDR(t *testing.T) {
	// Big-endian byte order (0x00) — should return unchanged.
	data := make([]byte, 16)
	data[0] = 0x00 // XDR (big-endian)
	data[4] = 0x20 // SRID flag set
	binary.BigEndian.PutUint32(data[5:9], 4269)

	result := rewriteEWKBSRID(data, 4326)
	assert.Equal(t, data, result, "non-NDR data should be returned unchanged")
}

func TestRewriteEWKBSRID_NoSRIDFlag(t *testing.T) {
	// NDR byte order but no SRID flag — should return unchanged.
	data := make([]byte, 16)
	data[0] = 0x01 // NDR (little-endian)
	data[4] = 0x03 // type byte without SRID flag (0x20 not set)
	binary.LittleEndian.PutUint32(data[5:9], 4269)

	result := rewriteEWKBSRID(data, 4326)
	assert.Equal(t, data, result, "data without SRID flag should be returned unchanged")
	// Verify SRID was NOT rewritten.
	assert.Equal(t, uint32(4269), binary.LittleEndian.Uint32(result[5:9]))
}

func TestBoundaryProperties_Empty(t *testing.T) {
	// No key-value pairs — should produce empty JSON object.
	result := boundaryProperties(nil)
	assert.Equal(t, []byte("{}"), result)
}

func TestBoundaryProperties_WithEmpty(t *testing.T) {
	// Empty values should be excluded from the output.
	result := boundaryProperties(nil,
		"mtfcc", "S1100",
		"funcstat", "",
		"aland", "12345",
		"awater", "",
	)
	// Only non-empty values should be present.
	assert.Contains(t, string(result), `"mtfcc":"S1100"`)
	assert.Contains(t, string(result), `"aland":"12345"`)
	assert.NotContains(t, string(result), "funcstat")
	assert.NotContains(t, string(result), "awater")
}

func TestWkbToWGS84(t *testing.T) {
	// Short data (< 9 bytes) should pass through.
	short := []byte{0x01, 0x02}
	assert.Equal(t, short, wkbToWGS84(short))

	// nil value returns nil.
	assert.Nil(t, wkbToWGS84(nil))

	// Non-byte value returns nil.
	assert.Nil(t, wkbToWGS84("not bytes"))
}

func TestRewriteEWKBSRID_ShortData(t *testing.T) {
	// Data shorter than 9 bytes should be returned unchanged.
	short := []byte{0x01, 0x02, 0x03}
	assert.Equal(t, short, rewriteEWKBSRID(short, 4326))
}

func TestBoundaryProperties_OddPairs(t *testing.T) {
	// Odd number of kv pairs — last key without a value is ignored.
	result := boundaryProperties(nil, "key1", "val1", "orphan")
	assert.Contains(t, string(result), `"key1":"val1"`)
	assert.NotContains(t, string(result), "orphan")
}

func TestParseLatLon_Empty(t *testing.T) {
	raw := []any{"", ""}
	lat, lon := parseLatLon(raw, 0, 1)
	assert.Equal(t, 0.0, lat)
	assert.Equal(t, 0.0, lon)
}

func TestParseInt64Val_Empty(t *testing.T) {
	raw := []any{""}
	assert.Nil(t, parseInt64Val(raw, 0))
}

func TestParseInt64Val_Valid(t *testing.T) {
	raw := []any{"12345"}
	result := parseInt64Val(raw, 0)
	require.NotNil(t, result)
	assert.Equal(t, int64(12345), *result)
}

func TestParseInt64Val_OutOfBounds(t *testing.T) {
	raw := []any{"12345"}
	assert.Nil(t, parseInt64Val(raw, 5))
}

func TestTigerURL_Override(t *testing.T) {
	url := tigerURL("http://test.local", 2024, "COUNTY/tl.zip")
	assert.Equal(t, "http://test.local/COUNTY/tl.zip", url)
}

func TestNewLinearWaterRow_EmptyLinearID(t *testing.T) {
	raw := []any{
		"01234567",         // ansicode
		"",                 // linearid (empty — triggers fallback)
		"Unnamed Stream",   // fullname
		"H3010",            // mtfcc
		[]byte{0x01, 0x02}, // wkb
	}

	row := newLinearWaterRow(raw)
	assert.Equal(t, "Unnamed Stream", row[0])
	// Fallback source_id uses ansicode instead of linearid.
	assert.Equal(t, "tiger/lw/01234567", row[7])
}

func TestNewZCTARow_ShortGeoid(t *testing.T) {
	raw := []any{
		"78701",            // zcta5ce20
		"7",                // geoid20 (short, less than 2 chars)
		"B5",               // classfp20
		"G6350",            // mtfcc20
		"S",                // funcstat20
		"2000000",          // aland20
		"50000",            // awater20
		"30.2672",          // intptlat20
		"-97.7431",         // intptlon20
		[]byte{0x01, 0x02}, // wkb
	}

	row := newZCTARow(raw)
	assert.Equal(t, "78701", row[0]) // zcta5
	assert.Equal(t, "", row[1])      // state_fips (geoid too short)
}
