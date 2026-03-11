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

func TestWkbToWGS84(t *testing.T) {
	// Short data (< 9 bytes) should pass through.
	short := []byte{0x01, 0x02}
	assert.Equal(t, short, wkbToWGS84(short))

	// nil value returns nil.
	assert.Nil(t, wkbToWGS84(nil))

	// Non-byte value returns nil.
	assert.Nil(t, wkbToWGS84("not bytes"))
}
