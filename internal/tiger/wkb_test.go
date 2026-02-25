package tiger

import (
	"testing"

	"github.com/jonas-p/go-shp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeWKB_Point(t *testing.T) {
	p := &shp.Point{X: -80.19, Y: 25.77}
	wkb, err := EncodeWKB(p)

	require.NoError(t, err)
	assert.NotNil(t, wkb)
	assert.True(t, len(wkb) > 0)
}

func TestEncodeWKB_Polygon(t *testing.T) {
	poly := &shp.Polygon{
		NumParts: 1,
		Parts:    []int32{0},
		Points: []shp.Point{
			{X: -80.0, Y: 25.0},
			{X: -80.0, Y: 26.0},
			{X: -79.0, Y: 26.0},
			{X: -79.0, Y: 25.0},
			{X: -80.0, Y: 25.0}, // closed ring
		},
	}

	wkb, err := EncodeWKB(poly)
	require.NoError(t, err)
	assert.NotNil(t, wkb)
	assert.True(t, len(wkb) > 0)
}

func TestEncodeWKB_PolyLine(t *testing.T) {
	pl := &shp.PolyLine{
		NumParts: 1,
		Parts:    []int32{0},
		Points: []shp.Point{
			{X: -80.0, Y: 25.0},
			{X: -80.1, Y: 25.1},
			{X: -80.2, Y: 25.2},
		},
	}

	wkb, err := EncodeWKB(pl)
	require.NoError(t, err)
	assert.NotNil(t, wkb)
}

func TestEncodeWKB_MultiPartPolygon(t *testing.T) {
	poly := &shp.Polygon{
		NumParts: 2,
		Parts:    []int32{0, 5},
		Points: []shp.Point{
			// Ring 1
			{X: -80.0, Y: 25.0},
			{X: -80.0, Y: 26.0},
			{X: -79.0, Y: 26.0},
			{X: -79.0, Y: 25.0},
			{X: -80.0, Y: 25.0},
			// Ring 2
			{X: -81.0, Y: 26.0},
			{X: -81.0, Y: 27.0},
			{X: -80.0, Y: 27.0},
			{X: -80.0, Y: 26.0},
			{X: -81.0, Y: 26.0},
		},
	}

	wkb, err := EncodeWKB(poly)
	require.NoError(t, err)
	assert.NotNil(t, wkb)
}

func TestEncodeWKB_NilShape(t *testing.T) {
	wkb, err := EncodeWKB(nil)
	require.NoError(t, err)
	assert.Nil(t, wkb)
}

func TestEncodeWKB_EmptyPolygon(t *testing.T) {
	poly := &shp.Polygon{
		NumParts: 0,
		Parts:    nil,
		Points:   nil,
	}

	wkb, err := EncodeWKB(poly)
	require.NoError(t, err)
	assert.Nil(t, wkb)
}

func TestEncodeWKB_EmptyPolyLine(t *testing.T) {
	pl := &shp.PolyLine{
		NumParts: 0,
		Parts:    nil,
		Points:   nil,
	}

	wkb, err := EncodeWKB(pl)
	require.NoError(t, err)
	assert.Nil(t, wkb)
}
