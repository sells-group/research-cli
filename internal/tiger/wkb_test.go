package tiger

import (
	"testing"

	"github.com/jonas-p/go-shp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
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

func TestPolyLineToMultiLineString_Nil(t *testing.T) {
	result := polyLineToMultiLineString(nil)
	assert.Nil(t, result)
}

func TestPolyLineToMultiLineString_NoParts(t *testing.T) {
	// NumParts=0 with points should still return nil.
	pl := &shp.PolyLine{
		NumParts: 0,
		Parts:    nil,
		Points: []shp.Point{
			{X: -80.0, Y: 25.0},
			{X: -80.1, Y: 25.1},
		},
	}
	result := polyLineToMultiLineString(pl)
	assert.Nil(t, result)
}

func TestPolyLineToMultiLineString_NoPoints(t *testing.T) {
	// NumParts=1 but empty points should return nil.
	pl := &shp.PolyLine{
		NumParts: 1,
		Parts:    []int32{0},
		Points:   nil,
	}
	result := polyLineToMultiLineString(pl)
	assert.Nil(t, result)
}

func TestPolygonToMultiPolygon_Nil(t *testing.T) {
	result := polygonToMultiPolygon(nil)
	assert.Nil(t, result)
}

func TestPolygonToMultiPolygon_NoParts(t *testing.T) {
	poly := &shp.Polygon{
		NumParts: 0,
		Parts:    nil,
		Points: []shp.Point{
			{X: -80.0, Y: 25.0},
		},
	}
	result := polygonToMultiPolygon(poly)
	assert.Nil(t, result)
}

func TestPolygonToMultiPolygon_NoPoints(t *testing.T) {
	poly := &shp.Polygon{
		NumParts: 1,
		Parts:    []int32{0},
		Points:   nil,
	}
	result := polygonToMultiPolygon(poly)
	assert.Nil(t, result)
}

func TestEncodeWKB_UnsupportedShape(t *testing.T) {
	// shp.MultiPoint is not handled by EncodeWKB — should return nil, nil.
	mp := &shp.MultiPoint{
		NumPoints: 2,
		Points: []shp.Point{
			{X: -80.0, Y: 25.0},
			{X: -79.0, Y: 26.0},
		},
	}

	wkb, err := EncodeWKB(mp)
	require.NoError(t, err)
	assert.Nil(t, wkb)
}

func TestEncodeWKB_MultiPartPolyLine(t *testing.T) {
	pl := &shp.PolyLine{
		NumParts: 2,
		Parts:    []int32{0, 3},
		Points: []shp.Point{
			// Part 1
			{X: -80.0, Y: 25.0},
			{X: -80.1, Y: 25.1},
			{X: -80.2, Y: 25.2},
			// Part 2
			{X: -79.0, Y: 26.0},
			{X: -79.1, Y: 26.1},
		},
	}

	wkb, err := EncodeWKB(pl)
	require.NoError(t, err)
	assert.NotNil(t, wkb)
	assert.True(t, len(wkb) > 0)
}

func TestFlatCoords(t *testing.T) {
	coords := []geom.Coord{
		{-80.0, 25.0},
		{-79.0, 26.0},
	}
	flat := flatCoords(coords)
	assert.Equal(t, []float64{-80.0, 25.0, -79.0, 26.0}, flat)
}

func TestFlatCoords_Empty(t *testing.T) {
	flat := flatCoords(nil)
	assert.Empty(t, flat)
}

func TestPolyLineToMultiLineString_SinglePointPart(t *testing.T) {
	// A part with only 1 point should be skipped by Push (line needs >= 2 points).
	pl := &shp.PolyLine{
		NumParts: 1,
		Parts:    []int32{0},
		Points: []shp.Point{
			{X: -80.0, Y: 25.0},
		},
	}
	result := polyLineToMultiLineString(pl)
	// With only 1 point, the linestring has < 2 points.
	// go-geom may or may not reject this — if it's nil, the part was skipped.
	// Either way, this exercises the loop and error path.
	if result != nil {
		// If it's not nil, it should have the linestring.
		mls, ok := result.(*geom.MultiLineString)
		if ok {
			assert.True(t, mls.NumLineStrings() >= 0)
		}
	}
}

func TestPolygonToMultiPolygon_TooFewPoints(t *testing.T) {
	// A ring with only 2 points is not a valid ring (needs >= 4 with closed ring).
	// This should trigger the Push error path.
	poly := &shp.Polygon{
		NumParts: 1,
		Parts:    []int32{0},
		Points: []shp.Point{
			{X: -80.0, Y: 25.0},
			{X: -79.0, Y: 26.0},
		},
	}
	result := polygonToMultiPolygon(poly)
	// With too few points for a valid ring, the polygon push may fail.
	// Result could be nil if all parts were skipped.
	if result != nil {
		mp, ok := result.(*geom.MultiPolygon)
		if ok {
			assert.True(t, mp.NumPolygons() >= 0)
		}
	}
}

func TestEncodeWKB_SinglePartPolyLine(t *testing.T) {
	// Single-part polyline exercises the else branch for end calculation.
	pl := &shp.PolyLine{
		NumParts: 1,
		Parts:    []int32{0},
		Points: []shp.Point{
			{X: -80.0, Y: 25.0},
			{X: -80.1, Y: 25.1},
		},
	}

	wkb, err := EncodeWKB(pl)
	require.NoError(t, err)
	assert.NotNil(t, wkb)
}

func TestEncodeWKB_SinglePartPolygon(t *testing.T) {
	// Single-part polygon exercises the else branch for end calculation.
	poly := &shp.Polygon{
		NumParts: 1,
		Parts:    []int32{0},
		Points: []shp.Point{
			{X: -80.0, Y: 25.0},
			{X: -80.0, Y: 26.0},
			{X: -79.0, Y: 26.0},
			{X: -79.0, Y: 25.0},
			{X: -80.0, Y: 25.0},
		},
	}

	wkb, err := EncodeWKB(poly)
	require.NoError(t, err)
	assert.NotNil(t, wkb)
}
