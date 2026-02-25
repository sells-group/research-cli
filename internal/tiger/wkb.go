package tiger

import (
	"github.com/jonas-p/go-shp"
	"github.com/rotisserie/eris"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
	"go.uber.org/zap"
)

// EncodeWKB converts a go-shp geometry to EWKB bytes with SRID 4326.
// Returns nil, nil for unsupported or nil shapes.
func EncodeWKB(shape shp.Shape) ([]byte, error) {
	if shape == nil {
		return nil, nil
	}

	var g geom.T

	switch s := shape.(type) {
	case *shp.Point:
		g = geom.NewPointFlat(geom.XY, []float64{s.X, s.Y}).SetSRID(4326)

	case *shp.PolyLine:
		g = polyLineToMultiLineString(s)

	case *shp.Polygon:
		g = polygonToMultiPolygon(s)

	default:
		return nil, nil
	}

	if g == nil {
		return nil, nil
	}

	data, err := ewkb.Marshal(g, ewkb.NDR)
	if err != nil {
		return nil, eris.Wrap(err, "tiger: encode WKB")
	}

	return data, nil
}

// polyLineToMultiLineString converts a shapefile PolyLine to a geom.MultiLineString.
func polyLineToMultiLineString(pl *shp.PolyLine) geom.T {
	if pl == nil || pl.NumParts == 0 || len(pl.Points) == 0 {
		return nil
	}

	mls := geom.NewMultiLineString(geom.XY).SetSRID(4326)

	for i := int32(0); i < pl.NumParts; i++ {
		start := pl.Parts[i]
		var end int32
		if i+1 < pl.NumParts {
			end = pl.Parts[i+1]
		} else {
			end = int32(len(pl.Points))
		}

		coords := make([]geom.Coord, 0, end-start)
		for j := start; j < end; j++ {
			coords = append(coords, geom.Coord{pl.Points[j].X, pl.Points[j].Y})
		}

		ls := geom.NewLineStringFlat(geom.XY, flatCoords(coords))
		if err := mls.Push(ls); err != nil {
			zap.L().Debug("tiger: skipping malformed linestring part", zap.Int32("part", i), zap.Error(err))
			continue
		}
	}

	if mls.NumLineStrings() == 0 {
		return nil
	}
	return mls
}

// polygonToMultiPolygon converts a shapefile Polygon to a geom.MultiPolygon.
func polygonToMultiPolygon(p *shp.Polygon) geom.T {
	if p == nil || p.NumParts == 0 || len(p.Points) == 0 {
		return nil
	}

	mp := geom.NewMultiPolygon(geom.XY).SetSRID(4326)

	for i := int32(0); i < p.NumParts; i++ {
		start := p.Parts[i]
		var end int32
		if i+1 < p.NumParts {
			end = p.Parts[i+1]
		} else {
			end = int32(len(p.Points))
		}

		coords := make([]geom.Coord, 0, end-start)
		for j := start; j < end; j++ {
			coords = append(coords, geom.Coord{p.Points[j].X, p.Points[j].Y})
		}

		ring := geom.NewLinearRingFlat(geom.XY, flatCoords(coords))
		poly := geom.NewPolygon(geom.XY)
		if err := poly.Push(ring); err != nil {
			zap.L().Debug("tiger: skipping malformed polygon ring", zap.Int32("part", i), zap.Error(err))
			continue
		}

		if err := mp.Push(poly); err != nil {
			zap.L().Debug("tiger: skipping malformed polygon part", zap.Int32("part", i), zap.Error(err))
			continue
		}
	}

	if mp.NumPolygons() == 0 {
		return nil
	}
	return mp
}

// flatCoords converts a slice of Coord to flat coordinate pairs for go-geom.
func flatCoords(coords []geom.Coord) []float64 {
	flat := make([]float64, 0, len(coords)*2)
	for _, c := range coords {
		flat = append(flat, c[0], c[1])
	}
	return flat
}
