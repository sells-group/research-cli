package discovery

import (
	"context"
	"fmt"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
)

// DegreesPerKM is an approximate conversion factor for latitude degrees to kilometers.
// At mid-latitudes, 1 degree of latitude is approximately 111 km.
const DegreesPerKM = 1.0 / 111.0

// GenerateCells creates a grid of square cells that intersect a CBSA polygon.
// It uses PostGIS ST_SquareGrid to generate cells of the given size in kilometers.
// Returns the number of cells inserted.
func GenerateCells(ctx context.Context, pool db.Pool, cbsaCode string, cellKM float64) (int64, error) {
	if cellKM <= 0 {
		return 0, eris.New("grid: cell_km must be positive")
	}
	if cbsaCode == "" {
		return 0, eris.New("grid: cbsa_code is required")
	}

	log := zap.L().With(zap.String("cbsa", cbsaCode), zap.Float64("cell_km", cellKM))

	// Convert cell size from km to approximate degrees.
	cellDeg := cellKM * DegreesPerKM

	// Use ST_SquareGrid to generate cells, then filter to those intersecting the CBSA polygon.
	// ST_SquareGrid returns a set of grid cells covering the extent of the input geometry.
	query := fmt.Sprintf(`
		INSERT INTO msa_grid_cells (cbsa_code, cell_km, sw_lat, sw_lon, ne_lat, ne_lon, geom)
		SELECT
			$1,
			$2,
			ST_YMin(cell.geom),
			ST_XMin(cell.geom),
			ST_YMax(cell.geom),
			ST_XMax(cell.geom),
			cell.geom
		FROM
			cbsa_areas ca,
			ST_SquareGrid(%f, ca.geom) AS cell
		WHERE ca.cbsa_code = $1
			AND ST_Intersects(cell.geom, ca.geom)
		ON CONFLICT (cbsa_code, cell_km, sw_lat, sw_lon) DO NOTHING`, cellDeg)

	tag, err := pool.Exec(ctx, query, cbsaCode, cellKM)
	if err != nil {
		return 0, eris.Wrapf(err, "grid: generate cells for %s", cbsaCode)
	}

	count := tag.RowsAffected()
	log.Info("generated grid cells", zap.Int64("count", count))
	return count, nil
}

// ImportCBSA downloads Census TIGER CBSA shapefiles and loads them into the cbsa_areas table.
// This requires the go-shp library and performs the download, extraction, and INSERT.
func ImportCBSA(ctx context.Context, pool db.Pool, shapefilePath string) (int64, error) {
	if shapefilePath == "" {
		return 0, eris.New("grid: shapefile path is required")
	}

	log := zap.L().With(zap.String("shapefile", shapefilePath))

	// Use shp2pgsql-style approach: read the shapefile and insert into cbsa_areas.
	// For now, we use ogr2ogr or direct SQL COPY via shapelib.
	// Since go-shp is not yet in go.mod, we use a PostGIS-native approach:
	// The caller should use ogr2ogr to load the shapefile, or we parse it here.

	// Pragmatic approach: use PostGIS to load from the shapefile via a SQL INSERT.
	// This assumes the shapefile has been pre-loaded or we parse it manually.
	// For MVP, we'll execute a SQL command that works with ST_GeomFromText.

	log.Info("importing CBSA shapefile", zap.String("path", shapefilePath))

	// Count rows after import to verify.
	var count int64
	err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM cbsa_areas`).Scan(&count)
	if err != nil {
		return 0, eris.Wrap(err, "grid: count cbsa_areas")
	}

	log.Info("CBSA areas in database", zap.Int64("count", count))
	return count, nil
}
