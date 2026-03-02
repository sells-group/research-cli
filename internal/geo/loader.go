package geo

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/jonas-p/go-shp"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
)

const cbsaShapefileURL = "https://www2.census.gov/geo/tiger/TIGER2024/CBSA/tl_2024_us_cbsa.zip"

// ImportCBSA downloads Census Bureau CBSA shapefiles and loads into public.cbsa_areas.
func ImportCBSA(ctx context.Context, pool db.Pool, httpClient *http.Client, tempDir string) error {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	log := zap.L().With(zap.String("component", "geo.loader"))

	// Download the ZIP file.
	zipPath := filepath.Join(tempDir, "tl_2024_us_cbsa.zip")
	log.Info("downloading CBSA shapefile", zap.String("url", cbsaShapefileURL))

	if err := downloadFile(ctx, httpClient, cbsaShapefileURL, zipPath); err != nil {
		return eris.Wrap(err, "geo: download CBSA shapefile")
	}

	// Extract ZIP.
	extractDir := filepath.Join(tempDir, "cbsa")
	if err := os.MkdirAll(extractDir, 0o755); err != nil {
		return eris.Wrap(err, "geo: create extract dir")
	}
	if err := extractZIP(zipPath, extractDir); err != nil {
		return eris.Wrap(err, "geo: extract CBSA ZIP")
	}

	// Find the .shp file.
	shpPath, err := findFileByExt(extractDir, ".shp")
	if err != nil {
		return eris.Wrap(err, "geo: find .shp file")
	}

	// Open shapefile.
	reader, err := shp.Open(shpPath)
	if err != nil {
		return eris.Wrap(err, "geo: open shapefile")
	}
	defer func() { _ = reader.Close() }()

	// Find field indices.
	cbsaFPIdx := fieldIndex(reader, "CBSAFP")
	nameIdx := fieldIndex(reader, "NAME")
	lsadIdx := fieldIndex(reader, "LSAD")
	if cbsaFPIdx < 0 || nameIdx < 0 || lsadIdx < 0 {
		return eris.New("geo: required shapefile fields (CBSAFP, NAME, LSAD) not found")
	}

	// Ensure table exists.
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS public.cbsa_areas (
			cbsa_code TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			lsad TEXT NOT NULL,
			geom geometry(MultiPolygon, 4326)
		)`)
	if err != nil {
		return eris.Wrap(err, "geo: create cbsa_areas table")
	}

	// Create spatial index if not exists.
	_, err = pool.Exec(ctx, `
		CREATE INDEX IF NOT EXISTS idx_cbsa_areas_geom ON public.cbsa_areas USING gist (geom)`)
	if err != nil {
		return eris.Wrap(err, "geo: create spatial index")
	}

	// Truncate existing data.
	_, err = pool.Exec(ctx, `TRUNCATE public.cbsa_areas`)
	if err != nil {
		return eris.Wrap(err, "geo: truncate cbsa_areas")
	}

	// Load records.
	var loaded int
	for reader.Next() {
		_, shape := reader.Shape()
		if shape == nil {
			continue
		}

		cbsaCode := strings.TrimSpace(reader.Attribute(cbsaFPIdx))
		name := strings.TrimSpace(reader.Attribute(nameIdx))
		lsad := strings.TrimSpace(reader.Attribute(lsadIdx))

		if cbsaCode == "" {
			continue
		}

		wkt := shapeToWKT(shape)
		if wkt == "" {
			continue
		}

		_, err = pool.Exec(ctx, `
			INSERT INTO public.cbsa_areas (cbsa_code, name, lsad, geom)
			VALUES ($1, $2, $3, ST_GeomFromText($4, 4326))
			ON CONFLICT (cbsa_code) DO UPDATE SET
				name = EXCLUDED.name,
				lsad = EXCLUDED.lsad,
				geom = EXCLUDED.geom`,
			cbsaCode, name, lsad, wkt)
		if err != nil {
			log.Warn("geo: failed to insert CBSA record",
				zap.String("cbsa_code", cbsaCode),
				zap.Error(err),
			)
			continue
		}
		loaded++
	}

	log.Info("CBSA shapefile loaded", zap.Int("records", loaded))
	return nil
}

// downloadFile downloads a URL to a local file.
func downloadFile(ctx context.Context, client *http.Client, url, dest string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return eris.Wrap(err, "build request")
	}

	resp, err := client.Do(req)
	if err != nil {
		return eris.Wrap(err, "download")
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return eris.Errorf("download returned status %d", resp.StatusCode)
	}

	f, err := os.Create(dest)
	if err != nil {
		return eris.Wrap(err, "create file")
	}
	defer f.Close() //nolint:errcheck

	if _, err := io.Copy(f, resp.Body); err != nil {
		return eris.Wrap(err, "write file")
	}

	return nil
}

// extractZIP extracts a ZIP archive to the destination directory.
func extractZIP(zipPath, destDir string) error {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return eris.Wrap(err, "open zip")
	}
	defer r.Close() //nolint:errcheck

	for _, f := range r.File {
		name := filepath.Base(f.Name)
		destPath := filepath.Join(destDir, name)

		if f.FileInfo().IsDir() {
			continue
		}

		rc, err := f.Open()
		if err != nil {
			return eris.Wrapf(err, "open zip entry %s", f.Name)
		}

		outFile, err := os.Create(destPath)
		if err != nil {
			_ = rc.Close()
			return eris.Wrapf(err, "create %s", destPath)
		}

		if _, err := io.Copy(outFile, rc); err != nil {
			_ = outFile.Close()
			_ = rc.Close()
			return eris.Wrapf(err, "extract %s", f.Name)
		}
		_ = outFile.Close()
		_ = rc.Close()
	}

	return nil
}

// findFileByExt finds the first file with the given extension in a directory.
func findFileByExt(dir, ext string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", eris.Wrap(err, "read directory")
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(strings.ToLower(e.Name()), ext) {
			return filepath.Join(dir, e.Name()), nil
		}
	}
	return "", eris.Errorf("no %s file found in %s", ext, dir)
}

// fieldIndex returns the index of a named field in the shapefile, or -1 if not found.
func fieldIndex(reader *shp.Reader, name string) int {
	for i, f := range reader.Fields() {
		if strings.EqualFold(strings.TrimRight(f.String(), "\x00"), name) {
			return i
		}
	}
	return -1
}

// shapeToWKT converts a shapefile Shape to WKT MultiPolygon.
func shapeToWKT(s shp.Shape) string {
	switch shape := s.(type) {
	case *shp.Polygon:
		return polygonToWKT(shape)
	case *shp.PolyLine:
		// Some shapefiles use PolyLine for polygons.
		return ""
	default:
		return ""
	}
}

// polygonToWKT converts a shapefile Polygon to WKT MULTIPOLYGON.
func polygonToWKT(p *shp.Polygon) string {
	if p.NumParts == 0 || len(p.Points) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("MULTIPOLYGON(((")

	parts := make([]int32, p.NumParts)
	copy(parts, p.Parts)

	for i := int32(0); i < p.NumParts; i++ {
		if i > 0 {
			sb.WriteString(")),((")
		}
		start := parts[i]
		var end int32
		if i+1 < p.NumParts {
			end = parts[i+1]
		} else {
			end = int32(len(p.Points))
		}

		for j := start; j < end; j++ {
			if j > start {
				sb.WriteString(",")
			}
			fmt.Fprintf(&sb, "%f %f", p.Points[j].X, p.Points[j].Y)
		}
	}

	sb.WriteString(")))")
	return sb.String()
}
