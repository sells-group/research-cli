package tiger

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/rotisserie/eris"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
	"github.com/twpayne/go-geom/encoding/geojson"
	"go.uber.org/zap"
)

// geoJSONFeature represents a single GeoJSON feature from ogr2ogr output.
type geoJSONFeature struct {
	Type       string          `json:"type"`
	Properties json.RawMessage `json:"properties"`
	Geometry   json.RawMessage `json:"geometry"`
}

// ParseGDB converts a GDB layer to a ParseResult using ogr2ogr -> GeoJSONSeq.
// Requires ogr2ogr (gdal-bin) to be installed. The geometry column ("the_geom")
// contains EWKB bytes with SRID 4326. The -t_srs flag reprojects to EPSG:4326.
func ParseGDB(ctx context.Context, gdbPath, layerName, tempDir string) (*ParseResult, error) {
	outPath := filepath.Join(tempDir, layerName+".geojsonl")

	cmd := exec.CommandContext(ctx, "ogr2ogr", // #nosec G204 -- args from internal code paths
		"-f", "GeoJSONSeq",
		"-t_srs", "EPSG:4326",
		outPath,
		gdbPath,
		layerName,
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		return nil, eris.Wrapf(err, "tiger: ogr2ogr failed for %s/%s: %s", gdbPath, layerName, string(output))
	}

	return parseGeoJSONSeq(outPath)
}

// GDBLayerNames lists layer names in a GDB file using ogrinfo.
func GDBLayerNames(ctx context.Context, gdbPath string) ([]string, error) {
	cmd := exec.CommandContext(ctx, "ogrinfo", "-so", "-q", gdbPath) // #nosec G204 -- args from internal code paths
	output, err := cmd.Output()
	if err != nil {
		return nil, eris.Wrapf(err, "tiger: ogrinfo failed for %s", gdbPath)
	}

	var names []string
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		// ogrinfo output format: "1: LayerName (Geometry Type)"
		parts := strings.SplitN(line, ":", 2)
		if len(parts) < 2 {
			continue
		}
		name := strings.TrimSpace(parts[1])
		// Remove geometry type suffix if present: "LayerName (Multi Polygon)"
		if idx := strings.Index(name, " ("); idx > 0 {
			name = name[:idx]
		}
		if name != "" {
			names = append(names, name)
		}
	}
	return names, nil
}

// parseGeoJSONSeq reads a newline-delimited GeoJSON file and returns a ParseResult.
// Each line is a GeoJSON Feature; properties become columns and the geometry is
// encoded as EWKB in the "the_geom" column with SRID 4326.
func parseGeoJSONSeq(path string) (*ParseResult, error) {
	f, err := os.Open(path) // #nosec G304 -- path from controlled temp dir
	if err != nil {
		return nil, eris.Wrapf(err, "tiger: open geojsonseq %s", path)
	}
	defer f.Close() //nolint:errcheck

	var columns []string
	var rows [][]any
	colIndex := make(map[string]int)
	scanner := bufio.NewScanner(f)
	// Increase scanner buffer for large geometries (up to 10 MB per line).
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)

	var skipped int

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var feat geoJSONFeature
		if err := json.Unmarshal(line, &feat); err != nil {
			skipped++
			continue
		}

		var props map[string]any
		if err := json.Unmarshal(feat.Properties, &props); err != nil {
			skipped++
			continue
		}

		// On first feature, build column list from property keys.
		if columns == nil {
			for k := range props {
				colIndex[k] = len(columns)
				columns = append(columns, strings.ToLower(k))
			}
			columns = append(columns, "the_geom")
		}

		row := make([]any, len(columns))
		for k, v := range props {
			idx, ok := colIndex[k]
			if !ok {
				continue
			}
			if v == nil {
				row[idx] = nil
				continue
			}
			switch val := v.(type) {
			case string:
				if val == "" {
					row[idx] = nil
				} else {
					row[idx] = val
				}
			case float64:
				row[idx] = val
			default:
				b, _ := json.Marshal(val)
				row[idx] = string(b)
			}
		}

		if feat.Geometry == nil || string(feat.Geometry) == "null" {
			skipped++
			continue
		}

		wkbBytes, encErr := geoJSONToEWKB(feat.Geometry)
		if encErr != nil {
			skipped++
			continue
		}
		row[len(row)-1] = wkbBytes

		rows = append(rows, row)
	}

	if err := scanner.Err(); err != nil {
		return nil, eris.Wrap(err, "tiger: scan geojsonseq")
	}

	if skipped > 0 {
		zap.L().Debug("tiger: skipped GDB features", zap.Int("skipped", skipped))
	}

	return &ParseResult{Columns: columns, Rows: rows}, nil
}

// geoJSONToEWKB converts raw GeoJSON geometry bytes to EWKB with SRID 4326
// using the go-geom library for correct encoding of all geometry types.
func geoJSONToEWKB(raw json.RawMessage) ([]byte, error) {
	var g geom.T
	if err := geojson.Unmarshal(raw, &g); err != nil {
		return nil, eris.Wrap(err, "tiger: unmarshal geojson geometry")
	}

	// Set SRID to 4326 (WGS84) since ogr2ogr reprojects to EPSG:4326.
	switch t := g.(type) {
	case *geom.Point:
		t.SetSRID(4326)
	case *geom.MultiPoint:
		t.SetSRID(4326)
	case *geom.LineString:
		t.SetSRID(4326)
	case *geom.MultiLineString:
		t.SetSRID(4326)
	case *geom.Polygon:
		t.SetSRID(4326)
	case *geom.MultiPolygon:
		t.SetSRID(4326)
	case *geom.GeometryCollection:
		t.SetSRID(4326)
	}

	data, err := ewkb.Marshal(g, ewkb.NDR)
	if err != nil {
		return nil, eris.Wrap(err, "tiger: encode EWKB from geojson")
	}
	return data, nil
}
