package tiger

import (
	"strings"

	"github.com/jonas-p/go-shp"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"
)

// ParseShapefile reads a shapefile and returns rows suitable for COPY loading.
// Each row is []any matching product.Columns; if the product has a GeomType,
// a WKB-encoded geometry column is appended as the final element.
func ParseShapefile(shpPath string, product Product) ([][]any, error) {
	reader, err := shp.Open(shpPath)
	if err != nil {
		return nil, eris.Wrapf(err, "tiger: open shapefile %s", shpPath)
	}
	defer func() { _ = reader.Close() }()

	// Build field name → index map.
	fields := reader.Fields()
	fieldIdx := make(map[string]int, len(fields))
	for i, f := range fields {
		name := strings.TrimRight(f.String(), "\x00")
		fieldIdx[strings.ToLower(name)] = i
	}

	hasGeom := product.GeomType != ""
	var rows [][]any
	var skipped int

	for reader.Next() {
		_, shape := reader.Shape()

		row := make([]any, 0, len(product.Columns)+1)
		for _, col := range product.Columns {
			idx, ok := fieldIdx[strings.ToLower(col)]
			if !ok {
				row = append(row, nil)
				continue
			}
			val := strings.TrimRight(reader.Attribute(idx), "\x00")
			val = strings.TrimSpace(val)
			if val == "" {
				row = append(row, nil)
			} else {
				row = append(row, val)
			}
		}

		if hasGeom {
			if shape == nil {
				skipped++
				continue
			}
			wkb, encErr := EncodeWKB(shape)
			if encErr != nil {
				skipped++
				continue
			}
			if wkb == nil {
				skipped++
				continue
			}
			row = append(row, wkb)
		}

		rows = append(rows, row)
	}

	if skipped > 0 {
		zap.L().Debug("tiger: skipped shapefile records",
			zap.String("product", product.Name),
			zap.Int("skipped", skipped),
		)
	}

	return rows, nil
}
