package tiger

import (
	"encoding/binary"
	"os"
	"strings"

	"github.com/jonas-p/go-shp"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"
)

// ParseShapefile reads a shapefile (.shp) or DBF file (.dbf) and returns a ParseResult
// containing all columns found in the file plus rows of data.
// If the product has a GeomType, a WKB-encoded geometry column ("the_geom") is appended.
// For products without geometry (ADDR, FEATNAMES), always uses the DBF reader.
func ParseShapefile(path string, product Product) (*ParseResult, error) {
	if product.GeomType == "" {
		// Tabular product — always use DBF reader, find the .dbf file.
		dbfPath := path
		if !strings.HasSuffix(strings.ToLower(path), ".dbf") {
			base := strings.TrimSuffix(path, ".shp")
			dbfPath = base + ".dbf"
		}
		return parseDBF(dbfPath, product)
	}
	if strings.HasSuffix(strings.ToLower(path), ".dbf") {
		return parseDBF(path, product)
	}
	return parseSHP(path, product)
}

// parseSHP reads a shapefile with geometry, extracting all columns.
func parseSHP(shpPath string, product Product) (*ParseResult, error) {
	reader, err := shp.Open(shpPath)
	if err != nil {
		return nil, eris.Wrapf(err, "tiger: open shapefile %s", shpPath)
	}
	defer func() { _ = reader.Close() }()

	// Read all field names from the shapefile header.
	fields := reader.Fields()
	columns := make([]string, len(fields))
	for i, f := range fields {
		name := strings.TrimRight(f.String(), "\x00")
		columns[i] = strings.ToLower(strings.TrimSpace(name))
	}

	hasGeom := product.GeomType != ""
	var rows [][]any
	var skipped int

	// If geometry, append "the_geom" to columns.
	resultCols := make([]string, len(columns))
	copy(resultCols, columns)
	if hasGeom {
		resultCols = append(resultCols, "the_geom")
	}

	for reader.Next() {
		_, shape := reader.Shape()

		row := make([]any, 0, len(columns)+1)
		for i := range columns {
			val := strings.TrimRight(reader.Attribute(i), "\x00")
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

	return &ParseResult{Columns: resultCols, Rows: rows}, nil
}

// parseDBF reads a standalone DBF file, extracting all columns.
func parseDBF(dbfPath string, _ Product) (*ParseResult, error) {
	f, err := os.Open(dbfPath) // #nosec G304 -- path from internal census data download
	if err != nil {
		return nil, eris.Wrapf(err, "tiger: open DBF %s", dbfPath)
	}
	defer f.Close() //nolint:errcheck

	// Read DBF header (32 bytes).
	var header [32]byte
	if _, err := f.Read(header[:]); err != nil {
		return nil, eris.Wrap(err, "tiger: read DBF header")
	}
	numRecords := binary.LittleEndian.Uint32(header[4:8])
	headerSize := binary.LittleEndian.Uint16(header[8:10])
	recordSize := binary.LittleEndian.Uint16(header[10:12])

	// Read field descriptors. Each is 32 bytes, terminated by 0x0D.
	type dbfField struct {
		name   string
		offset int // byte offset within record (after deletion flag)
		length int
	}
	var fields []dbfField
	offset := 0
	for {
		var desc [32]byte
		if _, err := f.Read(desc[:]); err != nil {
			return nil, eris.Wrap(err, "tiger: read DBF field descriptor")
		}
		if desc[0] == 0x0D {
			break
		}
		name := strings.TrimRight(string(desc[0:11]), "\x00")
		length := int(desc[16])
		fields = append(fields, dbfField{
			name:   strings.ToLower(strings.TrimSpace(name)),
			offset: offset,
			length: length,
		})
		offset += length
	}

	// Build column list from all fields.
	columns := make([]string, len(fields))
	for i, fld := range fields {
		columns[i] = fld.name
	}

	// Seek to start of records.
	if _, err := f.Seek(int64(headerSize), 0); err != nil {
		return nil, eris.Wrap(err, "tiger: seek to DBF records")
	}

	// Read records.
	recBuf := make([]byte, recordSize)
	rows := make([][]any, 0, numRecords)
	for i := uint32(0); i < numRecords; i++ {
		if _, err := f.Read(recBuf); err != nil {
			return nil, eris.Wrapf(err, "tiger: read DBF record %d", i)
		}
		// First byte is deletion flag (0x20 = active, 0x2A = deleted).
		if recBuf[0] == 0x2A {
			continue
		}

		row := make([]any, len(fields))
		for j, fld := range fields {
			// +1 to skip the deletion flag byte.
			val := string(recBuf[1+fld.offset : 1+fld.offset+fld.length])
			val = strings.TrimSpace(val)
			if val == "" {
				row[j] = nil
			} else {
				row[j] = val
			}
		}
		rows = append(rows, row)
	}

	return &ParseResult{Columns: columns, Rows: rows}, nil
}
