package tiger

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/jonas-p/go-shp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildDBF constructs a minimal DBF file in memory.
// fields: each entry is {name, type byte, length byte}.
// records: each entry is a slice of field values (strings padded to field length).
// deleted: indices of records that should be marked as deleted (0x2A).
func buildDBF(fields []struct {
	name   string
	typ    byte
	length byte
}, records [][]string, deleted map[int]bool) []byte {
	numFields := len(fields)
	// Header: 32 bytes + 32 bytes per field + 1 byte terminator (0x0D).
	headerSize := 32 + numFields*32 + 1

	// Compute record size: 1 byte deletion flag + sum of field lengths.
	recordSize := 1
	for _, f := range fields {
		recordSize += int(f.length)
	}

	numRecords := len(records)

	// Build the header.
	hdr := make([]byte, 32)
	hdr[0] = 0x03 // version: dBASE III
	hdr[1] = 26   // year (YY)
	hdr[2] = 1    // month
	hdr[3] = 1    // day
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(numRecords))
	binary.LittleEndian.PutUint16(hdr[8:10], uint16(headerSize))
	binary.LittleEndian.PutUint16(hdr[10:12], uint16(recordSize))
	// Remaining header bytes are zero (padding).

	var buf []byte
	buf = append(buf, hdr...)

	// Field descriptors (32 bytes each).
	for _, f := range fields {
		var desc [32]byte
		// Name: up to 11 bytes, null-padded.
		copy(desc[0:11], f.name)
		desc[11] = f.typ // field type (C=Character, N=Numeric, etc.)
		// Bytes 12-15: reserved.
		desc[16] = f.length // field length
		// Rest is zero padding.
		buf = append(buf, desc[:]...)
	}

	// Terminator byte.
	buf = append(buf, 0x0D)

	// Pad to headerSize if needed (should already be correct).
	for len(buf) < headerSize {
		buf = append(buf, 0x00)
	}

	// Records.
	for i, rec := range records {
		recBuf := make([]byte, recordSize)
		if deleted[i] {
			recBuf[0] = 0x2A // deleted
		} else {
			recBuf[0] = 0x20 // active
		}
		offset := 1
		for j, f := range fields {
			val := ""
			if j < len(rec) {
				val = rec[j]
			}
			// Right-pad with spaces to field length.
			padded := val
			for len(padded) < int(f.length) {
				padded += " "
			}
			copy(recBuf[offset:offset+int(f.length)], padded[:f.length])
			offset += int(f.length)
		}
		buf = append(buf, recBuf...)
	}

	return buf
}

func TestParseDBF_Basic(t *testing.T) {
	fields := []struct {
		name   string
		typ    byte
		length byte
	}{
		{name: "STATEFP", typ: 'C', length: 2},
		{name: "NAME", typ: 'C', length: 10},
	}
	records := [][]string{
		{"48", "Texas"},
		{"12", "Florida"},
	}

	data := buildDBF(fields, records, nil)

	dir := t.TempDir()
	dbfPath := filepath.Join(dir, "test.dbf")
	require.NoError(t, os.WriteFile(dbfPath, data, 0o644))

	result, err := parseDBF(dbfPath, Product{Name: "TEST"})
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, []string{"statefp", "name"}, result.Columns)
	require.Len(t, result.Rows, 2)

	assert.Equal(t, "48", result.Rows[0][0])
	assert.Equal(t, "Texas", result.Rows[0][1])
	assert.Equal(t, "12", result.Rows[1][0])
	assert.Equal(t, "Florida", result.Rows[1][1])
}

func TestParseDBF_DeletedRecord(t *testing.T) {
	fields := []struct {
		name   string
		typ    byte
		length byte
	}{
		{name: "TLID", typ: 'N', length: 10},
		{name: "FULLNAME", typ: 'C', length: 15},
	}
	records := [][]string{
		{"100", "Main St"},
		{"200", "Oak Ave"}, // will be deleted
		{"300", "Elm Blvd"},
	}
	deleted := map[int]bool{1: true}

	data := buildDBF(fields, records, deleted)

	dir := t.TempDir()
	dbfPath := filepath.Join(dir, "test.dbf")
	require.NoError(t, os.WriteFile(dbfPath, data, 0o644))

	result, err := parseDBF(dbfPath, Product{Name: "TEST"})
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, []string{"tlid", "fullname"}, result.Columns)
	require.Len(t, result.Rows, 2, "deleted record should be skipped")

	assert.Equal(t, "100", result.Rows[0][0])
	assert.Equal(t, "Main St", result.Rows[0][1])
	assert.Equal(t, "300", result.Rows[1][0])
	assert.Equal(t, "Elm Blvd", result.Rows[1][1])
}

func TestParseDBF_EmptyValues(t *testing.T) {
	fields := []struct {
		name   string
		typ    byte
		length byte
	}{
		{name: "STATEFP", typ: 'C', length: 2},
		{name: "ZIP", typ: 'C', length: 5},
	}
	// Second field is all spaces (empty).
	records := [][]string{
		{"48", "     "},
		{"12", "33101"},
	}

	data := buildDBF(fields, records, nil)

	dir := t.TempDir()
	dbfPath := filepath.Join(dir, "test.dbf")
	require.NoError(t, os.WriteFile(dbfPath, data, 0o644))

	result, err := parseDBF(dbfPath, Product{Name: "TEST"})
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)

	// All-spaces field should become nil.
	assert.Nil(t, result.Rows[0][1], "all-space field should be nil")
	assert.Equal(t, "33101", result.Rows[1][1])
}

func TestParseShapefile_TabularProduct(t *testing.T) {
	// Tabular products (GeomType="") should route to the DBF parser.
	fields := []struct {
		name   string
		typ    byte
		length byte
	}{
		{name: "TLID", typ: 'N', length: 10},
		{name: "FROMHN", typ: 'C', length: 12},
		{name: "TOHN", typ: 'C', length: 12},
		{name: "ZIP", typ: 'C', length: 5},
	}
	records := [][]string{
		{"123456", "100", "198", "33101"},
		{"789012", "200", "298", "33102"},
	}

	data := buildDBF(fields, records, nil)

	dir := t.TempDir()
	dbfPath := filepath.Join(dir, "tl_2024_12086_addr.dbf")
	require.NoError(t, os.WriteFile(dbfPath, data, 0o644))

	// ADDR has GeomType="" (tabular product).
	product := Product{Name: "ADDR", Table: "addr", PerCounty: true, GeomType: ""}

	result, err := ParseShapefile(dbfPath, product)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, []string{"tlid", "fromhn", "tohn", "zip"}, result.Columns)
	require.Len(t, result.Rows, 2)
	assert.Equal(t, "123456", result.Rows[0][0])
	assert.Equal(t, "33101", result.Rows[0][3])
}

func TestParseShapefile_TabularProduct_ShpExtension(t *testing.T) {
	// When called with a .shp path for a tabular product, it should convert to .dbf.
	fields := []struct {
		name   string
		typ    byte
		length byte
	}{
		{name: "TLID", typ: 'N', length: 10},
		{name: "FULLNAME", typ: 'C', length: 15},
	}
	records := [][]string{
		{"111", "Maple Rd"},
	}

	data := buildDBF(fields, records, nil)

	dir := t.TempDir()
	dbfPath := filepath.Join(dir, "test.dbf")
	require.NoError(t, os.WriteFile(dbfPath, data, 0o644))

	// Call ParseShapefile with .shp extension, product is tabular.
	shpPath := filepath.Join(dir, "test.shp")
	product := Product{Name: "FEATNAMES", Table: "featnames", GeomType: ""}

	result, err := ParseShapefile(shpPath, product)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, []string{"tlid", "fullname"}, result.Columns)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "111", result.Rows[0][0])
	assert.Equal(t, "Maple Rd", result.Rows[0][1])
}

func TestParseDBF_FileNotFound(t *testing.T) {
	_, err := parseDBF("/nonexistent/path/test.dbf", Product{Name: "TEST"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open DBF")
}

// createTestShapefile writes a shapefile using go-shp and fixes the DBF naming
// quirk (go-shp creates "{base}dbf" instead of "{base}.dbf").
func createTestShapefile(t *testing.T, dir, name string, shapeType shp.ShapeType, fields []shp.Field, shapes []shp.Shape, attrs [][]string) string {
	t.Helper()
	shpPath := filepath.Join(dir, name+".shp")

	w, err := shp.Create(shpPath, shapeType)
	require.NoError(t, err)

	err = w.SetFields(fields)
	require.NoError(t, err)

	for i, shape := range shapes {
		n := w.Write(shape)
		if i < len(attrs) {
			for j, val := range attrs[i] {
				if val != "" {
					require.NoError(t, w.WriteAttribute(int(n), j, val))
				}
			}
		}
	}

	w.Close()

	// Fix go-shp DBF naming: it creates "{base}dbf" instead of "{base}.dbf".
	badDBF := filepath.Join(dir, name+"dbf")
	goodDBF := filepath.Join(dir, name+".dbf")
	if _, statErr := os.Stat(badDBF); statErr == nil {
		require.NoError(t, os.Rename(badDBF, goodDBF))
	}

	return shpPath
}

// makePolygon creates a shp.Polygon with NumPoints properly set.
func makePolygon(parts []int32, points []shp.Point) *shp.Polygon {
	return &shp.Polygon{
		NumParts:  int32(len(parts)),
		NumPoints: int32(len(points)),
		Parts:     parts,
		Points:    points,
	}
}

func TestParseSHP_Polygon(t *testing.T) {
	// Create a real shapefile with a polygon geometry using go-shp writer.
	dir := t.TempDir()

	poly := makePolygon([]int32{0}, []shp.Point{
		{X: -80.0, Y: 25.0},
		{X: -80.0, Y: 26.0},
		{X: -79.0, Y: 26.0},
		{X: -79.0, Y: 25.0},
		{X: -80.0, Y: 25.0}, // closed ring
	})

	shpPath := createTestShapefile(t, dir, "test", shp.POLYGON,
		[]shp.Field{
			shp.StringField("STATEFP", 2),
			shp.StringField("NAME", 20),
		},
		[]shp.Shape{poly},
		[][]string{{"12", "Florida"}},
	)

	product := Product{Name: "STATE", Table: "state_all", GeomType: "MULTIPOLYGON"}
	result, err := parseSHP(shpPath, product)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Columns should include statefp, name, and the_geom.
	assert.Contains(t, result.Columns, "statefp")
	assert.Contains(t, result.Columns, "name")
	assert.Contains(t, result.Columns, "the_geom")

	require.Len(t, result.Rows, 1)

	// Find statefp and name in the row.
	colIdx := make(map[string]int)
	for i, col := range result.Columns {
		colIdx[col] = i
	}
	assert.Equal(t, "12", result.Rows[0][colIdx["statefp"]])
	assert.Equal(t, "Florida", result.Rows[0][colIdx["name"]])

	// the_geom should be non-nil WKB bytes.
	geomVal := result.Rows[0][colIdx["the_geom"]]
	assert.NotNil(t, geomVal)
	geomBytes, ok := geomVal.([]byte)
	assert.True(t, ok)
	assert.True(t, len(geomBytes) > 0)
}

func TestParseSHP_MultipleRows(t *testing.T) {
	dir := t.TempDir()

	makeRect := func(x, y float64) *shp.Polygon {
		pts := []shp.Point{
			{X: x, Y: y},
			{X: x, Y: y + 1},
			{X: x + 1, Y: y + 1},
			{X: x + 1, Y: y},
			{X: x, Y: y},
		}
		return makePolygon([]int32{0}, pts)
	}

	shpPath := createTestShapefile(t, dir, "test", shp.POLYGON,
		[]shp.Field{
			shp.StringField("STATEFP", 2),
			shp.StringField("COUNTYFP", 3),
			shp.StringField("NAME", 20),
		},
		[]shp.Shape{makeRect(-80, 25), makeRect(-81, 26)},
		[][]string{
			{"12", "086", "Miami-Dade"},
			{"12", "011", "Broward"},
		},
	)

	product := Product{Name: "COUNTY", Table: "county_all", GeomType: "MULTIPOLYGON"}
	result, err := parseSHP(shpPath, product)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Len(t, result.Rows, 2)

	colIdx := make(map[string]int)
	for i, col := range result.Columns {
		colIdx[col] = i
	}

	assert.Equal(t, "12", result.Rows[0][colIdx["statefp"]])
	assert.Equal(t, "086", result.Rows[0][colIdx["countyfp"]])
	assert.Equal(t, "Miami-Dade", result.Rows[0][colIdx["name"]])
	assert.Equal(t, "12", result.Rows[1][colIdx["statefp"]])
	assert.Equal(t, "011", result.Rows[1][colIdx["countyfp"]])
	assert.Equal(t, "Broward", result.Rows[1][colIdx["name"]])
}

func TestParseSHP_PolyLine(t *testing.T) {
	dir := t.TempDir()

	plPts := []shp.Point{
		{X: -80.0, Y: 25.0},
		{X: -80.1, Y: 25.1},
		{X: -80.2, Y: 25.2},
	}
	pl := &shp.PolyLine{
		NumParts:  1,
		NumPoints: int32(len(plPts)),
		Parts:     []int32{0},
		Points:    plPts,
	}

	shpPath := createTestShapefile(t, dir, "test", shp.POLYLINE,
		[]shp.Field{
			shp.StringField("TLID", 10),
			shp.StringField("FULLNAME", 20),
		},
		[]shp.Shape{pl},
		[][]string{{"999", "Main St"}},
	)

	product := Product{Name: "EDGES", Table: "edges", GeomType: "MULTILINESTRING"}
	result, err := parseSHP(shpPath, product)
	require.NoError(t, err)
	require.NotNil(t, result)

	colIdx := make(map[string]int)
	for i, col := range result.Columns {
		colIdx[col] = i
	}

	require.Len(t, result.Rows, 1)
	assert.Equal(t, "999", result.Rows[0][colIdx["tlid"]])
	assert.Equal(t, "Main St", result.Rows[0][colIdx["fullname"]])
	assert.NotNil(t, result.Rows[0][colIdx["the_geom"]])
}

func TestParseSHP_EmptyField(t *testing.T) {
	// Test that empty/whitespace-only fields become nil.
	dir := t.TempDir()

	poly := makePolygon([]int32{0}, []shp.Point{
		{X: -80.0, Y: 25.0},
		{X: -80.0, Y: 26.0},
		{X: -79.0, Y: 26.0},
		{X: -79.0, Y: 25.0},
		{X: -80.0, Y: 25.0},
	})

	shpPath := createTestShapefile(t, dir, "test", shp.POLYGON,
		[]shp.Field{
			shp.StringField("STATEFP", 2),
			shp.StringField("ZIP", 5),
		},
		[]shp.Shape{poly},
		[][]string{{"12", ""}}, // empty ZIP
	)

	product := Product{Name: "TEST", Table: "test", GeomType: "MULTIPOLYGON"}
	result, err := parseSHP(shpPath, product)
	require.NoError(t, err)

	colIdx := make(map[string]int)
	for i, col := range result.Columns {
		colIdx[col] = i
	}

	require.Len(t, result.Rows, 1)
	assert.Equal(t, "12", result.Rows[0][colIdx["statefp"]])
	assert.Nil(t, result.Rows[0][colIdx["zip"]], "empty field should be nil")
}

func TestParseSHP_FileNotFound(t *testing.T) {
	_, err := parseSHP("/nonexistent/path/test.shp", Product{Name: "TEST", GeomType: "MULTIPOLYGON"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open shapefile")
}

func TestParseShapefile_SpatialProduct_ShpPath(t *testing.T) {
	// When a spatial product is given a .shp path, it should use parseSHP.
	dir := t.TempDir()

	poly := makePolygon([]int32{0}, []shp.Point{
		{X: -80.0, Y: 25.0},
		{X: -80.0, Y: 26.0},
		{X: -79.0, Y: 26.0},
		{X: -79.0, Y: 25.0},
		{X: -80.0, Y: 25.0},
	})

	shpPath := createTestShapefile(t, dir, "test", shp.POLYGON,
		[]shp.Field{
			shp.StringField("STATEFP", 2),
		},
		[]shp.Shape{poly},
		[][]string{{"12"}},
	)

	product := Product{Name: "STATE", Table: "state_all", GeomType: "MULTIPOLYGON"}
	result, err := ParseShapefile(shpPath, product)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Contains(t, result.Columns, "the_geom")
	require.Len(t, result.Rows, 1)
}

func TestParseShapefile_SpatialProduct_DBFPath(t *testing.T) {
	// When a spatial product (GeomType != "") is given a .dbf path,
	// ParseShapefile should use the DBF reader instead of the SHP reader.
	fields := []struct {
		name   string
		typ    byte
		length byte
	}{
		{name: "STATEFP", typ: 'C', length: 2},
		{name: "COUNTYFP", typ: 'C', length: 3},
		{name: "NAME", typ: 'C', length: 10},
	}
	records := [][]string{
		{"48", "201", "Harris"},
		{"48", "113", "Dallas"},
	}

	data := buildDBF(fields, records, nil)

	dir := t.TempDir()
	dbfPath := filepath.Join(dir, "tl_2024_us_county.dbf")
	require.NoError(t, os.WriteFile(dbfPath, data, 0o644))

	// COUNTY has GeomType="MULTIPOLYGON" — spatial product.
	product := Product{Name: "COUNTY", Table: "county_all", GeomType: "MULTIPOLYGON"}

	result, err := ParseShapefile(dbfPath, product)
	require.NoError(t, err)
	require.NotNil(t, result)

	// DBF reader does not produce geometry; columns should be attribute-only.
	assert.Equal(t, []string{"statefp", "countyfp", "name"}, result.Columns)
	require.Len(t, result.Rows, 2)
	assert.Equal(t, "48", result.Rows[0][0])
	assert.Equal(t, "201", result.Rows[0][1])
	assert.Equal(t, "Harris", result.Rows[0][2])
}
