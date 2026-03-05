package tiger

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestDBF writes a minimal dBASE III file with the given fields and records.
func createTestDBF(t *testing.T, dir string, fields []dbfField, records [][]byte) string {
	t.Helper()
	path := filepath.Join(dir, "test.dbf")
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close() //nolint:errcheck

	// Calculate sizes.
	headerSize := uint16(32 + len(fields)*32 + 1) // header + fields + terminator
	var recordSize uint16 = 1                     // delete flag
	for _, fld := range fields {
		recordSize += uint16(fld.Length)
	}

	// Write header.
	hdr := dbfHeader{
		Version:    3,
		NumRecords: uint32(len(records)),
		HeaderSize: headerSize,
		RecordSize: recordSize,
	}
	require.NoError(t, binary.Write(f, binary.LittleEndian, &hdr))

	// Write field descriptors.
	for _, fld := range fields {
		require.NoError(t, binary.Write(f, binary.LittleEndian, &fld))
	}

	// Write terminator.
	_, err = f.Write([]byte{0x0D})
	require.NoError(t, err)

	// Write records.
	for _, rec := range records {
		_, err := f.Write(rec)
		require.NoError(t, err)
	}

	return path
}

func makeField(name string, length byte) dbfField {
	var f dbfField
	copy(f.Name[:], name)
	f.Type = 'C'
	f.Length = length
	return f
}

func makeRecord(delFlag byte, values ...string) []byte {
	rec := []byte{delFlag}
	for _, v := range values {
		rec = append(rec, []byte(v)...)
	}
	return rec
}

func TestParseDBF_Basic(t *testing.T) {
	dir := t.TempDir()

	fields := []dbfField{
		makeField("TLID", 10),
		makeField("FROMHN", 6),
		makeField("TOHN", 6),
		makeField("ZIP", 5),
		makeField("STATEFP", 2),
	}

	records := [][]byte{
		makeRecord(0x20, "1234567890", "100   ", "200   ", "12345", "01"),
		makeRecord(0x20, "9876543210", "300   ", "400   ", "67890", "02"),
		makeRecord(0x2A, "DELETED   ", "000   ", "000   ", "00000", "99"), // deleted
	}

	path := createTestDBF(t, dir, fields, records)

	product := Product{
		Name:    "ADDR",
		Table:   "addr",
		Columns: []string{"tlid", "fromhn", "tohn", "zip", "statefp"},
	}

	rows, err := ParseDBF(path, product)
	require.NoError(t, err)
	assert.Equal(t, 2, len(rows)) // deleted record skipped

	// First row.
	assert.Equal(t, "1234567890", rows[0][0])
	assert.Equal(t, "100", rows[0][1])
	assert.Equal(t, "200", rows[0][2])
	assert.Equal(t, "12345", rows[0][3])
	assert.Equal(t, "01", rows[0][4])

	// Second row.
	assert.Equal(t, "9876543210", rows[1][0])
	assert.Equal(t, "300", rows[1][1])
	assert.Equal(t, "400", rows[1][2])
	assert.Equal(t, "67890", rows[1][3])
	assert.Equal(t, "02", rows[1][4])
}

func TestParseDBF_MissingColumn(t *testing.T) {
	dir := t.TempDir()

	fields := []dbfField{
		makeField("TLID", 10),
		makeField("ZIP", 5),
	}

	records := [][]byte{
		makeRecord(0x20, "1234567890", "12345"),
	}

	path := createTestDBF(t, dir, fields, records)

	product := Product{
		Name:    "ADDR",
		Table:   "addr",
		Columns: []string{"tlid", "missing_col", "zip"},
	}

	rows, err := ParseDBF(path, product)
	require.NoError(t, err)
	assert.Equal(t, 1, len(rows))
	assert.Equal(t, "1234567890", rows[0][0])
	assert.Nil(t, rows[0][1]) // missing column → nil
	assert.Equal(t, "12345", rows[0][2])
}

func TestParseDBF_EmptyValues(t *testing.T) {
	dir := t.TempDir()

	fields := []dbfField{
		makeField("TLID", 10),
		makeField("ZIP", 5),
	}

	records := [][]byte{
		makeRecord(0x20, "1234567890", "     "), // empty ZIP
	}

	path := createTestDBF(t, dir, fields, records)

	product := Product{
		Name:    "ADDR",
		Table:   "addr",
		Columns: []string{"tlid", "zip"},
	}

	rows, err := ParseDBF(path, product)
	require.NoError(t, err)
	assert.Equal(t, 1, len(rows))
	assert.Equal(t, "1234567890", rows[0][0])
	assert.Nil(t, rows[0][1]) // empty → nil
}
