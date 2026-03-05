package tiger

import (
	"encoding/binary"
	"os"
	"strings"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
)

// dbfHeader is the dBASE III header (32 bytes).
type dbfHeader struct {
	Version    byte
	Date       [3]byte
	NumRecords uint32
	HeaderSize uint16
	RecordSize uint16
	_          [20]byte // reserved
}

// dbfField is a dBASE III field descriptor (32 bytes).
type dbfField struct {
	Name    [11]byte
	Type    byte
	_       [4]byte // reserved
	Length  byte
	Decimal byte
	_       [14]byte // reserved
}

// ParseDBF reads a dBASE III .dbf file and returns rows suitable for COPY loading.
// Used for non-spatial TIGER products (ADDR, FEATNAMES) that ship without .shp files.
func ParseDBF(dbfPath string, product Product) ([][]any, error) {
	f, err := os.Open(dbfPath) // #nosec G304 -- path from internal function
	if err != nil {
		return nil, eris.Wrapf(err, "tiger: open dbf %s", dbfPath)
	}
	defer f.Close() //nolint:errcheck

	// Read header.
	var hdr dbfHeader
	if err := binary.Read(f, binary.LittleEndian, &hdr); err != nil {
		return nil, eris.Wrap(err, "tiger: read dbf header")
	}

	// Read field descriptors.
	numFields := (int(hdr.HeaderSize) - 32 - 1) / 32 // -1 for terminator byte
	fields := make([]dbfField, numFields)
	for i := range numFields {
		if err := binary.Read(f, binary.LittleEndian, &fields[i]); err != nil {
			return nil, eris.Wrapf(err, "tiger: read dbf field %d", i)
		}
	}

	// Build field name → index + offset map.
	type fieldInfo struct {
		index  int
		offset int
		length int
	}
	fieldMap := make(map[string]fieldInfo, numFields)
	offset := 1 // skip delete flag byte
	for i, fld := range fields {
		name := strings.TrimRight(string(fld.Name[:]), "\x00 ")
		fieldMap[strings.ToLower(name)] = fieldInfo{
			index:  i,
			offset: offset,
			length: int(fld.Length),
		}
		offset += int(fld.Length)
	}

	// Seek to first record (past header + terminator).
	if _, err := f.Seek(int64(hdr.HeaderSize), 0); err != nil {
		return nil, eris.Wrap(err, "tiger: seek to dbf records")
	}

	// Read records.
	recBuf := make([]byte, hdr.RecordSize)
	var rows [][]any
	var skipped int

	for range hdr.NumRecords {
		if _, err := f.Read(recBuf); err != nil {
			return nil, eris.Wrap(err, "tiger: read dbf record")
		}

		// Skip deleted records.
		if recBuf[0] == 0x2A {
			skipped++
			continue
		}

		row := make([]any, 0, len(product.Columns))
		for _, col := range product.Columns {
			fi, ok := fieldMap[strings.ToLower(col)]
			if !ok {
				row = append(row, nil)
				continue
			}
			val := string(recBuf[fi.offset : fi.offset+fi.length])
			val = strings.TrimRight(val, "\x00")
			val = strings.TrimSpace(val)
			if val == "" {
				row = append(row, nil)
			} else {
				row = append(row, val)
			}
		}
		rows = append(rows, row)
	}

	if skipped > 0 {
		zap.L().Debug("tiger: skipped deleted dbf records",
			zap.String("path", dbfPath),
			zap.Int("skipped", skipped),
		)
	}

	return rows, nil
}
