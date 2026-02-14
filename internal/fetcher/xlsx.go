package fetcher

import (
	"context"

	"github.com/rotisserie/eris"
	"github.com/tealeg/xlsx/v2"
)

// XLSXOptions configures the XLSX parser.
type XLSXOptions struct {
	SheetIndex int            // default 0
	SheetName  string         // if set, overrides SheetIndex
	SkipRows   int            // number of header rows to skip
	HeaderCh   chan<- []string // optional: receives the first row
}

// ReadXLSX reads an XLSX file and returns all rows as string slices.
func ReadXLSX(path string, opts XLSXOptions) ([][]string, error) {
	f, err := xlsx.OpenFile(path)
	if err != nil {
		return nil, eris.Wrap(err, "xlsx: open file")
	}

	sheet, err := getSheet(f, opts)
	if err != nil {
		return nil, err
	}

	var rows [][]string
	for i, row := range sheet.Rows {
		cells := rowToStrings(row)

		if i == 0 && opts.HeaderCh != nil {
			opts.HeaderCh <- cells
		}

		if i < opts.SkipRows {
			continue
		}

		rows = append(rows, cells)
	}

	return rows, nil
}

// StreamXLSX reads an XLSX file and sends rows to a channel.
// Both channels are closed when processing completes.
func StreamXLSX(ctx context.Context, path string, opts XLSXOptions) (<-chan []string, <-chan error) {
	rowCh := make(chan []string, 64)
	errCh := make(chan error, 1)

	go func() {
		defer close(rowCh)
		defer close(errCh)

		f, err := xlsx.OpenFile(path)
		if err != nil {
			errCh <- eris.Wrap(err, "xlsx: open file")
			return
		}

		sheet, err := getSheet(f, opts)
		if err != nil {
			errCh <- err
			return
		}

		for i, row := range sheet.Rows {
			if ctx.Err() != nil {
				errCh <- eris.Wrap(ctx.Err(), "xlsx: context cancelled")
				return
			}

			cells := rowToStrings(row)

			if i == 0 && opts.HeaderCh != nil {
				select {
				case opts.HeaderCh <- cells:
				case <-ctx.Done():
					errCh <- eris.Wrap(ctx.Err(), "xlsx: context cancelled sending header")
					return
				}
			}

			if i < opts.SkipRows {
				continue
			}

			select {
			case rowCh <- cells:
			case <-ctx.Done():
				errCh <- eris.Wrap(ctx.Err(), "xlsx: context cancelled")
				return
			}
		}
	}()

	return rowCh, errCh
}

func getSheet(f *xlsx.File, opts XLSXOptions) (*xlsx.Sheet, error) {
	if opts.SheetName != "" {
		sheet, ok := f.Sheet[opts.SheetName]
		if !ok {
			return nil, eris.Errorf("xlsx: sheet %q not found", opts.SheetName)
		}
		return sheet, nil
	}

	if opts.SheetIndex >= len(f.Sheets) {
		return nil, eris.Errorf("xlsx: sheet index %d out of range (file has %d sheets)", opts.SheetIndex, len(f.Sheets))
	}

	return f.Sheets[opts.SheetIndex], nil
}

func rowToStrings(row *xlsx.Row) []string {
	cells := make([]string, len(row.Cells))
	for j, cell := range row.Cells {
		cells[j] = cell.String()
	}
	return cells
}
