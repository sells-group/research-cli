// Package fetcher downloads and parses data from HTTP, FTP, CSV, XML, JSON, XLSX, and ZIP sources.
package fetcher

import (
	"context"
	"encoding/csv"
	"io"
	"strings"

	"github.com/rotisserie/eris"
)

// CSVOptions configures the streaming CSV parser.
type CSVOptions struct {
	Delimiter  rune            // default ','
	HasHeader  bool            // if true, first row is skipped but sent to HeaderCh
	HeaderCh   chan<- []string // optional: receives the header row
	Comment    rune            // comment character (0 = none)
	LazyQuotes bool
	TrimSpace  bool
}

// StreamCSV reads a CSV file and sends rows to a channel.
// Caller must consume the returned row channel. Errors are sent on the error channel.
// Both channels are closed when processing completes.
func StreamCSV(ctx context.Context, r io.Reader, opts CSVOptions) (<-chan []string, <-chan error) {
	rowCh := make(chan []string, 64)
	errCh := make(chan error, 1)

	go func() {
		defer close(rowCh)
		defer close(errCh)

		var reader *csv.Reader
		if opts.TrimSpace {
			reader = csv.NewReader(&trimReader{r: r})
		} else {
			reader = csv.NewReader(r)
		}

		if opts.Delimiter != 0 {
			reader.Comma = opts.Delimiter
		}
		if opts.Comment != 0 {
			reader.Comment = opts.Comment
		}
		reader.LazyQuotes = opts.LazyQuotes
		reader.FieldsPerRecord = -1 // allow variable fields

		first := true
		for {
			if ctx.Err() != nil {
				errCh <- eris.Wrap(ctx.Err(), "csv: context cancelled")
				return
			}

			record, err := reader.Read()
			if err == io.EOF {
				return
			}
			if err != nil {
				errCh <- eris.Wrap(err, "csv: read row")
				return
			}

			if opts.TrimSpace {
				for i, field := range record {
					record[i] = strings.TrimSpace(field)
				}
			}

			if first && opts.HasHeader {
				first = false
				if opts.HeaderCh != nil {
					select {
					case opts.HeaderCh <- record:
					case <-ctx.Done():
						errCh <- eris.Wrap(ctx.Err(), "csv: context cancelled sending header")
						return
					}
				}
				continue
			}
			first = false

			select {
			case rowCh <- record:
			case <-ctx.Done():
				errCh <- eris.Wrap(ctx.Err(), "csv: context cancelled")
				return
			}
		}
	}()

	return rowCh, errCh
}

// trimReader wraps an io.Reader and is used to enable TrimSpace at the reader level.
// Actual field trimming happens after csv parsing in StreamCSV.
type trimReader struct {
	r io.Reader
}

func (t *trimReader) Read(p []byte) (int, error) {
	return t.r.Read(p)
}
