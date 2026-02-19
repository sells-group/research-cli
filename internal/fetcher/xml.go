package fetcher

import (
	"context"
	"encoding/xml"
	"io"

	"github.com/rotisserie/eris"
	"golang.org/x/text/encoding/htmlindex"
)

// StreamXML decodes XML elements matching the given local name and sends them to a channel.
// The type parameter T must be a struct with appropriate xml tags.
// Both channels are closed when processing completes.
func StreamXML[T any](ctx context.Context, r io.Reader, elementName string) (<-chan T, <-chan error) {
	outCh := make(chan T, 64)
	errCh := make(chan error, 1)

	go func() {
		defer close(outCh)
		defer close(errCh)

		decoder := xml.NewDecoder(r)
		decoder.CharsetReader = func(charset string, input io.Reader) (io.Reader, error) {
			enc, err := htmlindex.Get(charset)
			if err != nil {
				return nil, eris.Wrapf(err, "xml: unsupported charset %q", charset)
			}
			return enc.NewDecoder().Reader(input), nil
		}

		for {
			if ctx.Err() != nil {
				errCh <- eris.Wrap(ctx.Err(), "xml: context cancelled")
				return
			}

			tok, err := decoder.Token()
			if err == io.EOF {
				return
			}
			if err != nil {
				errCh <- eris.Wrap(err, "xml: read token")
				return
			}

			se, ok := tok.(xml.StartElement)
			if !ok {
				continue
			}

			if se.Name.Local != elementName {
				continue
			}

			var item T
			if err := decoder.DecodeElement(&item, &se); err != nil {
				errCh <- eris.Wrap(err, "xml: decode element")
				return
			}

			select {
			case outCh <- item:
			case <-ctx.Done():
				errCh <- eris.Wrap(ctx.Err(), "xml: context cancelled")
				return
			}
		}
	}()

	return outCh, errCh
}
