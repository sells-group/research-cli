package fetcher

import (
	"context"
	"encoding/json"
	"io"

	"github.com/rotisserie/eris"
)

// DecodeJSONArray decodes a JSON array streaming, sending each element to a channel.
// Expects input in the form [{...},{...}].
// Both channels are closed when processing completes.
func DecodeJSONArray[T any](ctx context.Context, r io.Reader) (<-chan T, <-chan error) {
	outCh := make(chan T, 64)
	errCh := make(chan error, 1)

	go func() {
		defer close(outCh)
		defer close(errCh)

		decoder := json.NewDecoder(r)

		// Expect opening bracket
		tok, err := decoder.Token()
		if err != nil {
			if err == io.EOF {
				return
			}
			errCh <- eris.Wrap(err, "json: read opening token")
			return
		}

		delim, ok := tok.(json.Delim)
		if !ok || delim != '[' {
			errCh <- eris.Errorf("json: expected '[', got %v", tok)
			return
		}

		for decoder.More() {
			if ctx.Err() != nil {
				errCh <- eris.Wrap(ctx.Err(), "json: context cancelled")
				return
			}

			var item T
			if err := decoder.Decode(&item); err != nil {
				errCh <- eris.Wrap(err, "json: decode element")
				return
			}

			select {
			case outCh <- item:
			case <-ctx.Done():
				errCh <- eris.Wrap(ctx.Err(), "json: context cancelled")
				return
			}
		}

		// Consume closing bracket
		if _, err := decoder.Token(); err != nil && err != io.EOF {
			errCh <- eris.Wrap(err, "json: read closing token")
		}
	}()

	return outCh, errCh
}

// DecodeJSONObject decodes a single JSON object from a reader.
func DecodeJSONObject[T any](r io.Reader) (*T, error) {
	var obj T
	if err := json.NewDecoder(r).Decode(&obj); err != nil {
		return nil, eris.Wrap(err, "json: decode object")
	}
	return &obj, nil
}
