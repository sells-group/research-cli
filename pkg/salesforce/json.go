package salesforce

import (
	"encoding/json"
	"io"

	"github.com/rotisserie/eris"
)

func decodeJSON(r io.Reader, out any) error {
	if err := json.NewDecoder(r).Decode(out); err != nil {
		return eris.Wrap(err, "decode json")
	}
	return nil
}
