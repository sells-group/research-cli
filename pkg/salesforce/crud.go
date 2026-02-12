package salesforce

import (
	"context"
	"fmt"

	"github.com/rotisserie/eris"
)

// UpdateAccount updates an Account record with the given fields.
func UpdateAccount(ctx context.Context, c Client, accountID string, fields map[string]any) error {
	if accountID == "" {
		return eris.New("sf: account id is required")
	}
	if len(fields) == 0 {
		return eris.New("sf: no fields to update")
	}
	if err := c.UpdateOne(ctx, "Account", accountID, fields); err != nil {
		return eris.Wrap(err, fmt.Sprintf("sf: update account %s", accountID))
	}
	return nil
}
