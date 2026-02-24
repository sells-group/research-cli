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

// CreateAccount creates a new Account record and returns the new Salesforce ID.
func CreateAccount(ctx context.Context, c Client, fields map[string]any) (string, error) {
	if fields["Name"] == nil || fields["Name"] == "" {
		return "", eris.New("sf: account Name is required")
	}
	id, err := c.InsertOne(ctx, "Account", fields)
	if err != nil {
		return "", eris.Wrap(err, "sf: create account")
	}
	return id, nil
}

// UpdateContact updates a Contact record with the given fields.
func UpdateContact(ctx context.Context, c Client, contactID string, fields map[string]any) error {
	if contactID == "" {
		return eris.New("sf: contact id is required")
	}
	if len(fields) == 0 {
		return eris.New("sf: no fields to update")
	}
	if err := c.UpdateOne(ctx, "Contact", contactID, fields); err != nil {
		return eris.Wrap(err, fmt.Sprintf("sf: update contact %s", contactID))
	}
	return nil
}

// CreateContact creates a new Contact record linked to the given Account and
// returns the new Salesforce ID.
func CreateContact(ctx context.Context, c Client, accountID string, fields map[string]any) (string, error) {
	if accountID == "" {
		return "", eris.New("sf: account id is required for contact")
	}
	if fields == nil {
		fields = make(map[string]any)
	}
	fields["AccountId"] = accountID
	id, err := c.InsertOne(ctx, "Contact", fields)
	if err != nil {
		return "", eris.Wrap(err, fmt.Sprintf("sf: create contact for account %s", accountID))
	}
	return id, nil
}
