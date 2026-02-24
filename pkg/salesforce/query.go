package salesforce

import (
	"context"
	"fmt"
	"strings"

	"github.com/rotisserie/eris"
)

// Account represents a Salesforce Account record.
type Account struct {
	ID                string  `json:"Id" salesforce:"Id"`
	Name              string  `json:"Name" salesforce:"Name"`
	Website           string  `json:"Website" salesforce:"Website"`
	Industry          string  `json:"Industry" salesforce:"Industry"`
	Description       string  `json:"Description" salesforce:"Description"`
	BillingCity       string  `json:"BillingCity" salesforce:"BillingCity"`
	BillingState      string  `json:"BillingState" salesforce:"BillingState"`
	BillingCountry    string  `json:"BillingCountry" salesforce:"BillingCountry"`
	BillingPostalCode string  `json:"BillingPostalCode" salesforce:"BillingPostalCode"`
	Phone             string  `json:"Phone" salesforce:"Phone"`
	NumberOfEmployees int     `json:"NumberOfEmployees" salesforce:"NumberOfEmployees"`
	AnnualRevenue     float64 `json:"AnnualRevenue" salesforce:"AnnualRevenue"`
	Type              string  `json:"Type" salesforce:"Type"`
}

// accountFields are the SOQL fields selected for Account queries.
var accountFields = []string{
	"Id", "Name", "Website", "Industry", "Description",
	"BillingCity", "BillingState", "BillingCountry", "BillingPostalCode",
	"Phone", "NumberOfEmployees", "AnnualRevenue", "Type",
}

// FindAccountByWebsite queries Salesforce for an Account matching the given website.
// Returns nil if no account is found.
func FindAccountByWebsite(ctx context.Context, c Client, website string) (*Account, error) {
	soql := fmt.Sprintf(
		"SELECT %s FROM Account WHERE Website LIKE '%s' LIMIT 1",
		strings.Join(accountFields, ", "),
		escapeSoql(website),
	)

	var accounts []Account
	if err := c.Query(ctx, soql, &accounts); err != nil {
		return nil, eris.Wrap(err, fmt.Sprintf("sf: find account by website %s", website))
	}
	if len(accounts) == 0 {
		return nil, nil
	}
	return &accounts[0], nil
}

// FindAccountByID queries Salesforce for an Account by its ID.
// Returns nil if no account is found.
func FindAccountByID(ctx context.Context, c Client, id string) (*Account, error) {
	soql := fmt.Sprintf(
		"SELECT %s FROM Account WHERE Id = '%s' LIMIT 1",
		strings.Join(accountFields, ", "),
		escapeSoql(id),
	)

	var accounts []Account
	if err := c.Query(ctx, soql, &accounts); err != nil {
		return nil, eris.Wrap(err, fmt.Sprintf("sf: find account by id %s", id))
	}
	if len(accounts) == 0 {
		return nil, nil
	}
	return &accounts[0], nil
}

// escapeSoql escapes single quotes in SOQL string literals to prevent injection.
func escapeSoql(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}
