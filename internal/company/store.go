package company

import "context"

// CompanyStore defines persistence operations for the company data model.
type CompanyStore interface { //nolint:revive // stutters but widely used across codebase
	// Company CRUD
	CreateCompany(ctx context.Context, c *CompanyRecord) error
	UpdateCompany(ctx context.Context, c *CompanyRecord) error
	GetCompany(ctx context.Context, id int64) (*CompanyRecord, error)
	GetCompanyByDomain(ctx context.Context, domain string) (*CompanyRecord, error)
	SearchCompaniesByName(ctx context.Context, name string, limit int) ([]CompanyRecord, error)

	// Identifiers
	UpsertIdentifier(ctx context.Context, id *Identifier) error
	GetIdentifiers(ctx context.Context, companyID int64) ([]Identifier, error)
	FindByIdentifier(ctx context.Context, system, identifier string) (*CompanyRecord, error)

	// Addresses
	UpsertAddress(ctx context.Context, addr *Address) error
	GetAddresses(ctx context.Context, companyID int64) ([]Address, error)

	// Contacts
	UpsertContact(ctx context.Context, c *Contact) error
	GetContacts(ctx context.Context, companyID int64) ([]Contact, error)
	GetContactsByRole(ctx context.Context, companyID int64, roleType string) ([]Contact, error)

	// Licenses
	UpsertLicense(ctx context.Context, l *License) error
	GetLicenses(ctx context.Context, companyID int64) ([]License, error)

	// Sources
	UpsertSource(ctx context.Context, s *Source) error
	GetSources(ctx context.Context, companyID int64) ([]Source, error)
	GetSource(ctx context.Context, companyID int64, sourceName, sourceID string) (*Source, error)

	// Financials
	UpsertFinancial(ctx context.Context, f *Financial) error
	GetFinancials(ctx context.Context, companyID int64, metric string) ([]Financial, error)

	// Tags
	SetTags(ctx context.Context, companyID int64, tagType string, values []string) error
	GetTags(ctx context.Context, companyID int64) ([]Tag, error)

	// Matches
	UpsertMatch(ctx context.Context, m *Match) error
	GetMatches(ctx context.Context, companyID int64) ([]Match, error)
	FindByMatch(ctx context.Context, matchedSource, matchedKey string) (*CompanyRecord, error)

	// Geocoding
	GetUngeocodedAddresses(ctx context.Context, limit int) ([]Address, error)
	UpdateAddressGeocode(ctx context.Context, id int64, lat, lon float64, source, quality string) error

	// MSA associations
	UpsertAddressMSA(ctx context.Context, am *AddressMSA) error
	GetAddressMSAs(ctx context.Context, addressID int64) ([]AddressMSA, error)
	GetCompanyMSAs(ctx context.Context, companyID int64) ([]AddressMSA, error)
}
