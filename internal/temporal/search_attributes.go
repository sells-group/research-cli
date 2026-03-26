package temporal

import (
	"time"

	temporalsdk "go.temporal.io/sdk/temporal"
)

var (
	// RequestIDSearchAttribute stores the originating HTTP request identifier.
	RequestIDSearchAttribute = temporalsdk.NewSearchAttributeKeyKeyword("ResearchRequestID")
	// TriggerSourceSearchAttribute stores the source of the workflow start request.
	TriggerSourceSearchAttribute = temporalsdk.NewSearchAttributeKeyKeyword("ResearchTriggerSource")
	// OriginalRunIDSearchAttribute stores the failed run being retried, when applicable.
	OriginalRunIDSearchAttribute = temporalsdk.NewSearchAttributeKeyKeyword("ResearchOriginalRunID")
	// DedupeKeySearchAttribute stores the idempotency key for API-triggered starts.
	DedupeKeySearchAttribute = temporalsdk.NewSearchAttributeKeyKeyword("ResearchDedupeKey")
	// CompanyHostSearchAttribute stores the normalized company host for webhook-triggered starts.
	CompanyHostSearchAttribute = temporalsdk.NewSearchAttributeKeyKeyword("ResearchCompanyHost")
	// RequestedAtSearchAttribute stores the API request timestamp.
	RequestedAtSearchAttribute = temporalsdk.NewSearchAttributeKeyTime("ResearchRequestedAt")
)

// EnrichmentSearchAttributeDefinitions returns the typed search attributes that
// must be registered on a Temporal server for API-triggered enrichment starts.
func EnrichmentSearchAttributeDefinitions() temporalsdk.SearchAttributes {
	return temporalsdk.NewSearchAttributes(
		RequestIDSearchAttribute.ValueSet("placeholder"),
		TriggerSourceSearchAttribute.ValueSet("placeholder"),
		OriginalRunIDSearchAttribute.ValueSet("placeholder"),
		DedupeKeySearchAttribute.ValueSet("placeholder"),
		CompanyHostSearchAttribute.ValueSet("placeholder"),
		RequestedAtSearchAttribute.ValueSet(time.Unix(0, 0).UTC()),
	)
}
