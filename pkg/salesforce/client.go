package salesforce

import (
	"context"
	"fmt"

	"github.com/k-capehart/go-salesforce/v3"
	"github.com/rotisserie/eris"
)

// Client defines the Salesforce API operations used by the pipeline.
type Client interface {
	Query(ctx context.Context, soql string, out any) error
	InsertOne(ctx context.Context, sObjectName string, record map[string]any) (string, error)
	UpdateOne(ctx context.Context, sObjectName string, id string, fields map[string]any) error
	UpdateCollection(ctx context.Context, sObjectName string, records []CollectionRecord) ([]CollectionResult, error)
	DescribeSObject(ctx context.Context, name string) (*SObjectDescription, error)
}

// QueryResult holds the decoded records from a SOQL query.
type QueryResult[T any] struct {
	Records []T
}

// CollectionRecord represents a single record in a collection update.
// Id is the Salesforce record ID; Fields contains the field values to set.
type CollectionRecord struct {
	ID     string         `json:"Id"`
	Fields map[string]any `json:"fields"`
}

// CollectionResult is the outcome of a single record in a collection operation.
type CollectionResult struct {
	ID      string   `json:"id"`
	Success bool     `json:"success"`
	Errors  []string `json:"errors"`
}

// SObjectField describes a single field on a Salesforce SObject.
type SObjectField struct {
	Name       string `json:"name"`
	Label      string `json:"label"`
	Type       string `json:"type"`
	Length     int    `json:"length"`
	Updateable bool   `json:"updateable"`
}

// SObjectDescription holds metadata about a Salesforce SObject.
type SObjectDescription struct {
	Name   string         `json:"name"`
	Label  string         `json:"label"`
	Fields []SObjectField `json:"fields"`
}

// sfClient wraps the go-salesforce/v3 Salesforce struct.
type sfClient struct {
	sf *salesforce.Salesforce
}

// NewClient creates a new Salesforce Client wrapping the given go-salesforce instance.
func NewClient(sf *salesforce.Salesforce) Client {
	return &sfClient{sf: sf}
}

func (c *sfClient) Query(_ context.Context, soql string, out any) error {
	if err := c.sf.Query(soql, out); err != nil {
		return eris.Wrap(err, "sf: query")
	}
	return nil
}

func (c *sfClient) InsertOne(_ context.Context, sObjectName string, record map[string]any) (string, error) {
	result, err := c.sf.InsertOne(sObjectName, record)
	if err != nil {
		return "", eris.Wrap(err, fmt.Sprintf("sf: insert %s", sObjectName))
	}
	if !result.Success {
		return "", eris.New(fmt.Sprintf("sf: insert %s failed: %v", sObjectName, result.Errors))
	}
	return result.Id, nil
}

func (c *sfClient) UpdateOne(_ context.Context, sObjectName string, id string, fields map[string]any) error {
	fields["Id"] = id
	if err := c.sf.UpdateOne(sObjectName, fields); err != nil {
		return eris.Wrap(err, fmt.Sprintf("sf: update %s %s", sObjectName, id))
	}
	return nil
}

func (c *sfClient) UpdateCollection(_ context.Context, sObjectName string, records []CollectionRecord) ([]CollectionResult, error) {
	// Convert CollectionRecords to maps for go-salesforce.
	maps := make([]map[string]any, len(records))
	for i, rec := range records {
		m := make(map[string]any, len(rec.Fields)+1)
		for k, v := range rec.Fields {
			m[k] = v
		}
		m["Id"] = rec.ID
		maps[i] = m
	}

	sfResults, err := c.sf.UpdateCollection(sObjectName, maps, 200)
	if err != nil {
		return nil, eris.Wrap(err, fmt.Sprintf("sf: update collection %s", sObjectName))
	}

	results := make([]CollectionResult, len(sfResults.Results))
	for i, r := range sfResults.Results {
		var errs []string
		for _, e := range r.Errors {
			errs = append(errs, e.Message)
		}
		results[i] = CollectionResult{
			ID:      r.Id,
			Success: r.Success,
			Errors:  errs,
		}
	}
	return results, nil
}

func (c *sfClient) DescribeSObject(_ context.Context, name string) (*SObjectDescription, error) {
	resp, err := c.sf.DoRequest("GET", "/sobjects/"+name+"/describe", nil)
	if err != nil {
		return nil, eris.Wrap(err, fmt.Sprintf("sf: describe %s", name))
	}
	defer resp.Body.Close()

	var desc SObjectDescription
	if err := decodeJSON(resp.Body, &desc); err != nil {
		return nil, eris.Wrap(err, fmt.Sprintf("sf: decode describe %s", name))
	}
	return &desc, nil
}
