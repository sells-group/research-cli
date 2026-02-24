// Package salesforce provides JWT-authenticated REST API access to Salesforce.
package salesforce

import (
	"context"
	"fmt"

	"github.com/k-capehart/go-salesforce/v3"
	"github.com/rotisserie/eris"
	"golang.org/x/time/rate"
)

// Client defines the Salesforce API operations used by the pipeline.
type Client interface {
	Query(ctx context.Context, soql string, out any) error
	InsertOne(ctx context.Context, sObjectName string, record map[string]any) (string, error)
	InsertCollection(ctx context.Context, sObjectName string, records []map[string]any) ([]CollectionResult, error)
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

// ClientOption configures the Salesforce client.
type ClientOption func(*sfClient)

// WithRateLimit sets a per-second rate limit for SF API calls.
// A burst equal to the integer portion of rps is allowed.
func WithRateLimit(rps float64) ClientOption {
	return func(c *sfClient) {
		if rps > 0 {
			c.limiter = rate.NewLimiter(rate.Limit(rps), max(int(rps), 1))
		}
	}
}

// sfClient wraps the go-salesforce/v3 Salesforce struct.
//
// NOTE: The underlying go-salesforce/v3 library does not accept context.Context,
// so all methods discard the ctx parameter for the SF call itself. However, the
// ctx is used for rate limiter waiting, so callers can still cancel that wait.
type sfClient struct {
	sf      *salesforce.Salesforce
	limiter *rate.Limiter
}

// NewClient creates a new Salesforce Client wrapping the given go-salesforce instance.
func NewClient(sf *salesforce.Salesforce, opts ...ClientOption) Client {
	c := &sfClient{sf: sf}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// wait blocks until the rate limiter allows one event, or ctx is cancelled.
func (c *sfClient) wait(ctx context.Context) error {
	if c.limiter == nil {
		return nil
	}
	return c.limiter.Wait(ctx)
}

func (c *sfClient) Query(ctx context.Context, soql string, out any) error {
	if err := c.wait(ctx); err != nil {
		return eris.Wrap(err, "sf: rate limit")
	}
	if err := c.sf.Query(soql, out); err != nil {
		return eris.Wrap(err, "sf: query")
	}
	return nil
}

func (c *sfClient) InsertOne(ctx context.Context, sObjectName string, record map[string]any) (string, error) {
	if err := c.wait(ctx); err != nil {
		return "", eris.Wrap(err, "sf: rate limit")
	}
	result, err := c.sf.InsertOne(sObjectName, record)
	if err != nil {
		return "", eris.Wrap(err, fmt.Sprintf("sf: insert %s", sObjectName))
	}
	if !result.Success {
		return "", eris.New(fmt.Sprintf("sf: insert %s failed: %v", sObjectName, result.Errors))
	}
	return result.Id, nil
}

func (c *sfClient) InsertCollection(ctx context.Context, sObjectName string, records []map[string]any) ([]CollectionResult, error) {
	if err := c.wait(ctx); err != nil {
		return nil, eris.Wrap(err, "sf: rate limit")
	}
	sfResults, err := c.sf.InsertCollection(sObjectName, records, maxBatchSize)
	if err != nil {
		return nil, eris.Wrap(err, fmt.Sprintf("sf: insert collection %s", sObjectName))
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

func (c *sfClient) UpdateOne(ctx context.Context, sObjectName string, id string, fields map[string]any) error {
	if err := c.wait(ctx); err != nil {
		return eris.Wrap(err, "sf: rate limit")
	}
	fields["Id"] = id
	if err := c.sf.UpdateOne(sObjectName, fields); err != nil {
		return eris.Wrap(err, fmt.Sprintf("sf: update %s %s", sObjectName, id))
	}
	return nil
}

func (c *sfClient) UpdateCollection(ctx context.Context, sObjectName string, records []CollectionRecord) ([]CollectionResult, error) {
	if err := c.wait(ctx); err != nil {
		return nil, eris.Wrap(err, "sf: rate limit")
	}
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

func (c *sfClient) DescribeSObject(ctx context.Context, name string) (*SObjectDescription, error) {
	if err := c.wait(ctx); err != nil {
		return nil, eris.Wrap(err, "sf: rate limit")
	}
	resp, err := c.sf.DoRequest("GET", "/sobjects/"+name+"/describe", nil)
	if err != nil {
		return nil, eris.Wrap(err, fmt.Sprintf("sf: describe %s", name))
	}
	defer resp.Body.Close() //nolint:errcheck

	var desc SObjectDescription
	if err := decodeJSON(resp.Body, &desc); err != nil {
		return nil, eris.Wrap(err, fmt.Sprintf("sf: decode describe %s", name))
	}
	return &desc, nil
}
