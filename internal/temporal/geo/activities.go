// Package geo provides Temporal workflows and activities for geo backfill operations.
package geo

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"
	"go.temporal.io/sdk/activity"

	"github.com/sells-group/research-cli/internal/company"
	"github.com/sells-group/research-cli/internal/config"
	igeo "github.com/sells-group/research-cli/internal/geo"
	"github.com/sells-group/research-cli/internal/geobackfill"
	"github.com/sells-group/research-cli/pkg/geocode"
)

// Activities holds dependencies for geo backfill Temporal activities.
type Activities struct {
	svc *geobackfill.Service
}

// NewActivities creates geo backfill Activities.
func NewActivities(pool *pgxpool.Pool, cs *company.PostgresStore, gc geocode.Client, assoc *igeo.Associator, cfg *config.Config) *Activities {
	return &Activities{
		svc: geobackfill.NewService(pool, cs, gc, assoc, cfg),
	}
}

// UnlinkedRecord represents a single record that needs geo backfill processing.
type UnlinkedRecord struct {
	Key       string   `json:"key"`
	Name      string   `json:"name"`
	Street1   string   `json:"street1,omitempty"`
	Street2   string   `json:"street2,omitempty"`
	City      string   `json:"city,omitempty"`
	State     string   `json:"state,omitempty"`
	Zip       string   `json:"zip,omitempty"`
	Website   string   `json:"website,omitempty"`
	Latitude  *float64 `json:"latitude,omitempty"`
	Longitude *float64 `json:"longitude,omitempty"`
}

// QueryUnlinkedParams is the input for QueryUnlinkedRecords.
type QueryUnlinkedParams struct {
	Source string `json:"source"`
	Limit  int    `json:"limit"`
}

// QueryUnlinkedResult is the output of QueryUnlinkedRecords.
type QueryUnlinkedResult struct {
	Records []UnlinkedRecord `json:"records"`
}

// QueryUnlinkedRecords finds records that haven't been linked to companies yet.
func (a *Activities) QueryUnlinkedRecords(ctx context.Context, params QueryUnlinkedParams) (*QueryUnlinkedResult, error) {
	records, err := a.svc.QueryUnlinkedRecords(ctx, geobackfill.Source(params.Source), params.Limit)
	if err != nil {
		return nil, eris.Wrapf(err, "query unlinked %s records", params.Source)
	}

	result := make([]UnlinkedRecord, len(records))
	for i, record := range records {
		result[i] = UnlinkedRecord{
			Key:       record.Key,
			Name:      record.Name,
			Street1:   record.Street1,
			Street2:   record.Street2,
			City:      record.City,
			State:     record.State,
			Zip:       record.Zip,
			Website:   record.Website,
			Latitude:  record.Latitude,
			Longitude: record.Longitude,
		}
	}

	return &QueryUnlinkedResult{Records: result}, nil
}

// ProcessBatchParams is the input for ProcessGeoBackfillBatch.
type ProcessBatchParams struct {
	Source  string           `json:"source"`
	Records []UnlinkedRecord `json:"records"`
	SkipMSA bool             `json:"skip_msa"`
}

// ProcessBatchResult is the output of ProcessGeoBackfillBatch.
type ProcessBatchResult struct {
	Created  int `json:"created"`
	Geocoded int `json:"geocoded"`
	Linked   int `json:"linked"`
	MSAs     int `json:"msas"`
	Branches int `json:"branches"`
	Failed   int `json:"failed"`
}

// ProcessGeoBackfillBatch processes a batch of unlinked records.
func (a *Activities) ProcessGeoBackfillBatch(ctx context.Context, params ProcessBatchParams) (*ProcessBatchResult, error) {
	for i := range params.Records {
		if i > 0 && i%10 == 0 {
			activity.RecordHeartbeat(ctx, fmt.Sprintf("processed %d/%d", i, len(params.Records)))
		}
	}

	records := make([]geobackfill.Record, len(params.Records))
	for i, rec := range params.Records {
		records[i] = geobackfill.Record{
			Key:       rec.Key,
			Name:      rec.Name,
			Street1:   rec.Street1,
			Street2:   rec.Street2,
			City:      rec.City,
			State:     rec.State,
			Zip:       rec.Zip,
			Website:   rec.Website,
			Latitude:  rec.Latitude,
			Longitude: rec.Longitude,
		}
	}

	result, err := a.svc.ProcessBatch(ctx, geobackfill.Source(params.Source), records, params.SkipMSA, false)
	if err != nil {
		return nil, err
	}

	return &ProcessBatchResult{
		Created:  result.Created,
		Geocoded: result.Geocoded,
		Linked:   result.Linked,
		MSAs:     result.MSAs,
		Branches: result.Branches,
		Failed:   result.Failed,
	}, nil
}
