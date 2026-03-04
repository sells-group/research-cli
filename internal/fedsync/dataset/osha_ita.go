package dataset

import (
	"context"
	"time"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
)

// OSHITA syncs OSHA ITA (Injury Tracking Application) inspection data.
// Disabled: upstream URL removed (osha.gov/severeinjury/xml/severeinjury.zip returns 404).
type OSHITA struct{}

// Name implements Dataset.
func (d *OSHITA) Name() string { return "osha_ita" }

// Table implements Dataset.
func (d *OSHITA) Table() string { return "fed_data.osha_inspections" }

// Phase implements Dataset.
func (d *OSHITA) Phase() Phase { return Phase2 }

// Cadence implements Dataset.
func (d *OSHITA) Cadence() Cadence { return Annual }

// ShouldRun implements Dataset.
func (d *OSHITA) ShouldRun(_ time.Time, _ *time.Time) bool {
	return false // disabled: upstream URL removed (osha.gov/severeinjury/xml/severeinjury.zip returns 404)
}

// Sync implements Dataset.
func (d *OSHITA) Sync(_ context.Context, _ db.Pool, _ fetcher.Fetcher, _ string) (*SyncResult, error) {
	return nil, eris.New("osha_ita: disabled — upstream URL removed (osha.gov/severeinjury/xml/severeinjury.zip returns 404)")
}
