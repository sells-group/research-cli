package dataset

import (
	"context"
	"time"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
)

// FormBD syncs FINRA/EDGAR Form BD broker-dealer registrations.
// Disabled: upstream URL removed (sec.gov/files/data/broker-dealer-data/bd_firm.zip returns 404).
type FormBD struct {
	cfg *config.Config
}

// Name implements Dataset.
func (d *FormBD) Name() string { return "form_bd" }

// Table implements Dataset.
func (d *FormBD) Table() string { return "fed_data.form_bd" }

// Phase implements Dataset.
func (d *FormBD) Phase() Phase { return Phase2 }

// Cadence implements Dataset.
func (d *FormBD) Cadence() Cadence { return Monthly }

// ShouldRun implements Dataset.
func (d *FormBD) ShouldRun(_ time.Time, _ *time.Time) bool {
	return false // disabled: upstream URL removed (sec.gov/files/data/broker-dealer-data/bd_firm.zip returns 404)
}

// Sync implements Dataset.
func (d *FormBD) Sync(_ context.Context, _ db.Pool, _ fetcher.Fetcher, _ string) (*SyncResult, error) {
	return nil, eris.New("form_bd: disabled — upstream URL removed (sec.gov/files/data/broker-dealer-data/bd_firm.zip returns 404)")
}
