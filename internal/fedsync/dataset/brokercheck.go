package dataset

import (
	"context"
	"os"
	"time"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
)

// BrokerCheck syncs FINRA BrokerCheck firm data (pipe-delimited).
// Disabled: upstream URL blocked (files.brokercheck.finra.org/firm/firm.zip returns 403).
type BrokerCheck struct{}

// Name implements Dataset.
func (d *BrokerCheck) Name() string { return "brokercheck" }

// Table implements Dataset.
func (d *BrokerCheck) Table() string { return "fed_data.brokercheck" }

// Phase implements Dataset.
func (d *BrokerCheck) Phase() Phase { return Phase2 }

// Cadence implements Dataset.
func (d *BrokerCheck) Cadence() Cadence { return Monthly }

// ShouldRun implements Dataset.
func (d *BrokerCheck) ShouldRun(_ time.Time, _ *time.Time) bool {
	return false // disabled: upstream URL blocked (files.brokercheck.finra.org/firm/firm.zip returns 403)
}

// Sync implements Dataset.
func (d *BrokerCheck) Sync(_ context.Context, _ db.Pool, _ fetcher.Fetcher, _ string) (*SyncResult, error) {
	return nil, eris.New("brokercheck: disabled — upstream URL blocked (files.brokercheck.finra.org/firm/firm.zip returns 403)")
}

func openFileForRead(path string) (*os.File, error) {
	return os.Open(path) // #nosec G304 -- path from function parameter in internal package, used for trusted downloaded files
}
