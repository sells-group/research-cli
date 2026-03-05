package tigerload

import (
	"context"
	"fmt"
	"time"

	"github.com/rotisserie/eris"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/tiger"
)

// Activities holds dependencies for TIGER load Temporal activities.
type Activities struct {
	pool    db.Pool
	tempDir string
	cfg     *config.Config
}

// NewActivities creates TIGER load Activities.
func NewActivities(pool db.Pool, tempDir string, cfg *config.Config) *Activities {
	return &Activities{
		pool:    pool,
		tempDir: tempDir,
		cfg:     cfg,
	}
}

// LoadNationalParams is the input for LoadNational.
type LoadNationalParams struct {
	Year        int      `json:"year"`
	Tables      []string `json:"tables,omitempty"`
	Incremental bool     `json:"incremental"`
}

// LoadNationalResult is the output of LoadNational.
type LoadNationalResult struct {
	Loaded int `json:"loaded"`
}

// LoadNational loads national TIGER products (STATE, COUNTY, PLACE, COUSUB, ZCTA5) sequentially.
func (a *Activities) LoadNational(ctx context.Context, params LoadNationalParams) (*LoadNationalResult, error) {
	log := zap.L().With(zap.String("component", "tigerload.national"))

	products := resolveProducts(params.Tables)
	var national []tiger.Product
	for _, p := range products {
		if p.National {
			national = append(national, p)
		}
	}

	opts := tiger.LoadOptions{
		Year:        params.Year,
		TempDir:     a.tempDir,
		Incremental: params.Incremental,
	}

	var loaded int
	for _, p := range national {
		activity.RecordHeartbeat(ctx, fmt.Sprintf("loading national product %s", p.Name))
		if _, err := tiger.LoadProduct(ctx, a.pool, p, "", "us", opts); err != nil {
			return nil, eris.Wrapf(err, "load national product %s", p.Name)
		}
		loaded++
		log.Info("national product loaded", zap.String("product", p.Name))
	}

	return &LoadNationalResult{Loaded: loaded}, nil
}

// CreateStateTablesParams is the input for CreateAllStateTables.
type CreateStateTablesParams struct {
	States []string `json:"states,omitempty"`
	Tables []string `json:"tables,omitempty"`
}

// CreateAllStateTables creates per-state child tables for all requested states.
func (a *Activities) CreateAllStateTables(ctx context.Context, params CreateStateTablesParams) error {
	products := resolveProducts(params.Tables)
	var perState []tiger.Product
	for _, p := range products {
		if !p.National {
			perState = append(perState, p)
		}
	}

	states := params.States
	if len(states) == 0 {
		states = tiger.AllStateAbbrs()
	}

	for _, st := range states {
		if err := tiger.CreateStateTables(ctx, a.pool, st, perState); err != nil {
			return eris.Wrapf(err, "create state tables for %s", st)
		}
	}
	return nil
}

// StateFIPS pairs a state abbreviation with its FIPS code.
type StateFIPS struct {
	Abbr string `json:"abbr"`
	FIPS string `json:"fips"`
}

// ResolveStatesParams is the input for ResolveStates.
type ResolveStatesParams struct {
	States []string `json:"states,omitempty"`
}

// ResolveStatesResult is the output of ResolveStates.
type ResolveStatesResult struct {
	States []StateFIPS `json:"states"`
}

// ResolveStates resolves the list of states to process, validating abbreviations and looking up FIPS codes.
func (a *Activities) ResolveStates(_ context.Context, params ResolveStatesParams) (*ResolveStatesResult, error) {
	abbrs := params.States
	if len(abbrs) == 0 {
		abbrs = tiger.AllStateAbbrs()
	}

	var states []StateFIPS
	for _, abbr := range abbrs {
		fips, ok := tiger.FIPSCodes[abbr]
		if !ok {
			return nil, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("unknown state: %s", abbr),
				"UnknownState", nil)
		}
		states = append(states, StateFIPS{Abbr: abbr, FIPS: fips})
	}
	return &ResolveStatesResult{States: states}, nil
}

// LoadStateProductsParams is the input for LoadStateProducts.
type LoadStateProductsParams struct {
	State       string   `json:"state"`
	FIPS        string   `json:"fips"`
	Year        int      `json:"year"`
	Products    []string `json:"products,omitempty"`
	Incremental bool     `json:"incremental"`
}

// LoadStateProductResult is the output of LoadStateProducts.
type LoadStateProductResult struct {
	RowsLoaded int64 `json:"rows_loaded"`
}

// LoadStateProducts loads all per-state products for a single state sequentially.
// It sends heartbeats every 30 seconds for liveness detection.
func (a *Activities) LoadStateProducts(ctx context.Context, params LoadStateProductsParams) (*LoadStateProductResult, error) {
	log := zap.L().With(
		zap.String("component", "tigerload.state"),
		zap.String("state", params.State),
	)

	products := resolveProducts(params.Products)
	var perState []tiger.Product
	for _, p := range products {
		if !p.National {
			perState = append(perState, p)
		}
	}

	opts := tiger.LoadOptions{
		Year:        params.Year,
		TempDir:     a.tempDir,
		Incremental: params.Incremental,
	}

	// Start heartbeat goroutine.
	heartbeatDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				activity.RecordHeartbeat(ctx, fmt.Sprintf("loading %s products", params.State))
			case <-heartbeatDone:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	defer close(heartbeatDone)

	var totalRows int64
	for _, p := range perState {
		log.Info("loading state product", zap.String("product", p.Name))
		rows, err := tiger.LoadProduct(ctx, a.pool, p, params.State, params.FIPS, opts)
		if err != nil {
			return nil, eris.Wrapf(err, "load %s for %s", p.Name, params.State)
		}
		totalRows += rows
		log.Info("state product loaded",
			zap.String("product", p.Name),
			zap.Int64("rows", rows),
		)
	}

	return &LoadStateProductResult{RowsLoaded: totalRows}, nil
}

// PopulateLookups fills tiger lookup tables after all state data is loaded.
func (a *Activities) PopulateLookups(ctx context.Context) error {
	activity.RecordHeartbeat(ctx, "populating lookup tables")
	return tiger.PopulateLookups(ctx, a.pool)
}

// resolveProducts converts table name strings to Product structs.
// If names is empty, returns all products.
func resolveProducts(names []string) []tiger.Product {
	if len(names) == 0 {
		return tiger.Products
	}
	var products []tiger.Product
	for _, name := range names {
		if p, ok := tiger.ProductByName(name); ok {
			products = append(products, p)
		}
	}
	if len(products) == 0 {
		return tiger.Products
	}
	return products
}
