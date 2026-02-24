// Package estimate provides revenue estimation logic from multiple data sources.
package estimate

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
)

// RevenueEstimate holds the result of a revenue estimation from CBP data.
type RevenueEstimate struct {
	Amount     int64   `json:"amount"`     // estimated annual revenue in dollars
	Confidence float64 `json:"confidence"` // 0.0-1.0
	Method     string  `json:"method"`     // "cbp_payroll_ratio"
	NAICSUsed  string  `json:"naics_used"` // NAICS code used for lookup
	Year       int     `json:"year"`       // data year
}

// RevenueEstimator estimates company revenue using CBP payroll-per-employee
// ratios by NAICS code and state from the fed_data.mv_market_size view.
type RevenueEstimator struct {
	pool db.Pool
}

// NewRevenueEstimator creates a new estimator. Returns nil if pool is nil.
func NewRevenueEstimator(pool db.Pool) *RevenueEstimator {
	if pool == nil {
		return nil
	}
	return &RevenueEstimator{pool: pool}
}

// payrollMultipliers maps 2-digit NAICS prefix to a payroll-to-revenue multiplier.
// These represent approximate (1 / payroll-as-fraction-of-revenue).
var payrollMultipliers = map[string]float64{
	"23": 2.85, // Construction: payroll ~ 35% of revenue
	"54": 2.2,  // Professional/Scientific/Technical: payroll ~ 45%
	"56": 2.5,  // Admin/Support/Waste: payroll ~ 40%
	"62": 2.5,  // Healthcare/Social: payroll ~ 40%
}

const defaultMultiplier = 3.3 // payroll ~ 30% of revenue

// marketRow holds the query result from mv_market_size.
type marketRow struct {
	naics        string
	year         int
	totalEmp     int64
	totalEst     int64
	totalPayroll int64 // annual payroll in thousands
}

// Estimate calculates estimated revenue for a company given its NAICS code,
// state FIPS code, and employee count. It queries CBP data and applies
// industry-specific payroll-to-revenue multipliers.
//
// NAICS fallback: tries 6-digit, then 4-digit, then 2-digit if data is
// missing or suppressed at more specific levels.
func (e *RevenueEstimator) Estimate(ctx context.Context, naics, stateFIPS string, employeeCount int) (*RevenueEstimate, error) {
	if employeeCount <= 0 {
		return nil, eris.New("estimate: employee count must be positive")
	}
	if naics == "" {
		return nil, eris.New("estimate: NAICS code is required")
	}

	// Try progressively broader NAICS codes: 6 -> 4 -> 2 digits.
	levels := naicsLevels(naics)
	var row *marketRow
	var usedNAICS string
	var fellBack bool

	for i, code := range levels {
		r, err := e.queryMarketSize(ctx, code, stateFIPS)
		if err != nil {
			return nil, eris.Wrapf(err, "estimate: query market size for NAICS %s", code)
		}
		if r != nil && r.totalEmp > 0 && r.totalPayroll > 0 {
			row = r
			usedNAICS = code
			fellBack = i > 0
			break
		}
	}

	if row == nil {
		return nil, eris.Errorf("estimate: no CBP data for NAICS %s in state %s", naics, stateFIPS)
	}

	// avg payroll per employee (in dollars; ap is stored in $1000s).
	avgPayrollPerEmp := float64(row.totalPayroll*1000) / float64(row.totalEmp)

	// Look up industry multiplier from 2-digit NAICS prefix.
	prefix := naics
	if len(prefix) > 2 {
		prefix = prefix[:2]
	}
	multiplier := defaultMultiplier
	if m, ok := payrollMultipliers[prefix]; ok {
		multiplier = m
	}

	estimatedRevenue := avgPayrollPerEmp * float64(employeeCount) * multiplier

	// Confidence calculation.
	confidence := 0.6
	if row.totalEst > 100 {
		confidence += 0.1
	}
	if row.totalEst > 1000 {
		confidence += 0.1
	}
	if len(usedNAICS) >= 6 {
		confidence += 0.1
	}
	if fellBack {
		confidence -= 0.1
	}
	confidence = math.Min(confidence, 0.9)
	confidence = math.Max(confidence, 0.1)

	zap.L().Info("estimate: revenue computed",
		zap.String("naics", naics),
		zap.String("naics_used", usedNAICS),
		zap.String("state_fips", stateFIPS),
		zap.Int("employee_count", employeeCount),
		zap.Int64("estimated_revenue", int64(estimatedRevenue)),
		zap.Float64("confidence", confidence),
		zap.Float64("multiplier", multiplier),
		zap.Int("data_year", row.year),
	)

	return &RevenueEstimate{
		Amount:     int64(math.Round(estimatedRevenue)),
		Confidence: confidence,
		Method:     "cbp_payroll_ratio",
		NAICSUsed:  usedNAICS,
		Year:       row.year,
	}, nil
}

// queryMarketSize queries mv_market_size for the given NAICS and state.
// If stateFIPS is empty, it aggregates across all states.
// Returns nil if no data is found.
func (e *RevenueEstimator) queryMarketSize(ctx context.Context, naics, stateFIPS string) (*marketRow, error) {
	var query string
	var args []any

	if stateFIPS != "" {
		query = `
			SELECT naics, year, COALESCE(SUM(total_emp), 0), COALESCE(SUM(total_est), 0), COALESCE(SUM(total_payroll), 0)
			FROM fed_data.mv_market_size
			WHERE naics LIKE $1 AND fips_state = $2
			GROUP BY naics, year
			ORDER BY year DESC
			LIMIT 1`
		args = []any{naics + "%", stateFIPS}
	} else {
		query = `
			SELECT naics, year, COALESCE(SUM(total_emp), 0), COALESCE(SUM(total_est), 0), COALESCE(SUM(total_payroll), 0)
			FROM fed_data.mv_market_size
			WHERE naics LIKE $1
			GROUP BY naics, year
			ORDER BY year DESC
			LIMIT 1`
		args = []any{naics + "%"}
	}

	var r marketRow
	err := e.pool.QueryRow(ctx, query, args...).Scan(
		&r.naics, &r.year, &r.totalEmp, &r.totalEst, &r.totalPayroll,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, eris.Wrap(err, "query mv_market_size")
	}

	return &r, nil
}

// naicsLevels returns NAICS codes from most specific to least specific.
// For "541512" it returns ["541512", "5415", "54"].
func naicsLevels(naics string) []string {
	var levels []string
	// Full code first.
	levels = append(levels, naics)

	// 4-digit if original is longer.
	if len(naics) > 4 {
		levels = append(levels, naics[:4])
	}
	// 2-digit if original is longer.
	if len(naics) > 2 {
		levels = append(levels, naics[:2])
	}
	return levels
}

// Multiplier returns the payroll-to-revenue multiplier for a 2-digit NAICS prefix.
// Exported for testing.
func Multiplier(naics2 string) float64 {
	if m, ok := payrollMultipliers[naics2]; ok {
		return m
	}
	return defaultMultiplier
}

// FormatRevenue formats a revenue amount in human-readable form.
func FormatRevenue(amount int64) string {
	switch {
	case amount >= 1_000_000_000:
		return fmt.Sprintf("$%.1fB", float64(amount)/1_000_000_000)
	case amount >= 1_000_000:
		return fmt.Sprintf("$%.1fM", float64(amount)/1_000_000)
	case amount >= 1_000:
		return fmt.Sprintf("$%.0fK", float64(amount)/1_000)
	default:
		return fmt.Sprintf("$%d", amount)
	}
}
