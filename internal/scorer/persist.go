package scorer

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"
)

// SaveScores persists scoring results to fed_data.firm_scores.
func SaveScores(ctx context.Context, pool *pgxpool.Pool, scores []FirmScore, pass int) error {
	if len(scores) == 0 {
		return nil
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return eris.Wrap(err, "scorer: begin transaction")
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	for _, s := range scores {
		components, err := json.Marshal(s.ComponentScores)
		if err != nil {
			return eris.Wrapf(err, "scorer: marshal components for CRD %d", s.CRDNumber)
		}
		var keywords []byte
		if len(s.MatchedKeywords) > 0 {
			keywords, err = json.Marshal(s.MatchedKeywords)
			if err != nil {
				return eris.Wrapf(err, "scorer: marshal keywords for CRD %d", s.CRDNumber)
			}
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO fed_data.firm_scores
				(crd_number, pass, score, component_scores, matched_keywords, passed, config_hash)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, s.CRDNumber, pass, s.Score, components, keywords, s.Passed, "")
		if err != nil {
			return eris.Wrapf(err, "scorer: insert score for CRD %d", s.CRDNumber)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return eris.Wrap(err, "scorer: commit scores")
	}

	zap.L().Info("scorer: saved scores",
		zap.Int("count", len(scores)),
		zap.Int("pass", pass),
	)
	return nil
}

// LoadPassResults loads the most recent scoring results for a given pass,
// filtered by minimum score. Returns only firms that passed.
func LoadPassResults(ctx context.Context, pool *pgxpool.Pool, pass int, minScore float64) ([]FirmScore, error) {
	rows, err := pool.Query(ctx, `
		WITH latest AS (
			SELECT DISTINCT ON (crd_number)
				crd_number, score, component_scores, matched_keywords, passed
			FROM fed_data.firm_scores
			WHERE pass = $1 AND passed = true AND score >= $2
			ORDER BY crd_number, scored_at DESC
		)
		SELECT l.crd_number, COALESCE(fc.firm_name, ''), COALESCE(fc.state, ''),
		       COALESCE(fc.aum, 0), COALESCE(fc.website, ''),
		       l.score, l.component_scores, l.matched_keywords, l.passed
		FROM latest l
		LEFT JOIN fed_data.mv_firm_combined fc ON fc.crd_number = l.crd_number
		ORDER BY l.score DESC
	`, pass, minScore)
	if err != nil {
		return nil, eris.Wrap(err, "scorer: query pass results")
	}
	defer rows.Close()

	var results []FirmScore
	for rows.Next() {
		var fs FirmScore
		var componentsJSON, keywordsJSON []byte
		err := rows.Scan(
			&fs.CRDNumber, &fs.FirmName, &fs.State,
			&fs.AUM, &fs.Website,
			&fs.Score, &componentsJSON, &keywordsJSON, &fs.Passed,
		)
		if err != nil {
			return nil, eris.Wrap(err, "scorer: scan pass result")
		}
		if len(componentsJSON) > 0 {
			if err := json.Unmarshal(componentsJSON, &fs.ComponentScores); err != nil {
				return nil, eris.Wrapf(err, "scorer: unmarshal components for CRD %d", fs.CRDNumber)
			}
		}
		if len(keywordsJSON) > 0 {
			if err := json.Unmarshal(keywordsJSON, &fs.MatchedKeywords); err != nil {
				return nil, eris.Wrapf(err, "scorer: unmarshal keywords for CRD %d", fs.CRDNumber)
			}
		}
		results = append(results, fs)
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "scorer: iterate pass results")
	}

	zap.L().Info("scorer: loaded pass results",
		zap.Int("pass", pass),
		zap.Int("count", len(results)),
	)
	return results, nil
}

// ConfigHash returns a SHA-256 hash of the scoring config for reproducibility.
func ConfigHash(cfg interface{}) string {
	data, err := json.Marshal(cfg)
	if err != nil {
		return ""
	}
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h[:16]) // 32 hex chars
}
