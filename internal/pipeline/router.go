package pipeline

import (
	"github.com/sells-group/research-cli/internal/model"
)

// RouteQuestions matches questions to classified pages based on page types
// and groups them by tier. Pure Go — no API calls.
func RouteQuestions(questions []model.Question, index model.PageIndex) *model.RoutedBatches {
	batches := &model.RoutedBatches{}

	for _, q := range questions {
		pages := findPagesForQuestion(q, index)

		if len(pages) == 0 {
			batches.Skipped = append(batches.Skipped, model.SkippedQuestion{
				Question: q,
				Reason:   "no matching pages found",
			})
			continue
		}

		rq := model.RoutedQuestion{
			Question: q,
			Pages:    pages,
		}

		switch q.Tier {
		case 1:
			batches.Tier1 = append(batches.Tier1, rq)
		case 2:
			batches.Tier2 = append(batches.Tier2, rq)
		case 3:
			batches.Tier3 = append(batches.Tier3, rq)
		default:
			// Default unspecified tier to Tier 1.
			batches.Tier1 = append(batches.Tier1, rq)
		}
	}

	return batches
}

// findPagesForQuestion returns classified pages matching the question's
// preferred page types. If no preferred types are set, all pages are eligible.
// External source pages (BBB, Google Maps, SoS, LinkedIn) are always included
// as supplementary context regardless of the question's PageTypes filter.
func findPagesForQuestion(q model.Question, index model.PageIndex) []model.ClassifiedPage {
	if len(q.PageTypes) == 0 {
		// No preference: return all pages.
		var all []model.ClassifiedPage
		for _, pages := range index {
			all = append(all, pages...)
		}
		return all
	}

	var result []model.ClassifiedPage
	seen := make(map[string]bool)

	// First, add pages matching the question's preferred types.
	for _, pt := range q.PageTypes {
		pages, ok := index[pt]
		if !ok {
			continue
		}
		for _, p := range pages {
			if !seen[p.URL] {
				seen[p.URL] = true
				result = append(result, p)
			}
		}
	}

	// Always append external source pages as supplementary context.
	for _, pt := range model.ExternalPageTypes() {
		pages, ok := index[pt]
		if !ok {
			continue
		}
		for _, p := range pages {
			if !seen[p.URL] {
				seen[p.URL] = true
				result = append(result, p)
			}
		}
	}

	return result
}

// EscalateQuestions takes T1 answers and escalates questions to T2 when
// a majority of their fields are null or low-confidence. This avoids
// re-running an entire multi-field question at T2 when most fields succeeded.
func EscalateQuestions(answers []model.ExtractionAnswer, questions []model.Question, index model.PageIndex, threshold float64) []model.RoutedQuestion {
	// Build a lookup from question ID to question.
	qMap := make(map[string]model.Question, len(questions))
	for _, q := range questions {
		qMap[q.ID] = q
	}

	// Aggregate per-question success rates.
	type qStats struct {
		total   int
		failed  int // null value or low confidence
	}
	byQ := make(map[string]*qStats)
	for _, a := range answers {
		st, ok := byQ[a.QuestionID]
		if !ok {
			st = &qStats{}
			byQ[a.QuestionID] = st
		}
		st.total++
		if a.Value == nil || a.Confidence < threshold {
			st.failed++
		}
	}

	// Escalate questions where >35% of fields failed (null or low confidence).
	seen := make(map[string]bool)
	var escalated []model.RoutedQuestion
	for qid, stats := range byQ {
		if stats.total == 0 {
			continue
		}
		failRate := float64(stats.failed) / float64(stats.total)
		if failRate <= 0.35 {
			continue // Majority succeeded, don't re-run at T2.
		}
		if seen[qid] {
			continue
		}
		seen[qid] = true

		q, ok := qMap[qid]
		if !ok {
			continue
		}
		pages := findPagesForQuestion(q, index)
		if len(pages) == 0 {
			continue
		}
		escalated = append(escalated, model.RoutedQuestion{
			Question: q,
			Pages:    pages,
		})
	}

	return escalated
}
