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

	return result
}

// EscalateQuestions takes T1 answers with low confidence and re-queues
// them into T2 batches. Returns new T2 routed questions.
func EscalateQuestions(answers []model.ExtractionAnswer, questions []model.Question, index model.PageIndex, threshold float64) []model.RoutedQuestion {
	// Build a lookup from question ID to question.
	qMap := make(map[string]model.Question, len(questions))
	for _, q := range questions {
		qMap[q.ID] = q
	}

	var escalated []model.RoutedQuestion
	for _, a := range answers {
		if a.Confidence >= threshold {
			continue
		}

		q, ok := qMap[a.QuestionID]
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
