package registry

import (
	"context"

	"github.com/jomei/notionapi"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/notion"
)

// LoadQuestionRegistry queries the Notion Question Registry database for all
// active questions and returns them as model.Question values.
func LoadQuestionRegistry(ctx context.Context, client notion.Client, dbID string) ([]model.Question, error) {
	filter := &notionapi.DatabaseQueryRequest{
		Filter: notionapi.PropertyFilter{
			Property: "Status",
			Status: &notionapi.StatusFilterCondition{
				Equals: "Active",
			},
		},
	}

	pages, err := notion.QueryAll(ctx, client, dbID, filter)
	if err != nil {
		return nil, eris.Wrap(err, "registry: load question registry")
	}

	var questions []model.Question
	for _, p := range pages {
		q, err := parseQuestionPage(p)
		if err != nil {
			zap.L().Warn("registry: skipping malformed question page",
				zap.String("page_id", string(p.ID)),
				zap.Error(err),
			)
			continue
		}
		questions = append(questions, q)
	}

	return questions, nil
}

func parseQuestionPage(p notionapi.Page) (model.Question, error) {
	q := model.Question{
		ID: string(p.ID),
	}

	// Text (title)
	if prop, ok := p.Properties["Text"]; ok {
		if tp, ok := prop.(*notionapi.TitleProperty); ok {
			q.Text = plainText(tp.Title)
		}
	}

	// Tier (number)
	if prop, ok := p.Properties["Tier"]; ok {
		if np, ok := prop.(*notionapi.NumberProperty); ok {
			q.Tier = int(np.Number)
		}
	}

	// FieldKey (rich_text)
	if prop, ok := p.Properties["FieldKey"]; ok {
		if rtp, ok := prop.(*notionapi.RichTextProperty); ok {
			q.FieldKey = plainText(rtp.RichText)
		}
	}

	// PageTypes (multi_select)
	if prop, ok := p.Properties["PageTypes"]; ok {
		if msp, ok := prop.(*notionapi.MultiSelectProperty); ok {
			for _, opt := range msp.MultiSelect {
				q.PageTypes = append(q.PageTypes, model.PageType(opt.Name))
			}
		}
	}

	// Instructions (rich_text)
	if prop, ok := p.Properties["Instructions"]; ok {
		if rtp, ok := prop.(*notionapi.RichTextProperty); ok {
			q.Instructions = plainText(rtp.RichText)
		}
	}

	// OutputFormat (select)
	if prop, ok := p.Properties["OutputFormat"]; ok {
		if sp, ok := prop.(*notionapi.SelectProperty); ok {
			q.OutputFormat = sp.Select.Name
		}
	}

	// Status (status)
	if prop, ok := p.Properties["Status"]; ok {
		if sp, ok := prop.(*notionapi.StatusProperty); ok {
			q.Status = sp.Status.Name
		}
	}

	if q.Text == "" {
		return q, eris.New("missing Text property")
	}

	return q, nil
}

// plainText concatenates the plain_text values from a slice of RichText.
func plainText(rts []notionapi.RichText) string {
	var s string
	for _, rt := range rts {
		s += rt.PlainText
	}
	return s
}
