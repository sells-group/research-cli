package registry

import (
	"context"

	"github.com/jomei/notionapi"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/notion"
)

// LoadFieldRegistry queries the Notion Field Registry database for all active
// field mappings and returns an indexed FieldRegistry.
func LoadFieldRegistry(ctx context.Context, client notion.Client, dbID string) (*model.FieldRegistry, error) {
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
		return nil, eris.Wrap(err, "registry: load field registry")
	}

	var fields []model.FieldMapping
	for _, p := range pages {
		f, err := parseFieldPage(p)
		if err != nil {
			zap.L().Warn("registry: skipping malformed field page",
				zap.String("page_id", string(p.ID)),
				zap.Error(err),
			)
			continue
		}
		fields = append(fields, f)
	}

	return model.NewFieldRegistry(fields), nil
}

func parseFieldPage(p notionapi.Page) (model.FieldMapping, error) {
	f := model.FieldMapping{
		ID: string(p.ID),
	}

	// Key (title)
	if prop, ok := p.Properties["Key"]; ok {
		if tp, ok := prop.(*notionapi.TitleProperty); ok {
			f.Key = plainText(tp.Title)
		}
	}

	// SFField (rich_text)
	if prop, ok := p.Properties["SFField"]; ok {
		if rtp, ok := prop.(*notionapi.RichTextProperty); ok {
			f.SFField = plainText(rtp.RichText)
		}
	}

	// SFObject (select)
	if prop, ok := p.Properties["SFObject"]; ok {
		if sp, ok := prop.(*notionapi.SelectProperty); ok {
			f.SFObject = sp.Select.Name
		}
	}

	// DataType (select)
	if prop, ok := p.Properties["DataType"]; ok {
		if sp, ok := prop.(*notionapi.SelectProperty); ok {
			f.DataType = sp.Select.Name
		}
	}

	// Required (checkbox)
	if prop, ok := p.Properties["Required"]; ok {
		if cp, ok := prop.(*notionapi.CheckboxProperty); ok {
			f.Required = cp.Checkbox
		}
	}

	// MaxLength (number)
	if prop, ok := p.Properties["MaxLength"]; ok {
		if np, ok := prop.(*notionapi.NumberProperty); ok {
			f.MaxLength = int(np.Number)
		}
	}

	// Validation (rich_text)
	if prop, ok := p.Properties["Validation"]; ok {
		if rtp, ok := prop.(*notionapi.RichTextProperty); ok {
			f.Validation = plainText(rtp.RichText)
		}
	}

	// Status (status)
	if prop, ok := p.Properties["Status"]; ok {
		if sp, ok := prop.(*notionapi.StatusProperty); ok {
			f.Status = sp.Status.Name
		}
	}

	if f.Key == "" {
		return f, eris.New("missing Key property")
	}

	return f, nil
}
