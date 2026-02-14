package scrape

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/pkg/jina"
	jinamocks "github.com/sells-group/research-cli/pkg/jina/mocks"
)

func TestJinaAdapter_Name(t *testing.T) {
	t.Parallel()
	adapter := NewJinaAdapter(jinamocks.NewMockClient(t))
	assert.Equal(t, "jina", adapter.Name())
}

func TestJinaAdapter_Supports(t *testing.T) {
	t.Parallel()
	adapter := NewJinaAdapter(jinamocks.NewMockClient(t))
	assert.True(t, adapter.Supports("https://example.com"))
	assert.True(t, adapter.Supports(""))
}

func TestJinaAdapter_Scrape_Success(t *testing.T) {
	t.Parallel()
	mock := jinamocks.NewMockClient(t)
	adapter := NewJinaAdapter(mock)

	mock.EXPECT().Read(context.Background(), "https://acme.com").Return(&jina.ReadResponse{
		Code: 200,
		Data: jina.ReadData{
			URL:     "https://acme.com",
			Title:   "Acme Corp",
			Content: "# Acme Corp\n\nWe build things and do stuff for people around the world. This is a long enough content string to pass the needsFallback check which requires 100 chars.",
			Usage:   jina.ReadUsage{Tokens: 500},
		},
	}, nil)

	result, err := adapter.Scrape(context.Background(), "https://acme.com")
	require.NoError(t, err)
	assert.Equal(t, "jina", result.Source)
	assert.Equal(t, "https://acme.com", result.Page.URL)
	assert.Equal(t, "Acme Corp", result.Page.Title)
	assert.Equal(t, 200, result.Page.StatusCode)
}

func TestJinaAdapter_Scrape_ClientError(t *testing.T) {
	t.Parallel()
	mock := jinamocks.NewMockClient(t)
	adapter := NewJinaAdapter(mock)

	mock.EXPECT().Read(context.Background(), "https://fail.com").Return(nil, errors.New("connection refused"))

	_, err := adapter.Scrape(context.Background(), "https://fail.com")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
}

func TestJinaAdapter_Scrape_NeedsFallback(t *testing.T) {
	t.Parallel()
	mock := jinamocks.NewMockClient(t)
	adapter := NewJinaAdapter(mock)

	mock.EXPECT().Read(context.Background(), "https://blocked.com").Return(&jina.ReadResponse{
		Code: 200,
		Data: jina.ReadData{
			URL:     "https://blocked.com",
			Title:   "",
			Content: "short",
		},
	}, nil)

	_, err := adapter.Scrape(context.Background(), "https://blocked.com")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "needs fallback")
}

func TestNeedsFallback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		resp *jina.ReadResponse
		want bool
	}{
		{
			name: "nil response",
			resp: nil,
			want: true,
		},
		{
			name: "non-200 code",
			resp: &jina.ReadResponse{Code: 403},
			want: true,
		},
		{
			name: "short content",
			resp: &jina.ReadResponse{
				Code: 200,
				Data: jina.ReadData{Content: "too short"},
			},
			want: true,
		},
		{
			name: "challenge signature in short content",
			resp: &jina.ReadResponse{
				Code: 200,
				Data: jina.ReadData{
					Content: "Checking your browser before accessing this site. Please enable JavaScript and cookies to continue.",
				},
			},
			want: true,
		},
		{
			name: "cloudflare in short content",
			resp: &jina.ReadResponse{
				Code: 200,
				Data: jina.ReadData{
					Content: "Attention Required! Cloudflare security check. Enable JavaScript and cookies to continue browsing this site.",
				},
			},
			want: true,
		},
		{
			name: "valid long content",
			resp: &jina.ReadResponse{
				Code: 200,
				Data: jina.ReadData{
					Content: "This is valid content that is long enough to pass the minimum length check. " +
						"It does not contain any challenge signatures and should be considered valid content for extraction. " +
						"Adding more text to make sure we are well over the 100 character minimum threshold.",
				},
			},
			want: false,
		},
		{
			name: "challenge signature in long content over 1000 chars is ok",
			resp: &jina.ReadResponse{
				Code: 200,
				Data: jina.ReadData{
					Content: makeLongContent("This page mentions cloudflare somewhere but has lots of real content."),
				},
			},
			want: false,
		},
		{
			name: "code 0 is acceptable",
			resp: &jina.ReadResponse{
				Code: 0,
				Data: jina.ReadData{
					Content: "This is valid content that is long enough to pass the minimum length check. " +
						"More text here to fill up the 100 character requirement for the content to be considered valid.",
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, needsFallback(tt.resp))
		})
	}
}

// makeLongContent creates a string > 1000 chars that includes the given prefix.
func makeLongContent(prefix string) string {
	content := prefix
	for len(content) < 1100 {
		content += " This is filler content to make the string longer than the 1000 character threshold."
	}
	return content
}
