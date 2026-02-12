package anthropic

import (
	"context"

	"github.com/rotisserie/eris"
)

// BuildCachedSystemBlocks constructs system content blocks with a cache
// breakpoint set to a 1-hour TTL. This is used for the primer strategy:
// send one sequential request to warm the cache, then submit batch requests
// that hit the warm cache.
func BuildCachedSystemBlocks(text string) []SystemBlock {
	return []SystemBlock{
		{
			Text: text,
			CacheControl: &CacheControl{
				TTL: "1h",
			},
		},
	}
}

// PrimerRequest sends a single message with the given request to warm the
// prompt cache. The request should include system blocks built with
// BuildCachedSystemBlocks. Returns the response (which can be discarded)
// or an error.
func PrimerRequest(ctx context.Context, client Client, req MessageRequest) (*MessageResponse, error) {
	resp, err := client.CreateMessage(ctx, req)
	if err != nil {
		return nil, eris.Wrap(err, "anthropic: primer request")
	}
	return resp, nil
}
