package fetcher

import (
	"context"
	"io"
)

// Fetcher defines the interface for downloading remote data.
type Fetcher interface {
	// Download fetches the URL and returns the response body.
	Download(ctx context.Context, url string) (io.ReadCloser, error)

	// DownloadToFile fetches the URL and writes it to the given path. Returns bytes written.
	DownloadToFile(ctx context.Context, url string, path string) (int64, error)

	// HeadETag performs a HEAD request and returns the ETag header value.
	HeadETag(ctx context.Context, url string) (string, error)

	// DownloadIfChanged fetches the URL only if the ETag has changed.
	// Returns (body, newETag, changed, error). If not changed, body is nil and changed is false.
	DownloadIfChanged(ctx context.Context, url string, etag string) (io.ReadCloser, string, bool, error)
}
