package tiger

import (
	"archive/zip"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// maxTigerDecompressSize is the maximum allowed size for a single decompressed TIGER file entry (10 GB).
const maxTigerDecompressSize = 10 << 30

// censusLimiter throttles requests to Census Bureau to avoid 429 rate limiting.
// Allows 1 request per 1.5 seconds across all goroutines.
var censusLimiter = struct {
	once    sync.Once
	limiter *rate.Limiter
}{}

func getCensusLimiter() *rate.Limiter {
	censusLimiter.once.Do(func() {
		// ~0.67 req/s sustained, burst of 2 for quick consecutive hits on cache.
		censusLimiter.limiter = rate.NewLimiter(rate.Every(1500*time.Millisecond), 2)
	})
	return censusLimiter.limiter
}

// Download fetches a TIGER/Line ZIP file from Census Bureau and extracts shapefiles.
// Returns the path to the extracted .shp file.
func Download(ctx context.Context, url, destDir string) (string, error) {
	log := zap.L().With(
		zap.String("component", "tiger.download"),
		zap.String("url", url),
	)

	if err := os.MkdirAll(destDir, 0o750); err != nil {
		return "", eris.Wrap(err, "tiger: create dest dir")
	}

	// Derive ZIP filename from URL.
	parts := strings.Split(url, "/")
	zipName := parts[len(parts)-1]
	zipPath := filepath.Join(destDir, zipName)

	// Skip download if ZIP already exists with valid content.
	if info, err := os.Stat(zipPath); err == nil && info.Size() > 0 {
		if vErr := validateZIP(zipPath); vErr != nil {
			log.Warn("cached zip is invalid, re-downloading", zap.String("path", zipPath), zap.Error(vErr))
			_ = os.Remove(zipPath)
		} else {
			log.Debug("zip already exists, skipping download", zap.String("path", zipPath))
		}
	}

	if _, err := os.Stat(zipPath); err != nil {
		log.Info("downloading TIGER shapefile")
		if err := downloadFile(ctx, url, zipPath); err != nil {
			return "", eris.Wrap(err, "tiger: download shapefile")
		}
	}

	// Extract ZIP.
	extractDir := filepath.Join(destDir, strings.TrimSuffix(zipName, ".zip"))
	if err := os.MkdirAll(extractDir, 0o750); err != nil {
		return "", eris.Wrap(err, "tiger: create extract dir")
	}

	if err := extractZIP(zipPath, extractDir); err != nil {
		return "", eris.Wrap(err, "tiger: extract ZIP")
	}

	// Find the primary data file: .shp for spatial products, .dbf for tabular.
	shpPath, err := findFileByExt(extractDir, ".shp")
	if err != nil {
		// Tabular products (ADDR, FEATNAMES) have no .shp — use .dbf directly.
		dbfPath, dbfErr := findFileByExt(extractDir, ".dbf")
		if dbfErr != nil {
			return "", eris.Wrap(err, "tiger: find .shp or .dbf file")
		}
		return dbfPath, nil
	}

	return shpPath, nil
}

// retryBackoffs defines the wait times between retry attempts for Census downloads.
// Aggressive backoff to respect Census Bureau rate limits.
var retryBackoffs = []time.Duration{5 * time.Second, 15 * time.Second, 45 * time.Second, 90 * time.Second}

// downloadFile downloads a URL to a local file with retry on transient errors (403, 429, 5xx).
func downloadFile(ctx context.Context, url, dest string) error {
	client := &http.Client{Timeout: 10 * time.Minute}
	lim := getCensusLimiter()

	var lastErr error
	for attempt := range len(retryBackoffs) + 1 {
		if attempt > 0 {
			backoff := retryBackoffs[attempt-1]
			zap.L().Debug("retrying download",
				zap.String("url", url), zap.Int("attempt", attempt), zap.Duration("backoff", backoff), zap.Error(lastErr))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}

		// Wait for rate limiter before each attempt.
		if err := lim.Wait(ctx); err != nil {
			return err
		}

		lastErr = doDownload(ctx, client, url, dest)
		if lastErr == nil {
			return nil
		}

		// Only retry on transient HTTP errors (403 rate limit, 429, 5xx).
		if !isRetryable(lastErr) {
			return lastErr
		}
	}
	return lastErr
}

// isRetryable returns true for HTTP errors worth retrying (rate limits, server errors).
func isRetryable(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "status 403") ||
		strings.Contains(msg, "status 429") ||
		strings.Contains(msg, "status 5") ||
		strings.Contains(msg, "not a valid ZIP")
}

// doDownload performs a single download attempt.
func doDownload(ctx context.Context, client *http.Client, url, dest string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return eris.Wrap(err, "build request")
	}

	resp, err := client.Do(req)
	if err != nil {
		return eris.Wrap(err, "download")
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return eris.Errorf("download returned status %d", resp.StatusCode)
	}

	f, err := os.Create(dest) // #nosec G304 -- path from function parameter in internal package
	if err != nil {
		return eris.Wrap(err, "create file")
	}
	defer f.Close() //nolint:errcheck

	if _, err := io.Copy(f, resp.Body); err != nil {
		return eris.Wrap(err, "write file")
	}

	// Validate the downloaded file is actually a ZIP (Census may return HTML error pages).
	if err := validateZIP(dest); err != nil {
		_ = os.Remove(dest)
		return eris.Wrapf(err, "download %s: not a valid ZIP", dest)
	}

	return nil
}

// validateZIP checks the file starts with the ZIP magic bytes (PK\x03\x04).
func validateZIP(path string) error {
	f, err := os.Open(path) // #nosec G304 -- path from internal function
	if err != nil {
		return err
	}
	defer f.Close() //nolint:errcheck

	var magic [4]byte
	if _, err := f.Read(magic[:]); err != nil {
		return eris.Wrap(err, "read magic bytes")
	}
	if magic != [4]byte{'P', 'K', 0x03, 0x04} {
		return eris.New("file does not have ZIP magic bytes")
	}
	return nil
}

// extractZIP extracts a ZIP archive to the destination directory.
func extractZIP(zipPath, destDir string) error {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return eris.Wrap(err, "open zip")
	}
	defer r.Close() //nolint:errcheck

	for _, f := range r.File {
		name := filepath.Base(f.Name)
		destPath := filepath.Join(destDir, name)

		if f.FileInfo().IsDir() {
			continue
		}

		rc, err := f.Open()
		if err != nil {
			return eris.Wrapf(err, "open zip entry %s", f.Name)
		}

		outFile, err := os.Create(destPath) // #nosec G304 -- destPath derived from filepath.Base of zip entry within known destDir
		if err != nil {
			_ = rc.Close()
			return eris.Wrapf(err, "create %s", destPath)
		}

		if _, err := io.Copy(outFile, io.LimitReader(rc, maxTigerDecompressSize)); err != nil {
			_ = outFile.Close()
			_ = rc.Close()
			return eris.Wrapf(err, "extract %s", f.Name)
		}
		_ = outFile.Close()
		_ = rc.Close()
	}

	return nil
}

// findFileByExt finds the first file with the given extension in a directory.
func findFileByExt(dir, ext string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", eris.Wrap(err, "read directory")
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(strings.ToLower(e.Name()), ext) {
			return filepath.Join(dir, e.Name()), nil
		}
	}
	return "", eris.Errorf("no %s file found in %s", ext, dir)
}
