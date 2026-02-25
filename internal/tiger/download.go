package tiger

import (
	"archive/zip"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
)

// Download fetches a TIGER/Line ZIP file from Census Bureau and extracts shapefiles.
// Returns the path to the extracted .shp file.
func Download(ctx context.Context, url, destDir string) (string, error) {
	log := zap.L().With(
		zap.String("component", "tiger.download"),
		zap.String("url", url),
	)

	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return "", eris.Wrap(err, "tiger: create dest dir")
	}

	// Derive ZIP filename from URL.
	parts := strings.Split(url, "/")
	zipName := parts[len(parts)-1]
	zipPath := filepath.Join(destDir, zipName)

	// Skip download if ZIP already exists with content.
	if info, err := os.Stat(zipPath); err == nil && info.Size() > 0 {
		log.Debug("zip already exists, skipping download", zap.String("path", zipPath))
	} else {
		log.Info("downloading TIGER shapefile")
		if err := downloadFile(ctx, url, zipPath); err != nil {
			return "", eris.Wrap(err, "tiger: download shapefile")
		}
	}

	// Extract ZIP.
	extractDir := filepath.Join(destDir, strings.TrimSuffix(zipName, ".zip"))
	if err := os.MkdirAll(extractDir, 0o755); err != nil {
		return "", eris.Wrap(err, "tiger: create extract dir")
	}

	if err := extractZIP(zipPath, extractDir); err != nil {
		return "", eris.Wrap(err, "tiger: extract ZIP")
	}

	// Find the .shp file.
	shpPath, err := findFileByExt(extractDir, ".shp")
	if err != nil {
		return "", eris.Wrap(err, "tiger: find .shp file")
	}

	return shpPath, nil
}

// downloadFile downloads a URL to a local file.
func downloadFile(ctx context.Context, url, dest string) error {
	client := &http.Client{Timeout: 10 * time.Minute}

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

	f, err := os.Create(dest)
	if err != nil {
		return eris.Wrap(err, "create file")
	}
	defer f.Close() //nolint:errcheck

	if _, err := io.Copy(f, resp.Body); err != nil {
		return eris.Wrap(err, "write file")
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

		outFile, err := os.Create(destPath)
		if err != nil {
			_ = rc.Close()
			return eris.Wrapf(err, "create %s", destPath)
		}

		if _, err := io.Copy(outFile, rc); err != nil {
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
