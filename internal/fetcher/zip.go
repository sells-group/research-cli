package fetcher

import (
	"archive/zip"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/rotisserie/eris"
)

// ExtractZIP extracts all files from a ZIP archive to the destination directory.
// Returns the list of extracted file paths.
func ExtractZIP(zipPath, destDir string) ([]string, error) {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, eris.Wrap(err, "zip: open archive")
	}
	defer r.Close() //nolint:errcheck

	var extracted []string
	for _, f := range r.File {
		path, err := extractZIPEntry(f, destDir)
		if err != nil {
			return extracted, err
		}
		if path != "" {
			extracted = append(extracted, path)
		}
	}

	return extracted, nil
}

// ExtractZIPFile extracts a single file from a ZIP archive by name.
// Returns the path to the extracted file.
func ExtractZIPFile(zipPath, fileName, destDir string) (string, error) {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return "", eris.Wrap(err, "zip: open archive")
	}
	defer r.Close() //nolint:errcheck

	for _, f := range r.File {
		if f.Name == fileName {
			return extractZIPEntry(f, destDir)
		}
	}

	return "", eris.Errorf("zip: file %q not found in archive", fileName)
}

// ExtractZIPSingle extracts the single file from a ZIP that contains exactly one file.
func ExtractZIPSingle(zipPath, destDir string) (string, error) {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return "", eris.Wrap(err, "zip: open archive")
	}
	defer r.Close() //nolint:errcheck

	// Filter to only files (skip directories)
	var files []*zip.File
	for _, f := range r.File {
		if !f.FileInfo().IsDir() {
			files = append(files, f)
		}
	}

	if len(files) != 1 {
		return "", eris.Errorf("zip: expected exactly 1 file, got %d", len(files))
	}

	return extractZIPEntry(files[0], destDir)
}

// extractZIPEntry extracts a single zip.File to the destination directory.
// Returns the extracted file path, or empty string for directories.
func extractZIPEntry(f *zip.File, destDir string) (string, error) {
	// Sanitize against zip slip
	destPath := filepath.Join(destDir, f.Name)
	if !strings.HasPrefix(filepath.Clean(destPath), filepath.Clean(destDir)+string(os.PathSeparator)) {
		return "", eris.Errorf("zip: illegal path %q (zip slip attempt)", f.Name)
	}

	if f.FileInfo().IsDir() {
		if err := os.MkdirAll(destPath, 0o755); err != nil {
			return "", eris.Wrap(err, "zip: create directory")
		}
		return "", nil
	}

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return "", eris.Wrap(err, "zip: create parent directory")
	}

	rc, err := f.Open()
	if err != nil {
		return "", eris.Wrap(err, "zip: open entry")
	}
	defer rc.Close() //nolint:errcheck

	out, err := os.Create(destPath)
	if err != nil {
		return "", eris.Wrap(err, "zip: create file")
	}
	defer out.Close() //nolint:errcheck

	if _, err := io.Copy(out, rc); err != nil {
		return "", eris.Wrap(err, "zip: write file")
	}

	return destPath, nil
}
