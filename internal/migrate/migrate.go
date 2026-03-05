// Package migrate provides declarative schema management via Atlas.
package migrate

import (
	"context"
	"embed"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"ariga.io/atlas-go-sdk/atlasexec"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"
)

//go:embed all:schema
var schemaFS embed.FS

// Options configures an Apply or Inspect call.
type Options struct {
	URL         string   // Database connection URL (required).
	DevURL      string   // Dev database URL for diffing (required for Apply).
	DryRun      bool     // Preview changes without applying.
	AutoApprove bool     // Skip interactive approval.
	Schemas     []string // Schemas to manage (default: public, fed_data).
	BinaryPath  string   // Path to atlas binary (default: "atlas").
}

// ApplyResult contains the result of a schema apply.
type ApplyResult struct {
	Changes string // SQL changes applied (or planned if DryRun).
	Applied int    // Number of changes applied.
}

// Apply compares the desired schema (embedded SQL files) against the live
// database and applies any necessary changes.
func Apply(ctx context.Context, opts Options) (*ApplyResult, error) {
	if opts.URL == "" {
		return nil, eris.New("migrate: database URL is required")
	}

	schemas := opts.Schemas
	if len(schemas) == 0 {
		schemas = []string{"public", "fed_data"}
	}

	binaryPath := opts.BinaryPath
	if binaryPath == "" {
		binaryPath = "atlas"
	}

	// Write embedded schema to a temp directory so atlas can read it.
	workDir, err := writeSchemaToTemp()
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(workDir) //nolint:errcheck

	client, err := atlasexec.NewClient(workDir, binaryPath)
	if err != nil {
		return nil, eris.Wrap(err, "migrate: create atlas client")
	}

	params := &atlasexec.SchemaApplyParams{
		Env: "prod",
		Vars: atlasexec.Vars2{
			"url":     opts.URL,
			"dev_url": opts.DevURL,
		},
		Schema:      schemas,
		DryRun:      opts.DryRun,
		AutoApprove: opts.AutoApprove,
	}

	zap.L().Info("applying schema",
		zap.Strings("schemas", schemas),
		zap.Bool("dry_run", opts.DryRun),
	)

	result, err := client.SchemaApply(ctx, params)
	if err != nil {
		return nil, eris.Wrap(err, "migrate: schema apply")
	}

	applied := &ApplyResult{}
	if result != nil {
		applied.Changes = strings.Join(result.Changes.Pending, "\n")
		// Count non-empty applied changes.
		for _, c := range result.Changes.Applied {
			if c != "" {
				applied.Applied++
			}
		}
	}

	zap.L().Info("schema apply complete",
		zap.Int("changes_applied", applied.Applied),
		zap.Bool("dry_run", opts.DryRun),
	)

	return applied, nil
}

// Inspect returns the current database schema as HCL.
func Inspect(ctx context.Context, opts Options) (string, error) {
	if opts.URL == "" {
		return "", eris.New("migrate: database URL is required")
	}

	schemas := opts.Schemas
	if len(schemas) == 0 {
		schemas = []string{"public", "fed_data"}
	}

	binaryPath := opts.BinaryPath
	if binaryPath == "" {
		binaryPath = "atlas"
	}

	client, err := atlasexec.NewClient(".", binaryPath)
	if err != nil {
		return "", eris.Wrap(err, "migrate: create atlas client")
	}

	result, err := client.SchemaInspect(ctx, &atlasexec.SchemaInspectParams{
		URL:    opts.URL,
		Schema: schemas,
	})
	if err != nil {
		return "", eris.Wrap(err, "migrate: schema inspect")
	}

	return result, nil
}

// buildSrcURLs returns the list of file:// URLs matching atlas.hcl src config.
func buildSrcURLs() []string {
	return []string{
		"file://extensions.sql",
		"file://public.sql",
		"file://fed_data.sql",
	}
}

// writeSchemaToTemp copies the embedded schema FS to a temp directory.
func writeSchemaToTemp() (string, error) {
	tmpDir, err := os.MkdirTemp("", "atlas-schema-*")
	if err != nil {
		return "", eris.Wrap(err, "migrate: create temp dir")
	}

	if err := copyEmbeddedSchema(tmpDir); err != nil {
		_ = os.RemoveAll(tmpDir)
		return "", err
	}

	return tmpDir, nil
}

// copyEmbeddedSchema walks the embedded schema FS and writes files to destDir.
func copyEmbeddedSchema(destDir string) error {
	const prefix = "schema/"
	return fs.WalkDir(schemaFS, "schema", func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == "schema" {
			return nil // skip the root entry itself
		}
		// Strip "schema/" prefix so files land at the dest root.
		relPath := path[len(prefix):]
		destPath := filepath.Join(destDir, relPath)
		if d.IsDir() {
			return os.MkdirAll(destPath, 0o750) //nolint:gosec
		}
		data, err := schemaFS.ReadFile(path)
		if err != nil {
			return eris.Wrapf(err, "migrate: read embedded %s", path)
		}
		return os.WriteFile(destPath, data, 0o600) //nolint:gosec
	})
}
