package main

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"

	"github.com/sells-group/research-cli/internal/fedsync/dataset"
)

const (
	generateTargetDocs     = "docs"
	generateTargetFrontend = "frontend"
)

var fedsyncGenerateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate dataset artifacts for docs and frontend",
	RunE: func(cmd *cobra.Command, _ []string) error {
		targetsValue, _ := cmd.Flags().GetString("targets")
		check, _ := cmd.Flags().GetBool("check")

		targets, err := parseGenerateTargets(targetsValue)
		if err != nil {
			return err
		}

		if err := dataset.ValidateCatalog(cfg); err != nil {
			return err
		}

		if targets[generateTargetFrontend] {
			rendered, renderErr := dataset.RenderTypeScriptCatalog(cfg)
			if renderErr != nil {
				return renderErr
			}
			if err := syncFrontendCatalog(rendered, check); err != nil {
				return err
			}
			reportGenerationStatus(cmd, check, frontendCatalogPath())
		}

		if targets[generateTargetDocs] {
			rendered := dataset.RenderMarkdownSummary(cfg)
			if err := syncReadmeSummary(rendered, check); err != nil {
				return err
			}
			reportGenerationStatus(cmd, check, "README.md")
			if err := syncAgentsSummary(rendered, check); err != nil {
				return err
			}
			reportGenerationStatus(cmd, check, "AGENTS.md")
		}

		return nil
	},
}

func init() {
	fedsyncGenerateCmd.Flags().String("targets", generateTargetDocs+","+generateTargetFrontend, "comma-separated generation targets: docs, frontend")
	fedsyncGenerateCmd.Flags().Bool("check", false, "fail if generated output is out of date")
	fedsyncCmd.AddCommand(fedsyncGenerateCmd)
}

func frontendCatalogPath() string {
	return filepath.Join("frontend", "src", "lib", "config", "datasets.generated.ts")
}

func parseGenerateTargets(raw string) (map[string]bool, error) {
	targets := map[string]bool{}
	for _, part := range strings.Split(raw, ",") {
		target := strings.TrimSpace(part)
		if target == "" {
			continue
		}

		switch target {
		case generateTargetDocs, generateTargetFrontend:
			targets[target] = true
		default:
			return nil, eris.Errorf("unknown generate target %q (valid: %s, %s)", target, generateTargetDocs, generateTargetFrontend)
		}
	}

	if len(targets) == 0 {
		return nil, eris.New("no generation targets selected")
	}
	return targets, nil
}

func syncFrontendCatalog(expected string, check bool) error {
	const path = "frontend/src/lib/config/datasets.generated.ts"

	current, err := os.ReadFile(path) // #nosec G304 -- fixed generated artifact path
	if err != nil && !os.IsNotExist(err) {
		return eris.Wrapf(err, "read generated file %s", path)
	}

	if string(current) == expected {
		return nil
	}
	if check {
		return eris.Errorf("%s is out of date; run `research-cli fedsync generate`", path)
	}

	if err := os.WriteFile(path, []byte(expected), 0o600); err != nil { // #nosec G306 -- checked-in generated source file
		return eris.Wrapf(err, "write generated file %s", path)
	}
	return nil
}

func syncReadmeSummary(rendered string, check bool) error {
	return syncSummaryFile("README.md", rendered, check)
}

func syncAgentsSummary(rendered string, check bool) error {
	return syncSummaryFile("AGENTS.md", rendered, check)
}

func syncSummaryFile(path string, rendered string, check bool) error {
	current, err := readSummaryFile(path)
	if err != nil {
		return eris.Wrapf(err, "read summary file %s", path)
	}

	updated, err := dataset.ReplaceSummaryBlock(string(current), rendered)
	if err != nil {
		return eris.Wrapf(err, "replace summary block in %s", path)
	}

	if updated == string(current) {
		return nil
	}
	if check {
		return eris.Errorf("%s has an out-of-date dataset summary; run `research-cli fedsync generate`", path)
	}

	if err := writeSummaryFile(path, updated); err != nil {
		return eris.Wrapf(err, "write summary file %s", path)
	}
	return nil
}

func readSummaryFile(path string) ([]byte, error) {
	switch path {
	case "README.md":
		return os.ReadFile("README.md") // #nosec G304 -- fixed documentation path
	case "AGENTS.md":
		return os.ReadFile("AGENTS.md") // #nosec G304 -- fixed documentation path
	default:
		return nil, eris.Errorf("unsupported summary path %s", path)
	}
}

func writeSummaryFile(path string, updated string) error {
	switch path {
	case "README.md":
		return os.WriteFile("README.md", []byte(updated), 0o600) // #nosec G306 -- checked-in documentation file
	case "AGENTS.md":
		return os.WriteFile("AGENTS.md", []byte(updated), 0o600) // #nosec G306 -- checked-in documentation file
	default:
		return eris.Errorf("unsupported summary path %s", path)
	}
}

func reportGenerationStatus(cmd *cobra.Command, check bool, path string) {
	if check {
		printOutputf(cmd, "checked %s\n", path)
		return
	}
	printOutputf(cmd, "updated %s\n", path)
}
