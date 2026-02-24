package ci

import (
	"strings"
	"testing"
)

const testProfile = `mode: atomic
github.com/sells-group/research-cli/internal/pipeline/crawl.go:25.40,30.2 3 5
github.com/sells-group/research-cli/internal/pipeline/crawl.go:32.50,40.2 5 0
github.com/sells-group/research-cli/internal/pipeline/extract.go:10.30,20.2 8 3
github.com/sells-group/research-cli/pkg/anthropic/client.go:15.40,25.2 6 6
github.com/sells-group/research-cli/pkg/anthropic/client.go:27.40,35.2 4 0
github.com/sells-group/research-cli/pkg/anthropic/batch.go:10.30,18.2 7 7
`

func TestParseProfile(t *testing.T) {
	prof, err := ParseProfile(strings.NewReader(testProfile))
	if err != nil {
		t.Fatalf("ParseProfile: %v", err)
	}

	if len(prof.Blocks) != 6 {
		t.Fatalf("expected 6 blocks, got %d", len(prof.Blocks))
	}

	// Verify first block
	b := prof.Blocks[0]
	if b.FileName != "github.com/sells-group/research-cli/internal/pipeline/crawl.go" {
		t.Errorf("FileName = %q", b.FileName)
	}
	if b.StartLine != 25 || b.StartCol != 40 {
		t.Errorf("start = %d.%d, want 25.40", b.StartLine, b.StartCol)
	}
	if b.EndLine != 30 || b.EndCol != 2 {
		t.Errorf("end = %d.%d, want 30.2", b.EndLine, b.EndCol)
	}
	if b.Statements != 3 {
		t.Errorf("Statements = %d, want 3", b.Statements)
	}
	if b.Count != 5 {
		t.Errorf("Count = %d, want 5", b.Count)
	}
}

func TestParseProfileBadMode(t *testing.T) {
	_, err := ParseProfile(strings.NewReader("not-a-mode-line\n"))
	if err == nil {
		t.Fatal("expected error for bad mode line")
	}
	if !strings.Contains(err.Error(), "expected mode line") {
		t.Errorf("error = %v, want 'expected mode line'", err)
	}
}

func TestParseProfileBadBlock(t *testing.T) {
	input := "mode: atomic\ngarbage-line\n"
	_, err := ParseProfile(strings.NewReader(input))
	if err == nil {
		t.Fatal("expected error for bad block")
	}
}

func TestParseProfileEmpty(t *testing.T) {
	prof, err := ParseProfile(strings.NewReader("mode: set\n"))
	if err != nil {
		t.Fatalf("ParseProfile: %v", err)
	}
	if len(prof.Blocks) != 0 {
		t.Fatalf("expected 0 blocks, got %d", len(prof.Blocks))
	}
}

func TestSummarize(t *testing.T) {
	prof, err := ParseProfile(strings.NewReader(testProfile))
	if err != nil {
		t.Fatalf("ParseProfile: %v", err)
	}

	report := prof.Summarize("github.com/sells-group/research-cli")

	if len(report.Packages) != 2 {
		t.Fatalf("expected 2 packages, got %d", len(report.Packages))
	}

	// Packages should be sorted alphabetically
	if report.Packages[0].Package != "internal/pipeline" {
		t.Errorf("first package = %q, want internal/pipeline", report.Packages[0].Package)
	}
	if report.Packages[1].Package != "pkg/anthropic" {
		t.Errorf("second package = %q, want pkg/anthropic", report.Packages[1].Package)
	}

	// internal/pipeline: 3 stmts covered (count>0) + 5 not + 8 covered = 11 covered of 16
	pipe := report.Packages[0]
	if pipe.Statements != 16 {
		t.Errorf("pipeline statements = %d, want 16", pipe.Statements)
	}
	if pipe.Covered != 11 {
		t.Errorf("pipeline covered = %d, want 11", pipe.Covered)
	}

	// pkg/anthropic: 6 covered + 4 not + 7 covered = 13 covered of 17
	anth := report.Packages[1]
	if anth.Statements != 17 {
		t.Errorf("anthropic statements = %d, want 17", anth.Statements)
	}
	if anth.Covered != 13 {
		t.Errorf("anthropic covered = %d, want 13", anth.Covered)
	}

	// Total: 24 covered of 33 = 72.7%
	if report.Total.Statements != 33 {
		t.Errorf("total statements = %d, want 33", report.Total.Statements)
	}
	if report.Total.Covered != 24 {
		t.Errorf("total covered = %d, want 24", report.Total.Covered)
	}

	expectedPct := 72.7
	if report.Total.Percent < expectedPct-0.1 || report.Total.Percent > expectedPct+0.1 {
		t.Errorf("total percent = %.1f, want ~%.1f", report.Total.Percent, expectedPct)
	}
}

func TestSummarizeNoPrefix(t *testing.T) {
	prof, err := ParseProfile(strings.NewReader(testProfile))
	if err != nil {
		t.Fatalf("ParseProfile: %v", err)
	}

	report := prof.Summarize("")
	// Without prefix stripping, package names include full module path
	if report.Packages[0].Package != "github.com/sells-group/research-cli/internal/pipeline" {
		t.Errorf("package = %q, want full path", report.Packages[0].Package)
	}
}

func TestSummarizeEmptyProfile(t *testing.T) {
	prof := &CoverageProfile{}
	report := prof.Summarize("mod")

	if len(report.Packages) != 0 {
		t.Errorf("expected 0 packages, got %d", len(report.Packages))
	}
	if report.Total.Percent != 0 {
		t.Errorf("expected 0%% total, got %.1f%%", report.Total.Percent)
	}
}

func TestCheckThresholdPass(t *testing.T) {
	report := &CoverageReport{
		Total: PackageCoverage{Percent: 65.0},
	}
	if err := CheckThreshold(report, 50.0); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCheckThresholdExact(t *testing.T) {
	report := &CoverageReport{
		Total: PackageCoverage{Percent: 50.0},
	}
	if err := CheckThreshold(report, 50.0); err != nil {
		t.Errorf("unexpected error at exact threshold: %v", err)
	}
}

func TestCheckThresholdFail(t *testing.T) {
	report := &CoverageReport{
		Total: PackageCoverage{Percent: 45.0},
	}
	err := CheckThreshold(report, 50.0)
	if err == nil {
		t.Fatal("expected error for coverage below threshold")
	}
	if !strings.Contains(err.Error(), "45.0%") {
		t.Errorf("error should mention actual coverage: %v", err)
	}
	if !strings.Contains(err.Error(), "50.0%") {
		t.Errorf("error should mention threshold: %v", err)
	}
}

func TestFormatMarkdown(t *testing.T) {
	report := &CoverageReport{
		Packages: []PackageCoverage{
			{Package: "internal/foo", Statements: 100, Covered: 80, Percent: 80.0},
			{Package: "pkg/bar", Statements: 50, Covered: 25, Percent: 50.0},
		},
		Total: PackageCoverage{
			Package:    "total",
			Statements: 150,
			Covered:    105,
			Percent:    70.0,
		},
	}

	md := FormatMarkdown(report)

	if !strings.Contains(md, "## Coverage Report") {
		t.Error("missing header")
	}
	if !strings.Contains(md, "`internal/foo`") {
		t.Error("missing internal/foo package")
	}
	if !strings.Contains(md, "`pkg/bar`") {
		t.Error("missing pkg/bar package")
	}
	if !strings.Contains(md, "80.0%") {
		t.Error("missing 80.0% coverage")
	}
	if !strings.Contains(md, "**70.0%**") {
		t.Error("missing total 70.0%")
	}
	if !strings.Contains(md, "| Package |") {
		t.Error("missing table header")
	}
}

func TestFormatBadgeJSON(t *testing.T) {
	tests := []struct {
		name    string
		percent float64
		color   string
	}{
		{"high", 85.0, "brightgreen"},
		{"medium", 65.0, "green"},
		{"low", 45.0, "yellow"},
		{"poor", 30.0, "red"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := &CoverageReport{
				Total: PackageCoverage{Percent: tt.percent},
			}
			badge := FormatBadgeJSON(report)

			if !strings.Contains(badge, tt.color) {
				t.Errorf("expected color %q in badge: %s", tt.color, badge)
			}
			if !strings.Contains(badge, "schemaVersion") {
				t.Error("missing schemaVersion in badge JSON")
			}
		})
	}
}

func TestPackageName(t *testing.T) {
	tests := []struct {
		filePath string
		prefix   string
		want     string
	}{
		{
			"github.com/foo/bar/internal/pkg/file.go",
			"github.com/foo/bar",
			"internal/pkg",
		},
		{
			"github.com/foo/bar/cmd/main.go",
			"github.com/foo/bar",
			"cmd",
		},
		{
			"main.go",
			"github.com/foo/bar",
			"main.go",
		},
		{
			"github.com/foo/bar/file.go",
			"github.com/foo/bar",
			"github.com/foo/bar",
		},
	}

	for _, tt := range tests {
		t.Run(tt.filePath, func(t *testing.T) {
			got := packageName(tt.filePath, tt.prefix)
			if got != tt.want {
				t.Errorf("packageName(%q, %q) = %q, want %q", tt.filePath, tt.prefix, got, tt.want)
			}
		})
	}
}
