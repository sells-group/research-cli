// Package ci provides coverage analysis and CI integration tools.
package ci

import (
	"bufio"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/rotisserie/eris"
)

// CoverageProfile represents a parsed Go coverage profile.
type CoverageProfile struct {
	Blocks []CoverageBlock
}

// CoverageBlock represents a single coverage block from a coverage.out file.
type CoverageBlock struct {
	FileName   string
	StartLine  int
	StartCol   int
	EndLine    int
	EndCol     int
	Statements int
	Count      int
}

// PackageCoverage holds aggregated coverage data for a single Go package.
type PackageCoverage struct {
	Package    string
	Statements int
	Covered    int
	Percent    float64
}

// CoverageReport holds the full coverage analysis result.
type CoverageReport struct {
	Packages []PackageCoverage
	Total    PackageCoverage
}

// ParseProfile reads a Go coverage profile (coverage.out) and returns
// the parsed blocks. The first line (mode line) is skipped.
func ParseProfile(r io.Reader) (*CoverageProfile, error) {
	scanner := bufio.NewScanner(r)
	var blocks []CoverageBlock
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// Skip the mode line (first line: "mode: atomic" or "mode: set")
		if lineNum == 1 {
			if !strings.HasPrefix(line, "mode:") {
				return nil, eris.Errorf("coverage: expected mode line, got %q", line)
			}
			continue
		}

		if line == "" {
			continue
		}

		block, err := parseBlock(line)
		if err != nil {
			return nil, eris.Wrapf(err, "coverage: line %d", lineNum)
		}
		blocks = append(blocks, block)
	}

	if err := scanner.Err(); err != nil {
		return nil, eris.Wrap(err, "coverage: reading profile")
	}

	return &CoverageProfile{Blocks: blocks}, nil
}

// parseBlock parses a single coverage block line.
// Format: "file.go:startLine.startCol,endLine.endCol statements count"
func parseBlock(line string) (CoverageBlock, error) {
	// Split "file:start,end stmts count"
	lastSpace := strings.LastIndex(line, " ")
	if lastSpace < 0 {
		return CoverageBlock{}, eris.Errorf("bad format: %q", line)
	}
	countStr := line[lastSpace+1:]
	rest := line[:lastSpace]

	secondLastSpace := strings.LastIndex(rest, " ")
	if secondLastSpace < 0 {
		return CoverageBlock{}, eris.Errorf("bad format: %q", line)
	}
	stmtStr := rest[secondLastSpace+1:]
	fileRange := rest[:secondLastSpace]

	count, err := strconv.Atoi(countStr)
	if err != nil {
		return CoverageBlock{}, eris.Wrapf(err, "bad count in %q", line)
	}
	stmts, err := strconv.Atoi(stmtStr)
	if err != nil {
		return CoverageBlock{}, eris.Wrapf(err, "bad statements in %q", line)
	}

	// Split "file:startLine.startCol,endLine.endCol"
	colonIdx := strings.LastIndex(fileRange, ":")
	if colonIdx < 0 {
		return CoverageBlock{}, eris.Errorf("bad format: no colon in %q", fileRange)
	}
	fileName := fileRange[:colonIdx]
	positions := fileRange[colonIdx+1:]

	parts := strings.Split(positions, ",")
	if len(parts) != 2 {
		return CoverageBlock{}, eris.Errorf("bad position format in %q", positions)
	}

	startParts := strings.Split(parts[0], ".")
	endParts := strings.Split(parts[1], ".")
	if len(startParts) != 2 || len(endParts) != 2 {
		return CoverageBlock{}, eris.Errorf("bad position format in %q", positions)
	}

	startLine, err := strconv.Atoi(startParts[0])
	if err != nil {
		return CoverageBlock{}, eris.Wrapf(err, "bad start line")
	}
	startCol, err := strconv.Atoi(startParts[1])
	if err != nil {
		return CoverageBlock{}, eris.Wrapf(err, "bad start col")
	}
	endLine, err := strconv.Atoi(endParts[0])
	if err != nil {
		return CoverageBlock{}, eris.Wrapf(err, "bad end line")
	}
	endCol, err := strconv.Atoi(endParts[1])
	if err != nil {
		return CoverageBlock{}, eris.Wrapf(err, "bad end col")
	}

	return CoverageBlock{
		FileName:   fileName,
		StartLine:  startLine,
		StartCol:   startCol,
		EndLine:    endLine,
		EndCol:     endCol,
		Statements: stmts,
		Count:      count,
	}, nil
}

// Summarize aggregates coverage blocks into per-package summaries and a total.
func (p *CoverageProfile) Summarize(modulePrefix string) *CoverageReport {
	pkgMap := make(map[string]*PackageCoverage)

	for _, b := range p.Blocks {
		pkg := packageName(b.FileName, modulePrefix)
		pc, ok := pkgMap[pkg]
		if !ok {
			pc = &PackageCoverage{Package: pkg}
			pkgMap[pkg] = pc
		}
		pc.Statements += b.Statements
		if b.Count > 0 {
			pc.Covered += b.Statements
		}
	}

	var packages []PackageCoverage
	totalStmts := 0
	totalCovered := 0

	for _, pc := range pkgMap {
		if pc.Statements > 0 {
			pc.Percent = float64(pc.Covered) / float64(pc.Statements) * 100
		}
		packages = append(packages, *pc)
		totalStmts += pc.Statements
		totalCovered += pc.Covered
	}

	sort.Slice(packages, func(i, j int) bool {
		return packages[i].Package < packages[j].Package
	})

	var totalPct float64
	if totalStmts > 0 {
		totalPct = float64(totalCovered) / float64(totalStmts) * 100
	}

	return &CoverageReport{
		Packages: packages,
		Total: PackageCoverage{
			Package:    "total",
			Statements: totalStmts,
			Covered:    totalCovered,
			Percent:    totalPct,
		},
	}
}

// packageName extracts the package path from a file path, stripping the
// module prefix for readability.
func packageName(filePath, modulePrefix string) string {
	// coverage.out files use "module/path/file.go"
	lastSlash := strings.LastIndex(filePath, "/")
	if lastSlash < 0 {
		return filePath
	}
	pkg := filePath[:lastSlash]
	if modulePrefix != "" {
		pkg = strings.TrimPrefix(pkg, modulePrefix+"/")
	}
	return pkg
}

// CheckThreshold returns an error if total coverage is below the threshold.
func CheckThreshold(report *CoverageReport, threshold float64) error {
	if report.Total.Percent < threshold {
		return eris.Errorf(
			"coverage %.1f%% is below threshold %.1f%%",
			report.Total.Percent, threshold,
		)
	}
	return nil
}

// FormatMarkdown produces a Markdown table summarizing coverage by package.
func FormatMarkdown(report *CoverageReport) string {
	var sb strings.Builder

	sb.WriteString("## Coverage Report\n\n")
	sb.WriteString("| Package | Statements | Covered | Coverage |\n")
	sb.WriteString("|:--------|----------:|---------:|---------:|\n")

	for _, pkg := range report.Packages {
		fmt.Fprintf(&sb, "| `%s` | %d | %d | %.1f%% |\n",
			pkg.Package, pkg.Statements, pkg.Covered, pkg.Percent)
	}

	fmt.Fprintf(&sb, "| **Total** | **%d** | **%d** | **%.1f%%** |\n",
		report.Total.Statements, report.Total.Covered, report.Total.Percent)

	return sb.String()
}

// FormatBadgeJSON produces a shields.io endpoint badge JSON.
func FormatBadgeJSON(report *CoverageReport) string {
	color := "red"
	switch {
	case report.Total.Percent >= 80:
		color = "brightgreen"
	case report.Total.Percent >= 60:
		color = "green"
	case report.Total.Percent >= 40:
		color = "yellow"
	}

	return fmt.Sprintf(
		`{"schemaVersion":1,"label":"coverage","message":"%.1f%%","color":"%s"}`,
		report.Total.Percent, color,
	)
}
