package dataset

import (
	"fmt"
	"strings"

	"github.com/sells-group/research-cli/internal/config"
)

// SummaryBlockStart marks the beginning of a generated dataset summary block.
const SummaryBlockStart = "<!-- BEGIN GENERATED DATASET SUMMARY -->"

// SummaryBlockEnd marks the end of a generated dataset summary block.
const SummaryBlockEnd = "<!-- END GENERATED DATASET SUMMARY -->"

// Descriptor describes one registered dataset.
type Descriptor struct {
	Name    string  `json:"name"`
	Table   string  `json:"table"`
	Phase   string  `json:"phase"`
	Cadence Cadence `json:"cadence"`
}

// Count groups dataset totals by a single dimension like phase or cadence.
type Count struct {
	Key   string `json:"key"`
	Count int    `json:"count"`
}

// Summary captures the live dataset inventory derived from the registry.
type Summary struct {
	Total     int          `json:"total"`
	ByPhase   []Count      `json:"by_phase"`
	ByCadence []Count      `json:"by_cadence"`
	Datasets  []Descriptor `json:"datasets"`
}

// BuildSummary returns a live summary of the current dataset registry.
func BuildSummary(cfg *config.Config) Summary {
	registry := NewRegistry(cfg)
	all := registry.All()

	descriptors := make([]Descriptor, 0, len(all))
	phaseCounts := map[string]int{}
	cadenceCounts := map[string]int{}

	for _, ds := range all {
		phase := ds.Phase().String()
		cadence := string(ds.Cadence())
		descriptors = append(descriptors, Descriptor{
			Name:    ds.Name(),
			Table:   ds.Table(),
			Phase:   phase,
			Cadence: ds.Cadence(),
		})
		phaseCounts[phase]++
		cadenceCounts[cadence]++
	}

	return Summary{
		Total:     len(descriptors),
		ByPhase:   orderedCounts(phaseCounts, []string{"1", "1b", "2", "3"}),
		ByCadence: orderedCounts(cadenceCounts, []string{string(Daily), string(Weekly), string(Monthly), string(Quarterly), string(Annual)}),
		Datasets:  descriptors,
	}
}

// RenderMarkdownSummary returns a generated Markdown summary block for docs.
func RenderMarkdownSummary(cfg *config.Config) string {
	summary := BuildSummary(cfg)
	byPhase := datasetsByPhase(summary.Datasets)

	var b strings.Builder
	b.WriteString(SummaryBlockStart + "\n")
	b.WriteString("## Live Fedsync Dataset Summary\n\n")
	fmt.Fprintf(&b, "- Total datasets: %d\n", summary.Total)
	fmt.Fprintf(&b, "- By phase: %s\n", renderCounts(summary.ByPhase))
	fmt.Fprintf(&b, "- By cadence: %s\n\n", renderCounts(summary.ByCadence))
	b.WriteString("| Phase | Datasets |\n")
	b.WriteString("|---|---|\n")
	for _, phase := range []string{"1", "1b", "2", "3"} {
		names := datasetNames(byPhase[phase])
		fmt.Fprintf(&b, "| `%s` | %s |\n", phase, strings.Join(names, ", "))
	}
	b.WriteString(SummaryBlockEnd)
	return b.String()
}

// RenderTextSummary returns a plain-text operator summary.
func RenderTextSummary(cfg *config.Config) string {
	summary := BuildSummary(cfg)

	var b strings.Builder
	fmt.Fprintf(&b, "Fedsync datasets: %d total\n", summary.Total)
	fmt.Fprintf(&b, "By phase: %s\n", renderCounts(summary.ByPhase))
	fmt.Fprintf(&b, "By cadence: %s\n\n", renderCounts(summary.ByCadence))
	b.WriteString("NAME\tPHASE\tCADENCE\tTABLE\n")
	for _, ds := range summary.Datasets {
		fmt.Fprintf(&b, "%s\t%s\t%s\t%s\n", ds.Name, ds.Phase, ds.Cadence, ds.Table)
	}
	return b.String()
}

func orderedCounts(values map[string]int, order []string) []Count {
	counts := make([]Count, 0, len(order))
	for _, key := range order {
		if values[key] == 0 {
			continue
		}
		counts = append(counts, Count{Key: key, Count: values[key]})
	}
	return counts
}

func renderCounts(counts []Count) string {
	parts := make([]string, 0, len(counts))
	for _, count := range counts {
		parts = append(parts, fmt.Sprintf("`%s`=%d", count.Key, count.Count))
	}
	return strings.Join(parts, ", ")
}

func datasetsByPhase(datasets []Descriptor) map[string][]Descriptor {
	grouped := make(map[string][]Descriptor)
	for _, ds := range datasets {
		grouped[ds.Phase] = append(grouped[ds.Phase], ds)
	}
	return grouped
}

func datasetNames(datasets []Descriptor) []string {
	names := make([]string, 0, len(datasets))
	for _, ds := range datasets {
		names = append(names, ds.Name)
	}
	return names
}
