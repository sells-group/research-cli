export interface PhaseConfig {
  name: string;
  label: string;
  color: string;
  icon: string;
}

export const enrichmentPhases: PhaseConfig[] = [
  { name: "crawl", label: "1A: Crawl", color: "text-blue-500", icon: "globe" },
  {
    name: "scrape",
    label: "1B: Scrape",
    color: "text-blue-400",
    icon: "file-text",
  },
  {
    name: "linkedin",
    label: "1C: LinkedIn",
    color: "text-blue-300",
    icon: "linkedin",
  },
  {
    name: "classify",
    label: "2: Classify",
    color: "text-purple-500",
    icon: "tags",
  },
  {
    name: "route",
    label: "3: Route",
    color: "text-purple-400",
    icon: "git-branch",
  },
  {
    name: "extract_t1",
    label: "4: Extract T1",
    color: "text-amber-500",
    icon: "cpu",
  },
  {
    name: "extract_t2",
    label: "5: Extract T2",
    color: "text-amber-400",
    icon: "cpu",
  },
  {
    name: "extract_t3",
    label: "6: Extract T3",
    color: "text-amber-300",
    icon: "cpu",
  },
  {
    name: "aggregate",
    label: "7: Aggregate",
    color: "text-green-500",
    icon: "merge",
  },
  {
    name: "report",
    label: "8: Report",
    color: "text-green-400",
    icon: "file-check",
  },
  {
    name: "gate",
    label: "9: Gate",
    color: "text-green-300",
    icon: "shield-check",
  },
];

export function getPhaseConfig(name: string): PhaseConfig | undefined {
  return enrichmentPhases.find((p) => p.name === name);
}

export const phaseStatusColors: Record<string, string> = {
  running: "text-blue-500",
  complete: "text-green-500",
  failed: "text-red-500",
  skipped: "text-muted-foreground",
};
