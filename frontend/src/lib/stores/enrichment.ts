import { writable } from "svelte/store";
import type { Run, RunStatus } from "$lib/types";

export interface EnrichmentFilter {
  status: RunStatus | "all";
  search: string;
  sortBy: "created_at" | "score" | "cost" | "duration";
  sortOrder: "asc" | "desc";
}

export const runs = writable<Run[]>([]);
export const selectedRun = writable<Run | null>(null);
export const runFilter = writable<EnrichmentFilter>({
  status: "all",
  search: "",
  sortBy: "created_at",
  sortOrder: "desc",
});
export const runsTotal = writable(0);
export const runsLoading = writable(false);
