import { writable } from "svelte/store";
import type { CompanyRecord, Run, DatasetStatus } from "$lib/types";

export interface SelectionState {
  company: CompanyRecord | null;
  run: Run | null;
  dataset: string | null;
  fedEntity: { table: string; id: number } | null;
}

export const selection = writable<SelectionState>({
  company: null,
  run: null,
  dataset: null,
  fedEntity: null,
});

/** Navigate to a company across views */
export function selectCompany(company: CompanyRecord) {
  selection.update((s) => ({ ...s, company }));
}

/** Navigate to an enrichment run */
export function selectRun(run: Run) {
  selection.update((s) => ({ ...s, run }));
}

/** Navigate to a dataset */
export function selectDataset(name: string) {
  selection.update((s) => ({ ...s, dataset: name }));
}
