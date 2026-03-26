import { writable } from "svelte/store";

export interface GlobalFilters {
  search: string;
  dateRange: { start: string; end: string } | null;
  phase: string;
  status: string;
}

export const globalFilters = writable<GlobalFilters>({
  search: "",
  dateRange: null,
  phase: "",
  status: "",
});
