import { writable } from "svelte/store";
import type { CompanyRecord } from "$lib/types";

export interface CompanyFilter {
  search: string;
  state: string;
  identifierType: string;
  scoreMin: number | null;
  scoreMax: number | null;
  sortBy: string;
  sortOrder: "asc" | "desc";
}

export const companies = writable<CompanyRecord[]>([]);
export const selectedCompany = writable<CompanyRecord | null>(null);
export const companyFilter = writable<CompanyFilter>({
  search: "",
  state: "",
  identifierType: "",
  scoreMin: null,
  scoreMax: null,
  sortBy: "name",
  sortOrder: "asc",
});
export const companiesTotal = writable(0);
export const companiesLoading = writable(false);
