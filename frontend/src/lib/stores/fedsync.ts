import { writable } from "svelte/store";
import type { DatasetStatus, SyncEntry } from "$lib/types";

export const datasets = writable<DatasetStatus[]>([]);
export const syncLog = writable<SyncEntry[]>([]);
export const selectedDataset = writable<string | null>(null);
export const datasetsLoading = writable(false);
