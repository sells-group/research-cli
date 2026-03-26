const BASE = "/api/v1";

const cache = new Map<string, { data: any; expires: number }>();
const inflight = new Map<string, Promise<any>>();

export const cacheTTLs: Record<string, number> = {
  "/metrics": 30 * 1000,
  "/fedsync/statuses": 60 * 1000,
  "/data/tables": 5 * 60 * 1000,
  "/analytics/": 2 * 60 * 1000,
  "/runs": 15 * 1000,
  "/queue/status": 15 * 1000,
};

export function getCacheTTL(path: string): number {
  for (const [pattern, ttl] of Object.entries(cacheTTLs)) {
    if (pattern.endsWith("/")) {
      // Prefix match (e.g. '/analytics/' matches '/analytics/sync-trends').
      if (path.includes(pattern)) return ttl;
    } else {
      // Exact match — path must equal pattern or have query params only.
      if (path === pattern) return ttl;
    }
  }
  return 0;
}

/** Clear the in-memory cache and inflight map (for testing). */
export function _resetCache(): void {
  cache.clear();
  inflight.clear();
}

async function get<T>(
  path: string,
  params?: Record<string, string>,
): Promise<T> {
  const url = new URL(path, window.location.origin);
  url.pathname = BASE + path;
  if (params) {
    Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
  }
  const key = url.toString();
  const ttl = getCacheTTL(path);

  if (ttl > 0) {
    const cached = cache.get(key);
    if (cached && cached.expires > Date.now()) return cached.data as T;

    const existing = inflight.get(key);
    if (existing) return existing as Promise<T>;
  }

  const promise = fetch(key)
    .then(async (res) => {
      if (!res.ok) throw new Error(`API error: ${res.status}`);
      const data = await res.json();
      if (ttl > 0) cache.set(key, { data, expires: Date.now() + ttl });
      return data;
    })
    .finally(() => {
      inflight.delete(key);
    });

  if (ttl > 0) inflight.set(key, promise);
  return promise;
}

async function post<T>(path: string, body?: any): Promise<T> {
  const res = await fetch(BASE + path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

// Import types
import type {
  Run,
  RunResult,
  FieldProvenance,
  MetricsSnapshot,
  CompanyRecord,
  Identifier,
  Address,
  AddressMSA,
  Match,
  DatasetStatus,
  SyncEntry,
  TableMeta,
  DataQueryResult,
  DataAggregateResult,
  SyncTrend,
  IdentifierCoverage,
  XrefCoverage,
  EnrichmentStats,
  CostBreakdown,
} from "./types";

export const api = {
  // Health
  health() {
    return get<{ status: string }>("/health");
  },

  // Metrics / Dashboard
  metrics() {
    return get<MetricsSnapshot>("/metrics");
  },

  // Enrichment Runs
  listRuns(params?: Record<string, string>) {
    return get<{ runs: Run[]; total: number }>("/runs", params);
  },

  getRun(id: string) {
    return get<Run>(`/runs/${id}`);
  },

  getRunProvenance(id: string) {
    return get<{ provenance: FieldProvenance[] }>(`/runs/${id}/provenance`);
  },

  retryRun(id: string) {
    return post<{ status: string }>(`/runs/${id}/retry`);
  },

  queueStatus() {
    return get<{
      queued: number;
      running: number;
      complete: number;
      failed: number;
    }>("/queue/status");
  },

  // Companies
  listCompanies(params?: Record<string, string>) {
    return get<{ companies: CompanyRecord[]; total: number }>(
      "/companies",
      params,
    );
  },

  getCompany(id: number) {
    return get<CompanyRecord>(`/companies/${id}`);
  },

  getCompanyIdentifiers(id: number) {
    return get<{ identifiers: Identifier[] }>(`/companies/${id}/identifiers`);
  },

  getCompanyAddresses(id: number) {
    return get<{ addresses: Address[] }>(`/companies/${id}/addresses`);
  },

  getCompanyMatches(id: number) {
    return get<{ matches: Match[] }>(`/companies/${id}/matches`);
  },

  getCompanyMSAs(id: number) {
    return get<{ msas: AddressMSA[] }>(`/companies/${id}/msas`);
  },

  getCompanyRuns(id: number) {
    return get<{ runs: Run[] }>(`/companies/${id}/runs`);
  },

  searchCompanies(params: Record<string, string>) {
    return get<{ companies: CompanyRecord[]; total: number }>(
      "/companies/search",
      params,
    );
  },

  // Fedsync
  fedsyncStatuses() {
    return get<{ datasets: DatasetStatus[] }>("/fedsync/statuses");
  },

  fedsyncSyncLog(params?: Record<string, string>) {
    return get<{ entries: SyncEntry[] }>("/fedsync/sync-log", params);
  },

  // Data Explorer
  listDataTables() {
    return get<{ tables: TableMeta[] }>("/data/tables");
  },

  queryDataTable(table: string, params?: Record<string, any>) {
    const p: Record<string, string> = {};
    if (params) {
      for (const [k, v] of Object.entries(params)) {
        if (v != null && v !== "") p[k] = String(v);
      }
    }
    return get<DataQueryResult>(`/data/${table}`, p);
  },

  getDataRow(table: string, id: number) {
    return get<Record<string, any>>(`/data/${table}/${id}`);
  },

  getFilterOptions(table: string, column: string) {
    return get<{ values?: string[]; min?: number; max?: number }>(
      `/data/${table}/filters/${column}`,
    );
  },

  aggregateData(table: string, params: Record<string, string>) {
    return get<DataAggregateResult>(`/data/${table}/aggregate`, params);
  },

  // Analytics
  syncTrends(params?: Record<string, string>) {
    return get<{ trends: SyncTrend[] }>("/analytics/sync-trends", params);
  },

  identifierCoverage() {
    return get<{ coverage: IdentifierCoverage[] }>(
      "/analytics/identifier-coverage",
    );
  },

  xrefCoverage() {
    return get<{ coverage: XrefCoverage[] }>("/analytics/xref-coverage");
  },

  enrichmentStats(params?: Record<string, string>) {
    return get<EnrichmentStats>("/analytics/enrichment-stats", params);
  },

  costBreakdown(params?: Record<string, string>) {
    return get<{ breakdown: CostBreakdown[] }>(
      "/analytics/cost-breakdown",
      params,
    );
  },
};
