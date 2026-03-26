import { datasetsMeta, type DatasetMeta } from "$lib/config/datasets";
import { categories } from "$lib/config/categories";
import type { TableMeta, DatasetStatus } from "$lib/types";

const ACRONYMS = new Set([
  "ADV",
  "BLS",
  "BEA",
  "CBP",
  "CPS",
  "CRS",
  "ECI",
  "EPA",
  "FDIC",
  "FPDS",
  "FRED",
  "IRS",
  "LAUS",
  "NES",
  "OEWS",
  "OSHA",
  "PPP",
  "QCEW",
  "SBA",
  "SEC",
  "SUSB",
  "XBRL",
  "ASM",
  "ABS",
  "BMF",
  "EO",
  "IA",
  "ITA",
  "ECHO",
  "EDGAR",
  "CBSA",
  "MSA",
  "ERISA",
  "IARD",
  "FINRA",
  "NCUA",
  "DOL",
]);

/** Convert a table ID like `adv_firms` to `ADV Firms`. */
export function humanizeTableName(id: string): string {
  return id
    .split("_")
    .map((word) => {
      const upper = word.toUpperCase();
      if (ACRONYMS.has(upper)) return upper;
      return word.charAt(0).toUpperCase() + word.slice(1);
    })
    .join(" ");
}

/** Find the DatasetMeta for a given table ID. */
export function getDatasetForTable(tableId: string): DatasetMeta | undefined {
  // Try direct table match (strip fed_data. prefix if present)
  const normalized = tableId.replace("fed_data.", "");
  return datasetsMeta.find((d) => {
    const dTable = d.table.replace("fed_data.", "");
    return dTable === normalized || dTable === tableId;
  });
}

/** Format a row count for display: 42531 → "42.5K", 10638666 → "10.6M" */
export function formatRowCount(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
}

export interface EnrichedTable extends TableMeta {
  dataset?: DatasetMeta;
  friendlyName: string;
  description: string;
  cadence?: string;
  lastSync?: string | null;
  lastStatus?: string;
}

/** Enrich tables with dataset metadata and sync status. */
export function enrichTables(
  tables: TableMeta[],
  statuses?: DatasetStatus[],
): EnrichedTable[] {
  const statusMap = new Map<string, DatasetStatus>();
  if (statuses) {
    for (const s of statuses) {
      statusMap.set(s.name, s);
      // Also map by table name for fallback matching
      const tableName = s.table?.replace("fed_data.", "");
      if (tableName) statusMap.set(tableName, s);
    }
  }

  return tables.map((t) => {
    const dataset = getDatasetForTable(t.id);
    const status = dataset ? statusMap.get(dataset.name) : statusMap.get(t.id);

    return {
      ...t,
      dataset,
      friendlyName: dataset?.label ?? humanizeTableName(t.name || t.id),
      description: dataset?.description ?? "",
      cadence: status?.cadence ?? dataset?.cadence,
      lastSync: status?.last_sync,
      lastStatus: status?.last_status,
    };
  });
}

/** Group enriched tables by category. Returns entries in display order. */
export function groupTablesByCategory(
  tables: EnrichedTable[],
): [string, EnrichedTable[]][] {
  const map = new Map<string, EnrichedTable[]>();
  for (const t of tables) {
    const cat = t.category || "Other";
    if (!map.has(cat)) map.set(cat, []);
    map.get(cat)!.push(t);
  }

  // Sort by category display order
  const order = categories.map((c) => c.key);
  return [...map.entries()].sort((a, b) => {
    const ai = order.indexOf(a[0]);
    const bi = order.indexOf(b[0]);
    if (ai >= 0 && bi >= 0) return ai - bi;
    if (ai >= 0) return -1;
    if (bi >= 0) return 1;
    return a[0].localeCompare(b[0]);
  });
}

/** Format a date string for display. */
export function formatSyncDate(dateStr: string | null | undefined): string {
  if (!dateStr) return "Never";
  const d = new Date(dateStr);
  const now = new Date();
  const diffMs = now.getTime() - d.getTime();
  const diffHours = diffMs / (1000 * 60 * 60);
  if (diffHours < 1) return "Just now";
  if (diffHours < 24) return `${Math.floor(diffHours)}h ago`;
  const diffDays = Math.floor(diffHours / 24);
  if (diffDays < 7) return `${diffDays}d ago`;
  return d.toLocaleDateString();
}
