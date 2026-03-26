// Enrichment pipeline types
export interface Run {
  id: string;
  company: Company;
  status: RunStatus;
  result: RunResult | null;
  error: RunError | null;
  created_at: string;
  updated_at: string;
}

export type RunStatus =
  | "queued"
  | "crawling"
  | "classifying"
  | "extracting"
  | "aggregating"
  | "writing_sf"
  | "complete"
  | "failed";

export interface Company {
  url: string;
  name: string;
  salesforce_id: string;
  notion_page_id: string;
  location: string;
  city: string;
  state: string;
  zip_code: string;
  street: string;
}

export interface RunResult {
  score: number;
  fields_found: number;
  fields_total: number;
  total_tokens: number;
  total_cost: number;
  phases: PhaseResult[];
  answers: ExtractionAnswer[];
  report: string;
  salesforce_sync: any;
  error: string;
}

export interface RunError {
  message: string;
  category: "transient" | "permanent";
  failed_phase: string;
  phases: PhaseResult[];
}

export interface PhaseResult {
  name: string;
  status: "running" | "complete" | "failed" | "skipped";
  duration_ms: number;
  token_usage: TokenUsage | null;
  error: string;
  metadata: Record<string, any> | null;
}

export interface TokenUsage {
  input_tokens: number;
  output_tokens: number;
  cache_creation_tokens: number;
  cache_read_tokens: number;
  cost: number;
}

export interface ExtractionAnswer {
  question_id: string;
  field_key: string;
  value: any;
  confidence: number;
  source: string;
  source_url: string;
  tier: number;
  reasoning: string;
  data_as_of: string | null;
  contradiction: Contradiction | null;
}

export interface Contradiction {
  other_tier: number;
  other_value: any;
  other_confidence: number;
}

export interface FieldProvenance {
  id: number;
  run_id: string;
  company_url: string;
  field_key: string;
  winner_source: string;
  winner_value: any;
  raw_confidence: number;
  effective_confidence: number;
  data_as_of: string | null;
  threshold: number;
  threshold_met: boolean;
  attempts: ProvenanceAttempt[];
  premium_cost_usd: number;
  previous_value: any;
  previous_run_id: string;
  value_changed: boolean;
  created_at: string;
}

export interface ProvenanceAttempt {
  source: string;
  source_url: string;
  value: any;
  confidence: number;
  tier: number;
  reasoning: string;
  data_as_of: string | null;
}

// Company golden record types
export interface CompanyRecord {
  id: number;
  name: string;
  legal_name: string;
  domain: string;
  website: string;
  description: string;
  naics_code: string;
  sic_code: string;
  business_model: string;
  year_founded: number | null;
  ownership_type: string;
  phone: string;
  email: string;
  employee_count: number | null;
  employee_estimate: string;
  revenue_estimate: string;
  revenue_range: string;
  revenue_confidence: number | null;
  street: string;
  city: string;
  state: string;
  zip_code: string;
  country: string;
  services_list: string;
  service_area: string;
  licenses_text: string;
  owner_name: string;
  customer_types: string;
  differentiators: string;
  reputation_summary: string;
  acquisition_assessment: string;
  enrichment_score: number | null;
  last_enriched_at: string | null;
  last_run_id: string;
  created_at: string;
  updated_at: string;
}

export interface Identifier {
  id: number;
  company_id: number;
  system: string;
  identifier: string;
  metadata: Record<string, any> | null;
  created_at: string;
  updated_at: string;
}

export interface Address {
  id: number;
  company_id: number;
  address_type: string;
  street: string;
  city: string;
  state: string;
  zip_code: string;
  country: string;
  latitude: number | null;
  longitude: number | null;
  geocode_source: string;
  geocode_quality: string;
  geocoded_at: string | null;
  county_fips: string;
  is_primary: boolean;
  source: string;
  confidence: number | null;
  created_at: string;
  updated_at: string;
}

export interface AddressMSA {
  id: number;
  address_id: number;
  cbsa_code: string;
  msa_name: string;
  is_within: boolean;
  distance_km: number | null;
  centroid_km: number | null;
  edge_km: number | null;
  classification: string;
  computed_at: string;
}

export interface Match {
  id: number;
  company_id: number;
  matched_source: string;
  matched_key: string;
  match_type: string;
  confidence: number;
  created_at: string;
}

export interface Contact {
  id: number;
  company_id: number;
  first_name: string;
  last_name: string;
  full_name: string;
  title: string;
  role_type: string;
  email: string;
  phone: string;
  linkedin_url: string;
  ownership_pct: number | null;
  is_control_person: boolean;
  is_primary: boolean;
  source: string;
  confidence: number | null;
  created_at: string;
  updated_at: string;
}

// Fedsync types
export interface DatasetStatus {
  name: string;
  table: string;
  phase: string;
  cadence: string;
  last_sync: string | null;
  last_status: string;
  rows_synced: number;
  row_count: number;
  next_due: string | null;
}

export interface SyncEntry {
  id: number;
  dataset: string;
  status: string;
  started_at: string;
  completed_at: string | null;
  rows_synced: number;
  error: string;
  metadata: Record<string, any> | null;
}

// Data explorer types
export interface TableColumn {
  key: string;
  label: string;
  type: "text" | "number" | "currency" | "category" | "date";
  sortable: boolean;
  filter?: "text" | "range" | "currency_range" | "select" | "date_range";
}

export interface TableMeta {
  id: string;
  name: string;
  category: string;
  estimated_row_count: number;
  columns: TableColumn[];
}

export interface DataQuerySort {
  column: string;
  direction: "asc" | "desc";
}

export interface DataQueryFilter {
  column: string;
  value: string;
}

export interface DataQueryResult {
  rows: Record<string, any>[];
  total_rows: number;
  page: number;
  page_size: number;
  sort?: DataQuerySort;
  filter?: DataQueryFilter;
}

export interface DataAggregateRow {
  key: string | number | null;
  value: number | string | null;
}

export interface DataAggregateResult {
  table: string;
  group_by: string;
  aggregation: string;
  value_field?: string;
  rows: DataAggregateRow[];
}

// Metrics
export interface MetricsSnapshot {
  pipeline_total: number;
  pipeline_complete: number;
  pipeline_failed: number;
  pipeline_queued: number;
  pipeline_fail_rate: number;
  pipeline_cost_usd: number;
  pipeline_avg_score: number;
  pipeline_avg_tokens: number;
  fedsync_total: number;
  fedsync_complete: number;
  fedsync_failed: number;
  fedsync_running: number;
  dlq_depth: number;
  lookback_hours: number;
  collected_at: string;
}

// Analytics types
export interface SyncTrend {
  date: string;
  dataset: string;
  rows_synced: number;
}

export interface IdentifierCoverage {
  system: string;
  count: number;
  total: number;
  percentage: number;
}

export interface XrefCoverage {
  source_a: string;
  source_b: string;
  count: number;
}

export interface EnrichmentStats {
  total_runs: number;
  avg_score: number;
  score_distribution: { bucket: string; count: number }[];
  phase_durations: { phase: string; avg_ms: number }[];
}

export interface CostBreakdown {
  date: string;
  tier: string;
  cost: number;
  tokens: number;
}
