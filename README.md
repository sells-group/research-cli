# research-cli

Automated account enrichment pipeline — import leads via CSV into a Notion lead tracker, crawl company websites + external sources, classify content, extract structured data via tiered Claude models, and push clean records to Salesforce.

## Overview

| | |
|---|---|
| **Input** | CSV file of companies (from Grata, manual research, etc.) → imported into a Notion Lead Tracker DB |
| **Output** | Enriched fields written to Salesforce Account records + status/quality updated on the Notion lead page |
| **Runtime** | Go binary (`research-cli`) on Fly.io (cron or webhook) · Neon Postgres (state) · SQLite (local dev) |
| **Stack** | Go (cobra · zap · eris · viper) · Fly.io (compute) · Neon Postgres (run log + staging) · Jina AI (search + read) → Firecrawl (fallback) · Perplexity + Haiku (LinkedIn) · Claude Haiku / Sonnet / Opus (tiered extraction) · Salesforce REST API (destination) · Notion API (lead tracker + registries) · ToolJet (manual review UI) |

---

## What Changed (n8n → Go)

**n8n as orchestration layer is out.** The pipeline's complexity (tiered model routing, batch API polling, prompt caching primer strategy, confidence escalation, cross-phase data dependencies) already required Code nodes for 60%+ of the logic. n8n was a glorified cron + HTTP client.

**Grata → Salesforce as the input path is out.** Leads now enter via CSV import directly into a Notion database. The Go binary reads from this Notion DB as its work queue.

**What replaced it:**

- **Go binary (`research-cli`)** — cobra-cli · **v1 commands:** `import`, `run`, `batch`, `serve`. Full control over concurrency (goroutines + errgroup), error handling (eris), structured logging (zap), and config (viper).
- **Fly.io** — per-second billing. Machine sleeps between runs. Webhook-triggered or cron-scheduled. ~$2/mo for batch workloads.
- **Neon Postgres** — run log, staging, cost tracking. Scale-to-zero when idle. SQLite for local dev (same schema, swappable via interface).
- **Notion Lead Tracker DB** — CSV import target. Each page = one company. The binary reads queued leads, processes them, updates status + quality score on the Notion page, then writes enriched fields to Salesforce.
- **Registries stay in Notion** — Question Registry and Field Registry remain Notion DBs. Seth edits them in Notion; the Go binary pulls them via Notion API at runtime.

---

## Pipeline Flow (Phases 0–9)

### Phase 0 — CSV Import → Notion Lead Tracker

- `research-cli import --csv leads.csv`
- Reads CSV rows, creates pages in the Notion Lead Tracker DB via Notion API
- Sets `Enrichment Status = Queued` on each new page
- Deduplicates by company URL or SF Account ID (skip if page already exists)
- CSV columns map to Notion DB properties: Company Name, Website, SF Account ID, City, State, etc.

### Phase 1A — Company Website (Local-First → Firecrawl Fallback)

1. **Probe:** `net/http` GET on homepage + `robots.txt` + `sitemap.xml`
2. **Block detection:** Check for Cloudflare challenge (403/503 + `cf-*` headers, JS challenge HTML), captcha, JS-only shell (tiny `<body>`, heavy `<script>`), meta-refresh to challenge URL
3. **If clean HTML:** Local crawl via `colly` (link discovery, depth 2, cap ~50 pages) + `html-to-markdown` conversion. **Zero Firecrawl credits.**
4. **If blocked/JS-rendered:** Fall back to Firecrawl async crawl (`/v2/crawl`). Full browser rendering + proxy rotation.

- Exclude paths: `/blog/*`, `/news/*`, `/press/*`, `/careers/*`
- Crawl results cached 24 hours; reused on re-run
- ~60% of company sites serve clean HTML → **~55% Firecrawl credit reduction**

### Phase 1B — External Scrape: Search-then-Scrape *(parallel with 1A)*

Uses **Jina AI Search** to discover profile URLs, then scrapes them via the scrape chain (Jina Reader → Firecrawl fallback):

- **Google Maps:** Direct URL construction from company name + location → scrape chain
- **BBB:** Jina Search with `site:bbb.org` filter → best result filter → scrape chain
- **Secretary of State:** Jina Search for state business entity registration → scrape chain

Each source runs in parallel. Pages are deduped by content hash, and address cross-referencing extracts structured location metadata.

### Phase 1C — Perplexity: LinkedIn *(parallel with 1A/1B)*

- Targeted Perplexity query (`sonar-pro` model) for company LinkedIn profile data
- Raw response piped through **Haiku** to enforce strict JSON output schema (company name, description, industry, headcount, headquarters, founded, specialties, company type)
- Output converted to synthetic `[linkedin]` page and merged into page set
- LinkedIn results cached per domain

### Phase 1D — PPP Loan Lookup *(parallel with 1A/1B/1C)*

- Queries PPP loan dataset for company name matches (fuzzy matching with similarity threshold 0.4)
- Returns loan match metadata (loan amount, jobs retained, NAICS code) if found
- PPP matches carried through to aggregation for revenue estimation

**Phase 1 Gate:** Requires at least 1 of the 3 data-producing phases (1A, 1B, 1C) to succeed. Continues with partial data if some sources fail; logs warnings. PPP (1D) is supplementary and does not gate.

### Phase 2 — Page Classification (Haiku)

- Each crawled page classified into one of 21 page types (see taxonomy below)
- External source pages auto-classified by title prefix (e.g., `[bbb]` → `bbb_profile`, `[google_maps]` → `google_maps`)
- URL path heuristics short-circuit LLM classification where possible (e.g., `/about` → `about`)
- Uses Batch API for >3 pages, direct Messages API for small sets
- Builds a **Page Index** — lookup table mapping page types → classified pages

### Phase 3 — Question Router (Go code)

- Reads QUESTION_REGISTRY from Notion API (cached in memory for the run)
- Each question declares preferred `page_types`; if none specified, all pages eligible
- External source pages always included as supplementary context
- Router matches questions → relevant pages from the Page Index
- Groups questions into batches by tier
- **Optimization:** Skips questions that already have high-confidence answers (≥ 0.8) from prior runs, reusing existing answers instead of re-extracting

### Phases 4+5 — Tier 1 + Tier 2 (overlapping execution)

T1, T2-native, and T2-escalated run with maximum overlap:

1. **T1 (Haiku)** and **T2-native (Sonnet)** start in parallel
2. **T2-escalated** starts as soon as T1 completes, overlapping with still-running T2-native
3. Results are combined into a single Phase 5 result

**Tier 1 — Haiku (~70 questions):**
- Single-page fact extraction, no reasoning
- Each batch gets **only** the pages classified as relevant
- Strict JSON output: `{"value", "confidence", "reasoning", "source_url"}`
- Low-confidence answers (< 0.4) escalate to Tier 2
- **Checkpoint/resume:** T1 answers saved on success; reused if pipeline fails in later phases

**Tier 2 — Sonnet (~25 questions):**
- **T2-native:** Questions routed directly to T2 (no T1 dependency)
- **T2-escalated:** Low-confidence T1 answers re-extracted with T1 context
- Multi-page synthesis (top 2000 chars per page), light reasoning

### Phase 6 — Tier 3: Opus (~5 questions)

Two-step process to minimize Opus input tokens:

1. **6A — Haiku Context Prep:** Single batch call reads full crawl + Page Index + T1/T2 answers. Produces a focused context extract per T3 question (~3K tokens each). Strips raw markdown; keeps only decision-relevant facts, quotes, and data points.
2. **6B — Opus Extraction:** Each of the 5 questions gets its prepared extract (~25K tokens) instead of raw crawl (~150K). Cross-page strategic reasoning on distilled, not raw, context.

- **Feature-gated** by `tier3_gate` config:
  - `"off"` (default): T3 skipped entirely. Use `--with-t3` flag to enable.
  - `"always"`: Run T3 unconditionally if T3 questions exist.
  - `"ambiguity_only"`: Run T3 only if T1+T2 answers have confidence < 0.6.
- **Cost budget gate:** T3 skipped if cumulative per-company cost exceeds `max_cost_per_company_usd` (default $10)
- ~87% cheaper than raw-context approach at batch pricing

### Phase 7 — Aggregation

- **MergeAnswers:** Combine T1 + T2 + T3 outputs, preferring higher tier and higher confidence. Flags contradictions when tiers disagree (both ≥ 0.5 confidence).
- **Revenue enrichment:** Augments answers with CBP-based revenue estimates from fedsync data
- **Validate & type-coerce:** Match answers to Field Registry, validate against regex patterns and length constraints
- **Merge existing answers:** Reuses high-confidence answers from prior runs that were skipped during extraction

### Phase 7B — Waterfall Cascade *(optional)*

Per-field resolution for low-confidence answers using premium data sources:

- Evaluates each field below `waterfall.confidence_threshold` (default 0.7)
- Applies configured premium providers (paid lookups, specialized APIs) per field
- Respects per-company budget: `waterfall.max_premium_cost_usd` (default $2)
- Results merged back into field values before report generation

### Phase 8 — Enrichment Report

- Markdown report per account written to a single SF long text field
- Contains: phase results, fields populated (✓), fields not found (✗), low confidence (⚠), tier breakdown, sources used, cost, timestamp

### Phase 9 — Quality Gate → Salesforce + Notion Update

- Quality score computed from field coverage + confidence
- Score >= `quality_score_threshold` (default 0.6) → CRUD update to SF via REST API (dynamic field mapping from Field Registry)
- Score < threshold → POST to ToolJet webhook for Research Team manual review
- **Always:** Update the Notion Lead Tracker page with enrichment status, quality score, fields populated count, and timestamp
- Salesforce + Notion updates run concurrently (independent operations)

---

## Architecture Diagram

```mermaid
flowchart TD
    CSV["CSV File"] -->|"research-cli import"| NLT["Notion Lead Tracker DB<br>(Queued leads)"]
    NLT -->|"research-cli batch"| GO["Go Binary<br>(Fly.io Machine)"]
    GO --> PROBE["Phase 1A: Local Crawl Probe<br>net/http + colly + html-to-md"]
    PROBE -->|"clean HTML"| CLASS["Phase 2: Page Classification<br>(Haiku)"]
    PROBE -->|"blocked / JS"| FC["Firecrawl Fallback<br>Browser + Proxy"]
    FC --> CLASS
    GO --> SCRAPE["Phase 1B: Search-then-Scrape<br>Jina Search → Scrape Chain<br>(Google Maps + BBB + SoS)"]
    GO --> LI["Phase 1C: Perplexity<br>LinkedIn → Haiku JSON"]
    GO --> PPP["Phase 1D: PPP Loan Lookup<br>(Fuzzy Name Match)"]
    SCRAPE --> CLASS
    CLASS --> IDX["Page Index"]
    IDX --> ROUTER["Phase 3: Question Router<br>(Go code + answer skip)"]
    ROUTER --> T1["T1: Haiku<br>~70 Qs · Fact Extraction"]
    ROUTER --> T2N["T2-native: Sonnet<br>~25 Qs · Multi-page"]
    T1 -->|"confidence < 0.4"| T2E["T2-escalated: Sonnet<br>Low-confidence re-extract"]
    T1 --> PREP["Phase 6A: Haiku Context Prep<br>Summarize crawl → focused extracts"]
    PREP --> T3["Phase 6B: Tier 3 — Opus<br>5 Qs · Prepared Context<br>(gated: off by default)"]
    T1 --> AGG["Phase 7: Aggregation<br>+ Revenue Enrichment"]
    T2N --> AGG
    T2E --> AGG
    T3 --> AGG
    LI --> AGG
    PPP --> AGG
    AGG --> WF["Phase 7B: Waterfall Cascade<br>(optional premium sources)"]
    WF --> RPT["Phase 8: Enrichment Report"]
    RPT --> GATE{"Phase 9:<br>Quality Gate"}
    GATE -->|"Score ≥ 0.6"| SF["Salesforce<br>(REST API CRUD)"]
    GATE -->|"Score < 0.6"| TJ["ToolJet Webhook<br>(Manual Review)"]
    GATE -->|"Always"| NLT2["Notion Lead Tracker<br>(Status Update)"]
    TJ --> SF
    GO -.->|"run log + cache"| NEON["Neon Postgres<br>(Runs · Phases · Caches · Checkpoints)"]
    GO -.->|"registry reads"| NREG["Notion DBs<br>(Question + Field Registries)"]
    GO -.->|"revenue estimates"| FED["fed_data.* Tables<br>(Fedsync)"]
```

---

## Project Structure

```
research-cli/
├── cmd/
│   ├── root.go              # cobra root command, viper config init, zap logger init
│   ├── import.go            # `research-cli import --csv leads.csv` → Notion Lead Tracker
│   ├── run.go               # `research-cli run --url acme.com --sf-id 001xxx` → single company
│   ├── batch.go             # `research-cli batch --limit 100` → process queued leads from Notion
│   ├── serve.go             # `research-cli serve --port 8080` → webhook listener (Fly auto-stop)
│   └── fedsync.go           # `research-cli fedsync {migrate,status,sync,xref}` → federal data sync
├── config/                  # waterfall config YAML, etc.
├── internal/
│   ├── config/
│   │   └── config.go        # viper config struct + loader (includes FedsyncConfig, WaterfallConfig, PricingConfig)
│   ├── pipeline/
│   │   ├── pipeline.go      # orchestrates phases 1-9 for a single company
│   │   ├── crawl.go         # Phase 1A: orchestrator (local-first → Firecrawl fallback)
│   │   ├── localcrawl.go    # Local crawl: net/http probe + colly link discovery + html-to-markdown
│   │   ├── blockdetect.go   # Cloudflare / captcha / JS-shell detection heuristics
│   │   ├── scrape.go        # Phase 1B: Jina Search → scrape chain (Google Maps, BBB, SoS)
│   │   ├── linkedin.go      # Phase 1C: Perplexity → Haiku JSON
│   │   ├── classify.go      # Phase 2: Haiku page classification (heuristic + LLM)
│   │   ├── router.go        # Phase 3: question → page matching + high-confidence skip
│   │   ├── extract.go       # Phases 4-6: tiered Claude calls (T1∥T2-native, T2-escalated, T3)
│   │   ├── aggregate.go     # Phase 7: merge + validate + revenue enrichment
│   │   ├── report.go        # Phase 8: enrichment report
│   │   └── gate.go          # Phase 9: quality gate + SF write + Notion update
│   ├── scrape/              # scrape chain abstraction (Jina Reader → Firecrawl fallback)
│   ├── waterfall/           # Phase 7B: per-field waterfall cascade
│   │   └── provider/        # premium data source providers
│   ├── cost/                # per-model cost calculation (Claude, Jina, Perplexity, Firecrawl)
│   ├── estimate/            # revenue estimation from CBP data
│   ├── registry/
│   │   ├── question.go      # reads Question Registry from Notion API
│   │   └── field.go         # reads Field Registry from Notion API
│   ├── store/
│   │   ├── store.go         # interface: Runs, Phases, Caches, Checkpoints (Neon or SQLite)
│   │   ├── postgres.go      # pgx implementation for Neon
│   │   └── sqlite.go        # modernc.org/sqlite implementation for local dev
│   ├── model/
│   │   ├── company.go       # Company, Run, RunResult, EnrichmentResult, FieldValue, Checkpoint
│   │   ├── page.go          # CrawledPage, PageIndex, PageClassification, CrawlResult, ProbeResult
│   │   ├── question.go      # Question, RoutedBatches, ExtractionAnswer, Contradiction, TokenUsage
│   │   └── field.go         # FieldMapping, FieldRegistry (indexed by key + SF name)
│   ├── db/                  # shared DB helpers (BulkUpsert, CopyFrom)
│   ├── fetcher/             # HTTP/FTP download, CSV/XML/JSON/XLSX/ZIP streaming
│   ├── ocr/                 # PDF text extraction (pdftotext → Mistral fallback)
│   ├── fedsync/             # federal data sync subsystem
│   │   ├── migrate.go       # embed.FS migration runner → fed_data.schema_migrations
│   │   ├── synclog.go       # sync log tracking (start, complete, fail)
│   │   ├── migrations/      # 40+ SQL migration files (001-040)
│   │   ├── dataset/         # 26 dataset implementations
│   │   │   ├── interface.go # Dataset interface, Phase, Cadence, SyncResult
│   │   │   ├── engine.go    # Engine: Run() orchestration loop
│   │   │   ├── registry.go  # Registry: maps names → Dataset impls
│   │   │   ├── schedule.go  # ShouldRun helpers: Daily, Weekly, Monthly, Quarterly, Annual
│   │   │   └── *.go         # 26 dataset files (cbp, qcew, fpds, adv_part1, form_d, etc.)
│   │   ├── transform/       # NAICS, FIPS, SIC normalization
│   │   ├── resolve/         # entity resolution (CRD↔CIK fuzzy matching)
│   │   └── xbrl/            # XBRL JSON-LD fact parser
│   └── company/             # company matching utilities
├── pkg/
│   ├── anthropic/           # Claude Messages + Batch + prompt caching
│   ├── firecrawl/           # Firecrawl v2: crawl, scrape (fallback only)
│   ├── jina/                # Jina AI: Reader (scrape) + Search (discovery)
│   ├── perplexity/          # Perplexity chat completions (sonar-pro)
│   ├── salesforce/          # JWT auth, SOQL, CRUD, sObject Collections
│   ├── notion/              # DB query, page create/update, CSV mapper
│   └── ppp/                 # PPP loan dataset querier (fuzzy name match)
├── testdata/                # test fixtures (CSV, JSON, baseline results)
├── config.yaml              # default config (viper)
├── Dockerfile
├── fly.toml
├── go.mod
└── go.sum
```

---

## Library Stack

| Library | Import | Purpose | Notes |
|---|---|---|---|
| **Cobra** | `github.com/spf13/cobra` | CLI framework — subcommands, flags, help | v1.8+. Pairs with Viper for config binding. |
| **Viper** | `github.com/spf13/viper` | Config management — YAML files, env vars, flags | Reads `config.yaml` · `RESEARCH_*` env vars. Fly secrets map to env vars. |
| **Zap** | `go.uber.org/zap` | Structured logging — JSON output for Fly log drain | Use `zap.L()` global logger. Fields: `company`, `phase`, `tier`, `duration_ms`, `tokens`. |
| **Eris** | `github.com/rotisserie/eris` | Error wrapping with stack traces | `eris.Wrap(err, "firecrawl crawl failed")`. Unwrap for structured error reporting in run log. |
| **pgx** | `github.com/jackc/pgx/v5` | Postgres driver for Neon | Connection pooling via `pgxpool`. Neon requires SSL (`sslmode=require`). |
| **modernc sqlite** | `modernc.org/sqlite` | Pure Go SQLite — local dev, no CGO | Same schema as Neon. Swapped via `store.Store` interface. |
| **errgroup** | `golang.org/x/sync/errgroup` | Structured concurrency for parallel phases | Fan out Phase 1A/1B/1C/1D in parallel. T1∥T2-native overlap. |
| **go-salesforce** | `github.com/k-capehart/go-salesforce/v3` | Salesforce REST API wrapper | SOQL, CRUD, Collections. On awesome-go. Actively maintained. |
| **notionapi** | `github.com/jomei/notionapi` | Notion API client | Database query, page create/update. Used for Lead Tracker + Registry reads. |
| **colly** | `github.com/gocolly/colly/v2` | Web crawling — link discovery, depth control, robots.txt | Phase 1A local crawl. Respects robots.txt. Depth 2, cap 50 pages. Falls back to Firecrawl on block. |
| **html-to-markdown** | `github.com/JohannesKaufmann/html-to-markdown/v2` | HTML → clean markdown conversion | Replaces Firecrawl's markdown output for locally crawled pages. Handles tables, lists, links. |
| **anthropic-sdk-go** | `github.com/anthropics/anthropic-sdk-go` | Official Anthropic SDK — Messages + Batch API | Supports prompt caching, batch create/poll/results. Use for all Claude calls. |
| **jlaffaye/ftp** | `github.com/jlaffaye/ftp` | FTP client for SEC EDGAR bulk downloads | Used by fedsync datasets. |
| **tealeg/xlsx** | `github.com/tealeg/xlsx/v2` | XLSX parsing | Used by fedsync for Excel-format datasets. |
| **x/time/rate** | `golang.org/x/time/rate` | Per-host rate limiting | SEC 10 req/s, SAM.gov 5 req/s, default 20 req/s. |

**No SDK for Firecrawl, Jina, or Perplexity in Go.** All are simple REST APIs — use `net/http` with typed request/response structs. Jina AI provides Reader (scrape) and Search (discovery) endpoints. Perplexity is OpenAI-compatible.

---

## API Reference

All external API contracts the Go binary depends on. Each section lists endpoints, auth, rate limits, and error handling. This is the source of truth for building `pkg/` clients.

### Salesforce REST API

**Auth:** OAuth 2.0 JWT Bearer Flow (server-to-server, no user interaction)

**Base URL:** `https://{instance}.salesforce.com/services/data/v62.0/`

**API Version:** v62.0 (Spring '26) — confirm against your org

**Credential:** Connected App with JWT certificate. Store private key in Fly secrets.

#### Authentication — JWT Bearer Token

```
POST /services/oauth2/token
Content-Type: application/x-www-form-urlencoded

grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer
&assertion={signed_jwt}
```

**JWT claims:**

```json
{
  "iss": "{connected_app_client_id}",
  "sub": "{sf_username}",
  "aud": "https://login.salesforce.com",
  "exp": "{unix_timestamp + 300}"
}
```

**Response:**

```json
{
  "access_token": "00D...",
  "instance_url": "https://sells.my.salesforce.com",
  "token_type": "Bearer"
}
```

**Token lifecycle:** Tokens expire based on session settings (typically 2 hours). The Go client should cache the token and refresh on 401. Implement in `pkg/salesforce/client.go` with a `sync.Mutex` around refresh.

#### SOQL Query

```
GET /services/data/v62.0/query?q={url_encoded_soql}
Authorization: Bearer {access_token}
```

**Example — find Account by website:**

```sql
SELECT Id, Name, Website, Legal_Name__c, BillingCity, BillingState
FROM Account
WHERE Website = 'https://acme.com'
LIMIT 1
```

**Response:**

```json
{
  "totalSize": 1,
  "done": true,
  "records": [
    {
      "attributes": { "type": "Account", "url": "/services/data/v62.0/sobjects/Account/001xx..." },
      "Id": "001xx000003ABCDEF",
      "Name": "Acme Industrial",
      "Website": "https://acme.com"
    }
  ]
}
```

**Pagination:** If `done: false`, follow `nextRecordsUrl`:

```
GET /services/data/v62.0/query/{query_locator}
```

#### Single Record Update (PATCH)

```
PATCH /services/data/v62.0/sobjects/Account/{account_id}
Authorization: Bearer {access_token}
Content-Type: application/json

{
  "Legal_Name__c": "Acme Industrial Services LLC",
  "BillingCity": "Dallas",
  "BillingState": "TX",
  "Enrichment_Report__c": "18/24 fields populated..."
}
```

**Response:** `204 No Content` on success. Error → `400` with:

```json
[{"message": "field Legal_Name__c does not exist", "errorCode": "INVALID_FIELD"}]
```

#### sObject Collections — Bulk Update (up to 200 records)

```
PATCH /services/data/v62.0/composite/sobjects
Authorization: Bearer {access_token}
Content-Type: application/json

{
  "allOrNone": false,
  "records": [
    {
      "attributes": { "type": "Account" },
      "id": "001xx000003ABCDEF",
      "Legal_Name__c": "Acme Industrial Services LLC",
      "BillingCity": "Dallas"
    },
    {
      "attributes": { "type": "Account" },
      "id": "001xx000003GHIJKL",
      "Legal_Name__c": "Beta Corp"
    }
  ]
}
```

**Response:** Array of results per record:

```json
[
  { "id": "001xx000003ABCDEF", "success": true, "errors": [] },
  { "id": "001xx000003GHIJKL", "success": false, "errors": [{"message": "...", "statusCode": "FIELD_CUSTOM_VALIDATION_EXCEPTION"}] }
]
```

**Use Collections for batch mode.** When `research-cli batch` processes 100 companies, accumulate SF updates and flush in batches of 200 via Collections. Single-record PATCH for `research-cli run` (one company).

#### Describe — Get Field Metadata

```
GET /services/data/v62.0/sobjects/Account/describe
Authorization: Bearer {access_token}
```

**Purpose:** Validate that all `sf_api_name` values in the Field Registry actually exist on the Account object. Run once at startup or as a healthcheck command.

**Response includes:** field name, type, length, picklist values, updateable flag, required flag.

#### SF Rate Limits & Quotas

| Limit | Value | Notes |
|---|---|---|
| Daily API requests | 100,000 base + 1,000 per user license (Enterprise) | At 10K companies/mo: ~10K SOQL queries + ~50 Collections calls/day = well within limits |
| Concurrent API requests | 25 long-running | Pipeline is sequential per company — no risk |
| sObject Collections | 200 records per request | Batch enrichment flushes every 200 records |
| SOQL query length | 100,000 characters | Not a concern |
| SOQL query rows returned | 50,000 per query | Paginate if needed |
| API response size | 15 MB | Not a concern for Account queries |
| Bulk API (v2) | 15,000 batches/day, 150M records/day | Not needed at current scale — Collections suffices |

**Error handling (Go):**

- `401 INVALID_SESSION_ID` → refresh token, retry once
- `429` → respect `Retry-After` header, back off
- `503 SERVICE_UNAVAILABLE` → exponential backoff, max 3 retries
- `400 INVALID_FIELD` → log to run log, skip field, do NOT retry
- `FIELD_CUSTOM_VALIDATION_EXCEPTION` → log, flag for manual review

Wrap all SF errors with `eris.Wrap(err, "sf: update account %s", accountID)`.

### Anthropic Messages API + Batch API

**Auth:** `x-api-key: {api_key}` header

**Base URL:** `https://api.anthropic.com`

**SDK:** `github.com/anthropics/anthropic-sdk-go` (official, supports batch + caching)

**API Version Header:** `anthropic-version: 2023-06-01`

#### Messages — Single Request

```json
POST /v1/messages
x-api-key: {api_key}
anthropic-version: 2023-06-01
Content-Type: application/json

{
  "model": "claude-haiku-4-5-20250214",
  "max_tokens": 4096,
  "system": [
    {
      "type": "text",
      "text": "You are a precise data extraction agent...",
      "cache_control": { "type": "ephemeral" }
    }
  ],
  "messages": [
    { "role": "user", "content": "Extract company info from:\n\n{page_markdown}" }
  ]
}
```

**Used for:** Classification (Phase 2), LinkedIn JSON formatting, Aggregation (Phase 7), Primer requests for caching.

#### Message Batches — Batch Processing

**Create batch:**

```json
POST /v1/messages/batches
x-api-key: {api_key}
Content-Type: application/json

{
  "requests": [
    {
      "custom_id": "q_company_legal_name",
      "params": {
        "model": "claude-haiku-4-5-20250214",
        "max_tokens": 4096,
        "system": "...",
        "messages": [{ "role": "user", "content": "..." }]
      }
    }
  ]
}
```

**Response:**

```json
{
  "id": "msgbatch_abc123",
  "type": "message_batch",
  "processing_status": "in_progress",
  "request_counts": { "processing": 70, "succeeded": 0, "errored": 0, "canceled": 0, "expired": 0 }
}
```

**Poll for completion:**

```
GET /v1/messages/batches/{batch_id}
```

Poll until `processing_status == "ended"`. Use exponential backoff: 5s → 10s → 20s → 30s cap.

**Retrieve results:**

```
GET /v1/messages/batches/{batch_id}/results
```

Returns JSONL stream. Each line:

```json
{"custom_id": "q_company_legal_name", "result": {"type": "succeeded", "message": { ... }}}
```

#### Prompt Caching

**Mechanics:**

- Add `"cache_control": {"type": "ephemeral"}` to content blocks to mark cache breakpoints
- Cache built from full prefix in order: `tools` → `system` → `messages`
- Up to 4 breakpoints per request
- Default TTL: 5 min (refreshed on hit). 1-hour TTL available at 2x write cost.

**Pricing multipliers:**

| Model | Cache Write (1.25x) | Cache Read (0.1x) | Min Cacheable Tokens |
|---|---|---|---|
| Opus 4.6 | $6.25 / MTok | $0.50 / MTok | 4,096 |
| Sonnet 4.5 | $3.75 / MTok | $0.30 / MTok | 1,024 |
| Haiku 4.5 | $1.25 / MTok | $0.10 / MTok | 4,096 |

**Primer + Batch strategy:** For Tier 2 and 3, send 1 primer request sequentially with 1-hour TTL to seed the cache, then submit remaining questions as a batch. Batch items hit the warm cache = 95% off base input price. Implemented in `pkg/anthropic/cache.go`.

#### Anthropic Rate Limits

| Limit | Value (Tier 2+) | Notes |
|---|---|---|
| Requests per minute (Messages) | 2,000 RPM (Haiku), 1,000 RPM (Sonnet), 500 RPM (Opus) | Classification + aggregation are single calls. Tier extraction uses Batch API. |
| Input tokens per minute | 300K (Haiku), 160K (Sonnet), 100K (Opus) | Primer requests count against this. |
| Batch requests per batch | 100,000 | Not a concern (max ~70 per company). |
| Batch completion time | Up to 24 hours (guaranteed) | Typical: 10-60 min depending on load. |
| Concurrent batches | 100 | Sufficient for per-company topology. |

### Firecrawl v2 API

**Auth:** `Authorization: Bearer {api_key}` header

**Base URL:** `https://api.firecrawl.dev/v2`

**Rate limits:** Plan-dependent (see pricing table below).

#### Firecrawl Pricing Plans

| Plan | Credits | Price | Concurrent Requests | Overage | Support |
|---|---|---|---|---|---|
| Free | 500 (one-time) | $0 | 2 | — | Low rate limits |
| Hobby | 3,000 / mo | $19/mo (annual) | 5 | $9 per 1K credits | Basic |
| **Standard** | **100,000 / mo** | **$99/mo (annual)** | **50** | **$57 per 30K credits** | **Standard** |
| Growth | 500,000 / mo | $399/mo (annual) | 100 | $217 per 150K credits | Priority |
| Scale | 1,000,000 / mo | $749/mo (annual) | 100 | Contact sales | Priority |
| Enterprise | Custom | Custom | Custom | Bulk discounts | Dedicated + SLA, SSO, zero-data retention |

**Plan recommendation by volume (with local-first crawl, ~24 credits/company avg):**

- **<=4,100 companies/mo** (~98K credits) → **Standard ($99/mo)**. Local-first doubles the coverage vs Firecrawl-only.
- **<=20,000 companies/mo** (~480K credits) → **Growth ($399/mo)**. Comfortably within 500K credits.
- **>20K companies/mo** → **Scale ($749/mo)** or Enterprise.
- *Fallback rate assumes ~60% of company sites serve clean HTML. Adjust if your target industry skews toward heavy JS / anti-bot sites.*

#### Crawl (async) — Company Website

**Start crawl:**

```json
POST /v2/crawl
Authorization: Bearer {api_key}
Content-Type: application/json

{
  "url": "https://acme.com",
  "limit": 50,
  "excludePaths": ["/blog/*", "/news/*", "/press/*", "/careers/*"],
  "scrapeOptions": {
    "formats": ["markdown"],
    "onlyMainContent": true
  }
}
```

**Response:**

```json
{
  "success": true,
  "id": "crawl_abc123",
  "url": "https://api.firecrawl.dev/v2/crawl/crawl_abc123"
}
```

**Poll for completion:**

```
GET /v2/crawl/{crawl_id}
```

**Response (in progress):**

```json
{ "status": "scraping", "total": 42, "completed": 15, "data": [...] }
```

**Response (complete):**

```json
{
  "status": "completed",
  "total": 42,
  "completed": 42,
  "data": [
    {
      "markdown": "# About Acme Industrial...",
      "metadata": { "title": "About Us", "sourceURL": "https://acme.com/about", "statusCode": 200 }
    }
  ]
}
```

Poll interval: 5s → 10s → 15s cap. Timeout at 5 min.

#### Scrape (sync) — Single Page

```json
POST /v2/scrape
Authorization: Bearer {api_key}
Content-Type: application/json

{
  "url": "https://www.google.com/maps/place/Acme+Industrial",
  "formats": ["markdown"],
  "onlyMainContent": true
}
```

**Response:**

```json
{
  "success": true,
  "data": {
    "markdown": "# Acme Industrial Services...",
    "metadata": { "title": "Acme Industrial", "sourceURL": "...", "statusCode": 200 }
  }
}
```

**Used for:** Google Places, BBB, PPP, Secretary of State — one call per source.

#### Batch Scrape (async) — Multiple URLs

```json
POST /v2/batch/scrape
Authorization: Bearer {api_key}
Content-Type: application/json

{
  "urls": [
    "https://www.google.com/maps/place/Acme+Industrial",
    "https://www.bbb.org/us/tx/dallas/profile/Acme+Industrial",
    "https://projects.propublica.org/coronavirus/bailouts/loans/acme-industrial"
  ],
  "formats": ["markdown"]
}
```

Poll `GET /v2/batch/scrape/{id}` same pattern as crawl.

#### Firecrawl Credit Usage

| Operation | Credits | Per Company |
|---|---|---|
| Local crawl (net/http + colly) | 0 credits | 0 credits (~60% of sites) |
| Firecrawl crawl fallback (per page) | 1 credit/page | ~50 credits (~40% of sites) |
| Scrape (single page) | 1 credit | ~4 credits (GP + BBB + PPP + SoS — always Firecrawl) |
| **Weighted avg per company** | | **~24 credits** (0.6 × 4 + 0.4 × 54) |

At 10K companies/mo: **~240K credits/month** (with local-first crawl). Growth plan ($399/mo) covers 500K with headroom.

### Perplexity API

**Auth:** `Authorization: Bearer {api_key}` header

**Base URL:** `https://api.perplexity.ai`

**Compatibility:** OpenAI chat completions format

**Model:** `sonar` (latest)

```json
POST /chat/completions
Authorization: Bearer {api_key}
Content-Type: application/json

{
  "model": "sonar",
  "messages": [
    {
      "role": "system",
      "content": "Return structured company information from LinkedIn."
    },
    {
      "role": "user",
      "content": "Find the LinkedIn company profile for Acme Industrial Services, Dallas TX. Return: company overview, employee count range, headquarters, industry, key personnel (name, title)."
    }
  ]
}
```

**Response:** Standard OpenAI chat completion format. Raw text piped through Haiku for JSON enforcement.

**Rate limits:** 50 RPM on Pro plan. 1 query per company = not a bottleneck.

**Cost:** ~$0.01/query. ~$100/mo at 10K companies.

### Notion API

**Auth:** `Authorization: Bearer {integration_token}` header

**Base URL:** `https://api.notion.com/v1`

**Version Header:** `Notion-Version: 2022-06-28`

**SDK:** `github.com/jomei/notionapi`

#### Query Database (read queued leads, read registries)

```json
POST /v1/databases/{database_id}/query
Authorization: Bearer {token}
Notion-Version: 2022-06-28
Content-Type: application/json

{
  "filter": {
    "property": "Enrichment Status",
    "status": { "equals": "Queued" }
  },
  "page_size": 100
}
```

**Pagination:** Response includes `has_more` and `next_cursor`. Loop until `has_more: false`.

**Used for:**

- **Lead Tracker:** Query `Enrichment Status = Queued` to get work queue
- **Question Registry:** Query `Status = Active` at startup, cache in memory
- **Field Registry:** Query `Status = Active` at startup, cache in memory

#### Create Page (CSV import)

```json
POST /v1/pages
Authorization: Bearer {token}
Notion-Version: 2022-06-28
Content-Type: application/json

{
  "parent": { "database_id": "{lead_tracker_db_id}" },
  "properties": {
    "Company Name": { "title": [{ "text": { "content": "Acme Industrial" } }] },
    "Website": { "url": "https://acme.com" },
    "SF Account ID": { "rich_text": [{ "text": { "content": "001xx000003ABCDEF" } }] },
    "Enrichment Status": { "status": { "name": "Queued" } },
    "City": { "rich_text": [{ "text": { "content": "Dallas" } }] },
    "State": { "rich_text": [{ "text": { "content": "TX" } }] }
  }
}
```

#### Update Page (enrichment status update)

```json
PATCH /v1/pages/{page_id}
Authorization: Bearer {token}
Notion-Version: 2022-06-28
Content-Type: application/json

{
  "properties": {
    "Enrichment Status": { "status": { "name": "Complete" } },
    "Quality Score": { "number": 0.85 },
    "Fields Populated": { "number": 18 },
    "Last Enriched": { "date": { "start": "2026-02-11T17:00:00-06:00" } }
  }
}
```

#### Notion Rate Limits

| Limit | Value | Notes |
|---|---|---|
| Requests per second | 3 req/s per integration | **This is the bottleneck for CSV import.** 100 pages = ~33s. 1,000 pages = ~5.5 min. Batch imports should pace with `time.Sleep`. |
| Page size limit | 100 items per query response | Paginate with `next_cursor`. |
| Payload size | 1 MB per request | Not a concern for property updates. |
| Rate limit response | `429` with `Retry-After` header | Respect and retry with backoff. |

**3 req/s is real.** For CSV imports of 1,000+ rows, consider:

1. Pacing with a `time.Ticker` at 300ms intervals
2. Dedup check first: query existing pages by Website URL, skip duplicates
3. Progress bar in CLI output (`research-cli import --csv leads.csv` shows `[342/1000] importing...`)

---

## Data Model

### Store Interface

The `Store` interface (`internal/store/store.go`) abstracts all persistence. Implemented by both `postgres.go` (Neon) and `sqlite.go` (local dev). Selected by `store.driver` in config.

```go
type Store interface {
    // Runs
    CreateRun(ctx context.Context, company model.Company) (*model.Run, error)
    UpdateRunStatus(ctx context.Context, runID string, status model.RunStatus) error
    UpdateRunResult(ctx context.Context, runID string, result *model.RunResult) error
    GetRun(ctx context.Context, runID string) (*model.Run, error)
    ListRuns(ctx context.Context, filter RunFilter) ([]model.Run, error)

    // Phases
    CreatePhase(ctx context.Context, runID string, name string) (*model.RunPhase, error)
    CompletePhase(ctx context.Context, phaseID string, result *model.PhaseResult) error

    // Crawl cache (24h TTL)
    GetCachedCrawl(ctx context.Context, companyURL string) (*model.CrawlCache, error)
    SetCachedCrawl(ctx context.Context, companyURL string, pages []model.CrawledPage, ttl time.Duration) error
    DeleteExpiredCrawls(ctx context.Context) (int, error)

    // LinkedIn cache (per-domain)
    GetCachedLinkedIn(ctx context.Context, domain string) ([]byte, error)
    SetCachedLinkedIn(ctx context.Context, domain string, data []byte, ttl time.Duration) error

    // Scrape cache (per-URL Firecrawl/Jina result)
    GetCachedScrape(ctx context.Context, urlHash string) ([]byte, error)
    SetCachedScrape(ctx context.Context, urlHash string, content []byte, ttl time.Duration) error

    // High-confidence answer lookup (skip re-extraction optimization)
    GetHighConfidenceAnswers(ctx context.Context, companyURL string, minConfidence float64) ([]model.ExtractionAnswer, error)

    // Checkpoint/resume (saves intermediate state for failure recovery)
    SaveCheckpoint(ctx context.Context, companyID string, phase string, data []byte) error
    LoadCheckpoint(ctx context.Context, companyID string) (*model.Checkpoint, error)
    DeleteCheckpoint(ctx context.Context, companyID string) error

    // Cache cleanup
    DeleteExpiredLinkedIn(ctx context.Context) (int, error)
    DeleteExpiredScrapes(ctx context.Context) (int, error)

    // Lifecycle
    Ping(ctx context.Context) error
    Migrate(ctx context.Context) error
    Close() error
}
```

### Core Model Types

```go
// Company — input to the pipeline
type Company struct {
    URL, Name, SalesforceID, NotionPageID string
    Location, City, State, ZipCode, Street string
}

// Run — one enrichment run per company
type Run struct {
    ID        string
    Company   Company
    Status    RunStatus  // queued → crawling → classifying → extracting → aggregating → writing_sf → complete | failed
    Result    *RunResult
    CreatedAt, UpdatedAt time.Time
}

// RunResult — final outcome of a run
type RunResult struct {
    Score          float64
    FieldsFound    int
    FieldsTotal    int
    TotalTokens    int
    TotalCost      float64
    Phases         []PhaseResult
    Answers        []ExtractionAnswer
    Report         string
    SalesforceSync bool
}

// ExtractionAnswer — result of extracting an answer for a question
type ExtractionAnswer struct {
    QuestionID    string
    FieldKey      string
    Value         any
    Confidence    float64          // 0.0–1.0
    Source        string           // source page URL
    SourceURL     string
    Tier          int              // 1, 2, or 3
    Reasoning     string
    DataAsOf      *time.Time
    Contradiction *Contradiction   // flagged when tiers disagree
}

// Contradiction — when two tiers disagree with moderate+ confidence on both sides
type Contradiction struct {
    OtherTier       int
    OtherValue      any
    OtherConfidence float64
}

// FieldValue — resolved value ready for Salesforce
type FieldValue struct {
    FieldKey   string
    SFField    string
    Value      any
    Confidence float64
    Source     string
    Tier       int
    DataAsOf   *time.Time
}

// FieldMapping — maps internal field key to SF field (from Field Registry)
type FieldMapping struct {
    Key             string
    SFField         string
    SFObject        string
    DataType        string
    Required        bool
    MaxLength       int
    ValidationRegex *regexp.Regexp  // pre-compiled at registry load
    Status          string
}

// FieldRegistry — indexed collection with O(1) lookup by key or SF name
type FieldRegistry struct {
    Fields   []FieldMapping
    byKey    map[string]*FieldMapping
    bySFName map[string]*FieldMapping
    required []*FieldMapping
}

// TokenUsage — tracks token consumption per phase
type TokenUsage struct {
    InputTokens         int
    OutputTokens        int
    CacheCreationTokens int     // prompt caching: tokens written to cache
    CacheReadTokens     int     // prompt caching: tokens read from cache
    Cost                float64 // USD
}

// EnrichmentResult — final pipeline output
type EnrichmentResult struct {
    Company     Company
    RunID       string
    Score       float64
    Answers     []ExtractionAnswer
    FieldValues map[string]FieldValue
    PPPMatches  []ppp.LoanMatch
    Report      string
    Phases      []PhaseResult
    TotalTokens int
    TotalCost   float64
}

// Checkpoint — intermediate state for resume after failure
type Checkpoint struct {
    CompanyID string
    Phase     string    // e.g., "t1_complete"
    Data      []byte    // JSON-encoded answers
    CreatedAt time.Time
}
```

### Neon Postgres Schema (Enrichment)

**Why Neon, not Notion, for the run log?** The Go binary writes status updates at every phase transition (~12 writes per company with all sub-phases). At 100 companies/batch, that's 1,200 writes in minutes. Notion's 3 req/s limit would bottleneck the pipeline. Neon handles this trivially.

Tables:
- `runs` — one row per enrichment run (id, company_url, company_name, status, result JSONB, timestamps)
- `run_phases` — one row per phase per run (id, run_id, name, status, result JSONB, started_at, completed_at)
- `crawl_cache` — cached crawl results per company URL (24h TTL)
- `linkedin_cache` — cached LinkedIn data per domain
- `scrape_cache` — cached scrape results per URL hash
- `checkpoints` — intermediate pipeline state for resume after failure

### Neon Postgres Schema (Fedsync)

All fedsync tables live in the `fed_data` schema, separate from enrichment tables. Migrations tracked in `fed_data.schema_migrations`. See the fedsync section below for details.

---

## Registries (Notion Databases)

**Registries stay in Notion.** Seth needs to manage questions and field mappings without touching code. The Go binary pulls all `Active` entries from Notion at startup and caches them in memory for the duration of the run. No runtime dependency on Notion API during extraction phases.

### Question Registry Schema (Notion DB)

| Property | Type | Purpose |
|---|---|---|
| Question Key | Title | Unique identifier (e.g., `company_legal_name`) |
| Tier | Select (`1`, `2`, `3`) | Which Claude model: 1 = Haiku, 2 = Sonnet, 3 = Opus |
| Category | Select | Logical grouping |
| Source | Select (`firecrawl`, `perplexity`) | Routing: page router vs LinkedIn bypass |
| Question Text | Text | Prompt sent to Claude |
| System Prompt | Text | System message for Claude call |
| Instructions | Text | Detailed extraction guidance, few-shot examples, edge cases |
| Relevant Page Types | Multi-select | Page types from taxonomy |
| Output Schema | Text | JSON schema string defining expected output shape |
| Target SF Fields | Text | Comma-separated SF API names |
| Failure Behavior | Select (`escalate`, `null`, `skip`) | What to do on low confidence |
| Confidence Required | Number | Minimum confidence (0.0–1.0) |
| Status | Status (`Draft`, `Active`, `Deprecated`) | Only `Active` pulled at runtime |
| Last Tested | Date | Last quality validation |
| Notes | Text | Edge cases, known issues |

### Field Registry Schema (Notion DB)

| Property | Type | Purpose |
|---|---|---|
| Field Key | Title | Unique identifier matching JSON key (e.g., `legal_name`) |
| SF API Name | Text | Salesforce field API name (e.g., `Legal_Name__c`) |
| Display Name | Text | Human-readable label |
| Data Type | Select (`string`, `number`, `boolean`, `date`, `picklist`) | SF field data type — used by aggregation to validate and cast |
| Max Length | Number | SF field character limit (default 255). Truncate if exceeded. |
| Category | Select | identity, people, products, compliance, business_model, growth, risk, narrative, digital_presence, culture, deal_relevance |
| Source Priority | Select (`firecrawl`, `perplexity`, `both`) | Which source wins if both return a value |
| Required for Quality | Checkbox | If checked, must be populated to pass quality gate |
| Question | Relation (to Question Registry) | Links to question(s) that populate this field |
| Status | Status (`Draft`, `Active`, `Deprecated`) | Only `Active` fields included in aggregation and SF write-back |
| Notes | Text | SF field quirks, picklist values, validation rules |

### Registry Loading (Go)

```go
// internal/registry/question.go
func LoadQuestionRegistry(ctx context.Context, client *notionapi.Client, dbID string) ([]model.Question, error) {
    var questions []model.Question
    var cursor *notionapi.Cursor

    for {
        resp, err := client.Database.Query(ctx, notionapi.DatabaseID(dbID), &notionapi.DatabaseQueryRequest{
            Filter: &notionapi.PropertyFilter{
                Property: "Status",
                Status:   &notionapi.StatusFilterCondition{Equals: "Active"},
            },
            PageSize: 100,
            StartCursor: cursor,
        })
        if err != nil {
            return nil, eris.Wrap(err, "notion: query question registry")
        }

        for _, page := range resp.Results {
            q, err := parseQuestionPage(page)
            if err != nil {
                zap.L().Warn("skipping malformed question", zap.String("page_id", string(page.ID)), zap.Error(err))
                continue
            }
            questions = append(questions, q)
        }

        if !resp.HasMore {
            break
        }
        cursor = resp.NextCursor
    }

    zap.L().Info("loaded question registry", zap.Int("count", len(questions)))
    return questions, nil
}
```

---

## Page Type Taxonomy

### Company Website Pages (from Phase 1A crawl)

| Page Type | Description |
|---|---|
| `homepage` | Main landing page — company overview, value prop |
| `about` | Company history, mission, values |
| `services` | Service descriptions, capabilities |
| `products` | Product pages, catalogs |
| `pricing` | Pricing tiers, packages, rate cards |
| `contact` | Contact info, office locations |
| `team` | Executive bios, org chart, key personnel |
| `careers` | Job listings, culture pages |
| `blog` | Blog posts |
| `news` | Press releases, news mentions |
| `faq` | Frequently asked questions |
| `testimonials` | Client stories, reviews |
| `case_studies` | Detailed case studies |
| `partners` | Partner ecosystem, integrations |
| `legal` | Terms, privacy policy, compliance disclosures |
| `investors` | IR pages, funding, financial disclosures |
| `other` | Catch-all for unclassifiable pages |

### External Source Pages (from Phase 1B/1C)

| Page Type | Source | Discovery |
|---|---|---|
| `google_maps` | Google Maps — address, phone, rating, hours | Direct URL construction → scrape chain |
| `bbb_profile` | BBB — accreditation, rating, complaints | Jina Search `site:bbb.org` → scrape chain |
| `government_registry` | State business registry — legal entity, officers, status | Jina Search → scrape chain |
| `linkedin` | LinkedIn — overview, headcount, key personnel | Perplexity → Haiku JSON → synthetic page |

External page types are auto-classified by title prefix (e.g., `[bbb]` → `bbb_profile`). They are always included as supplementary context during question routing, regardless of a question's preferred page types.

Defined as Go constants in `internal/model/page.go`.

---

## Question Router (Phase 3)

The router is pure Go in `internal/pipeline/router.go`. Matches questions to relevant pages from the Page Index and groups them by tier.

**Key routing rules:**

- Each question declares preferred `page_types`; if none specified, all pages are eligible
- **External source pages** (`bbb_profile`, `google_maps`, `government_registry`, `linkedin`) are always included as supplementary context regardless of a question's preferred types
- Questions with no matched pages are skipped (reason: `"no_source"`)
- **Tier 1 (Haiku)** gets only its matched pages — main cost optimization lever
- **Tier 2 (Sonnet)** gets relevant pages + all Tier 1 answers as additional context
- **Tier 3 (Opus)** gets **prepared context** from Haiku summarization — not raw crawl. Each question receives a focused ~25K token extract instead of the full ~150K token crawl + all prior answers
- **Confidence escalation:** Tier 1 answers below `confidence_escalation_threshold` (0.4) re-queue into Tier 2

**Post-routing optimizations (applied in `pipeline.go`):**

- **High-confidence answer skip:** Questions whose field keys already have answers with confidence ≥ `skip_confidence_threshold` (default 0.8) from prior runs are removed from all tier batches. Existing answers are merged during aggregation.
- **Checkpoint/resume:** If a prior run completed T1 but failed later, cached T1 answers are reused instead of re-running extraction.

---

## Configuration

```yaml
# config.yaml — loaded by Viper. Env vars override with RESEARCH_ prefix.

store:
  driver: "postgres"          # "postgres" (Neon) or "sqlite" (local dev)
  database_url: "${RESEARCH_DATABASE_URL}"
  max_conns: 10
  min_conns: 2

notion:
  token: "${RESEARCH_NOTION_TOKEN}"
  lead_db: "${RESEARCH_NOTION_LEAD_DB}"
  question_db: "${RESEARCH_NOTION_QUESTION_DB}"
  field_db: "${RESEARCH_NOTION_FIELD_DB}"

jina:
  key: "${RESEARCH_JINA_KEY}"
  base_url: "https://r.jina.ai"            # Reader (scrape)
  search_base_url: "https://s.jina.ai"     # Search (discovery)

firecrawl:
  key: "${RESEARCH_FIRECRAWL_KEY}"
  base_url: "https://api.firecrawl.dev/v2"  # Fallback only
  max_pages: 50

perplexity:
  key: "${RESEARCH_PERPLEXITY_KEY}"
  base_url: "https://api.perplexity.ai"
  model: "sonar-pro"

anthropic:
  key: "${RESEARCH_ANTHROPIC_KEY}"
  haiku_model: "claude-haiku-4-5-20251001"
  sonnet_model: "claude-sonnet-4-5-20250929"
  opus_model: "claude-opus-4-6"
  max_batch_size: 100
  no_batch: false                 # true = use Messages API instead of Batch API
  small_batch_threshold: 3        # bypass Batch API for <N items (latency optimization)

salesforce:
  client_id: "${RESEARCH_SF_CLIENT_ID}"
  username: "${RESEARCH_SF_USERNAME}"
  key_path: "${RESEARCH_SF_KEY_PATH}"
  login_url: "https://login.salesforce.com"

tooljet:
  webhook_url: "${RESEARCH_TOOLJET_WEBHOOK}"

ppp:
  similarity_threshold: 0.4       # fuzzy name match threshold
  max_candidates: 10

pipeline:
  confidence_escalation_threshold: 0.4   # T1 answers below this escalate to T2
  tier3_gate: "off"                      # "off", "always", "ambiguity_only"
  quality_score_threshold: 0.6           # below this → manual review
  max_cost_per_company_usd: 10.0         # cost budget gate for T3
  skip_confidence_threshold: 0.8         # reuse existing answers above this

crawl:
  max_pages: 50
  max_depth: 2
  timeout_secs: 60
  cache_ttl_hours: 24
  exclude_paths: ["/blog/*", "/news/*", "/press/*", "/careers/*"]

scrape:
  search_timeout_secs: 15
  search_retries: 1

waterfall:
  config_path: "config/waterfall.yaml"
  confidence_threshold: 0.7        # fields below this enter waterfall
  max_premium_cost_usd: 2.00       # per-company premium source budget

batch:
  max_concurrent_companies: 15     # parallel enrichment runs in batch mode

server:
  port: 8080

log:
  level: "info"    # debug, info, warn, error
  format: "json"   # json (prod) or console (dev)

# Fedsync config (see fedsync section)
fedsync:
  database_url: "${RESEARCH_FEDSYNC_DATABASE_URL}"  # falls back to store.database_url
  temp_dir: "/tmp/fedsync"
  sam_api_key: "${RESEARCH_FEDSYNC_SAM_API_KEY}"
  fred_api_key: "${RESEARCH_FEDSYNC_FRED_API_KEY}"
  bls_api_key: "${RESEARCH_FEDSYNC_BLS_API_KEY}"
  census_api_key: "${RESEARCH_FEDSYNC_CENSUS_API_KEY}"
  edgar_user_agent: "Sells Advisors blake@sellsadvisors.com"
  mistral_api_key: "${RESEARCH_FEDSYNC_MISTRAL_API_KEY}"
  ocr:
    provider: "local"              # "local" (pdftotext) or "mistral"

# Per-provider pricing rates (used by cost calculator)
pricing:
  anthropic:
    claude-haiku-4-5-20251001:
      input: 1.0      # $/MTok
      output: 5.0
      batch_discount: 0.5
      cache_write_mul: 1.25
      cache_read_mul: 0.1
    claude-sonnet-4-5-20250929:
      input: 3.0
      output: 15.0
      batch_discount: 0.5
      cache_write_mul: 1.25
      cache_read_mul: 0.1
    claude-opus-4-6:
      input: 5.0
      output: 25.0
      batch_discount: 0.5
      cache_write_mul: 1.25
      cache_read_mul: 0.1
  jina:
    per_mtok: 0.02
  perplexity:
    per_query: 0.005
  firecrawl:
    plan_monthly: 19.00
    credits_included: 3000
```

---

## Fedsync — Federal Data Sync

The fedsync subsystem incrementally syncs 26 federal datasets into `fed_data.*` Postgres tables. Runs daily via Fly.io cron; exits in <1s when no new data is expected.

### Dataset Interface

Each of 26 datasets implements the `Dataset` interface in `internal/fedsync/dataset/`:

```go
type Dataset interface {
    Name() string                                                       // e.g., "cbp"
    Table() string                                                     // e.g., "fed_data.cbp_data"
    Phase() Phase                                                      // Phase1, Phase1B, Phase2, Phase3
    Cadence() Cadence                                                  // Daily, Weekly, Monthly, Quarterly, Annual
    ShouldRun(now time.Time, lastSync *time.Time) bool                 // scheduling check
    Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error)
}
```

The `Engine` iterates the registry, checks `ShouldRun()` for each dataset, calls `Sync()`, and records outcomes in `fed_data.sync_log`.

### Datasets by Phase

| Phase | Category | Datasets | Cadence |
|---|---|---|---|
| **1** | Market Intelligence | Census CBP, SUSB · BLS QCEW, OEWS · SAM.gov FPDS · Census Economic Census | Annual–Daily |
| **1B** | Buyer Intelligence (SEC/EDGAR) | ADV Part 1A · IARD daily XML · 13F Holdings · Form D · EDGAR Submissions · Entity Cross-ref | Daily–Quarterly |
| **2** | Extended Intelligence | ADV Part 2 (OCR) · FINRA BrokerCheck · Form BD · OSHA ITA · EPA ECHO · Census NES, ASM · BLS ECI | Monthly–Annual |
| **3** | On-Demand | ADV Part 3/CRS (OCR) · XBRL Facts · FRED Series · Census ABS · BLS CPS/LAUS · Census M3 | Daily–Annual |

### Streaming Pattern (Large Datasets)

Memory stays bounded regardless of dataset size:

1. `fetcher.DownloadToFile()` → ZIP to temp dir
2. `fetcher.ExtractZIP()` → CSV/TSV to temp dir
3. `fetcher.StreamCSV()` → `<-chan []string` (row channel)
4. Consumer batches rows (5,000–10,000) → `db.BulkUpsert()` or `db.CopyFrom()`

### Rate Limiting

Per-host limiters in `internal/fetcher/http.go` via `golang.org/x/time/rate`:

| Host | Limit | Notes |
|---|---|---|
| SEC (efts/www/data.sec.gov) | 10 req/s | EDGAR requires `User-Agent` header |
| SAM.gov | 5 req/s | |
| Default | 20 req/s | |

### Migrations

SQL files embedded via `embed.FS` in `internal/fedsync/migrate.go`. Tracked in `fed_data.schema_migrations`. Applied in lexicographic order, idempotent (skips already-applied).

### CLI Commands

```bash
research-cli fedsync migrate                            # apply schema migrations
research-cli fedsync status                             # show sync log (last sync per dataset)
research-cli fedsync sync                               # sync all due datasets
research-cli fedsync sync --phase 1                     # sync Phase 1 only
research-cli fedsync sync --datasets cbp,fpds --force   # force specific datasets
research-cli fedsync sync --full                        # full historical reload
research-cli fedsync xref                               # build entity cross-reference (CRD↔CIK)
```

### Integration with Enrichment

Fedsync data feeds back into the enrichment pipeline in two ways:

1. **Phase 1D — PPP Loan Lookup:** Queries `fed_data.ppp_*` tables for company name matches
2. **Phase 7 — Revenue Enrichment:** Uses Census CBP data via the `estimate.RevenueEstimator` to augment extracted answers with revenue estimates based on NAICS code and employee count

---

## Cost Model

### API Pricing (Anthropic — Feb 2026)

| Model | Input | Output | Batch (50% off) |
|---|---|---|---|
| Opus 4.6 | $5 / MTok | $25 / MTok | $2.50 in / $12.50 out |
| Sonnet 4.5 | $3 / MTok | $15 / MTok | $1.50 in / $7.50 out |
| Haiku 4.5 | $1 / MTok | $5 / MTok | $0.50 in / $2.50 out |

### Per-Company Cost (Batch Pricing)

| Component | Est. Cost | Notes |
|---|---|---|
| Firecrawl (local-first + fallback) | ~24 credits avg | 60% local crawl (0 credits), 40% Firecrawl fallback (50), + 4 scrapes |
| Perplexity — LinkedIn | ~$0.01 | 1 query |
| Haiku — LinkedIn JSON | ~$0.01 | Parse → strict JSON |
| Haiku — classification | ~$0.13 | ~54 pages (batch 50% off) |
| Tier 1 — Haiku (70 Qs) | ~$0.23 | Single-page extraction (batch) |
| Tier 2 — Sonnet (25 Qs) | ~$0.68 | Multi-page synthesis (batch) |
| Haiku — context prep for T3 | ~$0.12 | Summarize crawl → 5 focused extracts (~3K tok each) |
| Tier 3 — Opus (5 Qs) | ~$0.35 | Prepared context (~25K tok/Q), not raw markdown (batch) |
| Haiku — aggregation | ~$0.08 | Merge + sanity check |
| **Total (with Opus)** | **~$1.61** | Down from $4.90 with raw-crawl approach |
| **Total (Sonnet for T3)** | **~$1.47** | Opus premium now only $0.14/company |

### Primer + Batch Cache Strategy

For Tier 2 and 3: send 1 primer request (sequential, 1-hour TTL cache) → then submit remaining questions as a batch. Batch items hit warm cache = massive savings.

| Component | Batch Only | Primer+Batch (worst) | Primer+Batch (best) |
|---|---|---|---|
| Tier 1 — Haiku | ~$0.23 | ~$0.23 | ~$0.23 |
| Tier 2 — Sonnet | ~$0.68 | ~$0.72 | ~$0.14 |
| Context prep (Haiku) | ~$0.12 | ~$0.12 | ~$0.12 |
| Tier 3 — Opus (5 Qs) | ~$0.35 | ~$0.44 | ~$0.22 |
| Other | ~$0.23 | ~$0.23 | ~$0.23 |
| **Total (Opus)** | **~$1.61** | **~$1.74** | **~$0.94** |
| **Total (Sonnet T3)** | **~$1.47** | **~$1.57** | **~$0.86** |

### Infrastructure Costs

| Component | Est. Monthly | Notes |
|---|---|---|
| Fly.io compute | ~$15-30 | performance-4x 8GB RAM. Per-second billing. Machine sleeps between runs. |
| Neon Postgres | $19 (Launch) or $5 min (usage-based) | Scale-to-zero. 10GB storage, 300 CU-hours included. |
| Fly.io bandwidth | ~$1-2 | $0.02/GB outbound NA/EU. Mostly API calls, minimal data. |
| **Total infra** | **~$35-50/mo** | Still <1% of total spend at scale |

### Monthly Cost at Scale (5K and 10K companies)

| Component | 5K — Batch Only | 5K — Primer+Batch | 10K — Batch Only | 10K — Primer+Batch |
|---|---|---|---|---|
| Tier 1 — Haiku | $1,150 | $1,150 | $2,300 | $2,300 |
| Tier 2 — Sonnet | $3,400 | $700 | $6,800 | $1,400 |
| Context prep (Haiku) | $600 | $600 | $1,200 | $1,200 |
| Tier 3 — Opus (5 Qs) | $1,750 | $1,100 | $3,500 | $2,200 |
| Other (classify, LI, agg) | $1,150 | $1,150 | $2,300 | $2,300 |
| **Claude subtotal (Opus T3)** | **$8,050** | **$4,700** | **$16,100** | **$9,400** |
| **Claude subtotal (Sonnet T3)** | **$7,350** | **$4,250** | **$14,700** | **$8,500** |
| Firecrawl | $399 (Growth) | $399 (Growth) | $399 (Growth) | $399 (Growth) |
| Perplexity | $50 | $50 | $100 | $100 |
| Infra (Fly + Neon) | $50 | $50 | $50 | $50 |
| **Grand total (Opus T3)** | **~$8,550** | **~$5,200** | **~$16,650** | **~$9,950** |
| **Grand total (Sonnet T3)** | **~$7,850** | **~$4,750** | **~$15,250** | **~$9,050** |

### Total Cost Per Company at Scale

All-in estimate (Claude + Firecrawl + Perplexity + infra) using **Opus T3 + primer+batch + context prep**.

| Volume | Claude/co | Firecrawl/co | Other/co | All-in/co |
|---|---|---|---|---|
| 100 / mo | ~$0.94 | ~$0.99 | ~$0.51 | **~$2.44** |
| 500 / mo | ~$0.94 | ~$0.20 | ~$0.11 | **~$1.25** |
| 1K / mo | ~$0.94 | ~$0.10 | ~$0.06 | **~$1.10** |
| 5K / mo | ~$0.94 | ~$0.08 | ~$0.02 | **~$1.04** |
| **10K / mo** | **~$0.94** | **~$0.04** | **~$0.02** | **~$1.00** |

**Key takeaways:**

- **Haiku context prep + 5 Opus Qs is a game-changer** — T3 cost dropped from $3.75 to $0.47/company (87% reduction). Never send raw crawl markdown to Opus.
- **Opus vs Sonnet T3 gap is now negligible** — only $0.14/company. Use Opus for quality; the cost argument for Sonnet T3 is largely gone.
- **Primer+batch caching saves ~42%** on Claude costs at both volumes.
- **Total cost at realistic volumes:** ~$5.2K/mo (5K, Opus, primer+batch) or ~$10K/mo (10K, Opus, primer+batch).
- **Firecrawl is 4-8% of total** — local-first crawl cuts credits ~55%. Growth plan ($399) covers up to ~20K companies.
- **Recommendation:** Use **Opus T3 + primer+batch + context prep** as the default.

---

## Deployment

### fly.toml

```toml
app = "sells-research"
primary_region = "dfw"

[build]
  dockerfile = "Dockerfile"

[env]
  RESEARCH_LOG_LEVEL = "info"
  RESEARCH_LOG_FORMAT = "json"
  RESEARCH_STORE_DRIVER = "postgres"

[http_service]
  internal_port = 8080
  auto_stop_machines = "stop"
  auto_start_machines = true
  min_machines_running = 0

[[vm]]
  memory = "8gb"
  cpu_kind = "performance"
  cpus = 4
```

### Dockerfile

```dockerfile
FROM golang:1.23-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /research-cli ./cmd

FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata
COPY --from=build /research-cli /usr/local/bin/research-cli
ENTRYPOINT ["research-cli"]
CMD ["serve", "--port", "8080"]
```

### Secrets

```bash
fly secrets set \
  RESEARCH_DATABASE_URL="postgres://user:pass@ep-xxx.us-east-2.aws.neon.tech/researchdb?sslmode=require" \
  RESEARCH_NOTION_TOKEN="ntn_xxx" \
  RESEARCH_NOTION_LEAD_DB="xxx" \
  RESEARCH_NOTION_QUESTION_DB="xxx" \
  RESEARCH_NOTION_FIELD_DB="xxx" \
  RESEARCH_FIRECRAWL_KEY="fc-xxx" \
  RESEARCH_PERPLEXITY_KEY="pplx-xxx" \
  RESEARCH_ANTHROPIC_KEY="sk-ant-xxx" \
  RESEARCH_SF_CLIENT_ID="xxx" \
  RESEARCH_SF_USERNAME="blake@sellsadvisors.com" \
  RESEARCH_TOOLJET_WEBHOOK="https://..."
```

### Trigger Modes

| Command | Trigger | Use Case |
|---|---|---|
| `research-cli import --csv leads.csv` | Manual / CI | Import CSV into Notion Lead Tracker. Run locally or via `fly ssh`. |
| `research-cli run --url acme.com --sf-id 001xx` | Manual | Enrich a single company. Dev/testing. |
| `research-cli batch --limit 100` | Cron / Manual | Process queued leads from Notion. Primary production trigger. |
| `research-cli serve --port 8080` | HTTP webhook | Fly auto-starts on request, auto-stops when idle. For SF triggers or ToolJet callbacks. |

### Deployment Commands

```bash
# Deploy
fly deploy

# Run batch manually
fly ssh console -C "research-cli batch --limit 100"

# Schedule cron (every 2 hours)
fly machine run --schedule "0 */2 * * *" -- research-cli batch --limit 50
```

---

## Local Development

```bash
# Clone + setup
git clone git@github.com:sells-group/research-cli.git
cd research-cli
cp config.example.yaml config.yaml  # fill in API keys

# config.yaml for local dev
# store:
#   driver: "sqlite"
#   sqlite:
#     path: "./data/research.db"

# Run
go run ./cmd import --csv testdata/sample_leads.csv
go run ./cmd run --url "https://acme.com" --sf-id "001xx"
go run ./cmd batch --limit 5

# Test
go test ./...
go test ./internal/pipeline/ -run TestRouter -v
go test ./pkg/anthropic/ -run TestBatchPoll -v
```

**Testing strategy:**

- `pkg/` clients: mock HTTP responses with `httptest.Server`
- `internal/pipeline/`: mock `pkg/` clients behind interfaces, test phase logic with canned data
- `internal/store/sqlite.go`: real SQLite in `t.TempDir()` for store tests
- **No external API calls in CI.** All mocked. External integration tests run manually with `--integration` flag.

---

## Ownership Matrix

| Person | Responsibilities | Phase(s) |
|---|---|---|
| **Blake** | End-to-end Go binary build; all `pkg/` API clients; pipeline orchestration; Fly.io deployment; Neon schema; prompt caching strategy | 0-9 |
| **Seth** | Question Registry content; Field Registry + SF field mapping; enrichment prompt authoring (system prompt + instructions + output schema) | 3, 4-6 |
| **Hutton** | Salesforce field schema; validate SF API names in Field Registry; Connected App setup for JWT auth; SOQL for account matching | 9 |
| **Research Team** | ToolJet validation on flagged records below quality threshold | 9 |

---

## Open Decisions

| # | Decision | Current Default / Notes |
|---|---|---|
| 1 | **Notion Lead Tracker DB schema** — confirm properties needed for CSV import | Minimum: Company Name, Website, SF Account ID, City, State, Enrichment Status, Quality Score, Fields Populated, Last Enriched. Add more as needed. |
| 2 | **SF Connected App** — JWT Bearer vs username-password flow? | JWT Bearer (server-to-server, no user interaction). Hutton to create Connected App + upload certificate. |
| 3 | **Tier 3 gating** — always run or only when Tier 2 flags ambiguity? | Config switch: `always` vs `ambiguity_only`. Opus is ~77% of cost. |
| 4 | **Confidence re-eval threshold** | 0.4 default. Tune after initial runs. |
| 5 | **Firecrawl exclude paths** | Current: `/blog/*`, `/news/*`, `/press/*`, `/careers/*`. Confirm. |
| 6 | **Question granularity** — grouped or one-per-field? | Start grouped (Seth's current structure). Break into per-field if quality issues emerge. |
| 7 | **PPP + SoS URL construction** — how to build scrapeable URLs? | Need to confirm URL patterns for PPP (ProPublica) and Secretary of State (varies by state). |
| 8 | **CSV format** — standardize column names for `research-cli import`? | Map common Grata export columns to Notion properties. Handle missing columns gracefully. |

---

## Next Steps

- [ ] **Blake:** Scaffold Go project (cobra root + subcommands, viper config, zap logger)
- [ ] **Blake:** Build `pkg/firecrawl` client (crawl + scrape + poll)
- [ ] **Blake:** Build `pkg/anthropic` client (messages + batch + cache primer)
- [ ] **Blake:** Build `pkg/salesforce` client (JWT auth, SOQL, CRUD, Collections)
- [ ] **Blake:** Build `pkg/notion` client (DB query, page create/update, CSV mapper)
- [ ] **Blake:** Build `pkg/perplexity` client (chat completions)
- [ ] **Blake:** Implement `internal/store` interface (Neon + SQLite)
- [ ] **Blake:** Implement `research-cli import` command (CSV → Notion Lead Tracker)
- [ ] **Blake:** Implement pipeline phases 1-9 in `internal/pipeline`
- [ ] **Blake:** Fly.io deployment (Dockerfile, fly.toml, secrets)
- [ ] **Blake:** Neon DB setup (schema migration, connection)
- [ ] **Hutton:** Create SF Connected App for JWT Bearer auth
- [ ] **Hutton:** Validate SF field API names for all 24 target fields
- [ ] **Seth:** Migrate Research Prompts DB to v2 schema
- [ ] **Seth:** Split monolithic prompts into System Prompt + Instructions + Output Schema
- [ ] **Team:** Create Notion Lead Tracker DB with agreed schema
- [ ] **Team:** Decide Tier 3 gating (always vs. ambiguity_only)
- [ ] **Team:** Confirm PPP + Secretary of State URL patterns
