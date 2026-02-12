# research-cli

Automated account enrichment pipeline — import leads via CSV into a Notion lead tracker, crawl company websites + external sources, classify content, extract structured data via tiered Claude models, and push clean records to Salesforce.

## Overview

| | |
|---|---|
| **Input** | CSV file of companies (from Grata, manual research, etc.) → imported into a Notion Lead Tracker DB |
| **Output** | Enriched fields written to Salesforce Account records + status/quality updated on the Notion lead page |
| **Runtime** | Go binary (`research-cli`) on Fly.io (cron or webhook) · Neon Postgres (state) · SQLite (local dev) |
| **Stack** | Go (cobra · zap · eris · viper) · Fly.io (compute) · Neon Postgres (run log + staging) · Firecrawl (crawl + scrape) · Perplexity + Haiku (LinkedIn) · Claude Haiku / Sonnet / Opus (tiered extraction) · Salesforce REST API (destination) · Notion API (lead tracker + registries) · ToolJet (manual review UI) |

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

- Exclude paths apply to both local and Firecrawl: `/blog/*`, `/news/*`, `/press/*`, `/careers/*`
- Estimated ~60% of company sites serve clean HTML → **~55% Firecrawl credit reduction**

### Phase 1B — Firecrawl: Google Places + BBB + PPP + SoS *(parallel with 1A)*

- Construct target URLs from company name + location
- Firecrawl `/v2/scrape` single page per source
- Returns markdown — fed into classification and extraction alongside company pages

### Phase 1C — Perplexity: LinkedIn *(parallel with 1A/1B)*

- Targeted Perplexity query for company LinkedIn profile data
- Raw response piped through **Haiku** to enforce strict JSON output schema
- Output merged into enrichment data alongside crawled pages

### Phase 2 — Page Classification (Haiku)

- Each crawled page classified against the PAGE_TYPE_TAXONOMY
- Output per page: `page_types[]`, `relevance_tags[]`, `has_structured_data`, `content_summary`, `quality`
- Builds a **Page Index** — lookup table mapping page types → URLs

### Phase 3 — Question Router (Go code)

- Reads QUESTION_REGISTRY from Notion API (cached in memory for the run)
- Each question declares which `page_types` it needs
- Router matches questions → relevant pages from the Page Index
- Groups questions into batches by tier

### Phase 4 — Tier 1: Haiku (~70 questions)

- Single-page fact extraction, no reasoning
- Each batch gets **only** the pages classified as relevant
- Strict JSON output per question
- Low-confidence answers (< 0.4) escalate to Tier 2

### Phase 5 — Tier 2: Sonnet (~25 questions)

- Multi-page synthesis, light reasoning
- Gets Page Index + all Tier 1 answers as context
- Also handles confidence re-evals escalated from Tier 1

### Phase 6 — Tier 3: Opus (~5 questions)

Two-step process to minimize Opus input tokens:

1. **6A — Haiku Context Prep:** Single batch call reads full crawl + Page Index + T1/T2 answers. Produces a focused context extract per T3 question (~3K tokens each). Strips raw markdown; keeps only decision-relevant facts, quotes, and data points.
2. **6B — Opus Extraction:** Each of the 5 questions gets its prepared extract (~25K tokens) instead of raw crawl (~150K). Cross-page strategic reasoning on distilled, not raw, context.

- Feature-gated: config switch for `"always"` vs `"ambiguity_only"`
- ~87% cheaper than raw-context approach at batch pricing

### Phase 7 — Aggregation (Haiku)

- Merge all tier outputs + Google Places/BBB/LinkedIn/PPP/SoS data
- Validate against master JSON schema from Field Registry
- Per-field confidence scoring
- Light Haiku sanity check for contradictions/hallucinations

### Phase 8 — Enrichment Report

- Plaintext report per account written to a single SF long text field
- Contains: fields populated (✓), fields not found (✗), low confidence (⚠), sources used, errors, timestamp, quality score

### Phase 9 — Quality Gate → Salesforce + Notion Update

- Score >= threshold → CRUD update to SF via REST API (dynamic field mapping from Field Registry)
- Score < threshold → POST to ToolJet webhook for Research Team manual review
- **Always:** Update the Notion Lead Tracker page with enrichment status, quality score, fields populated count, and timestamp

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
    GO --> GPBBB["Phase 1B: Firecrawl<br>GP + BBB + PPP + SoS"]
    GO --> LI["Phase 1C: Perplexity<br>LinkedIn -> Haiku JSON"]
    GPBBB --> CLASS
    CLASS --> IDX["Page Index"]
    IDX --> ROUTER["Phase 3: Question Router<br>(Go code)"]
    ROUTER --> T1["Phase 4: Tier 1 -- Haiku<br>~70 Qs · Fact Extraction"]
    T1 -->|"confidence < 0.4"| T2
    ROUTER --> T2["Phase 5: Tier 2 -- Sonnet<br>~25 Qs · Multi-page Synthesis"]
    T2 --> PREP["Phase 6A: Haiku Context Prep<br>Summarize crawl -> focused extracts"]
    PREP --> T3["Phase 6B: Tier 3 -- Opus<br>5 Qs · Prepared Context"]
    T1 --> AGG["Phase 7: Aggregation<br>(Haiku)"]
    T2 --> AGG
    T3 --> AGG
    LI --> AGG
    AGG --> RPT["Phase 8: Enrichment Report"]
    RPT --> GATE{"Phase 9:<br>Quality Gate"}
    GATE -->|"Score >= threshold"| SF["Salesforce<br>(REST API CRUD)"]
    GATE -->|"Score < threshold"| TJ["ToolJet Webhook<br>(Research Team Review)"]
    GATE -->|"Always"| NLT2["Notion Lead Tracker<br>(Status Update)"]
    TJ --> SF
    GO -.->|"run log writes"| NEON["Neon Postgres<br>(Run Log + Staging)"]
    GO -.->|"registry reads"| NREG["Notion DBs<br>(Question + Field Registries)"]
```

---

## Project Structure

```
research-cli/
├── cmd/
│   ├── root.go              # cobra root command, viper config init, zap logger init
│   ├── import.go            # `research-cli import --csv leads.csv` -> Notion Lead Tracker
│   ├── run.go               # `research-cli run --url acme.com --sf-id 001xxx` -> single company
│   ├── batch.go             # `research-cli batch --limit 100` -> process queued leads from Notion
│   └── serve.go             # `research-cli serve --port 8080` -> webhook listener (Fly auto-stop)
├── internal/
│   ├── config/
│   │   └── config.go        # viper config struct + loader
│   ├── pipeline/
│   │   ├── pipeline.go      # orchestrates phases 1-9 for a single company
│   │   ├── crawl.go         # Phase 1A: orchestrator (local-first -> Firecrawl fallback)
│   │   ├── localcrawl.go    # Local crawl: net/http probe + colly link discovery + html-to-markdown
│   │   ├── blockdetect.go   # Cloudflare / captcha / JS-shell detection heuristics
│   │   ├── scrape.go        # Phase 1B: Firecrawl single-page scrapes (GP, BBB, PPP, SoS)
│   │   ├── linkedin.go      # Phase 1C: Perplexity -> Haiku JSON
│   │   ├── classify.go      # Phase 2: Haiku page classification
│   │   ├── router.go        # Phase 3: question -> page matching
│   │   ├── extract.go       # Phases 4-6: tiered Claude calls
│   │   ├── aggregate.go     # Phase 7: merge + validate
│   │   ├── report.go        # Phase 8: enrichment report
│   │   └── gate.go          # Phase 9: quality gate + SF write + Notion update
│   ├── registry/
│   │   ├── question.go      # reads Question Registry from Notion API
│   │   └── field.go         # reads Field Registry from Notion API
│   ├── store/
│   │   ├── store.go         # interface: RunLog, Staging (Neon or SQLite)
│   │   ├── postgres.go      # pgx implementation for Neon
│   │   └── sqlite.go        # modernc.org/sqlite implementation for local dev
│   └── model/
│       ├── company.go       # Company, EnrichmentResult, QualityReport
│       ├── page.go          # CrawledPage, PageIndex, PageClassification
│       ├── question.go      # Question, QuestionRegistry, TierBatch
│       └── field.go         # FieldMapping, FieldRegistry
├── pkg/
│   ├── anthropic/
│   │   ├── client.go        # Claude Messages API client
│   │   ├── batch.go         # Batch API: create, poll, retrieve results
│   │   └── cache.go         # prompt caching helpers (primer strategy)
│   ├── firecrawl/
│   │   ├── client.go        # Firecrawl v2: crawl, scrape, batch scrape
│   │   └── poll.go          # async job polling with backoff
│   ├── perplexity/
│   │   └── client.go        # Perplexity chat completions (OpenAI-compatible)
│   ├── salesforce/
│   │   ├── client.go        # auth (OAuth 2.0 JWT Bearer), token refresh
│   │   ├── query.go         # SOQL queries
│   │   ├── crud.go          # single record CRUD
│   │   └── composite.go     # sObject Collections (bulk update up to 200)
│   └── notion/
│       ├── client.go        # Notion API client wrapper
│       ├── database.go      # query database, create page, update page
│       └── csv.go           # CSV -> Notion page mapper
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
| **errgroup** | `golang.org/x/sync/errgroup` | Structured concurrency for parallel phases | Fan out Phase 1A/1B/1C in parallel. Limit concurrency for API rate limits. |
| **go-salesforce** | `github.com/k-capehart/go-salesforce/v3` | Salesforce REST API wrapper | SOQL, CRUD, Collections. On awesome-go. Actively maintained. |
| **notionapi** | `github.com/jomei/notionapi` | Notion API client | Database query, page create/update. Used for Lead Tracker + Registry reads. |
| **colly** | `github.com/gocolly/colly/v2` | Web crawling — link discovery, depth control, robots.txt | Phase 1A local crawl. Respects robots.txt. Depth 2, cap 50 pages. Falls back to Firecrawl on block. |
| **html-to-markdown** | `github.com/JohannesKaufmann/html-to-markdown/v2` | HTML → clean markdown conversion | Replaces Firecrawl's markdown output for locally crawled pages. Handles tables, lists, links. |
| **anthropic-sdk-go** | `github.com/anthropics/anthropic-sdk-go` | Official Anthropic SDK — Messages + Batch API | Supports prompt caching, batch create/poll/results. Use for all Claude calls. |

**No SDK for Firecrawl or Perplexity in Go.** Both are simple REST APIs — use `net/http` with typed request/response structs. Perplexity is OpenAI-compatible, so any OpenAI Go client works, but raw HTTP is cleaner for a single endpoint.

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

### Neon Postgres (Run Log + Staging)

**Why Neon, not Notion, for the run log?** The Go binary writes status updates at every phase transition (~9 writes per company). At 100 companies/batch, that's 900 writes in minutes. Notion's 3 req/s limit would bottleneck the pipeline. Neon handles this trivially. The Notion Lead Tracker gets a single summary update at the end of each run.

```sql
-- Run log: one row per company enrichment run
CREATE TABLE runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_name    TEXT NOT NULL,
    company_url     TEXT NOT NULL,
    sf_account_id   TEXT,
    notion_page_id  TEXT,
    status          TEXT NOT NULL DEFAULT 'queued',
        -- queued -> crawling -> classifying -> extracting -> aggregating -> writing_sf -> complete | failed
    quality_score   REAL,
    fields_populated INTEGER,
    fields_missing   INTEGER,
    fields_low_conf  INTEGER,
    pages_crawled    INTEGER,
    tier3_used       BOOLEAN DEFAULT FALSE,
    estimated_cost   REAL,
    error_summary    TEXT,
    sent_to_tooljet  BOOLEAN DEFAULT FALSE,
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_runs_status ON runs(status);
CREATE INDEX idx_runs_sf_account ON runs(sf_account_id);
CREATE INDEX idx_runs_created ON runs(created_at DESC);

-- Phase timing: one row per phase per run
CREATE TABLE run_phases (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id      UUID REFERENCES runs(id),
    phase       TEXT NOT NULL,  -- 'crawl', 'classify', 'tier1', 'tier2', 'tier3', 'aggregate', 'sf_write'
    status      TEXT NOT NULL DEFAULT 'running',  -- running -> complete | failed
    started_at  TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    input_tokens  INTEGER,
    output_tokens INTEGER,
    cached_tokens INTEGER,
    estimated_cost REAL,
    error_detail TEXT,
    metadata     JSONB  -- phase-specific data (e.g., pages crawled, questions routed, batch IDs)
);

CREATE INDEX idx_phases_run ON run_phases(run_id);

-- Crawl cache: avoid re-crawling recently crawled sites
CREATE TABLE crawl_cache (
    company_url  TEXT PRIMARY KEY,
    crawl_data   JSONB NOT NULL,  -- full crawl result (page markdown + metadata)
    page_index   JSONB,           -- classified page index
    crawled_at   TIMESTAMPTZ DEFAULT NOW(),
    expires_at   TIMESTAMPTZ DEFAULT NOW() + INTERVAL '7 days'
);

CREATE INDEX idx_cache_expires ON crawl_cache(expires_at);
```

### SQLite (Local Dev — Same Schema)

```go
// internal/store/store.go
type Store interface {
    CreateRun(ctx context.Context, run *model.Run) error
    UpdateRunStatus(ctx context.Context, runID string, status string) error
    UpdateRunResult(ctx context.Context, runID string, result *model.RunResult) error
    GetRun(ctx context.Context, runID string) (*model.Run, error)
    ListRuns(ctx context.Context, filter RunFilter) ([]model.Run, error)

    CreatePhase(ctx context.Context, phase *model.RunPhase) error
    CompletePhase(ctx context.Context, phaseID string, result *model.PhaseResult) error

    GetCachedCrawl(ctx context.Context, companyURL string) (*model.CrawlCache, error)
    SetCachedCrawl(ctx context.Context, cache *model.CrawlCache) error
}
```

Both `postgres.go` and `sqlite.go` implement this interface. Viper config selects which:

```yaml
# config.yaml
store:
  driver: "postgres"  # or "sqlite"
  postgres:
    url: "${RESEARCH_DATABASE_URL}"  # Neon connection string from Fly secrets
  sqlite:
    path: "./data/research.db"
```

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

| Page Type | Description | Source |
|---|---|---|
| `homepage` | Main landing page — company overview, value prop | Firecrawl (company) |
| `about` | Company history, mission, values, team overview | Firecrawl (company) |
| `leadership` | Executive bios, org chart, key personnel | Firecrawl (company) |
| `services` | Product/service descriptions, capabilities | Firecrawl (company) |
| `contact` | Contact info, office locations | Firecrawl (company) |
| `legal` | Terms, privacy policy, compliance disclosures | Firecrawl (company) |
| `pricing` | Pricing tiers, packages, rate cards | Firecrawl (company) |
| `case_studies` | Client stories, testimonials | Firecrawl (company) |
| `industries` | Vertical/sector-specific pages | Firecrawl (company) |
| `partners` | Partner ecosystem, integrations | Firecrawl (company) |
| `investors` | IR pages, funding, financial disclosures | Firecrawl (company) |
| `google_places` | Google Places — address, phone, rating, hours | Firecrawl (scrape) |
| `bbb` | BBB — accreditation, rating, complaints | Firecrawl (scrape) |
| `linkedin` | LinkedIn — overview, headcount, key personnel | Perplexity → Haiku |
| `ppp` | PPP Federal Loan — revenue estimate, employee count | Firecrawl (scrape) |
| `secretary_of_state` | State registry — legal entity, officers, status | Firecrawl (scrape) |
| `other` | Catch-all for unclassifiable pages | Firecrawl (company) |

Defined as a Go constant map in `internal/model/page.go`. Not a runtime config — too small and static.

---

## Question Router (Phase 3)

The router is pure Go in `internal/pipeline/router.go`. Same logic as the original design but with proper types.

```go
func RouteQuestions(pageIndex model.PageIndex, registry []model.Question) *model.RoutedBatches {
    result := &model.RoutedBatches{
        Tier1: make([]model.RoutedQuestion, 0),
        Tier2: make([]model.RoutedQuestion, 0),
        Tier3: make([]model.RoutedQuestion, 0),
    }

    for _, q := range registry {
        // LinkedIn questions bypass routing
        if q.Source == "perplexity" {
            continue
        }

        // Match relevant pages from the Page Index
        var matched []model.ClassifiedPage
        for _, pt := range q.RelevantPageTypes {
            if pages, ok := pageIndex[pt]; ok {
                for _, p := range pages {
                    if p.Quality != "irrelevant" {
                        matched = append(matched, p)
                    }
                }
            }
        }

        if len(matched) == 0 {
            result.Skipped = append(result.Skipped, model.SkippedQuestion{
                Key: q.Key, Reason: "no_source",
            })
            continue
        }

        routed := model.RoutedQuestion{Question: q, MatchedPages: matched}
        switch q.Tier {
        case 1:
            result.Tier1 = append(result.Tier1, routed)
        case 2:
            result.Tier2 = append(result.Tier2, routed)
        case 3:
            result.Tier3 = append(result.Tier3, routed)
        }
    }

    return result
}
```

**Key routing rules:**

- **Tier 1 (Haiku)** gets only its matched pages — main cost optimization lever
- **Tier 2 (Sonnet)** gets Page Index + all Tier 1 answers as additional context
- **Tier 3 (Opus)** gets **prepared context** from Haiku summarization — not raw crawl. Each question receives a focused ~25K token extract instead of the full ~150K token crawl + all prior answers
- **Confidence escalation:** Tier 1 answers below `confidence_escalation_threshold` (0.4) re-queue into Tier 2

---

## Configuration

```yaml
# config.yaml -- loaded by Viper. Env vars override with RESEARCH_ prefix.

store:
  driver: "postgres"  # "postgres" (Neon) or "sqlite" (local dev)
  postgres:
    url: "${RESEARCH_DATABASE_URL}"
  sqlite:
    path: "./data/research.db"

notion:
  token: "${RESEARCH_NOTION_TOKEN}"
  lead_tracker_db: "${RESEARCH_NOTION_LEAD_DB}"
  question_registry_db: "${RESEARCH_NOTION_QUESTION_DB}"
  field_registry_db: "${RESEARCH_NOTION_FIELD_DB}"

firecrawl:
  api_key: "${RESEARCH_FIRECRAWL_KEY}"
  base_url: "https://api.firecrawl.dev/v2"
  max_pages: 50
  exclude_paths:
    - "/blog/*"
    - "/news/*"
    - "/press/*"
    - "/careers/*"

perplexity:
  api_key: "${RESEARCH_PERPLEXITY_KEY}"
  base_url: "https://api.perplexity.ai"
  model: "sonar"

anthropic:
  api_key: "${RESEARCH_ANTHROPIC_KEY}"
  tier1_model: "claude-haiku-4-5-20250214"
  tier2_model: "claude-sonnet-4-5-20250514"
  tier3_model: "claude-opus-4-6-20260115"
  linkedin_json_model: "claude-haiku-4-5-20250214"
  confidence_escalation_threshold: 0.4
  tier3_gate: "always"  # "always" or "ambiguity_only"

salesforce:
  client_id: "${RESEARCH_SF_CLIENT_ID}"
  username: "${RESEARCH_SF_USERNAME}"
  private_key_path: "${RESEARCH_SF_KEY_PATH}"
  login_url: "https://login.salesforce.com"
  api_version: "v62.0"
  quality_threshold: 0.7

tooljet:
  webhook_url: "${RESEARCH_TOOLJET_WEBHOOK}"

batch:
  tier1_batch_size: 10
  tier2_batch_size: 5
  tier3_sequential: true
  max_concurrent_companies: 10  # parallel enrichment runs in batch mode

crawl_cache:
  enabled: true
  ttl_days: 7

log:
  level: "info"  # debug, info, warn, error
  format: "json"  # json (prod) or console (dev)
```

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
