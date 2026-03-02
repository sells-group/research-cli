# Pipeline Data Flow

> Phase-by-phase reference for the enrichment pipeline, covering data shapes, decision points, and external service interactions.
>
> For architecture context, see [System Architecture](architecture.md).

## Full Pipeline Flowchart

```mermaid
flowchart TD
    Input["Input: Company<br/>(URL, Name, SF ID)"] --> P0

    P0["Phase 0: Derive<br/>HTTP probe → extract name/city/state"] --> P1

    subgraph "Phase 1: Data Collection (parallel)"
        P1A["1A: Crawl<br/>local colly → Firecrawl fallback<br/>→ CrawledPage[]"]
        P1B["1B: Scrape<br/>GP + BBB + SoS via Jina/Google<br/>→ CrawledPage[]"]
        P1C["1C: LinkedIn<br/>Perplexity → Haiku JSON<br/>→ LinkedInData"]
        P1D["1D: PPP<br/>Postgres fuzzy match<br/>→ LoanMatch[]"]
    end

    P1 --> P1A & P1B & P1C & P1D

    P1A & P1B & P1C & P1D --> Merge["Merge all pages<br/>+ LinkedIn synthetic page"]

    Merge --> P2["Phase 2: Classify<br/>Haiku classifies each page<br/>→ PageIndex (type → pages)"]

    P2 --> P3["Phase 3: Route<br/>Map questions → best pages<br/>→ RoutedBatches (T1/T2/T3)"]

    P3 --> Optimize["Optimizations:<br/>- Skip existing high-confidence answers<br/>- ADV pre-fill from fed_data<br/>- Resume from T1 checkpoint"]

    Optimize --> Extract

    subgraph "Phases 4-6: Extraction"
        T1["Phase 4: T1 Extract<br/>Haiku batch (~70 Qs)<br/>→ ExtractionAnswer[]"]
        T2N["Phase 5a: T2 Native<br/>Sonnet batch (~25 Qs)<br/>+ T1 answers as context"]
        T2E["Phase 5b: T2 Escalated<br/>Sonnet batch (low-conf T1)<br/>threshold < 0.4"]
        T3["Phase 6: T3 Extract<br/>Opus batch (~5 Qs)<br/>Haiku-prepared context"]
    end

    Extract --> T1
    T1 -->|"T1 done"| T2N & T2E
    T1 -->|"confidence < 0.4"| T2E
    T2N & T2E --> T3Check{Cost budget OK<br/>+ tier3_gate?}
    T3Check -->|Yes| T3
    T3Check -->|No| SkipT3[Skip T3]

    T1 & T2N & T2E & T3 & SkipT3 --> P7

    P7["Phase 7: Aggregate<br/>Merge answers + validate + NAICS + contacts<br/>→ FieldValue map"] --> P7B

    P7B["Phase 7B: Waterfall<br/>Premium data cascade"] --> P7C

    P7C["Phase 7C: Provenance<br/>Track field sources + changes"] --> P7D

    P7D["Phase 7D: Geocode<br/>Google Geocoding + spatial"] --> P8

    P8["Phase 8: Report<br/>Format enrichment report"] --> P9

    P9["Phase 9: Quality Gate<br/>Score → SF write or manual review<br/>→ Salesforce Account + Notion status"]
```

## Phase 1: Parallel Data Collection

Four goroutines fan out via `errgroup`, collecting data from independent sources:

```mermaid
flowchart LR
    Start([errgroup.WithContext]) --> Fork

    Fork --> G1["goroutine 1A:<br/>CrawlPhase()"]
    Fork --> G2["goroutine 1B:<br/>ScrapePhase()"]
    Fork --> G3["goroutine 1C:<br/>LinkedInPhase()"]
    Fork --> G4["goroutine 1D:<br/>PPPPhase()"]

    G1 --> |"CrawlResult<br/>(pages, source, count)"| Join
    G2 --> |"CrawledPage[]<br/>(GP, BBB, SoS)"| Join
    G3 --> |"LinkedInData<br/>(exec contacts)"| Join
    G4 --> |"LoanMatch[]<br/>(PPP fuzzy matches)"| Join

    Join([g.Wait — merge gate]) --> Combined["All pages combined<br/>+ address extraction<br/>+ name recovery"]
```

**Conditional execution:**
- **1A (Crawl):** Always runs
- **1B (Scrape):** Requires company name; skipped in sourcing mode
- **1C (LinkedIn):** Requires company name
- **1D (PPP):** Requires company name + location

**Error handling:** Individual phase failures don't abort the pipeline. If all data-producing phases (1A/1B/1C) fail, the pipeline aborts with a categorized error.

## Phase Details

### Phase 0: Derive Company Info

| | |
|---|---|
| **Input** | Company with URL only (no name) |
| **Output** | Derived name, city, state |
| **Services** | None (HTTP probe only) |
| **Files** | `pipeline.go`, `localcrawl.go` |
| **Decision** | Skipped if company name is already set |

Probes the homepage via `net/http`, extracts company name from `<title>` / meta tags, and city/state from structured data.

### Phase 1A: Crawl

| | |
|---|---|
| **Input** | Company URL |
| **Output** | `CrawlResult` (pages, source, count, from_cache) |
| **Services** | Local HTTP → colly → Firecrawl (fallback) |
| **Files** | `crawl.go`, `localcrawl.go`, `blockdetect.go` |
| **Decision** | Blocked site (Cloudflare/captcha/JS-shell) → Firecrawl fallback |

1. Check cache (24-hour TTL)
2. HTTP probe: homepage + robots.txt + sitemap.xml
3. Block detection: Cloudflare 403/503, cf-* headers, captcha, JS-only shell
4. If clean: colly crawl (depth 2, cap 50 pages) + html-to-markdown
5. If blocked: Firecrawl async crawl with exponential backoff polling

### Phase 1B: External Scrape

| | |
|---|---|
| **Input** | Company name + URL |
| **Output** | `CrawledPage[]` from external sources |
| **Services** | Jina Search/Reader, Google Places, Perplexity |
| **Files** | `scrape.go` |
| **Decision** | Each source independent; partial results OK |

Searches for company data across: Google Business Profile, BBB, Secretary of State filings. Uses Jina for reading and Google Places for business validation.

### Phase 1C: LinkedIn

| | |
|---|---|
| **Input** | Company name |
| **Output** | `LinkedInData` (employee count, exec contacts, description) |
| **Services** | Perplexity (search), Anthropic Haiku (JSON parse) |
| **Files** | `linkedin.go` |
| **Decision** | Perplexity query → Haiku extracts structured JSON |

### Phase 1D: PPP Loan Lookup

| | |
|---|---|
| **Input** | Company name + location |
| **Output** | `LoanMatch[]` (loan amounts, employee counts, NAICS) |
| **Services** | PPP Postgres (trigram fuzzy match) |
| **Files** | `pipeline.go` (inline) |
| **Decision** | Similarity threshold: 0.4, max 10 candidates |

### Phase 2: Classification

| | |
|---|---|
| **Input** | All `CrawledPage[]` from Phase 1 |
| **Output** | `PageIndex` (map of page type → pages) |
| **Services** | Anthropic Haiku |
| **Files** | `classify.go` |

Haiku classifies each page into types (about, services, team, contact, pricing, etc.). Creates an index for efficient question routing.

### Phase 3: Routing

| | |
|---|---|
| **Input** | `PageIndex` + `Question[]` from registry |
| **Output** | `RoutedBatches` (Tier1, Tier2, Tier3, Skipped) |
| **Services** | None (deterministic routing logic) |
| **Files** | `router.go` |

Maps each question to its best matching pages based on the question's `Pages` multi-select field and the page classifications from Phase 2.

### Phase 4: T1 Extraction

| | |
|---|---|
| **Input** | `RoutedBatches.Tier1` (~70 questions) |
| **Output** | `ExtractionAnswer[]` with confidence scores |
| **Services** | Anthropic Haiku (Batch API) |
| **Files** | `extract.go` |
| **Decision** | Saves checkpoint for resume on failure |

Single-page fact extraction. Each question is answered from its routed page with strict JSON output.

### Phase 5: T2 Extraction

| | |
|---|---|
| **Input** | T2-native questions + T1 low-confidence escalations |
| **Output** | `ExtractionAnswer[]` (native + escalated merged) |
| **Services** | Anthropic Sonnet (Batch API + primer cache) |
| **Files** | `extract.go` |
| **Decision** | T1 answers with confidence < 0.4 are re-queued to T2 |

Two parallel streams merge:
- **T2-native:** Questions routed directly to Sonnet (multi-page synthesis)
- **T2-escalated:** T1 answers below the confidence threshold, re-extracted with Sonnet

Both receive T1 answers as supplementary context for better synthesis.

### Phase 6: T3 Extraction

| | |
|---|---|
| **Input** | `RoutedBatches.Tier3` (~5 questions) + merged T1+T2 answers |
| **Output** | `ExtractionAnswer[]` |
| **Services** | Anthropic Opus (Batch API + primer cache) |
| **Files** | `extract.go` |
| **Decision** | Gated by `tier3_gate` config + cost budget ($10 default) |

Uses Haiku-prepared context (~25K tokens) instead of raw crawl (~150K tokens). Only runs when:
- `tier3_gate` is `"always"` or `"ambiguity_only"` (with remaining ambiguous answers)
- Cumulative cost is below `max_cost_per_company_usd`

### Phase 7: Aggregate

| | |
|---|---|
| **Input** | All `ExtractionAnswer[]` from T1+T2+T3 + ADV pre-fill + existing answers |
| **Output** | `FieldValue` map |
| **Services** | None |
| **Files** | `aggregate.go` |

Merges all answer sources, validates NAICS codes, normalizes business models, injects LinkedIn contacts, cross-validates employee counts, enriches from PPP data, and builds the final field value map.

### Phase 7B: Waterfall Cascade

| | |
|---|---|
| **Input** | Company + `FieldValue` map |
| **Output** | Enriched `FieldValue` map |
| **Services** | Premium data providers (configurable) |
| **Files** | `internal/waterfall/` |

Fills remaining gaps via premium data cascades.

### Phase 7C: Provenance

| | |
|---|---|
| **Input** | `FieldValue` map + answers + waterfall results |
| **Output** | Provenance records (field → source tracking) |
| **Services** | Store (Postgres/SQLite) |
| **Files** | `pipeline.go` (inline) |

Tracks which source produced each field value, detects overrides vs. prior runs.

### Phase 7D: Geocode

| | |
|---|---|
| **Input** | Company address fields |
| **Output** | Geocoded location (lat/lng) + spatial associations |
| **Services** | Google Geocoding |
| **Files** | `pipeline.go`, `internal/geo/` |
| **Decision** | Only runs if `geo.enabled` is true and geocoder is configured |

### Phase 8: Report

| | |
|---|---|
| **Input** | All answers, field values, phase results, token usage |
| **Output** | Formatted enrichment report string |
| **Services** | None |
| **Files** | `report.go` |

### Phase 9: Quality Gate

| | |
|---|---|
| **Input** | Full `EnrichmentResult` |
| **Output** | SF Account update + Notion status update |
| **Services** | Salesforce REST API, Notion API, ToolJet webhook |
| **Files** | `gate.go` |
| **Decision** | Quality score >= threshold (0.6) → auto-write; below → manual review via ToolJet |

Two modes:
- **Immediate:** Single-company run — executes SF writes inline
- **Deferred:** Batch mode — builds write intents, flushes in bulk after all companies complete

## Extraction Tier Flow

```mermaid
flowchart LR
    subgraph "Parallel Start"
        T1["T1: Haiku Batch<br/>~70 questions"]
        T2N["T2 Native: Sonnet Batch<br/>~25 questions"]
    end

    T1 -->|"T1 done signal<br/>(channel close)"| Escalate{Confidence < 0.4?}
    Escalate -->|Yes| T2E["T2 Escalated:<br/>Sonnet re-extract"]
    Escalate -->|No| Merge

    T1 -->|"T1 answers<br/>as context"| T2N
    T2N --> Merge
    T2E --> Merge

    Merge[Merge T2] --> CostCheck{Cumulative cost<br/>< budget?}
    CostCheck -->|"Yes + gate enabled"| T3["T3: Opus Batch<br/>~5 questions<br/>Haiku-prepared context"]
    CostCheck -->|No| Skip[Skip T3]

    T3 --> Final[Phase 7: Aggregate]
    Skip --> Final
```

## Data Shape Transformations

```mermaid
flowchart LR
    CSV["CSV file<br/>(name, url, sf_id)"] --> Notion["Notion pages<br/>(Lead Tracker)"]
    Notion --> Company["model.Company<br/>(URL, Name, SFID)"]
    Company --> Pages["CrawledPage[]<br/>(url, title, markdown)"]
    Pages --> Index["PageIndex<br/>(type → pages map)"]
    Index --> Batches["RoutedBatches<br/>(T1/T2/T3 question lists)"]
    Batches --> Answers["ExtractionAnswer[]<br/>(field, value, confidence)"]
    Answers --> Fields["FieldValue map<br/>(sf_field → typed value)"]
    Fields --> SF["Salesforce Account<br/>(PATCH fields)"]
    Fields --> NotionStatus["Notion page<br/>(status: Complete)"]
```

## Related Docs

- [System Architecture](architecture.md) — system context and components
- [API Cost Model](cost-model.md) — where costs are incurred per phase
- [Operational Runbook](runbook.md) — troubleshooting phase failures
