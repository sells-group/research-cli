# CLAUDE.md — research-cli

Automated account enrichment pipeline + federal data sync in Go. Two subsystems:
1. **Enrichment:** Crawls company websites, classifies pages, extracts structured data via tiered Claude models (Haiku → Sonnet → Opus), writes to Salesforce. Leads enter via CSV → Notion; registries (questions + fields) live in Notion.
2. **Fedsync:** Incrementally syncs 26 federal datasets (Census, BLS, SEC EDGAR, FINRA, OSHA, EPA, FRED) into `fed_data.*` Postgres tables. Runs daily via Fly.io cron, exits in <1s when no new data is expected.

## Stack

| Layer | Tech |
|---|---|
| Language | Go 1.23+ |
| CLI | cobra + viper + zap + eris |
| Compute | Fly.io (per-second billing, auto-stop) |
| DB (prod) | Neon Postgres (pgx) |
| DB (dev) | SQLite (modernc.org/sqlite, no CGO) |
| Crawl | colly (local-first) → Firecrawl v2 (fallback) |
| HTML→MD | html-to-markdown/v2 |
| LLM | anthropic-sdk-go (Messages + Batch + caching) |
| LinkedIn | Perplexity API → Haiku JSON |
| Destination | Salesforce REST API (go-salesforce/v3) |
| Lead Tracker | Notion API (notionapi) |
| Concurrency | errgroup |
| FTP | jlaffaye/ftp |
| XLSX | tealeg/xlsx/v2 |
| Rate Limit | golang.org/x/time/rate |
| OCR | pdftotext (local) → Mistral API (fallback) |

## Commands

```bash
go build -o research-cli ./cmd                          # build
go test ./...                                            # test all
go test ./internal/pipeline/ -run TestRouter -v          # test specific
go run ./cmd import --csv leads.csv                      # import CSV → Notion
go run ./cmd run --url acme.com --sf-id 001xx            # single company
go run ./cmd batch --limit 100                           # batch from Notion queue
go run ./cmd serve --port 8080                           # webhook server
fly deploy                                               # deploy to Fly.io
fly ssh console -C "research-cli batch --limit 100"      # run on Fly

# Fedsync commands
go run ./cmd fedsync migrate                              # apply schema migrations
go run ./cmd fedsync status                               # show sync log
go run ./cmd fedsync sync                                 # sync all due datasets
go run ./cmd fedsync sync --phase 1                       # sync Phase 1 only
go run ./cmd fedsync sync --datasets cbp,fpds --force     # force specific datasets
go run ./cmd fedsync xref                                 # build entity cross-reference
```

## Project Structure

```
cmd/                        # cobra commands: root, import, run, batch, serve, fedsync
internal/
  config/config.go          # viper struct + loader (includes FedsyncConfig)
  pipeline/                 # enrichment pipeline (phases 1-9)
    pipeline.go             # orchestrates phases 1-9 per company
    crawl.go                # Phase 1A: local-first → Firecrawl fallback
    localcrawl.go           # net/http probe + colly + html-to-markdown
    blockdetect.go          # Cloudflare/captcha/JS-shell heuristics
    scrape.go               # Phase 1B: GP + BBB + PPP + SoS via Firecrawl
    linkedin.go             # Phase 1C: Perplexity → Haiku JSON
    classify.go             # Phase 2: Haiku page classification
    router.go               # Phase 3: question → page matching
    extract.go              # Phases 4-6: tiered Claude extraction
    aggregate.go            # Phase 7: merge + validate
    report.go               # Phase 8: enrichment report
    gate.go                 # Phase 9: quality gate → SF + Notion
  registry/
    question.go             # load Question Registry from Notion
    field.go                # load Field Registry from Notion
  store/
    store.go                # Store interface (Neon or SQLite)
    postgres.go             # pgx implementation
    sqlite.go               # modernc sqlite implementation
  model/                    # company, page, question, field types
  db/                       # shared DB helpers
    copy.go                 # pgx CopyFrom wrapper
    upsert.go               # BulkUpsert via temp table + ON CONFLICT
  fedsync/                  # federal data sync subsystem
    migrate.go              # embed.FS migration runner → fed_data.schema_migrations
    synclog.go              # sync log tracking (start, complete, fail)
    migrations/*.sql        # 40 SQL migration files (001-040)
    dataset/                # 26 dataset implementations
      interface.go          # Dataset interface, Phase, Cadence, SyncResult
      schedule.go           # ShouldRun helpers: Daily, Weekly, Monthly, Quarterly, Annual
      registry.go           # Registry: maps names → Dataset impls
      engine.go             # Engine: Run() orchestration loop
      parse.go              # shared parse helpers (parseInt, trimQuotes)
      cbp.go                # Census CBP (Phase 1, annual)
      susb.go               # Census SUSB (Phase 1, annual)
      qcew.go               # BLS QCEW (Phase 1, quarterly)
      oews.go               # BLS OEWS (Phase 1, annual)
      fpds.go               # SAM.gov FPDS (Phase 1, daily)
      econ_census.go        # Census Economic Census (Phase 1, annual)
      adv_part1.go          # SEC ADV Part 1A (Phase 1B, monthly)
      ia_compilation.go     # IARD daily XML (Phase 1B, daily)
      holdings_13f.go       # SEC 13F holdings (Phase 1B, quarterly)
      form_d.go             # EDGAR Form D (Phase 1B, daily)
      edgar_submissions.go  # EDGAR bulk JSON (Phase 1B, weekly)
      entity_xref.go        # CRD↔CIK cross-ref (Phase 1B)
      adv_part2.go          # ADV brochure PDFs → OCR (Phase 2, monthly)
      brokercheck.go        # FINRA BrokerCheck (Phase 2, monthly)
      form_bd.go            # Form BD broker-dealer (Phase 2, monthly)
      osha_ita.go           # OSHA ITA (Phase 2, annual)
      epa_echo.go           # EPA ECHO (Phase 2, monthly)
      nes.go                # Census NES (Phase 2, annual)
      asm.go                # Census ASM (Phase 2, annual)
      eci.go                # BLS ECI (Phase 2, quarterly)
      adv_part3.go          # CRS PDFs → OCR (Phase 3, monthly)
      xbrl_facts.go         # EDGAR XBRL facts (Phase 3, daily)
      fred.go               # FRED series (Phase 3, monthly)
      abs.go                # Census ABS (Phase 3, annual)
      cps_laus.go           # BLS CPS/LAUS (Phase 3, monthly)
      m3.go                 # Census M3 (Phase 3, monthly)
    transform/              # NAICS, FIPS, SIC normalization
    resolve/                # entity resolution (CRD↔CIK fuzzy matching)
    xbrl/                   # XBRL JSON-LD fact parser
  fetcher/                  # download + parse (HTTP, FTP, CSV, XML, JSON, XLSX, ZIP)
  ocr/                      # PDF text extraction (pdftotext → Mistral fallback)
pkg/
  anthropic/                # Messages + Batch + cache primer
  firecrawl/                # crawl, scrape, batch scrape + poll
  perplexity/               # chat completions (OpenAI-compatible)
  salesforce/               # JWT auth, SOQL, CRUD, Collections
  notion/                   # DB query, page create/update, CSV mapper
```

## Key Patterns

### Error handling — eris
- Always wrap: `eris.Wrap(err, "context message")`
- Include identifiers: `eris.Wrap(err, "sf: update account %s", id)`
- Unwrap for structured reporting in run log

### Logging — zap
- Use `zap.L()` global logger
- Standard fields: `company`, `phase`, `tier`, `duration_ms`, `tokens`
- JSON format in prod, console in dev

### Config — viper
- File: `config.yaml` at project root
- Env override prefix: `RESEARCH_` (e.g., `RESEARCH_DATABASE_URL`)
- Fly secrets → env vars → viper reads them

### Store interface
- `internal/store/store.go` defines the interface
- `postgres.go` (Neon) and `sqlite.go` (local dev) both implement it
- Selected by `store.driver` in config: `"postgres"` or `"sqlite"`

### Registry loading
- Question Registry + Field Registry are Notion DBs
- Loaded once at startup via Notion API, cached in memory
- No Notion dependency during extraction phases
- Filter: `Status = Active`, paginate with `next_cursor`

### Tiered extraction (Phases 4-6)
- **Tier 1 (Haiku, ~70 Qs):** single-page fact extraction, strict JSON
- **Tier 2 (Sonnet, ~25 Qs):** multi-page synthesis + T1 answers as context
- **Tier 3 (Opus, ~5 Qs):** prepared context from Haiku summarization (~25K tok), NOT raw crawl (~150K tok)
- Confidence < 0.4 escalates from T1 → T2

### Prompt caching primer strategy
- For T2/T3: send 1 sequential primer request (1-hour TTL) to warm cache
- Submit remaining questions as Batch API calls → hit warm cache
- Implemented in `pkg/anthropic/cache.go`
- Saves ~42% on Claude costs

### Local-first crawl (Phase 1A)
- Probe with `net/http`: homepage + robots.txt + sitemap.xml
- Detect blocks: Cloudflare (403/503 + cf-* headers), captcha, JS-only shell
- Clean HTML → colly (depth 2, cap 50 pages) + html-to-markdown → **0 Firecrawl credits**
- Blocked → Firecrawl async crawl fallback
- ~60% of sites serve clean HTML → ~55% credit reduction

### Confidence escalation
- T1 answers with confidence < `confidence_escalation_threshold` (default 0.4) re-queue into T2
- T3 gating: `"always"` or `"ambiguity_only"` (config)

### Fedsync — Dataset interface
- Each of 26 datasets implements `Dataset` in `internal/fedsync/dataset/`
- `ShouldRun(now, lastSync)` checks cadence (daily/weekly/monthly/quarterly/annual)
- `Sync(ctx, pool, fetcher, tempDir)` returns `*SyncResult` with row count + metadata
- Engine iterates registry, checks `ShouldRun()`, calls `Sync()`, records in `fed_data.sync_log`
- Phases: 1 (Market Intelligence), 1B (SEC/EDGAR), 2 (Extended), 3 (On-Demand)

### Fedsync — Streaming large datasets
- `fetcher.DownloadToFile()` → ZIP to temp dir
- `fetcher.ExtractZIP()` → CSV to temp dir
- `fetcher.StreamCSV()` → `<-chan []string` (row channel)
- Consumer batches rows (5,000–10,000) → `db.BulkUpsert()` or `db.CopyFrom()`
- Keeps memory bounded regardless of dataset size

### Fedsync — Rate limiting
- Per-host limiters in `internal/fetcher/http.go` via `golang.org/x/time/rate`
- SEC (efts/www/data.sec.gov): 10 req/s
- SAM.gov: 5 req/s
- Default: 20 req/s
- EDGAR requires `User-Agent` header from `cfg.Fedsync.EDGARUserAgent`

### Fedsync — Migrations
- SQL files embedded via `embed.FS` in `internal/fedsync/migrate.go`
- Tracked in `fed_data.schema_migrations`
- Applied in lexicographic filename order, idempotent (skips already-applied)
- All tables live in `fed_data` schema (separate from enrichment)

## API Client Pattern (`pkg/`)

Each external API gets its own package in `pkg/`:
- Define an **interface** for the client operations
- Implement with a **struct** wrapping `net/http` (or SDK)
- Firecrawl + Perplexity: raw `net/http` with typed req/resp structs (no SDK)
- Anthropic: use `anthropic-sdk-go` official SDK
- Salesforce: use `go-salesforce/v3`
- Notion: use `notionapi`
- Token refresh (SF): cache token, refresh on 401, protect with `sync.Mutex`
- Async polling (Firecrawl, Anthropic Batch): exponential backoff with cap

## Testing

- `pkg/` clients: mock HTTP with `httptest.Server`
- `internal/pipeline/`: mock `pkg/` clients behind interfaces, test with canned data
- `internal/store/`: real SQLite in `t.TempDir()`
- `internal/fedsync/dataset/`: mock `Fetcher`, canned CSV/JSON/XML from `testdata/` fixtures
- `internal/fetcher/`: `httptest.NewServer` for HTTP, embedded fixtures for parsers
- `internal/db/`: pgxmock for upsert/copy validation logic
- `internal/ocr/`: mock `exec.Command` for pdftotext, `httptest` for Mistral
- **No external API calls in CI** — all mocked
- Integration tests: `go test -tags=integration` (manual, needs real API keys)

## Conventions

- **Naming:** snake_case for JSON/YAML keys, CamelCase for Go types
- **Context:** pass `context.Context` as first param everywhere
- **Struct tags:** `json:"field_name"` on all model structs, `yaml:"field_name"` on config
- **Env prefix:** `RESEARCH_` (e.g., `RESEARCH_ANTHROPIC_KEY`)
- **Parallel phases:** 1A/1B/1C fan out via `errgroup`
- **Notion rate limit:** 3 req/s — pace imports with `time.Ticker` at 300ms

## Environment Variables

### Required (production)

| Variable | Purpose |
|---|---|
| `RESEARCH_DATABASE_URL` | Neon Postgres connection string |
| `RESEARCH_NOTION_TOKEN` | Notion integration token |
| `RESEARCH_NOTION_LEAD_DB` | Lead Tracker database ID |
| `RESEARCH_NOTION_QUESTION_DB` | Question Registry database ID |
| `RESEARCH_NOTION_FIELD_DB` | Field Registry database ID |
| `RESEARCH_FIRECRAWL_KEY` | Firecrawl API key |
| `RESEARCH_PERPLEXITY_KEY` | Perplexity API key |
| `RESEARCH_ANTHROPIC_KEY` | Anthropic API key |
| `RESEARCH_SF_CLIENT_ID` | Salesforce Connected App client ID |
| `RESEARCH_SF_USERNAME` | Salesforce username for JWT auth |
| `RESEARCH_SF_KEY_PATH` | Path to SF JWT private key |
| `RESEARCH_TOOLJET_WEBHOOK` | ToolJet webhook URL for manual review |

### Optional

| Variable | Default | Purpose |
|---|---|---|
| `RESEARCH_STORE_DRIVER` | `postgres` | `postgres` or `sqlite` |
| `RESEARCH_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `RESEARCH_LOG_FORMAT` | `json` | `json` (prod) or `console` (dev) |

### Fedsync

| Variable | Default | Purpose |
|---|---|---|
| `RESEARCH_FEDSYNC_DATABASE_URL` | (falls back to `DATABASE_URL`) | Fedsync Postgres connection |
| `RESEARCH_FEDSYNC_SAM_API_KEY` | | SAM.gov FPDS API key |
| `RESEARCH_FEDSYNC_FRED_API_KEY` | | FRED API key |
| `RESEARCH_FEDSYNC_BLS_API_KEY` | | BLS API key |
| `RESEARCH_FEDSYNC_CENSUS_API_KEY` | | Census API key |
| `RESEARCH_FEDSYNC_EDGAR_USER_AGENT` | `Sells Advisors blake@sellsadvisors.com` | SEC EDGAR required User-Agent |
| `RESEARCH_FEDSYNC_N8N_WEBHOOK_URL` | | n8n webhook for notifications |
| `RESEARCH_FEDSYNC_MISTRAL_API_KEY` | | Mistral OCR API key |
| `RESEARCH_FEDSYNC_TEMP_DIR` | `/tmp/fedsync` | Temp directory for downloads |
| `RESEARCH_FEDSYNC_OCR_PROVIDER` | `local` | `local` (pdftotext) or `mistral` |
