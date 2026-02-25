# Salesforce Enrichment Audit — Meeting Prep with Hutton

**Date:** 2026-02-25
**Participants:** Blake, Hutton (SF Admin)
**Goal:** Get the enrichment pipeline writing to Salesforce end-to-end.

---

## 1. Code Readiness

**The enrichment → SF write path is fully implemented and tested.** No stubs, TODOs, or placeholder code anywhere in the SF write path.

### What works today

- **Full 9-phase pipeline**: crawl → classify → route → extract (Haiku/Sonnet/Opus) → aggregate → report → quality gate → SF write
- **JWT auth** to Salesforce via `go-salesforce/v3` — token refresh handled automatically by the library
- **Health check on startup** — calls `DescribeSObject("Account")` with a 10s timeout; fails fast if creds are wrong
- **Account dedup** by website URL (`SOQL LIKE` query with injection prevention)
- **Contact dedup** by email (primary), then by first+last name (fallback)
- **Batch mode** with Collections API (200 records/batch, auto-split for larger sets)
- **Quality gate** scores enrichment (confidence, completeness, diversity, freshness) — only writes to SF if score passes threshold
- **Notion writeback** of SF Account ID after create or dedup match
- **Graceful degradation** — if SF `client_id` is empty, SF writes are skipped; pipeline still runs and writes to DB/Notion

### Test coverage

- **543 tests** in `internal/pipeline/`, **47 tests** in `pkg/salesforce/`
- **63 quality gate tests** covering: create, update, dedup, contacts, error paths, retry logic
- All tests are mock-backed — zero external API calls in CI
- No skipped tests, no TODOs in test code

### To run a test

```bash
# Single company (existing SF account)
go run ./cmd run --url https://some-company.com --sf-id 001XXXXXXXXXXXX

# Single company (net new — will dedup or create)
go run ./cmd run --url https://some-company.com

# Batch from Notion queue
go run ./cmd batch --limit 5
```

---

## 2. What's Needed from Hutton

### A. Connected App (JWT Bearer Flow)

Create a Connected App in Salesforce for JWT auth:

1. **Setup → App Manager → New Connected App**
2. Enable OAuth, check **"Enable for Device Flow"** and **"Use digital signatures"**
3. Upload a certificate (Blake generates the keypair):
   ```bash
   openssl req -x509 -newkey rsa:2048 -keyout sf_private_key.pem -out sf_cert.crt -days 365 -nodes
   ```
4. OAuth scopes: `api`, `refresh_token`, `offline_access`
5. **Pre-authorize the app** for the API user: Setup → Installed Connected Apps → Manage → select profile

**From Hutton:**
- **Consumer Key** (Connected App client ID) → `RESEARCH_SF_CLIENT_ID`
- **SF username** for the API user → `RESEARCH_SF_USERNAME`

**Blake keeps:**
- Private key file → `RESEARCH_SF_KEY_PATH`

### B. Custom Fields

The pipeline writes to **29 custom Account fields** and **1 custom Contact field**. These must exist in SF before writes will work. (6 exec/people fields removed — that data lives on Contacts via the related list.)

#### Account Custom Fields (29)

| # | API Name | Label | Type | Notes |
|---|---|---|---|---|
| 1 | `Legal_Name__c` | Legal Name | Text(255) | |
| 2 | `Year_Founded__c` | Year Founded | Number(4,0) | |
| 3 | `Primary_Email__c` | Primary Email | Email | |
| 4 | `Services__c` | Services | Long Text(1000) | |
| 5 | `Service_Area__c` | Service Area | Text(500) | |
| 6 | `Licenses_Certifications__c` | Licenses & Certifications | Long Text(1000) | |
| 7 | `Revenue_Range__c` | Revenue Range | Picklist | Values TBD from Hutton |
| 8 | `Customer_Types__c` | Customer Types | Picklist | Values TBD from Hutton |
| 9 | `Differentiators__c` | Differentiators | Long Text(2000) | |
| 10 | `Reputation_Summary__c` | Reputation Summary | Long Text(2000) | |
| 11 | `Acquisition_Assessment__c` | Acquisition Assessment | Long Text(5000) | |
| 12 | `Description` | Description | Standard field | Uses standard SF Description (no custom `__c` needed) |
| 13 | `Business_Model__c` | Business Model | Picklist | Values TBD from Hutton |
| 14 | `NAICS_Code__c` | NAICS Code | Text(10) | |
| 15 | `Google_Reviews__c` | Google Reviews | Number(8,0) | Renamed from `Review_Count__c` |
| 16 | `Google_Review_Rating__c` | Google Review Rating | Number(3,2) | Renamed from `Review_Rating__c` |
| 17 | `Revenue_Estimate__c` | Revenue Estimate | Number(12,0) | |
| 18 | `Revenue_Confidence__c` | Revenue Confidence | Number(3,2) / Picklist? | Type conflict: pipeline sends float, Hutton has picklist. Needs discussion. |
| 19 | `Employee_Estimate__c` | Employee Estimate | Number(8,0) | |
| 20 | `LinkedIn_Employees__c` | LinkedIn Employees | Number(8,0) | Renamed from `Employees_LinkedIn__c` |
| 21 | `Enrichment_Report__c` | Enrichment Report | Long Text(131072) | Markdown report — needs max LTA size |
| 22 | `Longitude_and_Lattitude__Latitude__s` | Latitude | Geolocation (sub-field) | Native SF Geolocation compound field |
| 23 | `Longitude_and_Lattitude__Longitude__s` | Longitude | Geolocation (sub-field) | Native SF Geolocation compound field |
| 24 | `Company_MSA__c` | Company MSA | Text(255) | Renamed from `MSA_Name__c` |
| 25 | `MSA_CBSA_Code__c` | MSA CBSA Code | Text(10) | |
| 26 | `Urban_Classification__c` | Urban Classification | Picklist | `urban_core`, `suburban`, `exurban`, `rural` |
| 27 | `Distance_to_MSA_Center_km__c` | Distance to MSA Center (km) | Number(8,2) | |
| 28 | `Distance_to_MSA_Edge_km__c` | Distance to MSA Edge (km) | Number(8,2) | |
| 29 | `County_FIPS__c` | County FIPS | Text(10) | |

#### Contact Custom Fields (1)

| # | API Name | Label | Type |
|---|---|---|---|
| 1 | `LinkedIn__c` | LinkedIn | URL | Renamed from `LinkedIn_URL__c` |

#### Standard Fields Also Written (no action needed)

**Account:** `Name`, `Website`, `Phone`, `BillingStreet`, `NumberOfEmployees`, `Description` (also used for enriched description)
**Contact:** `FirstName`, `LastName`, `Title`, `Email`, `Phone`

### C. API Permissions / Profile Setup

The API user's profile needs:
- **API Enabled** permission
- **Read/Create/Edit** on Account and Contact objects
- **Field-level security**: read+edit on all 29+1 custom fields above
- Connected App **pre-authorized** for the user's profile

### D. Sandbox vs Production

**Start with Sandbox.** Set in config:
```yaml
salesforce:
  login_url: https://test.salesforce.com   # sandbox
  # login_url: https://login.salesforce.com  # production (default)
```

Or via env: `RESEARCH_SALESFORCE_LOGIN_URL=https://test.salesforce.com`

Test with 3-5 accounts in sandbox before going to production.

---

## 3. How Net New vs Existing Accounts Work

### Existing Account (has SF ID)

```
go run ./cmd run --url acme.com --sf-id 001XXXXXXXXXXXX

→ Pipeline runs phases 1-8 (crawl, classify, extract, aggregate, report)
→ Phase 9: Quality gate scores enrichment
→ If passed: UpdateAccount(sf_id, fields)
→ Upsert contacts (dedup by email, then name)
→ Update Notion status
```

### Net New Account (no SF ID)

```
go run ./cmd run --url acme.com

→ Pipeline runs phases 1-8
→ Phase 9: Quality gate scores
→ If passed:
  → FindAccountByWebsite("acme.com") — dedup check
    → Match found → UPDATE existing account
    → No match → CreateAccount(fields)
  → Write resolved SF ID back to Notion
  → Upsert contacts under that Account
```

### Batch Mode

```
go run ./cmd batch --limit 100

→ Polls Notion Lead Tracker for Status="Queued"
→ Each lead may or may not have a SalesforceID
→ Same logic per company: dedup by website, create or update
→ Collections API for bulk writes (200/batch)
→ Returns FlushSummary with created/updated/failed counts
```

### Safety Features

| Feature | How it works |
|---|---|
| **Website dedup** | Same URL always maps to same Account — prevents duplicates |
| **Contact dedup** | Match by email (primary) or first+last name (fallback) |
| **Quality gate** | Low-quality enrichments rejected → manual review via ToolJet |
| **Name fallback** | Always guaranteed: company name → field values → domain heuristic (`acme-construction.com` → "Acme Construction") |
| **Collections errors** | Per-field errors logged but don't fail the whole batch |

---

## 4. Pre-Test Checklist

| # | Item | Owner | Status |
|---|---|---|---|
| 1 | Connected App created (JWT Bearer) | Hutton | Pending |
| 2 | Consumer Key provided | Hutton | Pending |
| 3 | Private key generated & cert uploaded | Blake + Hutton | Pending |
| 4 | Connected App pre-authorized for user | Hutton | Pending |
| 5 | 29 custom Account fields created | Hutton | Pending |
| 6 | 1 custom Contact field created (`LinkedIn__c`) | Hutton | Pending |
| 7 | Field-level security set (API user) | Hutton | Pending |
| 8 | Sandbox URL confirmed | Hutton | Pending |
| 9 | Notion registries populated (Questions + Fields) | Blake | Check |
| 10 | API keys set (Anthropic, Firecrawl, Perplexity) | Blake | Check |
| 11 | config.yaml updated with SF creds | Blake | Pending |
| 12 | Health check passes (`DescribeSObject("Account")`) | Blake | Pending |
| 13 | Single-company test run | Blake | Pending |

---

## 5. Test Plan

1. **Hutton creates Connected App + custom fields in Sandbox**
2. **Blake configures creds** in `config.yaml` / env vars
3. **Health check**: run against any URL — should pass the SF health check on startup (or fail with a clear error pointing to creds/permissions)
4. **Net new account**: Run a company URL with no SF ID → verify account + contacts created in SF
5. **Dedup test**: Run same URL again → verify it matches the existing account and updates (not duplicates)
6. **Existing SF ID**: Run with `--sf-id` of a real sandbox account → verify it updates the right record
7. **Quality gate**: Run against a very thin/empty website → verify it gates correctly and doesn't write garbage
8. **Batch test**: Import 5 leads via CSV → Notion, run `batch --limit 5`, verify all 5 in SF

---

## 6. Risks / Gotchas

| Risk | Impact | Mitigation |
|---|---|---|
| **Field name mismatch** | If custom field API names in SF don't exactly match the list above, writes silently fail for those fields. Collections API returns per-field errors but pipeline logs them rather than hard-failing. | Double-check API names character-by-character. The `__c` suffix is auto-appended by SF. |
| **Long Text limits** | SF Long Text Area max is 131,072 chars. `Enrichment_Report__c` needs this max. | Set LTA length to 131,072 when creating the field. |
| **Notion registries** | If Notion DBs aren't populated, pipeline falls back to `testdata/fields.json` fixtures — fine for testing but not production. | Populate registries before production batch runs. |
| **Sandbox rate limits** | Config defaults to 25 req/s. Sandbox API limits may be lower. | Watch for `REQUEST_LIMIT_EXCEEDED` errors; lower `salesforce.rate_limit` in config if needed. |
| **Picklist values** | Several fields changed to picklists (`Revenue_Range__c`, `Customer_Types__c`, `Business_Model__c`, `Urban_Classification__c`). Pipeline sends string values that must match picklist entries. | Get exact picklist values from Hutton, or use unrestricted picklists. |
| **Revenue_Confidence__c type** | Pipeline sends float 0.0–1.0 but Hutton has it as a picklist. | Decide: change SF field to Number, or map float to picklist bucket strings. |
