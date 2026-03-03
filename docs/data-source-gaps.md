# Data Source Gap Analysis

Living reference for missing data sources across federal, state, county, and commercial layers. The system targets **M&A advisory for financial services firms** (RIAs, broker-dealers, insurance agencies).

Last updated: 2026-03-01

---

## 1. Current Coverage Summary

### Fedsync Datasets (34 implementations)

| Phase | Dataset | Source | Cadence | Table |
|-------|---------|--------|---------|-------|
| 1 | CBP | Census County Business Patterns | Annual | `fed_data.cbp_data` |
| 1 | SUSB | Census Statistics of US Businesses | Annual | `fed_data.susb_data` |
| 1 | QCEW | BLS Quarterly Census of Employment & Wages | Quarterly | `fed_data.qcew_data` |
| 1 | OEWS | BLS Occupational Employment & Wage Statistics | Annual | `fed_data.oews_data` |
| 1 | FPDS | SAM.gov Federal Procurement Data System | Daily | `fed_data.fpds_contracts` |
| 1 | Econ Census | Census Economic Census | Annual | `fed_data.economic_census` |
| 1 | PPP | SBA Paycheck Protection Program | One-time | `fed_data.ppp_loans` |
| 1 | EO BMF | IRS Exempt Org Business Master File | Monthly | `fed_data.eo_bmf` |
| 1B | ADV Part 1 | SEC ADV Part 1A (Investment Advisors) | Monthly | `fed_data.adv_firms` |
| 1B | IA Compilation | IARD Daily XML Compilation | Daily | `fed_data.adv_firms` |
| 1B | Holdings 13F | SEC 13F Institutional Holdings | Quarterly | `fed_data.f13_holdings` |
| 1B | Form D | SEC EDGAR Form D (Private Offerings) | Daily | `fed_data.form_d` |
| 1B | EDGAR Submissions | SEC EDGAR Bulk Entity Filings | Weekly | `fed_data.edgar_entities` |
| 1B | Entity Xref | CRD↔CIK Cross-Reference | Internal | `fed_data.entity_xref` |
| 2 | ADV Part 2 | SEC ADV Part 2A Brochures (OCR) | Monthly | `fed_data.adv_brochures` |
| 2 | BrokerCheck | FINRA BrokerCheck (BD Disciplinary) | Monthly | `fed_data.brokercheck` |
| 2 | Form BD | SEC Form BD (Broker-Dealer Registration) | Monthly | `fed_data.form_bd` |
| 2 | OSHA ITA | OSHA Inspection Tracking | Annual | `fed_data.osha_inspections` |
| 2 | EPA ECHO | EPA Enforcement & Compliance History | Monthly | `fed_data.epa_facilities` |
| 2 | NES | Census Nonemployer Statistics | Annual | `fed_data.nes_data` |
| 2 | ASM | Census Annual Survey of Manufactures | Annual | `fed_data.asm_data` |
| 2 | ECI | BLS Employment Cost Index | Quarterly | `fed_data.eci_data` |
| 2 | SEC Enforcement | SEC Enforcement Actions | Monthly | `fed_data.sec_enforcement_actions` |
| 2 | FDIC BankFind | FDIC Institution Financial Data | Weekly | `fed_data.fdic_institutions` |
| 2 | N-CEN | SEC Form N-CEN (Investment Company Census) | Quarterly | `fed_data.ncen_registrants` |
| 3 | ADV Part 3 | SEC Form CRS (Client Relationship Summary) | Monthly | `fed_data.adv_crs` |
| 3 | XBRL Facts | SEC EDGAR XBRL Financial Facts | Daily | `fed_data.xbrl_facts` |
| 3 | FRED | Federal Reserve Economic Data | Monthly | `fed_data.fred_series` |
| 3 | ABS | Census Annual Business Survey | Annual | `fed_data.abs_data` |
| 3 | CPS/LAUS | BLS Current Population Survey & Local Unemployment | Monthly | `fed_data.laus_data` |
| 3 | M3 | Census Monthly Wholesale Trade Survey | Monthly | `fed_data.m3_data` |
| — | ADV Enrichment | Internal ADV brochure structured extraction | Monthly | `fed_data.adv_firms` |
| — | ADV Extract | Internal ADV text extraction pipeline | Monthly | `fed_data.adv_brochures` |

### Geo Scrapers (7 implementations)

| Scraper | Source | Table |
|---------|--------|-------|
| Census Demographics | ACS tract-level demographics + TIGERweb polygons | `geo.demographics` |
| EPA Sites | EPA ECHO facility locations | `geo.epa_sites` |
| FEMA Flood | FEMA flood hazard zones | `geo.flood_zones` |
| HIFLD Pipelines | Homeland Infrastructure pipeline routes | `geo.infrastructure` |
| HIFLD Power Plants | Homeland Infrastructure power generation | `geo.infrastructure` |
| HIFLD Substations | Homeland Infrastructure electrical substations | `geo.infrastructure` |
| HIFLD Transmission Lines | Homeland Infrastructure transmission lines | `geo.infrastructure` |

### Enrichment Pipeline External Sources (6 sources)

| Source | Phase | Method | File |
|--------|-------|--------|------|
| Website Crawl | 1A | Local HTTP + Colly (depth 2) → Firecrawl fallback | `pipeline/crawl.go`, `localcrawl.go` |
| Google Maps | 1B | Direct URL + Perplexity/Google Maps API | `pipeline/scrape.go` |
| BBB | 1B | Jina Search (site:bbb.org) | `pipeline/scrape.go` |
| Secretary of State | 1B | Jina Search (generic SoS lookup) | `pipeline/scrape.go` |
| LinkedIn | 1C | Perplexity API → Haiku JSON (7-day cache) | `pipeline/linkedin.go` |
| ADV Pre-fill | — | Fed data lookup for known RIAs | `pipeline/prefill.go` |

---

## 2. Missing Federal Datasets

### 2.1 High Priority — Direct Business Intelligence

#### DOL Form 5500 (ERISA) — DONE

- **Status:** Implemented in `internal/fedsync/dataset/form_5500.go`
- **Tables:** `fed_data.form_5500` (140 cols), `form_5500_sf` (191), `form_5500_schedule_h` (166), `form_5500_providers` (15)
- **Entity Backfill:** `cmd/geo_backfill_5500.go` — stub companies by EIN, geocode, MSA-associate
- **Linker:** `matchEIN` in `internal/company/link.go`
- **Production:** 5.9M rows across 4 tables (2020–2025)

#### FDIC BankFind — DONE

- **Status:** Implemented in `internal/fedsync/dataset/fdic_bankfind.go`
- **Table:** `fed_data.fdic_institutions`
- **Entity Backfill:** `cmd/geo_backfill_fdic.go` — stub companies by FDIC cert, geocode, MSA-associate
- **Linker:** `matchFDIC` in `internal/company/link.go`
- **Production:** Bank branch locations, financials, deposits. Cross-references BDs/RIAs that are bank-affiliated.

#### NCUA Credit Union 5300 Call Reports

- **Access:** Bulk CSV download (`ncua.gov/analysis`)
- **Format:** CSV/ZIP
- **Cadence:** Quarterly
- **Relevance:** Credit union financials. Same cross-ref use as FDIC but for credit union-affiliated advisors. Identifies CU-affiliated wealth management programs.

#### USAspending (Federal Awards)

- **Access:** REST API (`api.usaspending.gov`) + bulk download
- **Format:** CSV/JSON
- **Cadence:** Daily
- **Relevance:** Federal contract and grant recipients. Complements existing FPDS (which covers procurement only) by adding grants, loans, direct payments. Identifies firms with government relationships.

#### IRS Exempt Org BMF — DONE

- **Status:** Implemented in `internal/fedsync/dataset/eo_bmf.go`
- **Table:** `fed_data.eo_bmf` (28 columns, ~1.94M rows)
- **Entity Backfill:** `cmd/geo_backfill_990.go` — stub companies by EIN, geocode, MSA-associate
- **Linker:** `matchEOBMF` in `internal/company/link.go`
- **Production:** 1,935,635 rows (4 regional CSVs from IRS SOI)
- **Remaining:** Form 990 financial extracts (~300K returns/year) — natural Phase 2 follow-up via ProPublica Nonprofit Explorer API

#### USPTO Patent Assignments + Trademarks

- **Access:** REST API (`developer.uspto.gov`)
- **Format:** JSON/XML
- **Cadence:** Weekly
- **Relevance:** IP ownership signals firm value and innovation — M&A valuation input. Assignment transfers signal ownership changes (acquisition indicator). Trademark filings for brand protection status.

#### ~~SBA 7(a) and 504 Loan Data~~ — DONE

- **Access:** Bulk FOIA download via CKAN API (`data.sba.gov`)
- **Format:** CSV (6 files: 4 for 7(a), 2 for 504)
- **Cadence:** Quarterly
- **Relevance:** Small business lending beyond PPP (which was one-time). Identifies firms with SBA debt — acquisition financing signal. Outstanding loan balance indicates capital structure.
- **Implementation:** `fed_data.sba_loans` table, `sba_7a_504` dataset. Single unified table with `program` column (7A/504). 10 entity cross-reference passes including direct FDIC cert linkage for 7(a) bank matching. `geo backfill-sba` command for entity aggregation.

#### CFPB Consumer Complaint Database

- **Access:** REST API + bulk CSV download
- **Format:** CSV/JSON
- **Cadence:** Daily
- **Relevance:** Compliance risk signal for BDs and lenders. Complaint volume and type as due diligence input. High complaint counts are M&A red flags.

#### Treasury/FinCEN SAR Statistics

- **Access:** Bulk download (public aggregate data)
- **Format:** CSV
- **Cadence:** Annual
- **Relevance:** Anti-money-laundering enforcement patterns by institution type. Aggregate risk context — not firm-specific. Note: Beneficial Ownership Information (BOI) reporting for US companies was effectively eliminated in March 2025.

#### DOJ Antitrust Case Filings

- **Access:** PACER + press releases (HTML scrape)
- **Format:** HTML/PDF
- **Cadence:** Irregular
- **Relevance:** M&A deal review history, enforcement actions against financial firms. Historical context for deal structure and regulatory risk assessment.

#### FINRA Disciplinary Actions (Expanded)

- **Access:** HTML scrape from FINRA.org disciplinary actions database
- **Format:** HTML
- **Cadence:** Monthly
- **Relevance:** Expanded enforcement history beyond BrokerCheck. Current BrokerCheck covers individual rep disclosures but not all firm-level disciplinary proceedings, arbitration awards, and administrative actions.

### 2.2 Medium Priority — Enrichment & Context

#### HUD FHA Lender List + Housing Data

- **Access:** API + bulk download
- **Format:** CSV
- **Cadence:** Monthly
- **Relevance:** Mortgage lender identification. Relevant for insurance and lending-adjacent M&A targets.

#### FHFA Fannie/Freddie Seller-Servicer Data

- **Access:** Bulk download
- **Format:** CSV/XLSX
- **Cadence:** Quarterly
- **Relevance:** Identifies mortgage industry participants — seller-servicer approval status and volume.

#### NAIC Insurance Company Data

- **Access:** No uniform API; some state commissioners provide bulk data
- **Format:** Varies by state
- **Cadence:** Annual
- **Relevance:** Insurance company financials, premium volume, market share. Critical for insurance agency M&A. Biggest gap for insurance sector coverage.

#### GAO/IG Federal Audit Findings

- **Access:** HTML scrape from GAO.gov
- **Format:** HTML/PDF
- **Cadence:** Irregular
- **Relevance:** Government contractor compliance issues. Niche use for firms with federal contracts.

#### FTC Enforcement Actions + HSR Filings (Public)

- **Access:** HTML scrape from FTC.gov
- **Format:** HTML
- **Cadence:** Irregular
- **Relevance:** Pre-merger Hart-Scott-Rodino filing data (public portions). Signals M&A activity in the financial services sector.

#### SEC N-CEN (Investment Company Census) — DONE

- **Status:** Implemented in `internal/fedsync/dataset/ncen.go`
- **Tables:** `fed_data.ncen_registrants`, `fed_data.ncen_funds`, `fed_data.ncen_advisers`
- **Source:** SEC DERA quarterly ZIP files (TSV), 2018 Q3–present
- **Data:** ~436 registrants/quarter, ~2,400 funds/quarter, ~3,800 advisers/quarter
- **Entity Linking:** CRD numbers in adviser table, CIK in registrant table
- **Remaining:** N-PORT (portfolio holdings) is a separate, larger dataset for Phase 2 follow-up

#### SEC Regulation D (Expanded Fields)

- **Access:** Current Form D already ingested; additional field extraction from same source
- **Format:** XML
- **Cadence:** Daily
- **Relevance:** Already have core Form D fields. Could expand to pull offering amounts, investor counts, intermediary information, and revenue/use-of-proceeds data.

### 2.3 Lower Priority — Macro & Supplemental

#### FDIC Failed Bank List + Loss-Share Agreements

- **Access:** API (`api.fdic.gov/failures`)
- **Format:** JSON
- **Cadence:** Weekly
- **Relevance:** Historical context for M&A due diligence on bank-affiliated firms.

#### Federal Reserve H.8 / Y-9C

- **Access:** FRED + bulk download
- **Format:** CSV
- **Cadence:** Weekly (H.8) / Quarterly (Y-9C)
- **Relevance:** Macro banking industry context. Some series may already be available through existing FRED dataset.

#### SSA Wage Statistics

- **Access:** Bulk download
- **Format:** CSV
- **Cadence:** Annual
- **Relevance:** Compensation benchmarking for advisory firm staffing analysis.

#### Expanded XBRL Taxonomy

- **Access:** Same EDGAR source already ingested (`internal/fedsync/xbrl/taxonomy.go`)
- **Format:** JSON
- **Cadence:** Daily
- **Relevance:** Currently only track 16 US-GAAP facts. Could expand to 100+ covering debt, equity, cash flow, segments. Minimal incremental effort — update taxonomy definitions only.

---

## 3. State-Level Data Sources

State data is the biggest gap and the hardest to close. Every state runs its own systems with different interfaces, formats, and access policies.

### 3.1 Secretary of State — Business Entity Filings

**What it provides:** Legal entity registration, status (active/dissolved/revoked), formation date, registered agent, officers/directors (some states), annual report filings.

**Why it matters:** Entity status is a basic due diligence signal. Dissolved/revoked entities are red flags. Officer names enable network mapping. Formation dates indicate firm age.

**Access landscape:**
- API access exists in ~5 states (CA, IA, OH, CO, FL) with varying pricing ($0–$10K/yr)
- Bulk download available in ~8 states (IN, CA, WA, NY, TX, FL, MA, IL)
- Web scrape only for ~37 states — each with different HTML structure

**Current coverage:** Enrichment pipeline searches SoS via Jina (`internal/pipeline/scrape.go`). This finds basic entity info but doesn't extract structured data like officers or annual report status.

**Recommendation:** Supplement existing Jina-based SoS search with direct API calls for high-volume states (CA, FL, TX, NY). For comprehensive coverage, evaluate commercial aggregators:
- **Cobalt Intelligence API** ($299+/mo for 50 states)
- **OpenCorporates** (200M+ entities worldwide, free tier 500 req/day + commercial)

### 3.2 State Securities Regulators (NASAA Members)

**What it provides:** State-registered investment advisor filings (not on SEC ADV), state enforcement actions, exam findings.

**Why it matters:** Firms under $100M AUM register with states, not SEC. Current ADV data misses ~15,000+ state-only RIAs. This is a significant blind spot for the M&A pipeline targeting smaller RIAs.

**Access landscape:**
- IARD/NASAA CRD system covers both SEC and state registrants — but public data from FINRA BrokerCheck and IAPD may not include all state-level detail
- Individual state regulators have their own searchable databases (e.g., TX SSBT, CA DBO, NY DFS)
- No uniform API or bulk download across states

**Recommendation:** First investigate whether IAPD FOIA data (ADV Part 1) already includes state-registered advisors. If state-only RIAs are missing, scrape individual state regulator sites for the top 10 states by RIA count (CA, TX, NY, FL, IL, MA, OH, PA, NJ, CT).

### 3.3 State Insurance Departments

**What it provides:** Licensed insurance agents/agencies, appointment records, disciplinary actions, company filings.

**Why it matters:** Insurance agency M&A is a major adjacent market. Licensed agent counts, carrier appointments, and complaint history are key valuation inputs. Currently zero insurance sector coverage.

**Access landscape:**
- **NIPR (National Insurance Producer Registry)** — centralized license lookup, no public API; commercial access via NIPR Gateway
- **State-by-state databases** — each department has a producer lookup (web only)
- **NAIC** — aggregated data, mostly behind paywall

**Recommendation:** NIPR is the single best source but requires a commercial relationship. For a scraping approach, target top 10 states by insurance premium volume. Natural fit for the geo scraper `GeoScraper` interface pattern.

### 3.4 State Court Records

**What it provides:** Civil litigation, judgments, liens, bankruptcies (state-level), regulatory proceedings.

**Why it matters:** Litigation history is a critical M&A due diligence input. Outstanding judgments and liens affect firm valuation.

**Access landscape:**
- **PACER** (federal courts) — API available, $0.10/page, covers federal civil/criminal
- **State courts** — wildly fragmented. Some states have unified e-filing (TX, CA, NY), most are county-by-county
- **Commercial aggregators:** LexisNexis, Westlaw, CourtListener (free for federal)

**Recommendation:** Start with PACER for federal court records (standardized API, reasonable cost). CourtListener's free API covers federal courts and some state appellate courts. State court records are better accessed through commercial aggregators rather than per-state scraping.

### 3.5 State UCC Filings

**What it provides:** Secured transaction records (liens on business assets, equipment, accounts receivable).

**Why it matters:** UCC filings reveal debt structure and encumbrances on assets — critical for M&A valuation. A firm with heavy UCC liens has different acquisition economics.

**Access landscape:** Filed with Secretary of State in most states (some at county level). Many SoS sites include UCC search alongside entity search. No uniform API.

**Recommendation:** Bundle with SoS scraping (§3.1) since UCC data often lives on the same systems.

### 3.6 State Professional Licensing Boards

**What it provides:** CPA licenses, law licenses, real estate licenses, etc.

**Why it matters:** Relevant for multi-disciplinary firms (CPA/RIA combos, law firm/RIA combos). Professional license status validates credentials.

**Recommendation:** Low priority. Only relevant for specific M&A scenarios (e.g., accounting firm acquisitions).

---

## 4. County & Local Data

### 4.1 Property Records (Assessor/Recorder)

**What it provides:** Property ownership, valuations, tax assessments, transfers, mortgages.

**Why it matters:** Commercial property owned by advisory firms indicates stability and asset base. Property transfers can signal ownership changes (pre-acquisition).

**Access:** ~3,100 counties, each with their own assessor system. Some states have centralized portals (FL, TX, CA). Commercial aggregators: ATTOM Data, CoreLogic, Zillow.

**Recommendation:** Not worth building scrapers for 3,100 county systems. Use commercial aggregator API (ATTOM has a REST API with property ownership and transaction data) — single integration point.

### 4.2 Business Licenses (City/County)

**What it provides:** Local business license records, occupancy permits, DBA filings.

**Why it matters:** Limited — mainly confirms a business is actively operating at a claimed location.

**Recommendation:** Low priority. Google Maps data (already in enrichment pipeline) provides better active-business signals.

### 4.3 Building Permits

**What it provides:** Construction/renovation permits, commercial real estate activity.

**Why it matters:** New construction signals business expansion — useful for geo-level market analysis but not firm-specific M&A targeting.

**Recommendation:** Already partially covered by HIFLD infrastructure data. Only relevant for the geo scraper subsystem's market analysis.

---

## 5. General Scraper Categories

Patterns/categories of scraping that apply across many similar but non-uniform sources.

### 5.1 Industry Association Directories

**What it provides:** Membership lists for professional associations (FPA, NAPFA, CFP Board, AICPA, state CPA societies, bar associations).

**Why it matters:** Association membership signals firm type, specialization, and professional standing. CFP Board's public directory lists every CFP professional with firm affiliations.

**Access:** Web scrape per-association. CFP Board has a public search at `letsmakeaplan.org`.

**Priority:** High. Build as enrichment external sources (same pattern as Google Maps, BBB, SoS in `internal/pipeline/scrape.go`). Order: CFP Board → FPA/NAPFA → AICPA.

### 5.2 Job Posting Aggregators

**What it provides:** Active job listings by company — hiring volume, role types, salary ranges.

**Why it matters:** Hiring activity signals growth (scaling headcount = growth story; not replacing departures = potential wind-down candidate).

**Access:** Indeed API (deprecated), LinkedIn Jobs (no public API), Google Jobs (structured data in HTML), Glassdoor.

**Priority:** Medium. Could integrate as an enrichment external source using Jina search for `[company] hiring financial advisor` or similar. No dedicated scraper needed.

### 5.3 Review & Rating Sites

**What it provides:** Client reviews, employee reviews (Glassdoor), BBB ratings, Google reviews.

**Why it matters:** Reputation and client satisfaction are M&A quality signals. Employee reviews reveal cultural issues.

**Current coverage:** BBB and Google Maps already in enrichment pipeline.

**Priority:** Low incremental value. Already covered for the most actionable sources.

### 5.4 News & Press Monitoring

**What it provides:** M&A announcements, leadership changes, regulatory actions, firm milestones.

**Why it matters:** Real-time deal sourcing signals. Succession challenges and regulatory issues indicate potential acquisition targets.

**Access:** Google News API (deprecated), NewsAPI.org ($449/mo), GDELT Project (free, research use), Perplexity (already integrated).

**Current coverage:** Perplexity already used for LinkedIn phase. Could extend queries to include news context per company.

**Priority:** Low incremental cost. Extend existing Perplexity integration rather than adding new sources.

### 5.5 Domain/WHOIS & Web Infrastructure

**What it provides:** Domain registration dates, registrant info, hosting provider, SSL cert details, tech stack.

**Why it matters:** Domain age and tech stack indicate firm sophistication. WHOIS changes can signal ownership transitions.

**Access:** WhoisXML API, DomainTools, BuiltWith API, Wappalyzer.

**Priority:** Low for M&A targeting. Could be a lightweight enrichment signal in the future.

### 5.6 Social Media Presence

**What it provides:** Company social media profiles, follower counts, posting frequency, engagement metrics.

**Why it matters:** Marketing sophistication signal. Firms with no social presence may be "lifestyle practices" with different acquisition profiles.

**Current coverage:** LinkedIn already covered via Perplexity.

**Priority:** Low. Incremental value from Twitter/Facebook is minimal for financial services firms.

---

## 6. Commercial Aggregators (Buy vs. Build)

Some gaps are better filled by purchasing access rather than building scrapers.

| Provider | Data | API? | Cost Range | Covers | Replaces |
|----------|------|------|-----------|--------|----------|
| **OpenCorporates** | 200M+ company records, 170+ jurisdictions | REST | Free (500 req/day) + commercial | SoS entity data across all states | Per-state SoS scraping |
| **Cobalt Intelligence** | SoS entity data for all 50 states | REST | $299+/mo | Entity status, officers, annual reports | Building 50 state SoS scrapers |
| **ATTOM Data** | Property records, transactions, valuations | REST | Custom pricing | County property records | Scraping 3,100 county assessors |
| **Dun & Bradstreet** | Company hierarchy, firmographics, credit | API | Enterprise pricing | Parent-subsidiary relationships | No current equivalent — biggest entity resolution gap |
| **PitchBook / Crunchbase** | M&A transactions, funding rounds, investors | API | $20K+/yr (PitchBook) | Competitive intelligence — who's buying whom | Manual deal tracking |
| **NIPR** | Insurance producer licenses, all states | Gateway API | Commercial | Licensed agents/agencies, appointments | Scraping 50 state insurance departments |
| **CourtListener** | Federal court records + some state appellate | REST | Free | Federal litigation history, opinions | PACER ($0.10/page) for basic coverage |

---

## 7. Prioritized Roadmap

### Tier 1 — High Impact, Feasible Now

New fedsync datasets using the existing `Dataset` interface in `internal/fedsync/dataset/`:

| # | Dataset | Effort | Value | Notes |
|---|---------|--------|-------|-------|
| 1 | **DOL Form 5500** | ~~Medium~~ | Very High | **DONE.** 4 tables, 512 columns, 5.9M rows. `form_5500.go` + `geo_backfill_5500.go` |
| 2 | **FDIC BankFind** | ~~Low~~ | High | **DONE.** REST API. `fdic_bankfind.go` + `geo_backfill_fdic.go` |
| 3 | **NCUA Call Reports** | Low | High | Bulk CSV. Credit union parallel to FDIC |
| 4 | **IRS Exempt Org BMF** | ~~Medium~~ | Very High | **DONE.** 1.94M rows. `eo_bmf.go` + `geo_backfill_990.go` (EIN→eo_bmf) |
| 5 | **USAspending** | Medium | High | REST API + bulk download. Complements existing FPDS |
| 6 | **Expanded XBRL taxonomy** | Low | High | Same source. Expand from 16 to 100+ facts. Minimal effort |
| 7 | **SEC N-CEN** | ~~Medium~~ | Medium | **DONE.** 3 tables (registrants, funds, advisers). `ncen.go`. N-PORT is separate follow-up |

> **Entity aggregation pattern:** Datasets #3 and #5 (NCUA, USAspending) contain entity-level records with addresses. Each should include a `geo backfill-<source>` command following the existing pattern in `cmd/geo_backfill_5500.go`, `cmd/geo_backfill_990.go`, and `cmd/geo_backfill_fdic.go`.

### Tier 2 — High Impact, More Effort

New enrichment external sources or standalone integrations:

| # | Source | Effort | Value | Notes |
|---|--------|--------|-------|-------|
| 1 | **CFP Board directory** | Medium | High | Web scrape. CFP professional-to-firm mappings |
| 2 | **PACER federal court records** | Medium | High | API with per-page cost. Litigation due diligence |
| 3 | **CFPB complaints** | Low | Medium | Free API. Compliance risk signal |
| 4 | ~~**SBA 7(a)/504 loans**~~ | Low | Medium | **DONE** — `fed_data.sba_loans`, `sba_7a_504` dataset, quarterly via CKAN API. 10 xref passes (direct FDIC cert + name+zip/state). `geo backfill-sba` command. |
| 5 | **OpenCorporates integration** | Medium | High | Single API for SoS entity data across all jurisdictions |

### Tier 3 — Strategic but Complex

Per-state scraper implementations:

| # | Source | Effort | Value | Notes |
|---|--------|--------|-------|-------|
| 1 | **State securities regulators (top 10)** | High | Very High | Captures ~15K state-only RIAs missing from IAPD |
| 2 | **State insurance departments (top 10)** | High | High | Licensed agents/agencies for insurance M&A |
| 3 | **SoS entity data (top 10 states)** | High | Medium | Direct API/scrape for formation, status, officers |

### Tier 4 — Evaluate Later

- Property records via ATTOM API
- D&B parent-subsidiary data for entity resolution
- Job posting signals via Jina search
- News/press monitoring expansion (extend Perplexity queries)
- USPTO patent/trademark signals
- State court records via commercial aggregator
- FINRA expanded disciplinary actions
- SEC Reg D expanded field extraction

---

## 8. Gap Matrix by Business Function

| Business Function | Current Coverage | Biggest Gap | Priority Source |
|-------------------|-----------------|-------------|-----------------|
| **Firm identification** | ADV, EDGAR, BrokerCheck, Form BD, SoS (Jina) | State-only RIAs (~15K firms not in SEC data) | State securities regulators |
| **AUM / assets** | ADV Part 1, 13F, XBRL (16 facts), **Form 5500** | Expanded XBRL (100+ facts), N-PORT fund data | XBRL expansion |
| **Revenue / fees** | XBRL (limited), ADV brochure OCR, **EO BMF** (nonprofit assets/income) | Expanded XBRL facts, Form 990 detail | XBRL expansion + Form 990 |
| **Compliance risk** | SEC enforcement, BrokerCheck, OSHA, EPA ECHO | CFPB complaints, state enforcement, FINRA expanded | CFPB complaint database |
| **Entity relationships** | entity_xref (CRD↔CIK), ADV enrichment | Parent-subsidiary chains (no source) | D&B (commercial) |
| **Geography** | 7 geo scrapers + Census ACS + CBP | Property records, county-level economic data | ATTOM Data (commercial) |
| **M&A signals** | Form D (offerings), FPDS (contracts), PPP | USAspending (grants), news monitoring, job postings | USAspending API |
| **Insurance sector** | None | NAIC data, state insurance producer licenses | NIPR (commercial) or state scraping |
| **Banking sector** | FDIC BankFind | NCUA credit unions | NCUA Call Reports |
| **Advisory personnel** | LinkedIn (Perplexity), BrokerCheck reps | CFP Board directory, FPA/NAPFA membership | CFP Board scrape |
| **Debt / capital structure** | PPP, **SBA 7(a)/504**, **Form 5500** (Schedule H plan loans) | UCC filings | UCC search integration |
| **IP / innovation** | None | Patent assignments, trademark filings | USPTO API |
