# Salesforce Setup for Research CLI Integration

## What This Is

We have an automated enrichment pipeline that crawls company websites, extracts structured data (services, revenue estimates, key people, certifications, etc.), and writes the results into Salesforce Accounts and Contacts. This doc covers everything you need to set up on the Salesforce side so we can start testing.

**Target timeline:** Get a sandbox working so we can run 3-5 test accounts end-to-end, then move to production.

---

## Part 1: Connected App (API Authentication)

The pipeline authenticates to Salesforce using the **JWT Bearer Flow** — no interactive login, no stored passwords. This requires a Connected App.

### Steps

1. **Go to Setup → App Manager → New Connected App**
2. Fill in basics:
   - **Connected App Name:** `Research CLI`
   - **API Name:** `Research_CLI`
   - **Contact Email:** your email
3. Under **API (Enable OAuth Settings):**
   - Check **Enable OAuth Settings**
   - **Callback URL:** `https://login.salesforce.com/services/oauth2/callback` (required but unused by JWT flow)
   - Check **Use digital signatures**
   - Upload the certificate file I'll send you (`sf_cert.crt`) — I generate this keypair on my end
   - **Selected OAuth Scopes** — add these three:
     - `Access the identity URL service (id, profile, email, address, phone)`
     - `Manage user data via APIs (api)`
     - `Perform requests at any time (refresh_token, offline_access)`
4. Save the Connected App

### After Saving

5. Go to **Setup → Manage Connected Apps** (or click "Manage" on the app you just created)
6. Under **OAuth Policies:**
   - Set **Permitted Users** to **"Admin approved users are pre-authorized"**
   - Save
7. Under **Profiles** (or **Permission Sets**):
   - Add the profile of the API user we'll use (e.g., System Administrator, or a dedicated integration profile)
   - This pre-authorizes the app so it can authenticate without a browser prompt

### What I Need Back

| Item | Description |
|---|---|
| **Consumer Key** | The long alphanumeric string on the Connected App detail page (under API → Consumer Key) |
| **API Username** | The Salesforce username the pipeline will authenticate as (e.g., `blake@sellsadvisors.com` or a dedicated integration user) |
| **Sandbox URL** | Confirm whether we're using `https://test.salesforce.com` (sandbox) or a custom domain |

I keep the private key on my side — you never need to see it.

---

## Part 2: Custom Fields

The pipeline writes to **29 custom fields on Account** and **1 custom field on Contact**. These need to be created before any data will flow. (6 exec/people fields were removed — that data lives on Contacts via the related list.)

Standard fields like `Name`, `Website`, `Phone`, `Description`, `BillingStreet`, and `NumberOfEmployees` on Account (and `FirstName`, `LastName`, `Title`, `Email`, `Phone` on Contact) are already built into Salesforce — no action needed for those.

### How to Create Custom Fields

**Setup → Object Manager → Account → Fields & Relationships → New**

For each field below, create it with the exact **Field Label** shown. Salesforce will auto-generate the API Name (appending `__c`). The API Name column is what the pipeline uses internally — if the auto-generated name doesn't match, rename it.

### Account Custom Fields (29 total)

#### Company Basics

| # | Field Label | API Name | Field Type | Length / Precision | Notes |
|---|---|---|---|---|---|
| 1 | Legal Name | `Legal_Name__c` | Text | 255 | Full legal entity name |
| 2 | Year Founded | `Year_Founded__c` | Number | 4 digits, 0 decimal | e.g., 2005 |
| 3 | Primary Email | `Primary_Email__c` | Email | — | Auto-validates email format |
| 4 | Business Model | `Business_Model__c` | Picklist | — | e.g., "B2B Services" — values TBD |
| 5 | NAICS Code | `NAICS_Code__c` | Text | 10 | Industry classification code |

#### Services & Capabilities

| # | Field Label | API Name | Field Type | Length / Precision | Notes |
|---|---|---|---|---|---|
| 6 | Services | `Services__c` | Long Text Area | 1,000 | List of services offered |
| 7 | Service Area | `Service_Area__c` | Text | 500 | Geographic coverage |
| 8 | Licenses & Certifications | `Licenses_Certifications__c` | Long Text Area | 1,000 | Professional licenses held |
| 9 | Customer Types | `Customer_Types__c` | Picklist | — | e.g., "SMB, Mid-Market" — values TBD |

#### Financials & Sizing

| # | Field Label | API Name | Field Type | Length / Precision | Notes |
|---|---|---|---|---|---|
| 10 | Revenue Range | `Revenue_Range__c` | Picklist | — | e.g., "$5M-$10M" — values TBD |
| 11 | Revenue Estimate | `Revenue_Estimate__c` | Number | 12 digits, 0 decimal | Numeric estimate in USD |
| 12 | Revenue Confidence | `Revenue_Confidence__c` | Number / Picklist? | 3 digits, 2 decimal | **Type conflict**: pipeline sends float 0.0–1.0, Hutton has picklist. Needs discussion. |
| 13 | Employee Estimate | `Employee_Estimate__c` | Number | 8 digits, 0 decimal | Estimated headcount |
| 14 | LinkedIn Employees | `LinkedIn_Employees__c` | Number | 8 digits, 0 decimal | Employee count from LinkedIn |

#### Reputation & Reviews

| # | Field Label | API Name | Field Type | Length / Precision | Notes |
|---|---|---|---|---|---|
| 15 | Google Reviews | `Google_Reviews__c` | Number | 8 digits, 0 decimal | Total Google reviews |
| 16 | Google Review Rating | `Google_Review_Rating__c` | Number | 3 digits, 2 decimal | e.g., 4.70 |
| 17 | Reputation Summary | `Reputation_Summary__c` | Long Text Area | 2,000 | AI-generated summary of online reputation |

#### Analysis & Narrative

| # | Field Label | API Name | Field Type | Length / Precision | Notes |
|---|---|---|---|---|---|
| 18 | Differentiators | `Differentiators__c` | Long Text Area | 2,000 | What makes this company stand out |
| 19 | Acquisition Assessment | `Acquisition_Assessment__c` | Long Text Area | 5,000 | Fit assessment for acquisition |
| 20 | Description | `Description` | Standard field | — | Uses the standard SF Description field (no custom `__c` needed) |
| 21 | Enrichment Report | `Enrichment_Report__c` | Long Text Area | **131,072** | Full enrichment report in Markdown. **Must be set to maximum Long Text Area length (131,072 characters).** |

#### Geographic Data

These fields are populated by geocoding the company's address. They power location-based analytics (MSA proximity, urban/rural classification, etc.). Latitude and Longitude use the native SF **Geolocation** compound field `Longitude_and_Lattitude__c`.

| # | Field Label | API Name | Field Type | Length / Precision | Notes |
|---|---|---|---|---|---|
| 22 | Latitude | `Longitude_and_Lattitude__Latitude__s` | Geolocation (sub-field) | — | Native SF compound field |
| 23 | Longitude | `Longitude_and_Lattitude__Longitude__s` | Geolocation (sub-field) | — | Native SF compound field |
| 24 | Company MSA | `Company_MSA__c` | Text | 255 | Metropolitan Statistical Area name |
| 25 | MSA CBSA Code | `MSA_CBSA_Code__c` | Text | 10 | Census CBSA code |
| 26 | Urban Classification | `Urban_Classification__c` | Picklist | — | Values: `urban_core`, `suburban`, `exurban`, `rural` |
| 27 | Distance to MSA Center (km) | `Distance_to_MSA_Center_km__c` | Number | 8 digits, 2 decimal | Km from metro center |
| 28 | Distance to MSA Edge (km) | `Distance_to_MSA_Edge_km__c` | Number | 8 digits, 2 decimal | Km from metro boundary |
| 29 | County FIPS | `County_FIPS__c` | Text | 10 | Federal county code |

### Contact Custom Fields (1 total)

**Setup → Object Manager → Contact → Fields & Relationships → New**

| # | Field Label | API Name | Field Type | Notes |
|---|---|---|---|---|
| 1 | LinkedIn | `LinkedIn__c` | URL | Contact's LinkedIn profile |

---

## Part 3: Permissions

The API user (the username from Part 1) needs the following permissions. This is typically set at the **Profile** or **Permission Set** level.

### Object Permissions

| Object | Read | Create | Edit | Delete |
|---|---|---|---|---|
| Account | Yes | Yes | Yes | No |
| Contact | Yes | Yes | Yes | No |

### System Permissions

| Permission | Required |
|---|---|
| API Enabled | Yes |
| Modify All Data | No (not needed) |

### Field-Level Security

All **30 custom fields** above (29 Account + 1 Contact) need **Read** and **Edit** access for the API user's profile. The standard fields (`Name`, `Website`, `Phone`, etc.) typically already have access, but worth double-checking.

**Quickest way:** Go to **Setup → Profiles → [API User's Profile] → Field-Level Security**, then check Account and Contact custom fields.

---

## Part 4: Sandbox Setup (Recommended)

We should test in a **Salesforce Sandbox** before touching production. If you don't already have one:

1. **Setup → Sandboxes → New Sandbox**
2. Pick **Developer** sandbox (free, lightweight, good for testing)
3. Wait for it to activate (can take a few minutes to a few hours)
4. The sandbox login URL will be `https://test.salesforce.com` (or `https://[yourdomain]--[sandboxname].sandbox.my.salesforce.com`)

Everything above (Connected App, custom fields, permissions) needs to be done **in the sandbox**, not production. When we're ready to go live, we can either recreate in production or use a Change Set to promote.

---

## Part 5: Verification Checklist

Once everything is set up, I'll run a health check from our side. Here's what I'll be testing:

| Test | What It Validates |
|---|---|
| **Authentication** | Consumer Key + private key + username → successful JWT token |
| **Schema check** | `DescribeSObject("Account")` returns all custom fields |
| **Create Account** | Pipeline creates a new Account with enriched data |
| **Dedup check** | Running the same company twice matches the existing Account (no duplicate) |
| **Update Account** | Updating an existing Account by SF ID writes correctly |
| **Create Contact** | Contacts are created under the Account with LinkedIn URL |
| **Contact dedup** | Running the same company twice matches existing Contacts (no duplicates) |

If any of these fail, the error messages will point to exactly what's wrong (usually a field name mismatch or missing permission).

---

## Common Issues & Troubleshooting

### "INVALID_FIELD" error on specific fields

The custom field API name in Salesforce doesn't match what the pipeline expects. Check the API Name column in the tables above — Salesforce auto-generates API Names from the label, but sometimes adds numbers or changes casing. You can rename the API Name when creating the field (just not after).

### "INSUFFICIENT_ACCESS" errors

The API user's profile is missing Read/Create/Edit on Account or Contact, or field-level security isn't set for the custom fields.

### "INVALID_SESSION_ID" or JWT auth failure

Usually means:
- The Connected App isn't pre-authorized for the user's profile (Step 7 in Part 1)
- The Consumer Key is wrong
- The certificate uploaded doesn't match the private key I'm using

### Fields get created but data is blank

Likely a field-level security issue — the API user can create/update the record but can't write to specific fields. Check FLS for each custom field.

---

## Quick Reference: Everything I Need from You

| # | Item | Details |
|---|---|---|
| 1 | Consumer Key | From the Connected App detail page |
| 2 | API Username | The SF user the pipeline authenticates as |
| 3 | Sandbox URL | `https://test.salesforce.com` or custom domain |
| 4 | Confirmation | Custom fields created (29 Account + 1 Contact) |
| 5 | Confirmation | FLS set for API user on all custom fields |
| 6 | Confirmation | Connected App pre-authorized for API user profile |

Once I have items 1-3, I can run the health check immediately. Items 4-6 are needed before data actually flows.
