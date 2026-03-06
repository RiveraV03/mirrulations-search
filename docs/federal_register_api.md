# FederalRegister.gov API — Full Documentation

> **Base URL:** `https://www.federalregister.gov/api/v1`  
> **Authentication:** None required — no API key needed, just an HTTP client or browser.

---

## Table of Contents

1. [Overview](#overview)
2. [Federal Register Documents](#federal-register-documents)
   - [GET /documents/{document_number}.{format}](#1-get-documentsdocument_numberformat)
   - [GET /documents/{document_numbers}.{format}](#2-get-documentsdocument_numbersformat)
   - [GET /documents.{format}](#3-get-documentsformat)
   - [GET /documents/facets/{facet}](#4-get-documentsfacetsfacet)
   - [GET /issues/{publication_date}.{format}](#5-get-issuespublication_dateformat)
3. [Public Inspection Documents](#public-inspection-documents)
   - [GET /public-inspection-documents/{document_number}.{format}](#6-get-public-inspection-documentsdocument_numberformat)
   - [GET /public-inspection-documents/{document_numbers}.{format}](#7-get-public-inspection-documentsdocument_numbersformat)
   - [GET /public-inspection-documents/current.{format}](#8-get-public-inspection-documentscurrentformat)
   - [GET /public-inspection-documents.{format}](#9-get-public-inspection-documentsformat)
4. [Agencies](#agencies)
   - [GET /agencies](#10-get-agencies)
   - [GET /agencies/{slug}](#11-get-agenciesslug)
5. [Images](#images)
   - [GET /images/{identifier}](#12-get-imagesidentifier)
6. [Suggested Searches](#suggested-searches)
   - [GET /suggested_searches](#13-get-suggested_searches)
   - [GET /suggested_searches/{slug}](#14-get-suggested_searchesslug)
7. [Document Types Reference](#document-types-reference)
8. [Common Fields Reference](#common-fields-reference)

---

## Overview

FederalRegister.gov provides free, open public API endpoints for searching and retrieving Federal Register documents, public inspection documents, agency information, and more. No API key or registration is required.

The API covers Federal Register documents published since **1994**.

**Supported response formats:**
- `json`
- `csv` *(for document search)*

---

## Federal Register Documents

---

### 1. GET `/documents/{document_number}.{format}`

Fetch a single Federal Register document by its document number.

**URL:**
```
GET https://www.federalregister.gov/api/v1/documents/{document_number}.{format}
```

#### Path Parameters

| Parameter         | Type   | Required | Description                                          |
| ----------------- | ------ | -------- | ---------------------------------------------------- |
| `document_number` | string | Yes      | Federal Register document number (e.g. `2020-01234`) |
| `format`          | string | Yes      | Response format: `json`                              |

#### Query Parameters

| Parameter  | Type          | Description                                                              |
| ---------- | ------------- | ------------------------------------------------------------------------ |
| `fields[]` | array[string] | Specific document attributes to return. Returns a default set if omitted |

#### Example Request

```bash
GET /api/v1/documents/2020-01234.json
GET /api/v1/documents/2020-01234.json?fields[]=title&fields[]=publication_date&fields[]=abstract
```

#### Response (200 OK)

Returns a single document object with the requested or default fields.

---

### 2. GET `/documents/{document_numbers}.{format}`

Fetch multiple Federal Register documents in a single request.

**URL:**
```
GET https://www.federalregister.gov/api/v1/documents/{document_numbers}.{format}
```

#### Path Parameters

| Parameter          | Type          | Required | Description                                               |
| ------------------ | ------------- | -------- | --------------------------------------------------------- |
| `document_numbers` | array[string] | Yes      | Comma-separated list of Federal Register document numbers |
| `format`           | string        | Yes      | Response format: `json`                                   |

#### Query Parameters

| Parameter  | Type          | Description                            |
| ---------- | ------------- | -------------------------------------- |
| `fields[]` | array[string] | Specific document attributes to return |

#### Example Request

```bash
GET /api/v1/documents/2020-01234,2020-05678.json
```

#### Response (200 OK)

Returns an array of document objects.

---

### 3. GET `/documents.{format}`

Search all Federal Register documents published since 1994.

**URL:**
```
GET https://www.federalregister.gov/api/v1/documents.{format}
```

#### Path Parameters

| Parameter | Type   | Required | Description                      |
| --------- | ------ | -------- | -------------------------------- |
| `format`  | string | Yes      | Response format: `json` or `csv` |

#### Query Parameters

**Pagination & Display:**

| Parameter  | Type          | Default | Description                           |
| ---------- | ------------- | ------- | ------------------------------------- |
| `per_page` | integer       | 20      | Number of results per page (max 1000) |
| `page`     | integer       | 1       | Page number of the result set         |
| `order`    | array[string] | —       | Sort order for results                |
| `fields[]` | array[string] | —       | Specific attributes to return         |

**Full Text Search:**

| Parameter          | Type   | Description              |
| ------------------ | ------ | ------------------------ |
| `conditions[term]` | string | Full text keyword search |

**Publication Date Filters:**

| Parameter                            | Type                  | Description                         |
| ------------------------------------ | --------------------- | ----------------------------------- |
| `conditions[publication_date][is]`   | string (`YYYY-MM-DD`) | Exact publication date match        |
| `conditions[publication_date][year]` | string (`YYYY`)       | Documents published in a given year |
| `conditions[publication_date][gte]`  | string (`YYYY-MM-DD`) | Published on or after this date     |
| `conditions[publication_date][lte]`  | string (`YYYY-MM-DD`) | Published on or before this date    |

**Effective Date Filters:**

| Parameter                          | Type                  | Description                           |
| ---------------------------------- | --------------------- | ------------------------------------- |
| `conditions[effective_date][is]`   | string (`YYYY-MM-DD`) | Exact effective date match            |
| `conditions[effective_date][year]` | string (`YYYY`)       | Effective date in a given year        |
| `conditions[effective_date][gte]`  | string (`YYYY-MM-DD`) | Effective date on or after this date  |
| `conditions[effective_date][lte]`  | string (`YYYY-MM-DD`) | Effective date on or before this date |

**Document Filters:**

| Parameter                                  | Type          | Description                                                         |
| ------------------------------------------ | ------------- | ------------------------------------------------------------------- |
| `conditions[agencies][]`                   | array[string] | Filter by publishing agency (use agency slug)                       |
| `conditions[type][]`                       | array[string] | Document type: `RULE`, `PRORULE`, `NOTICE`, `PRESDOCU`              |
| `conditions[presidential_document_type][]` | array[string] | Presidential document type (Presidential Documents only)            |
| `conditions[president][]`                  | array[string] | Signing president (Presidential Documents only)                     |
| `conditions[docket_id]`                    | string        | Agency docket number                                                |
| `conditions[regulation_id_number]`         | string        | Regulation ID Number (RIN)                                          |
| `conditions[sections][]`                   | array[string] | Limit to a specific FederalRegister.gov section                     |
| `conditions[topics][]`                     | array[string] | CFR Indexing topic term                                             |
| `conditions[significant]`                  | string        | EO 12866 significance: `"0"` = Not Significant, `"1"` = Significant |

**CFR Filters:**

| Parameter                | Type    | Description                                                         |
| ------------------------ | ------- | ------------------------------------------------------------------- |
| `conditions[cfr][title]` | integer | CFR title number                                                    |
| `conditions[cfr][part]`  | integer | CFR part or part range (e.g. `17` or `1-50`). Requires `cfr[title]` |

**Location Filters:**

| Parameter                    | Type    | Description                                   |
| ---------------------------- | ------- | --------------------------------------------- |
| `conditions[near][location]` | string  | ZIP code or "City, State"                     |
| `conditions[near][within]`   | integer | Max distance in miles from location (max 200) |

#### Example Requests

```bash
# Full text search
GET /api/v1/documents.json?conditions[term]=clean+water

# Filter by agency
GET /api/v1/documents.json?conditions[agencies][]=environmental-protection-agency

# Filter by document type
GET /api/v1/documents.json?conditions[type][]=RULE&conditions[type][]=PRORULE

# Filter by publication date range
GET /api/v1/documents.json?conditions[publication_date][gte]=2023-01-01&conditions[publication_date][lte]=2023-12-31

# Filter by CFR title and part
GET /api/v1/documents.json?conditions[cfr][title]=40&conditions[cfr][part]=60

# Filter by docket ID
GET /api/v1/documents.json?conditions[docket_id]=EPA-HQ-OAR-2021-0317

# Filter by RIN
GET /api/v1/documents.json?conditions[regulation_id_number]=2060-AU40

# Filter significant rules only
GET /api/v1/documents.json?conditions[significant]=1

# Location search
GET /api/v1/documents.json?conditions[near][location]=20001&conditions[near][within]=50

# Paginate with custom page size
GET /api/v1/documents.json?per_page=100&page=2

# Return specific fields only
GET /api/v1/documents.json?conditions[term]=water&fields[]=title&fields[]=document_number&fields[]=publication_date
```

#### Response (200 OK)

```json
{
  "count": 1500,
  "description": "...",
  "total_pages": 75,
  "next_page_url": "https://www.federalregister.gov/api/v1/documents.json?page=2&...",
  "results": [
    {
      "document_number": "string",
      "title": "string",
      "type": "Rule",
      "abstract": "string",
      "publication_date": "YYYY-MM-DD",
      "effective_date": "YYYY-MM-DD",
      "agencies": [...],
      "docket_ids": ["string"],
      "regulation_id_number_info": {...},
      "html_url": "string",
      "pdf_url": "string",
      "full_text_xml_url": "string",
      "body_html_url": "string",
      "json_url": "string"
    }
  ]
}
```

---

### 4. GET `/documents/facets/{facet}`

Fetch counts of matching Federal Register documents grouped by a facet. Useful for analytics and summaries.

**URL:**
```
GET https://www.federalregister.gov/api/v1/documents/facets/{facet}
```

#### Path Parameters

| Parameter | Type   | Required | Description                                                                                 |
| --------- | ------ | -------- | ------------------------------------------------------------------------------------------- |
| `facet`   | string | Yes      | Grouping dimension (e.g. `daily`, `weekly`, `monthly`, `yearly`, `agency`, `type`, `topic`) |

> NOTE: Using `daily` grouping may require limiting requests to smaller date ranges.

#### Query Parameters

Supports all the same `conditions[]` filters as [GET /documents.{format}](#3-get-documentsformat), **except** pagination and `fields[]`.

#### Example Requests

```bash
# Count documents per agency
GET /api/v1/documents/facets/agency?conditions[term]=water

# Count documents per year
GET /api/v1/documents/facets/yearly?conditions[agencies][]=environmental-protection-agency

# Count documents per type in a date range
GET /api/v1/documents/facets/type?conditions[publication_date][gte]=2023-01-01&conditions[publication_date][lte]=2023-12-31

# Count per day (use narrow date range)
GET /api/v1/documents/facets/daily?conditions[publication_date][gte]=2023-06-01&conditions[publication_date][lte]=2023-06-30
```

#### Response (200 OK)

Returns an object with facet labels as keys and document counts as values.

---

### 5. GET `/issues/{publication_date}.{format}`

Fetch the document table of contents for a specific Federal Register print edition date.

**URL:**
```
GET https://www.federalregister.gov/api/v1/issues/{publication_date}.{format}
```

#### Path Parameters

| Parameter          | Type                  | Required | Description                         |
| ------------------ | --------------------- | -------- | ----------------------------------- |
| `publication_date` | string (`YYYY-MM-DD`) | Yes      | Exact publication date of the issue |
| `format`           | string                | Yes      | Response format: `json`             |

#### Example Request

```bash
GET /api/v1/issues/2023-06-15.json
```

#### Response (200 OK)

Returns the table of contents for that day's Federal Register issue, including document listings grouped by agency and section.

---

## Public Inspection Documents

Public inspection documents are documents that have been filed with the Office of the Federal Register but **not yet published** in the Federal Register. Once published, use the document search endpoints.

---

### 6. GET `/public-inspection-documents/{document_number}.{format}`

Fetch a single public inspection document.

**URL:**
```
GET https://www.federalregister.gov/api/v1/public-inspection-documents/{document_number}.{format}
```

#### Path Parameters

| Parameter         | Type   | Required | Description                      |
| ----------------- | ------ | -------- | -------------------------------- |
| `document_number` | string | Yes      | Federal Register document number |
| `format`          | string | Yes      | Response format: `json`          |

#### Example Request

```bash
GET /api/v1/public-inspection-documents/2023-12345.json
```

#### Response (200 OK)

Returns a single public inspection document object.

---

### 7. GET `/public-inspection-documents/{document_numbers}.{format}`

Fetch multiple public inspection documents in a single request.

**URL:**
```
GET https://www.federalregister.gov/api/v1/public-inspection-documents/{document_numbers}.{format}
```

#### Path Parameters

| Parameter          | Type          | Required | Description                              |
| ------------------ | ------------- | -------- | ---------------------------------------- |
| `document_numbers` | array[string] | Yes      | Comma-separated list of document numbers |
| `format`           | string        | Yes      | Response format: `json`                  |

#### Example Request

```bash
GET /api/v1/public-inspection-documents/2023-12345,2023-67890.json
```

#### Response (200 OK)

Returns an array of public inspection document objects.

---

### 8. GET `/public-inspection-documents/current.{format}`

Fetch all public inspection documents currently on public inspection.

**URL:**
```
GET https://www.federalregister.gov/api/v1/public-inspection-documents/current.{format}
```

#### Path Parameters

| Parameter | Type   | Required | Description             |
| --------- | ------ | -------- | ----------------------- |
| `format`  | string | Yes      | Response format: `json` |

#### Example Request

```bash
GET /api/v1/public-inspection-documents/current.json
```

#### Response (200 OK)

Returns all documents currently available on public inspection, grouped by filing type (regular and special filings).

---

### 9. GET `/public-inspection-documents.{format}`

Search public inspection documents available on a specific inspection date.

> NOTE: Use the [document search endpoint](#3-get-documentsformat) to find documents that have already been published.

**URL:**
```
GET https://www.federalregister.gov/api/v1/public-inspection-documents.{format}
```

#### Path Parameters

| Parameter | Type   | Required | Description             |
| --------- | ------ | -------- | ----------------------- |
| `format`  | string | Yes      | Response format: `json` |

#### Query Parameters

| Parameter                    | Type                  | Required | Description                                                 |
| ---------------------------- | --------------------- | -------- | ----------------------------------------------------------- |
| `conditions[available_on]`   | string (`YYYY-MM-DD`) | Yes      | Public inspection issue date                                |
| `fields[]`                   | array[string]         | —        | Specific attributes to return                               |
| `per_page`                   | integer               | —        | Results per page (max 1000, default 20)                     |
| `page`                       | integer               | —        | Page number                                                 |
| `conditions[term]`           | string                | —        | Full text search                                            |
| `conditions[agencies][]`     | array[string]         | —        | Filter by publishing agency                                 |
| `conditions[type][]`         | array[string]         | —        | Document type: `RULE`, `PRORULE`, `NOTICE`, `PRESDOCU`      |
| `conditions[special_filing]` | string                | —        | Filing type: `"0"` = Regular Filing, `"1"` = Special Filing |
| `conditions[docket_id]`      | string                | —        | Agency docket number                                        |

#### Example Requests

```bash
# Get all public inspection docs for a date
GET /api/v1/public-inspection-documents.json?conditions[available_on]=2024-03-15

# Filter by agency and date
GET /api/v1/public-inspection-documents.json?conditions[available_on]=2024-03-15&conditions[agencies][]=department-of-justice

# Filter special filings only
GET /api/v1/public-inspection-documents.json?conditions[available_on]=2024-03-15&conditions[special_filing]=1

# Full text search on a date
GET /api/v1/public-inspection-documents.json?conditions[available_on]=2024-03-15&conditions[term]=immigration
```

#### Response (200 OK)

Returns paginated list of public inspection documents matching the criteria.

---

## Agencies

---

### 10. GET `/agencies`

Fetch details for all Federal Register agencies.

**URL:**
```
GET https://www.federalregister.gov/api/v1/agencies
```

#### Parameters

None.

#### Example Request

```bash
GET /api/v1/agencies
```

#### Response (200 OK)

Returns an array of all agency objects, each containing:

```json
[
  {
    "id": 0,
    "name": "string",
    "short_name": "string",
    "display_name": "string",
    "slug": "string",
    "url": "string",
    "json_url": "string",
    "parent_id": null,
    "description": "string"
  }
]
```

---

### 11. GET `/agencies/{slug}`

Fetch details for a specific agency.

**URL:**
```
GET https://www.federalregister.gov/api/v1/agencies/{slug}
```

#### Path Parameters

| Parameter | Type   | Required | Description                                                                   |
| --------- | ------ | -------- | ----------------------------------------------------------------------------- |
| `slug`    | string | Yes      | Federal Register slug for the agency (e.g. `environmental-protection-agency`) |

#### Query Parameters

| Parameter | Type    | Description                                       |
| --------- | ------- | ------------------------------------------------- |
| `id`      | integer | *(Deprecated)* Federal Register ID for the agency |

#### Example Request

```bash
GET /api/v1/agencies/environmental-protection-agency
```

#### Response (200 OK)

Returns a single agency object with full details including name, slug, parent agency (if applicable), and related URLs.

---

## Images

---

### 12. GET `/images/{identifier}`

Fetch available image variants and their metadata for a single image used in Federal Register documents.

**URL:**
```
GET https://www.federalregister.gov/api/v1/images/{identifier}
```

#### Path Parameters

| Parameter    | Type   | Required | Description                       |
| ------------ | ------ | -------- | --------------------------------- |
| `identifier` | string | Yes      | Federal Register image identifier |

#### Example Request

```bash
GET /api/v1/images/EP15JN23.000
```

#### Response (200 OK)

Returns image metadata including available size variants and their URLs.

#### Response (404 Not Found)

Returned when the image identifier does not exist.

---

## Suggested Searches

Suggested searches are curated search presets tied to specific sections of FederalRegister.gov.

---

### 13. GET `/suggested_searches`

Fetch all suggested searches, optionally filtered by section.

**URL:**
```
GET https://www.federalregister.gov/api/v1/suggested_searches
```

#### Query Parameters

| Parameter              | Type          | Description                                |
| ---------------------- | ------------- | ------------------------------------------ |
| `conditions[sections]` | array[string] | Filter by FederalRegister.gov section slug |

#### Example Requests

```bash
# All suggested searches
GET /api/v1/suggested_searches

# Filter by section
GET /api/v1/suggested_searches?conditions[sections]=environment
```

#### Response (200 OK)

Returns an array of suggested search objects.

---

### 14. GET `/suggested_searches/{slug}`

Fetch details for a specific suggested search.

**URL:**
```
GET https://www.federalregister.gov/api/v1/suggested_searches/{slug}
```

#### Path Parameters

| Parameter | Type   | Required | Description                                    |
| --------- | ------ | -------- | ---------------------------------------------- |
| `slug`    | string | Yes      | Federal Register slug for the suggested search |

#### Example Request

```bash
GET /api/v1/suggested_searches/clean-water-act
```

#### Response (200 OK)

Returns a single suggested search object with its query parameters and metadata.

---

## Document Types Reference

| Code       | Description           |
| ---------- | --------------------- |
| `RULE`     | Final Rule            |
| `PRORULE`  | Proposed Rule         |
| `NOTICE`   | Notice                |
| `PRESDOCU` | Presidential Document |

---

## Common Fields Reference

These fields are commonly available on Federal Register document objects. Use `fields[]` query parameters to request specific ones.

| Field                       | Description                                                        |
| --------------------------- | ------------------------------------------------------------------ |
| `document_number`           | Unique document identifier                                         |
| `title`                     | Document title                                                     |
| `type`                      | Document type (Rule, Proposed Rule, Notice, Presidential Document) |
| `abstract`                  | Brief summary of the document                                      |
| `publication_date`          | Date published in the Federal Register (`YYYY-MM-DD`)              |
| `effective_date`            | Date the rule or action takes effect (`YYYY-MM-DD`)                |
| `agencies`                  | Array of publishing agency objects                                 |
| `docket_ids`                | Associated agency docket numbers                                   |
| `regulation_id_number_info` | RIN details                                                        |
| `significant`               | Whether deemed significant under EO 12866                          |
| `cfr_references`            | Associated CFR titles and parts                                    |
| `html_url`                  | URL to the human-readable document page                            |
| `pdf_url`                   | URL to the PDF version                                             |
| `full_text_xml_url`         | URL to the full text XML                                           |
| `body_html_url`             | URL to the body HTML                                               |
| `json_url`                  | URL to this document's JSON API response                           |
| `page_views`                | Page view statistics                                               |
| `start_page`                | Starting page in the Federal Register print edition                |
| `end_page`                  | Ending page in the Federal Register print edition                  |
| `topics`                    | Associated CFR indexing topics                                     |
| `sections`                  | FederalRegister.gov sections this document appeared in             |

---

## Public Inspection Document Fields Reference

| Field             | Description                                |
| ----------------- | ------------------------------------------ |
| `document_number` | Unique document identifier                 |
| `title`           | Document title                             |
| `type`            | Document type                              |
| `agencies`        | Publishing agency information              |
| `filing_type`     | `regular` or `special`                     |
| `available_on`    | Public inspection issue date               |
| `docket_ids`      | Associated docket numbers                  |
| `pdf_url`         | URL to the PDF                             |
| `json_url`        | URL to this document's JSON representation |

---

*Documentation based on the FederalRegister.gov Public API — No authentication required.*