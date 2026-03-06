# Regulations.gov API v4 — Full Documentation

> **Base URL:** `https://api.regulations.gov/v4`  
> **Staging URL:** `https://api-staging.regulations.gov/v4`  
> **Spec:** [OpenAPI Specification File](https://api.regulations.gov/v4/openapi)

---

## Table of Contents

1. [Overview](#overview)
2. [Authentication](#authentication)
3. [Rate Limits](#rate-limits)
4. [Endpoints](#endpoints)
   - [GET /documents](#1-get-documents)
   - [GET /documents/{documentId}](#2-get-documentsdocumentid)
   - [GET /comments](#3-get-comments)
   - [GET /comments/{commentId}](#4-get-commentscommentid)
   - [GET /dockets](#5-get-dockets)
   - [GET /dockets/{docketId}](#6-get-docketsdocketid)
5. [Pagination](#pagination)
6. [Retrieving Large Comment Sets](#retrieving-large-comment-sets)
7. [Data Limitations](#data-limitations)
8. [Error Codes](#error-codes)
9. [FAQ](#faq)

---

## Overview

When Congress passes laws, federal agencies implement them through regulations. [Regulations.gov](https://www.regulations.gov) is where users can find and comment on regulations. This API allows developers to search and retrieve regulatory documents, comments, and dockets programmatically.

There are three core resource types:

| Resource     | Description                                                                            |
| ------------ | -------------------------------------------------------------------------------------- |
| **Document** | A regulatory filing. Types include Proposed Rule, Rule, Supporting & Related, or Other |
| **Comment**  | Public feedback submitted in response to a document                                    |
| **Docket**   | An organizational folder grouping multiple related documents                           |

---

## Authentication

All requests require an API key passed as an HTTP header.

```
X-Api-Key: YOUR_API_KEY
```

Register for a key at: [https://api.data.gov/signup](https://api.data.gov/signup)

> **Note:** `DEMO_KEY` is available for exploration only. Do **not** use it in production.

---

## Rate Limits

| Key Type           | Limit                                                                       |
| ------------------ | --------------------------------------------------------------------------- |
| GET API (standard) | See [api.data.gov/docs/rate-limits](https://api.data.gov/docs/rate-limits/) |
| Commenting API     | 50 requests/minute, 500 requests/hour                                       |

Rate limit status is returned in response headers. Contact GSA to request a rate limit increase (reviewed case-by-case).

---

## Endpoints

---

### 1. GET `/documents`

Search for a list of documents.

**URL:**
```
GET https://api.regulations.gov/v4/documents
```

#### Query Parameters

| Parameter                     | Type                             | Description                                                                                                                                                     |
| ----------------------------- | -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `filter[agencyId]`            | string                           | Filter by agency acronym (e.g. `EPA`, `FDA`)                                                                                                                    |
| `filter[commentEndDate]`      | date (`yyyy-MM-dd`)              | Filter by comment end date. Supports `[ge]` (≥) and `[le]` (≤) modifiers                                                                                        |
| `filter[docketId]`            | string                           | Filter by docket ID                                                                                                                                             |
| `filter[documentType]`        | string                           | One of: `Notice`, `Rule`, `Proposed Rule`, `Supporting & Related Material`, `Other`                                                                             |
| `filter[frDocNum]`            | string                           | Filter by Federal Register document number                                                                                                                      |
| `filter[searchTerm]`          | string                           | Full-text keyword search                                                                                                                                        |
| `filter[postedDate]`          | date (`yyyy-MM-dd`)              | Filter by posted date. Supports `[ge]` and `[le]` modifiers                                                                                                     |
| `filter[lastModifiedDate]`    | datetime (`yyyy-MM-dd HH:mm:ss`) | Filter by last modified date. Supports `[ge]` and `[le]` modifiers                                                                                              |
| `filter[subtype]`             | string                           | Filter by document subtype                                                                                                                                      |
| `filter[withinCommentPeriod]` | boolean                          | `true` returns only documents currently open for comment                                                                                                        |
| `sort`                        | string                           | Sort field. Options: `commentEndDate`, `postedDate`, `lastModifiedDate`, `documentId`, `title`. Prepend `-` for descending. Comma-separate for multi-field sort |
| `page[number]`                | integer (1–20)                   | Page number                                                                                                                                                     |
| `page[size]`                  | integer (5–250)                  | Results per page                                                                                                                                                |

#### Example Requests

```bash
# Keyword search
GET /v4/documents?filter[searchTerm]=water&api_key=DEMO_KEY

# Filter by exact posted date
GET /v4/documents?filter[postedDate]=2020-09-01&api_key=DEMO_KEY

# Filter by date range
GET /v4/documents?filter[postedDate][ge]=2020-09-01&filter[postedDate][le]=2020-09-30&api_key=DEMO_KEY

# Filter by docket
GET /v4/documents?filter[docketId]=FAA-2018-1084&api_key=DEMO_KEY

# Only open for comment
GET /v4/documents?filter[withinCommentPeriod]=true&api_key=DEMO_KEY

# Sort descending by posted date
GET /v4/documents?sort=-postedDate&api_key=DEMO_KEY

# Sort ascending by posted date
GET /v4/documents?sort=postedDate&api_key=DEMO_KEY
```

#### Response (200 OK)

```json
{
  "data": [
    {
      "id": "string",
      "type": "string",
      "attributes": {
        "agencyId": "string",
        "commentEndDate": "2024-01-01T00:00:00Z",
        "commentStartDate": "2024-01-01T00:00:00Z",
        "docketId": "string",
        "documentType": "Notice",
        "frDocNum": "string",
        "highlightedContent": "string",
        "lastModifiedDate": "2024-01-01T00:00:00Z",
        "objectId": "string",
        "openForComment": true,
        "postedDate": "string",
        "subtype": "string",
        "title": "string",
        "withdrawn": false
      },
      "links": [{ "self": "string" }]
    }
  ],
  "meta": {
    "hasNextPage": true,
    "hasPreviousPage": false,
    "numberOfElements": 20,
    "pageNumber": 1,
    "pageSize": 20,
    "totalElements": 150,
    "totalPages": 8,
    "firstPage": true,
    "lastPage": false
  }
}
```

---

### 2. GET `/documents/{documentId}`

Get detailed information for a single document.

**URL:**
```
GET https://api.regulations.gov/v4/documents/{documentId}
```

#### Path Parameters

| Parameter    | Type   | Required | Description                                   |
| ------------ | ------ | -------- | --------------------------------------------- |
| `documentId` | string | Required | The document ID (e.g. `FDA-2009-N-0501-0012`) |

#### Query Parameters

| Parameter | Type   | Description                                                   |
| --------- | ------ | ------------------------------------------------------------- |
| `include` | string | Pass `attachments` to include attachment data in the response |

#### Example Requests

```bash
# Without attachments
GET /v4/documents/FDA-2009-N-0501-0012?api_key=DEMO_KEY

# With attachments
GET /v4/documents/FDA-2009-N-0501-0012?include=attachments&api_key=DEMO_KEY
```

#### Response (200 OK)

```json
{
  "id": "string",
  "type": "string",
  "attributes": {
    "agencyId": "string",
    "category": "string",
    "comment": "string",
    "docAbstract": "string",
    "docketId": "string",
    "documentType": "Proposed Rule",
    "field1": "string",
    "field2": "string",
    "fileFormats": [
      {
        "fileUrl": "string",
        "format": "pdf",
        "size": 204800
      }
    ],
    "firstName": "string",
    "lastName": "string",
    "organization": "string",
    "govAgency": "string",
    "govAgencyType": "string",
    "legacyId": "string",
    "modifyDate": "2024-01-01T00:00:00Z",
    "objectId": "string",
    "openForComment": true,
    "pageCount": "12",
    "postedDate": "2024-01-01T00:00:00Z",
    "postmarkDate": "2024-01-01T00:00:00Z",
    "receiveDate": "2024-01-01T00:00:00Z",
    "restrictReason": "string",
    "restrictReasonType": "string",
    "reasonWithdrawn": "string",
    "stateProvinceRegion": "string",
    "subtype": "string",
    "title": "string",
    "trackingNbr": "string",
    "withdrawn": false,
    "zip": "string",
    "additionalRins": ["string"],
    "allowLateComments": false,
    "authorDate": "2024-01-01T00:00:00Z",
    "authors": ["string"],
    "cfrPart": "string",
    "commentEndDate": "2024-01-01T00:00:00Z",
    "commentStartDate": "2024-01-01T00:00:00Z",
    "effectiveDate": "2024-01-01T00:00:00Z",
    "frDocNum": "string",
    "frVolNum": "string",
    "implementationDate": "2024-01-01T00:00:00Z",
    "media": "string",
    "ombApproval": "string",
    "sourceCitation": "string",
    "startEndPage": "string",
    "subject": "string",
    "topics": ["string"]
  },
  "relationships": {
    "data": [{ "type": "string", "id": "string" }],
    "links": { "self": "string", "related": "string" }
  },
  "links": [{ "self": "string" }],
  "included": [
    {
      "id": "string",
      "type": "string",
      "attributes": {
        "agencyNote": "string",
        "authors": ["string"],
        "docAbstract": "string",
        "docOrder": 1,
        "fileFormats": [{ "fileUrl": "string", "format": "pdf", "size": 204800 }],
        "modifyDate": "2024-01-01T00:00:00Z",
        "publication": "string",
        "restrictReason": "string",
        "restrictReasonType": "string",
        "title": "string"
      },
      "links": [{ "self": "string" }]
    }
  ]
}
```

---

### 3. GET `/comments`

Search for a list of comments.

**URL:**
```
GET https://api.regulations.gov/v4/comments
```

#### Query Parameters

| Parameter                  | Type                             | Description                                                                                                                     |
| -------------------------- | -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `filter[agencyId]`         | string                           | Filter by agency acronym (e.g. `EPA`)                                                                                           |
| `filter[searchTerm]`       | string                           | Full-text keyword search                                                                                                        |
| `filter[postedDate]`       | date (`yyyy-MM-dd`)              | Filter by posted date. Supports `[ge]` and `[le]` modifiers                                                                     |
| `filter[lastModifiedDate]` | datetime (`yyyy-MM-dd HH:mm:ss`) | Filter by last modified date. Supports `[ge]` and `[le]` modifiers                                                              |
| `filter[commentOnId]`      | string                           | Filter by the `objectId` of the document being commented on                                                                     |
| `sort`                     | string                           | Sort field. Options: `postedDate`, `lastModifiedDate`, `documentId`. Prepend `-` for descending. Comma-separate for multi-field |
| `page[number]`             | integer (1–20)                   | Page number                                                                                                                     |
| `page[size]`               | integer (5–250)                  | Results per page                                                                                                                |

#### Example Requests

```bash
# Keyword search
GET /v4/comments?filter[searchTerm]=water&api_key=DEMO_KEY

# Filter by date
GET /v4/comments?filter[postedDate]=2020-09-01&api_key=DEMO_KEY

# Filter by date range
GET /v4/comments?filter[postedDate][ge]=2020-09-01&filter[postedDate][le]=2020-09-30&api_key=DEMO_KEY

# Get comments for a specific document (using objectId)
GET /v4/comments?filter[commentOnId]=0900006483a6cba3&api_key=DEMO_KEY

# Sort descending
GET /v4/comments?sort=-postedDate&api_key=DEMO_KEY

# Paginate with multi-field sort (needed for large sets)
GET /v4/comments?filter[commentOnId]=09000064846eebaf&page[size]=250&page[number]=1&sort=lastModifiedDate,documentId&api_key=DEMO_KEY
```

#### Response (200 OK)

```json
{
  "data": [
    {
      "id": "string",
      "type": "string",
      "attributes": {
        "agencyId": "string",
        "documentType": "Notice",
        "highlightedContent": "string",
        "lastModifiedDate": "2024-01-01T00:00:00Z",
        "objectId": "string",
        "postedDate": "string",
        "title": "string",
        "withdrawn": false
      },
      "links": [{ "self": "string" }]
    }
  ],
  "meta": {
    "hasNextPage": true,
    "hasPreviousPage": false,
    "numberOfElements": 20,
    "pageNumber": 1,
    "pageSize": 20,
    "totalElements": 88061,
    "totalPages": 20,
    "firstPage": true,
    "lastPage": false
  }
}
```

---

### 4. GET `/comments/{commentId}`

Get detailed information for a single comment.

**URL:**
```
GET https://api.regulations.gov/v4/comments/{commentId}
```

#### Path Parameters

| Parameter   | Type   | Required | Description                                    |
| ----------- | ------ | -------- | ---------------------------------------------- |
| `commentId` | string | Required | The comment ID (e.g. `HHS-OCR-2018-0002-5313`) |

#### Query Parameters

| Parameter | Type   | Description                                   |
| --------- | ------ | --------------------------------------------- |
| `include` | string | Pass `attachments` to include attachment data |

#### Example Requests

```bash
# Without attachments
GET /v4/comments/HHS-OCR-2018-0002-5313?api_key=DEMO_KEY

# With attachments
GET /v4/comments/HHS-OCR-2018-0002-5313?include=attachments&api_key=DEMO_KEY
```

#### Response (200 OK)

```json
{
  "id": "string",
  "type": "string",
  "attributes": {
    "agencyId": "string",
    "category": "string",
    "city": "string",
    "comment": "string",
    "country": "string",
    "docAbstract": "string",
    "docketId": "string",
    "documentType": "Notice",
    "duplicateComments": 0,
    "field1": "string",
    "field2": "string",
    "fileFormats": [
      { "fileUrl": "string", "format": "pdf", "size": 204800 }
    ],
    "firstName": "string",
    "govAgency": "string",
    "govAgencyType": "string",
    "lastName": "string",
    "legacyId": "string",
    "modifyDate": "2024-01-01T00:00:00Z",
    "objectId": "string",
    "openForComment": false,
    "organization": "string",
    "originalDocumentId": "string",
    "pageCount": "2",
    "postedDate": "2024-01-01T00:00:00Z",
    "postmarkDate": "2024-01-01T00:00:00Z",
    "reasonWithdrawn": "string",
    "receiveDate": "2024-01-01T00:00:00Z",
    "restrictReason": "string",
    "restrictReasonType": "string",
    "stateProvinceRegion": "string",
    "subtype": "string",
    "title": "string",
    "trackingNbr": "string",
    "withdrawn": false,
    "zip": "string",
    "commentOnDocumentId": "string"
  },
  "relationships": [
    {
      "data": [{ "type": "string", "id": "string" }],
      "links": { "self": "string", "related": "string" }
    }
  ],
  "links": [{ "self": "string" }]
}
```

---

### 5. GET `/dockets`

Search for a list of dockets.

**URL:**
```
GET https://api.regulations.gov/v4/dockets
```

#### Query Parameters

| Parameter                  | Type                             | Description                                                                              |
| -------------------------- | -------------------------------- | ---------------------------------------------------------------------------------------- |
| `filter[agencyId]`         | string                           | Filter by agency acronym. Supports comma-separated multiple agencies (e.g. `GSA,EPA`)    |
| `filter[searchTerm]`       | string                           | Full-text keyword search                                                                 |
| `filter[lastModifiedDate]` | datetime (`yyyy-MM-dd HH:mm:ss`) | Filter by last modified date. Supports `[ge]` and `[le]` modifiers                       |
| `filter[docketType]`       | string                           | One of: `Rulemaking`, `Nonrulemaking`                                                    |
| `sort`                     | string                           | Sort field. Options: `title`, `docketId`, `lastModifiedDate`. Prepend `-` for descending |
| `page[number]`             | integer (1–20)                   | Page number                                                                              |
| `page[size]`               | integer (5–250)                  | Results per page                                                                         |

#### Example Requests

```bash
# Keyword search
GET /v4/dockets?filter[searchTerm]=water&api_key=DEMO_KEY

# Search by docket ID
GET /v4/dockets?filter[searchTerm]=EPA-HQ-OAR-2003-0129&api_key=DEMO_KEY

# Filter by multiple agencies
GET /v4/dockets?filter[agencyId]=GSA,EPA&api_key=DEMO_KEY

# Sort by title ascending
GET /v4/dockets?sort=title&api_key=DEMO_KEY

# Sort by title descending
GET /v4/dockets?sort=-title&api_key=DEMO_KEY
```

#### Response (200 OK)

```json
{
  "data": [
    {
      "id": "string",
      "type": "string",
      "attributes": {
        "agencyId": "string",
        "docketType": "Rulemaking",
        "highlightedContent": "string",
        "lastModifiedDate": "2024-01-01T00:00:00Z",
        "objectId": "string",
        "title": "string"
      },
      "links": [{ "self": "string" }]
    }
  ],
  "meta": {
    "hasNextPage": true,
    "hasPreviousPage": false,
    "numberOfElements": 20,
    "pageNumber": 1,
    "pageSize": 20,
    "totalElements": 500,
    "totalPages": 20,
    "firstPage": true,
    "lastPage": false
  }
}
```

---

### 6. GET `/dockets/{docketId}`

Get detailed information for a single docket.

**URL:**
```
GET https://api.regulations.gov/v4/dockets/{docketId}
```

#### Path Parameters

| Parameter  | Type   | Required | Description                                 |
| ---------- | ------ | -------- | ------------------------------------------- |
| `docketId` | string | Required | The docket ID (e.g. `EPA-HQ-OAR-2003-0129`) |

#### Example Request

```bash
GET /v4/dockets/EPA-HQ-OAR-2003-0129?api_key=DEMO_KEY
```

#### Response (200 OK)

```json
{
  "id": "string",
  "type": "string",
  "attributes": {
    "agencyId": "string",
    "category": "string",
    "dkAbstract": "string",
    "docketType": "Rulemaking",
    "effectiveDate": "2024-01-01T00:00:00Z",
    "field1": "string",
    "field2": "string",
    "generic": "string",
    "keywords": ["string"],
    "legacyId": "string",
    "modifyDate": "2024-01-01T00:00:00Z",
    "objectId": "string",
    "organization": "string",
    "petitionNbr": "string",
    "program": "string",
    "rin": "string",
    "shortTitle": "string",
    "subType": "string",
    "subType2": "string",
    "title": "string"
  },
  "links": [{ "self": "string" }]
}
```

---

## Pagination

| Rule                  | Value |
| --------------------- | ----- |
| Max page number       | 20    |
| Max page size         | 250   |
| Max records per query | 5,000 |

For result sets larger than 5,000 records, use date-based pagination (see below).

---

## Retrieving Large Comment Sets

### Under 5,000 comments

**Step 1:** Get all documents for a docket to obtain their `objectId` values:
```
GET /v4/documents?filter[docketId]=FAA-2018-1084&api_key=DEMO_KEY
```

**Step 2:** Get comments for each document using `objectId`:
```
GET /v4/comments?filter[commentOnId]=0900006483a6cba3&api_key=DEMO_KEY
```

---

### Over 5,000 comments

**Step 1:** Get documents for the docket:
```
GET /v4/documents?filter[docketId]=EOIR-2020-0003&api_key=DEMO_KEY
```
Note the `objectId` of the Proposed Rule document.

**Step 2:** Get comments using `objectId`:
```
GET /v4/comments?filter[commentOnId]=09000064846eebaf&api_key=DEMO_KEY
```
Check `meta.totalElements` to confirm total count.

**Step 3:** Page through first 5,000 (pages 1–20, sorted by `lastModifiedDate`):
```
GET /v4/comments?filter[commentOnId]=09000064846eebaf&page[size]=250&page[number]=N&sort=lastModifiedDate,documentId&api_key=DEMO_KEY
```
On the last page (page 20), note the `lastModifiedDate` of the final record.

**Step 4:** Use that date to fetch the next 5,000 (repeat as needed):
```
GET /v4/comments?filter[commentOnId]=09000064846eebaf&filter[lastModifiedDate][ge]=2020-08-10 11:58:52&page[size]=250&page[number]=N&sort=lastModifiedDate,documentId&api_key=DEMO_KEY
```

> **Note:** Use `[ge]` (≥) not `[gt]` (>) to avoid missing records that share the same `lastModifiedDate`.  
> **Note:** The `lastModifiedDate` filter value uses **Eastern Time**.

---

## Data Limitations

### Fields always publicly visible on a comment

- `agencyId`
- `comment`
- `commentOnId`
- `docketId`
- `documentId`
- `documentType`
- `postedDate`
- `receiveDate`
- `restrictReason` *(if restrictReasonType = "Other")*
- `restrictReasonType` *(if document is restricted)*
- `reasonWithdrawn` *(if withdrawn)*
- `title`
- `trackingNbr`
- `withdrawn`

### Agency-configurable fields *(may appear or disappear at any time)*

- `city`
- `country`
- `docAbstract`
- `firstName`
- `govAgency`
- `govAgencyType`
- `lastName`
- `legacyId`
- `organization`
- `pageCount`
- `postmarkDate`
- `stateProvinceRegion`
- `subtype`
- `zip`

### Fields never publicly visible

- `originalDocumentId`
- `address1`
- `address2`
- `email`
- `phone`
- `fax`

---

## Error Codes

All error responses follow this structure:

```json
{
  "errors": [
    {
      "status": 400,
      "title": "string",
      "detail": "string"
    }
  ]
}
```

| Code  | Meaning                                              |
| ----- | ---------------------------------------------------- |
| `400` | Validation error — bad or malformed query parameters |
| `403` | Missing or invalid API key                           |
| `404` | Resource not found                                   |

---

## FAQ

**Q: I'm not seeing all fields from the v3 API in v4. Where did they go?**  
A: v4 splits what was previously one search endpoint into three separate endpoints (documents, comments, dockets). Some fields (like `rin`) moved to the details endpoint — e.g. retrieve `rin` via `/dockets/{docketId}`.

**Q: How do I check if a document is active or withdrawn?**  
A: The `/v4/documents` endpoint includes a `withdrawn` boolean field. If `true`, the document is withdrawn.

**Q: What is the `DEMO_KEY`?**  
A: A shared key for testing and exploring the API only. It has very low rate limits and should never be used in production applications.

**Q: What is the staging environment?**  
A: `https://api-staging.regulations.gov` — use this for testing before deploying to production.

**Q: How do I know when I'm about to hit the rate limit?**  
A: Check the rate-limit headers returned with each response. See [api.data.gov/docs/rate-limits](https://api.data.gov/docs/rate-limits/) for full details.

**Q: Can I get a rate limit increase?**  
A: Yes. GSA may grant increases for GET API keys on a case-by-case basis. Submit a request with justification.

---

## Contact

Questions or feedback? Contact the [Regulations.gov Help Desk](https://www.regulations.gov/support).

---

*Documentation based on the Regulations.gov Public API v4.0 — OAS 3.0*
