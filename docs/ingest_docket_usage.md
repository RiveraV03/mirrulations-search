# Using ingest_docket.py

## Overview

`ingest_docket.py` is a Python script that downloads docket bundles from the mirrulations S3 bucket and ingests regulatory data into PostgreSQL. It processes:

- **Docket metadata** (dockets table)
- **Regulatory documents** (documentsWithFRdoc table)
- **Public comments** (comments table)

The script fetches data from regulations.gov v4 API exports stored in S3 and loads them into your local PostgreSQL database.

## Quick Start

### 1. Interactive Mode (Simplest)

Just run the script with no arguments. You'll be prompted to enter a docket ID:

```bash
python db/ingest_docket.py
```

Output:

```
Docket ingest — enter a regulations.gov docket ID.
Docket ID: FAA-2025-0618
```

The script will:

- Download the docket bundle from S3 to `./FAA-2025-0618/`
- Ingest dockets, documents, and comments into PostgreSQL
- Display a summary of what was loaded

### 2. Download a Specific Docket

```bash
python db/ingest_docket.py --download-s3 FAA-2025-0618
```

This downloads the docket and all its data (documents, comments, derived data) from S3 to `./FAA-2025-0618/`, then ingests it.

### 3. Use Local Docket Directory

If you already have docket data downloaded locally:

```bash
python db/ingest_docket.py --docket-dir ./path/to/docket-folder
```

This ingests data from an existing local directory without downloading from S3.

## Usage Examples

### Workflow 1: Quick Test

```bash
# 1. Setup empty DB
./db/create_empty_db.sh

# 2. Run ingest
python db/ingest_docket.py --download-s3 FAA-2025-0618

# 3. Testing
psql -d mirrulations -c "SELECT * FROM dockets LIMIT 10;"
psql -d mirrulations -c "SELECT * FROM comments LIMIT 10;"
psql -d mirrulations -c "SELECT * FROM comments LIMIT 10;"

```

### Workflow 2: Just Get Documents (Fast)

```bash
# Download and ingest documents only, no comments
python db/ingest_docket.py \
  --download-s3 FAA-2025-0618 \
  --skip-comments-download \
  --skip-comments-ingest
```

### Workflow 3: Update Existing Data

```bash
# Re-ingest a docket (upserts existing records)
python db/ingest_docket.py --download-s3 FAA-2025-0618
```

## Command-Line Options

### Ingest Options

**`--docket-dir PATH`**

- Path to a local folder containing `raw-data/docket/` and `raw-data/documents/`
- Use when data is already downloaded locally
- Ignored if `--download-s3` is specified

**`--skip-comments-ingest`**

- Ingest dockets and documents but skip the comments table
- Faster if you don't need comment data

**`--dry-run`**

- Parse JSON and validate data without writing to the database
- Useful for testing before committing
- Shows what would be ingested

### S3 Download Options

**`--download-s3 DOCKET_ID`**

- Download a specific docket from S3 before ingesting
- Docket ID examples: `FAA-2025-0618`, `CMS-2025-0240`
- Normalized automatically (case-insensitive)

**`--output-folder PATH`** (default: `.`)

- Parent directory where S3 downloads are saved
- Downloads go to `<output-folder>/<DOCKET-ID>/`

**`--skip-comments-download`**

- Download docket and documents only; skip comments and derived data
- Faster and uses less disk space
- Useful if you only need documents, not comments

**`--include-binary`**

- Also download binary files from S3 (PDFs, etc.)
- Only works with `--download-s3`
- Downloads to `<output-folder>/<DOCKET-ID>/raw-data/`

**`--download-only`**

- Download from S3 but skip database ingestion
- Useful for just fetching the data without writing to the database

### After downloading from S3, the data is organized as:

```
<output-folder>/<DOCKET-ID>/
├── raw-data/
│   ├── docket/
│   │   └── *.json          # Docket metadata
│   ├── documents/
│   │   └── *.json          # Document metadata + FR references
│   ├── comments/
│   │   └── *.json          # Public comments (if downloaded)
│   └── binary-<DOCKET-ID>/ # Binary files (if --include-binary)
│       └── *.*
└── derived-data/           # Pre-computed data (if available)
    └── *.json
```

## Output and Logging

The script logs to stdout with timestamps and log levels:

```
2025-03-29 10:15:23 [INFO] Connecting to PostgreSQL at localhost:5432/mirrulations …
2025-03-29 10:15:23 [INFO] Found 1 docket JSON file(s).
2025-03-29 10:15:23 [INFO] Upserted docket FAA-2025-0618
2025-03-29 10:15:24 [INFO] Upserted 125 document(s).
2025-03-29 10:15:45 [INFO] Upserted 3456 comment(s).
2025-03-29 10:15:45 [INFO] --- Summary ---
2025-03-29 10:15:45 [INFO] Docket: FAA-2025-0618
2025-03-29 10:15:45 [INFO] Title: Establishment of Enhanced Airworthiness Standards
2025-03-29 10:15:45 [INFO] In database: 125 document(s), 3456 comment(s) for this docket_id
```

## See Also

- [PostgreSQL Setup](PostgresInstall.md)
- [Database Schema](PostgresDB.md)
- [Federal Register API](federal_register_api.md)
- [OpensearchInfo](OpensearchInfo.md) for search integration
