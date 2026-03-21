# How to Ingest Dummy Data into OpenSearch Database

This guide explains how to:

- Create and Activate a virtual environment
- Ingest dummy data
- Verify indexed data

Create/Activate Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

1. Run OpenSearch Locally

Follow `docs/OpensearchInstall.md` to start OpenSearch, then continue here.

1. Ingest Dummy Data

```bash
python db/ingest_opensearch.py
```

If successful, the script will print:

```bash
Ingested 5 documents and 6 comments
DEA-2024-0059: 3 docs, 2 comments (term: 'meaningful use')
CMS-2025-0240: 2 docs, 4 comments (terms: 'medicare', 'updates')
```

1. Verify Indexed Data

```bash
curl "http://localhost:9200/documents/_search?pretty"
curl "http://localhost:9200/comments/_search?pretty"
```

If the ingest worked correctly, this command will return JSON containing the
indexed documents.

Stop OpenSearch

```bash
brew services stop opensearch
```
