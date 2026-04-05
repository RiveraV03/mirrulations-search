# Full docket ingest (`db/ingest.py`)

`ingest.py` is the end-to-end pipeline for a single regulations.gov docket: download raw data with **mirrulations-fetch**, load **PostgreSQL** (dockets, documents, comments, Federal Register documents), then index **OpenSearch** (document HTML, comment bodies, extracted attachment text).

For **Federal Register–only** ingest (without the full flow here), see `INGEST_FEDERAL_REGISTER.md` and `ingest_fed_reg_docs_for_docket.py`.

## Prerequisites

1. **Python environment** — From the repo root:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **mirrulations-fetch** — Must be on `PATH` when fetch is not skipped:

   ```bash
   pip install -e /path/to/mirrulations-fetch
   ```

3. **PostgreSQL** — Database reachable with the usual `DATABASE_URL` or `--host` / `--port` / `--dbname` / `--user` / `--password`. Load the schema if needed:

   ```bash
   psql "$DATABASE_URL" -f db/schema-postgres.sql
   ```

   Tables used by this script include `dockets`, `documentsWithFRdoc`, `comments`, and (unless `--skip-federal-register`) `federal_register_documents` and `cfrparts`.

4. **OpenSearch** (optional for DB-only runs) — After Postgres ingest, the script connects via `mirrsearch.db.get_opensearch_connection()` and indexes HTM/HTML and comments. If OpenSearch is unavailable, those steps log a warning and do not fail the run.

5. **SSL / Federal Register API** — HTTPS to `federalregister.gov` uses the standard library plus `certifi` when installed (`pip install certifi`).

## Usage

Run from the **repository root** (so `db/` imports resolve):

```bash
python3 db/ingest.py FAA-2025-0618
```

### Common flags

| Flag | Purpose |
|------|---------|
| `--output-dir DIR` | Where mirrulations-fetch writes `<docket-id>/` (default: current directory). |
| `--skip-fetch` | Use existing `./<output-dir>/<docket-id>/`; do not run `mirrulations-fetch`. |
| `--skip-comments-ingest` | Skip loading comments into Postgres and skip indexing `raw-data/comments/*.json` into OpenSearch. |
| `--skip-federal-register` | Skip FR API fetch and `federal_register_documents` / `cfrparts` upserts. |
| `--dry-run` | Validate and log what would be written; no Postgres writes. |

### PostgreSQL connection

Defaults: `localhost`, port `5432`, database `mirrulations`, user `postgres`. Override with `--host`, `--port`, `--dbname`, `--user`, `--password`.

Optional: set `DATABASE_URL` or use a `.env` file at the repo root (loaded when `python-dotenv` is installed).

### Help

```bash
python3 db/ingest.py --help
```

## What the script does (order of operations)

1. **Fetch** — Runs `mirrulations-fetch <docket_id>` in `--output-dir`, producing `<docket_id>/` unless `--skip-fetch`.
2. **Postgres** — Uses `ingest_docket` to upsert docket metadata, documents, and (unless skipped) comments; then (unless `--skip-federal-register`) collects `frDocNum` from `raw-data/documents/*.json`, fetches each document from the Federal Register API, and upserts `federal_register_documents` and `cfrparts`.
3. **OpenSearch** — Indexes, when possible:
   - `documents` — text from `raw-data/documents/**/*.htm` and `**/*.html`;
   - `comments` — from `raw-data/comments/*.json` (unless comments skipped);
   - `comments_extracted_text` — from derived `extracted_txt` JSON and `*_extracted.txt` files (if any).

## On-disk layout (after fetch)

Typical paths under `<docket-id>/`:

- `raw-data/docket/`, `raw-data/documents/`, `raw-data/comments/`
- `derived-data/.../extracted_txt/` — optional; see `extracted_txt_dir()` in `ingest.py` for resolution order

## Notes

- Re-running ingest for the same docket is intended to be safe (upserts / `ON CONFLICT` in the underlying modules).
- OpenSearch failures are caught and logged; Postgres ingest may still have completed.
- `--dry-run` exercises validation paths without committing Postgres changes.
