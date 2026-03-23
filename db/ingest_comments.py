#!/usr/bin/env python3
"""
Ingest comment JSON files into the mirrulations PostgreSQL database.

Usage:
    python ingest_comments.py [--base-dir BASE_DIR] [--host HOST] [--port PORT]
                              [--dbname DBNAME] [--user USER] [--password PASSWORD]
                              [--dry-run]

Directory structure expected:
    <base-dir>/<DOCKET-ID>/raw-data/comments/<DOCKET-ID>-XXXX.json

Example:
    python ingest_comments.py --base-dir . --host localhost --dbname mirrulations
"""

import argparse
import glob
import json
import logging
import os
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Mapping helpers
# ---------------------------------------------------------------------------

def extract_comment(data: dict) -> dict:
    """Extract and map fields from a regulations.gov comment JSON to DB columns."""
    attrs = data.get("attributes", {})
    links = data.get("links", {})

    return {
        "comment_id":               data.get("id"),
        "api_link":                 links.get("self"),
        "document_id":              attrs.get("commentOnDocumentId"),
        "duplicate_comment_count":  attrs.get("duplicateComments", 0) or 0,
        "address1":                 attrs.get("address1"),
        "address2":                 attrs.get("address2"),
        "agency_id":                attrs.get("agencyId"),
        "city":                     attrs.get("city"),
        "comment_category":         attrs.get("category"),
        "comment":                  attrs.get("comment"),
        "country":                  attrs.get("country"),
        "docket_id":                attrs.get("docketId"),
        "document_type":            attrs.get("documentType"),
        "email":                    attrs.get("email"),
        "fax":                      attrs.get("fax"),
        "flex_field1":              attrs.get("field1"),
        "flex_field2":              attrs.get("field2"),
        "first_name":               attrs.get("firstName"),
        "submitter_gov_agency":     attrs.get("govAgency"),
        "submitter_gov_agency_type":attrs.get("govAgencyType"),
        "last_name":                attrs.get("lastName"),
        "modification_date":        attrs.get("modifyDate"),
        "submitter_org":            attrs.get("organization"),
        "phone":                    attrs.get("phone"),
        "posted_date":              attrs.get("postedDate"),
        "postmark_date":            attrs.get("postmarkDate"),
        "reason_withdrawn":         attrs.get("reasonWithdrawn"),
        "received_date":            attrs.get("receiveDate"),
        "restriction_reason":       attrs.get("restrictReason"),
        "restriction_reason_type":  attrs.get("restrictReasonType"),
        "state_province_region":    attrs.get("stateProvinceRegion"),
        "comment_subtype":          attrs.get("subtype"),
        "comment_title":            attrs.get("title"),
        "is_withdrawn":             attrs.get("withdrawn", False) or False,
        "postal_code":              attrs.get("zip"),
    }


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

UPSERT_SQL = """
INSERT INTO comments (
    comment_id, api_link, document_id, duplicate_comment_count,
    address1, address2, agency_id, city, comment_category, comment,
    country, docket_id, document_type, email, fax,
    flex_field1, flex_field2, first_name,
    submitter_gov_agency, submitter_gov_agency_type,
    last_name, modification_date, submitter_org, phone,
    posted_date, postmark_date, reason_withdrawn, received_date,
    restriction_reason, restriction_reason_type,
    state_province_region, comment_subtype, comment_title,
    is_withdrawn, postal_code
)
VALUES %s
ON CONFLICT (comment_id) DO UPDATE SET
    api_link                  = EXCLUDED.api_link,
    document_id               = EXCLUDED.document_id,
    duplicate_comment_count   = EXCLUDED.duplicate_comment_count,
    address1                  = EXCLUDED.address1,
    address2                  = EXCLUDED.address2,
    agency_id                 = EXCLUDED.agency_id,
    city                      = EXCLUDED.city,
    comment_category          = EXCLUDED.comment_category,
    comment                   = EXCLUDED.comment,
    country                   = EXCLUDED.country,
    docket_id                 = EXCLUDED.docket_id,
    document_type             = EXCLUDED.document_type,
    email                     = EXCLUDED.email,
    fax                       = EXCLUDED.fax,
    flex_field1               = EXCLUDED.flex_field1,
    flex_field2               = EXCLUDED.flex_field2,
    first_name                = EXCLUDED.first_name,
    submitter_gov_agency      = EXCLUDED.submitter_gov_agency,
    submitter_gov_agency_type = EXCLUDED.submitter_gov_agency_type,
    last_name                 = EXCLUDED.last_name,
    modification_date         = EXCLUDED.modification_date,
    submitter_org             = EXCLUDED.submitter_org,
    phone                     = EXCLUDED.phone,
    posted_date               = EXCLUDED.posted_date,
    postmark_date             = EXCLUDED.postmark_date,
    reason_withdrawn          = EXCLUDED.reason_withdrawn,
    received_date             = EXCLUDED.received_date,
    restriction_reason        = EXCLUDED.restriction_reason,
    restriction_reason_type   = EXCLUDED.restriction_reason_type,
    state_province_region     = EXCLUDED.state_province_region,
    comment_subtype           = EXCLUDED.comment_subtype,
    comment_title             = EXCLUDED.comment_title,
    is_withdrawn              = EXCLUDED.is_withdrawn,
    postal_code               = EXCLUDED.postal_code
;
"""

COLUMN_ORDER = [
    "comment_id", "api_link", "document_id", "duplicate_comment_count",
    "address1", "address2", "agency_id", "city", "comment_category", "comment",
    "country", "docket_id", "document_type", "email", "fax",
    "flex_field1", "flex_field2", "first_name",
    "submitter_gov_agency", "submitter_gov_agency_type",
    "last_name", "modification_date", "submitter_org", "phone",
    "posted_date", "postmark_date", "reason_withdrawn", "received_date",
    "restriction_reason", "restriction_reason_type",
    "state_province_region", "comment_subtype", "comment_title",
    "is_withdrawn", "postal_code",
]


def row_tuple(record: dict) -> tuple:
    return tuple(record[col] for col in COLUMN_ORDER)


def fetch_valid_document_ids(conn) -> set[str]:
    """Return the set of all document_ids currently in the documents table."""
    with conn.cursor() as cur:
        cur.execute("SELECT document_id FROM documents;")
        return {row[0] for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def prompt_docket_dir() -> Path:
    """Interactively ask the user for the docket directory and validate it."""
    while True:
        raw = input("Enter path to docket directory (e.g. ./CMS-2025-0240): ").strip()
        if not raw:
            print("Path cannot be empty. Please try again.")
            continue
        docket_dir = Path(raw).expanduser().resolve()
        comments_dir = docket_dir / "raw-data" / "comments"
        if not docket_dir.is_dir():
            print(f"  Directory not found: {docket_dir}")
            print("  Please enter a valid path.")
            continue
        if not comments_dir.is_dir():
            print(f"  Expected comments folder not found: {comments_dir}")
            print("  Make sure the docket has a raw-data/comments/ subdirectory.")
            continue
        return docket_dir


def find_json_files(docket_dir: Path) -> list[Path]:
    """Find all comment JSON files under <docket_dir>/raw-data/comments/."""
    pattern = str(docket_dir / "raw-data" / "comments" / "*.json")
    files = [Path(p) for p in glob.glob(pattern)]
    log.info("Found %d JSON file(s) in %s", len(files), docket_dir)
    return files


def load_json(path: Path) -> dict | None:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
        return payload.get("data") if isinstance(payload, dict) else None
    except (json.JSONDecodeError, OSError) as exc:
        log.warning("Skipping %s — could not read/parse: %s", path, exc)
        return None


def ingest(files: list[Path], conn, dry_run: bool = False) -> tuple[int, int]:
    """Ingest all files; returns (inserted_or_updated, skipped)."""
    batch: list[tuple] = []
    skipped = 0
    nulled_doc_ids = 0
    BATCH_SIZE = 500

    # Load all known document_ids up front so we can null out missing FK refs
    # rather than letting the insert fail with a FK constraint violation.
    valid_doc_ids: set[str] = set()
    if not dry_run:
        valid_doc_ids = fetch_valid_document_ids(conn)
        log.info("Loaded %d known document_id(s) from documents table.", len(valid_doc_ids))

    def flush(batch):
        if dry_run:
            log.info("[DRY RUN] Would upsert %d row(s).", len(batch))
        else:
            with conn.cursor() as cur:
                execute_values(cur, UPSERT_SQL, batch)
            conn.commit()
            log.info("Upserted %d row(s).", len(batch))

    for path in files:
        data = load_json(path)
        if data is None:
            skipped += 1
            continue

        record = extract_comment(data)

        # Basic validation: required NOT NULL columns
        missing = [c for c in ("comment_id", "api_link", "agency_id", "document_type", "posted_date")
                   if not record.get(c)]
        if missing:
            log.warning("Skipping %s — missing required fields: %s", path.name, missing)
            skipped += 1
            continue

        # Null out document_id if it doesn't exist in the documents table
        # to avoid FK constraint violations (e.g. cross-docket references).
        doc_id = record.get("document_id")
        if doc_id and not dry_run and doc_id not in valid_doc_ids:
            log.warning(
                "%s: document_id '%s' not found in documents table — setting to NULL.",
                path.name, doc_id,
            )
            record["document_id"] = None
            nulled_doc_ids += 1

        batch.append(row_tuple(record))

        if len(batch) >= BATCH_SIZE:
            flush(batch)
            batch.clear()

    if batch:
        flush(batch)

    if nulled_doc_ids:
        log.info(
            "%d comment(s) had their document_id set to NULL due to missing FK references.",
            nulled_doc_ids,
        )

    processed = len(files) - skipped
    return processed, skipped


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Ingest comment JSONs into mirrulations DB.")
    p.add_argument("--host",     default="localhost")
    p.add_argument("--port",     type=int, default=5432)
    p.add_argument("--dbname",   default="mirrulations")
    p.add_argument("--user",     default=os.getenv("PGUSER", os.getenv("USER", "postgres")))
    p.add_argument("--password", default=os.getenv("PGPASSWORD", ""))
    p.add_argument("--dry-run",  action="store_true",
                   help="Parse files and validate but do not write to the DB.")
    return p.parse_args()


def main():
    args = parse_args()

    docket_dir = prompt_docket_dir()
    files = find_json_files(docket_dir)
    if not files:
        log.error("No JSON files found in %s/raw-data/comments/", docket_dir)
        sys.exit(1)

    if args.dry_run:
        log.info("DRY RUN mode — no database writes.")
        processed, skipped = ingest(files, conn=None, dry_run=True)
    else:
        log.info("Connecting to PostgreSQL at %s:%d/%s …", args.host, args.port, args.dbname)
        try:
            conn = psycopg2.connect(
                host=args.host,
                port=args.port,
                dbname=args.dbname,
                user=args.user,
                password=args.password,
            )
        except psycopg2.OperationalError as exc:
            log.error("Could not connect to database: %s", exc)
            sys.exit(1)

        try:
            processed, skipped = ingest(files, conn)
        finally:
            conn.close()

    log.info("Done. Processed: %d | Skipped: %d", processed, skipped)


if __name__ == "__main__":
    main()