#!/usr/bin/env python3
"""
Ingest comment JSON files into the mirrulations PostgreSQL database.

Optional: download docket text (including comments) from public S3 first.

Directory layout after S3 download:
    <output-folder>/<DOCKET-ID>/raw-data/comments/*.json

Examples:
    python db/ingest_comments.py   # prompts for docket ID; S3 download if ./<ID>/ has no comments yet
    python db/ingest_comments.py --docket-dir ./CMS-2025-0240
    python db/ingest_comments.py --download-s3 CMS-2025-0240 --output-folder .
    python db/ingest_comments.py --download-s3 CMS-2025-0240 --download-only
"""
from __future__ import annotations

import argparse
import glob
import json
import logging
import os
import queue
import sys
import warnings
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3
import psycopg2
from botocore import UNSIGNED
from botocore.config import Config
from psycopg2.extras import execute_values

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

# Boto3 warns on Python 3.9 EOL; unrelated to this script.
warnings.filterwarnings(
    "ignore",
    message=".*Boto3 will no longer support Python 3.9.*",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# S3 download (mirrulations public bucket, unsigned)
# ---------------------------------------------------------------------------

S3_BUCKET = "mirrulations"
RAW_DATA_PREFIX = "raw-data"
DERIVED_DATA_PREFIX = "derived-data"
_s3_client = None


def _s3():
    global _s3_client  # pylint: disable=global-statement
    if _s3_client is None:
        _s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    return _s3_client


def _normalize_docket_id(docket_id: str) -> str:
    """
    Mirrulations S3 keys use upper-case agency (e.g. FAA-2025-0618).
    Accepts user input in any case.
    """
    s = docket_id.strip()
    if not s:
        return s
    if "-" not in s:
        return s.upper()
    head, tail = s.split("-", 1)
    return f"{head.split('_')[0].upper()}-{tail}"


def _s3_agency(docket: str) -> str:
    return _normalize_docket_id(docket).split("-")[0]


def _s3_key_exists(prefix: str) -> bool:
    resp = _s3().list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=1)
    return "Contents" in resp and len(resp["Contents"]) > 0


def _s3_download_file(s3_key: str, local_path: str) -> None:
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    _s3().download_file(S3_BUCKET, s3_key, local_path)


def _s3_get_file_list(prefix: str, label: Optional[str] = None) -> Tuple[List[Dict[str, Any]], int]:
    files: List[Dict[str, Any]] = []
    total_size = 0
    paginator = _s3().get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            files.append({"Key": obj["Key"], "Size": obj["Size"]})
        total_size = sum(f["Size"] for f in files)
        if label:
            print(f"{label}: {len(files)}", end="\r", flush=True)
    if label:
        print(f"{label}: {len(files)}")
    return files, total_size


def _s3_rel_path(s3_key: str, base_prefix: str) -> str:
    return os.path.relpath(s3_key, base_prefix)


def _s3_print_stats(stats: dict, totals: dict, start_times: dict) -> None:
    text_total = totals["text"]
    text_done = (
        stats["docket"]
        + stats["documents"]
        + stats["comments"]
        + stats.get("derived", 0)
    )
    elapsed_text = time.time() - start_times["text"]
    text_rate = text_done / elapsed_text if elapsed_text > 0 else 0
    text_remain = text_total - text_done
    text_eta = (text_remain / text_rate) if text_rate > 0 else float("inf")
    text_eta_str = (
        f"{int(text_eta // 60):2d}m{int(text_eta % 60):02d}s"
        if text_eta != float("inf")
        else "  N/A "
    )
    output = f"Text: {text_done:6}/{text_total:6} ETA:{text_eta_str:7}"
    if "binary" in stats:
        bin_total = totals["binary"]
        bin_done = stats["binary"]
        if start_times["binary"] is None and bin_done > 0:
            start_times["binary"] = time.time()
        elapsed_bin = (time.time() - start_times["binary"]) if start_times["binary"] else 0
        bin_rate = bin_done / elapsed_bin if elapsed_bin > 0 else 0
        bin_remain = bin_total - bin_done
        bin_eta = (bin_remain / bin_rate) if bin_rate > 0 else float("inf")
        bin_eta_str = (
            f"{int(bin_eta // 60):2d}m{int(bin_eta % 60):02d}s"
            if bin_eta != float("inf")
            else "  N/A "
        )
        output += f" | Bin: {bin_done:6}/{bin_total:6} ETA:{bin_eta_str:7}"
    print(f"\r{output.ljust(80)}", end="", flush=True)


def _s3_download_worker(
    q: queue.Queue,
    stats: dict,
    totals: dict,
    start_times: dict,
    base_prefix: dict,
    output_folder: str,
) -> None:
    while True:
        item = q.get()
        if item is None:
            break
        s3_key, file_type, _size = item
        rel_path = _s3_rel_path(s3_key, base_prefix[file_type])
        if file_type in ("docket", "documents", "comments", "binary"):
            local_path = os.path.join(output_folder, "raw-data", rel_path)
        elif file_type == "derived":
            local_path = os.path.join(output_folder, "derived-data", rel_path)
        else:
            local_path = os.path.join(output_folder, rel_path)
        try:
            _s3_download_file(s3_key, local_path)
            if file_type in stats:
                stats[file_type] += 1
        except Exception as e:  # pylint: disable=broad-except
            print(f"\nError downloading {s3_key}: {e}", file=sys.stderr)
            sys.exit(1)
        stats["remaining"][file_type] -= 1
        _s3_print_stats(stats, totals, start_times)
        q.task_done()


def download_docket_from_s3(
    docket_id: str,
    output_folder: str = ".",
    include_binary: bool = False,
    no_comments: bool = False,
) -> Path:
    """
    Download mirrulations S3 data into ``{output_folder}/{docket_id}/``.
    Returns path to the docket directory (for ingest).
    """
    docket_id = _normalize_docket_id(docket_id)
    agency = _s3_agency(docket_id)
    raw_agency_docket_prefix = f"{RAW_DATA_PREFIX}/{agency}/{docket_id}/"
    raw_text_prefix = f"{RAW_DATA_PREFIX}/{agency}/{docket_id}/text-{docket_id}/"
    raw_binary_prefix = f"{RAW_DATA_PREFIX}/{agency}/{docket_id}/binary-{docket_id}/"
    derived_prefix = f"{DERIVED_DATA_PREFIX}/{agency}/{docket_id}/"

    if not _s3_key_exists(raw_agency_docket_prefix):
        log.error(
            "No data at s3://%s/%s — docket may be missing or id casing wrong.",
            S3_BUCKET,
            raw_agency_docket_prefix,
        )
        log.error(
            "Expected layout: raw-data/<AGENCY>/<DOCKET-ID>/ (AGENCY upper-case, e.g. FAA-2025-0618)."
        )
        log.error("You passed normalized id %r, agency %r.", docket_id, agency)
        sys.exit(1)
    if not _s3_key_exists(raw_text_prefix):
        log.error(
            "No text bundle at s3://%s/%s (need text-%s/ under the docket folder).",
            S3_BUCKET,
            raw_text_prefix,
            docket_id,
        )
        sys.exit(1)

    docket_root = Path(output_folder).resolve() / docket_id
    docket_root_str = str(docket_root)

    print("Preparing download lists...")
    file_lists: Dict[str, List] = {}
    total_sizes: Dict[str, int] = {}

    file_lists["docket"], total_sizes["docket"] = _s3_get_file_list(
        f"{RAW_DATA_PREFIX}/{agency}/{docket_id}/text-{docket_id}/docket/", "docket"
    )
    print(f"Docket total size:   {total_sizes['docket']/1e6:.2f} MB")

    file_lists["documents"], total_sizes["documents"] = _s3_get_file_list(
        f"{RAW_DATA_PREFIX}/{agency}/{docket_id}/text-{docket_id}/documents/", "documents"
    )
    print(f"Document total size: {total_sizes['documents']/1e6:.2f} MB")

    if no_comments:
        file_lists["comments"], total_sizes["comments"] = [], 0
        print("Comments: skipped (--s3-no-comments)")
    else:
        file_lists["comments"], total_sizes["comments"] = _s3_get_file_list(
            f"{RAW_DATA_PREFIX}/{agency}/{docket_id}/text-{docket_id}/comments/", "comments"
        )
        print(f"Comment total size:  {total_sizes['comments']/1e6:.2f} MB")

    if no_comments:
        print("Derived data: skipped (--s3-no-comments)")
    elif _s3_key_exists(derived_prefix):
        file_lists["derived"], total_sizes["derived"] = _s3_get_file_list(derived_prefix, "derived")
        print(f"Derived total size:  {total_sizes['derived']/1e6:.2f} MB")
    else:
        print("Derived data not found - skipping")

    if include_binary and _s3_key_exists(raw_binary_prefix):
        file_lists["binary"], total_sizes["binary"] = _s3_get_file_list(raw_binary_prefix, "binary")
        print(f"Binary total size:   {total_sizes['binary']/1e6:.2f} MB")

    totals = {
        "text": len(file_lists["docket"])
        + len(file_lists["documents"])
        + len(file_lists["comments"])
    }
    if "derived" in file_lists:
        totals["text"] += len(file_lists["derived"])

    stats = {
        "docket": 0,
        "documents": 0,
        "comments": 0,
        "remaining": {k: len(v) for k, v in file_lists.items()},
    }
    if "derived" in file_lists:
        stats["derived"] = 0
    start_times = {"text": time.time(), "binary": None}
    if "binary" in file_lists:
        totals["binary"] = len(file_lists["binary"])
        stats["binary"] = 0

    base_prefix = {
        "docket": f"{RAW_DATA_PREFIX}/{agency}/{docket_id}/text-{docket_id}/",
        "documents": f"{RAW_DATA_PREFIX}/{agency}/{docket_id}/text-{docket_id}/",
        "comments": f"{RAW_DATA_PREFIX}/{agency}/{docket_id}/text-{docket_id}/",
        "binary": f"{RAW_DATA_PREFIX}/{agency}/{docket_id}/",
        "derived": derived_prefix,
    }

    q: queue.Queue = queue.Queue()
    for file_type, files in file_lists.items():
        for f in files:
            q.put((f["Key"], file_type, f["Size"]))

    num_threads = min(8, max(1, q.qsize()))
    threads: List[threading.Thread] = []
    for _ in range(num_threads):
        t = threading.Thread(
            target=_s3_download_worker,
            args=(q, stats, totals, start_times, base_prefix, docket_root_str),
        )
        t.start()
        threads.append(t)
    q.join()
    for _ in threads:
        q.put(None)
    for t in threads:
        t.join()
    print("\nS3 download finished.")
    print(f"Files for {docket_id} → {docket_root}")
    return docket_root


# ---------------------------------------------------------------------------
# Mapping helpers
# ---------------------------------------------------------------------------

def extract_comment(data: dict) -> dict:
    """Extract and map fields from a regulations.gov comment JSON to DB columns."""
    attrs = data.get("attributes", {})
    links = data.get("links", {})

    return {
        "comment_id": data.get("id"),
        "api_link": links.get("self"),
        "document_id": attrs.get("commentOnDocumentId"),
        "duplicate_comment_count": attrs.get("duplicateComments", 0) or 0,
        "address1": attrs.get("address1"),
        "address2": attrs.get("address2"),
        "agency_id": attrs.get("agencyId"),
        "city": attrs.get("city"),
        "comment_category": attrs.get("category"),
        "comment": attrs.get("comment"),
        "country": attrs.get("country"),
        "docket_id": attrs.get("docketId"),
        "document_type": attrs.get("documentType"),
        "email": attrs.get("email"),
        "fax": attrs.get("fax"),
        "flex_field1": attrs.get("field1"),
        "flex_field2": attrs.get("field2"),
        "first_name": attrs.get("firstName"),
        "submitter_gov_agency": attrs.get("govAgency"),
        "submitter_gov_agency_type": attrs.get("govAgencyType"),
        "last_name": attrs.get("lastName"),
        "modification_date": attrs.get("modifyDate"),
        "submitter_org": attrs.get("organization"),
        "phone": attrs.get("phone"),
        "posted_date": attrs.get("postedDate"),
        "postmark_date": attrs.get("postmarkDate"),
        "reason_withdrawn": attrs.get("reasonWithdrawn"),
        "received_date": attrs.get("receiveDate"),
        "restriction_reason": attrs.get("restrictReason"),
        "restriction_reason_type": attrs.get("restrictReasonType"),
        "state_province_region": attrs.get("stateProvinceRegion"),
        "comment_subtype": attrs.get("subtype"),
        "comment_title": attrs.get("title"),
        "is_withdrawn": attrs.get("withdrawn", False) or False,
        "postal_code": attrs.get("zip"),
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


def fetch_valid_docket_ids(conn) -> set[str]:
    """Return docket_id values present in dockets (comments.docket_id FK)."""
    with conn.cursor() as cur:
        cur.execute("SELECT docket_id FROM dockets;")
        return {row[0] for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# Core ingest logic
# ---------------------------------------------------------------------------

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
    """Ingest all files; returns (processed_count, skipped)."""
    batch: list[tuple] = []
    skipped = 0
    nulled_doc_ids = 0
    nulled_docket_ids = 0
    batch_size = 500

    valid_doc_ids: set[str] = set()
    valid_docket_ids: set[str] = set()
    if not dry_run:
        valid_doc_ids = fetch_valid_document_ids(conn)
        valid_docket_ids = fetch_valid_docket_ids(conn)
        log.info("Loaded %d document_id(s), %d docket_id(s) from DB.", len(valid_doc_ids), len(valid_docket_ids))

    def flush(batch_local):
        if dry_run:
            log.info("[DRY RUN] Would upsert %d row(s).", len(batch_local))
        else:
            with conn.cursor() as cur:
                execute_values(cur, UPSERT_SQL, batch_local)
            conn.commit()
            log.info("Upserted %d row(s).", len(batch_local))

    for path in files:
        data = load_json(path)
        if data is None:
            skipped += 1
            continue

        record = extract_comment(data)

        missing = [
            c
            for c in (
                "comment_id",
                "api_link",
                "agency_id",
                "document_type",
                "posted_date",
            )
            if not record.get(c)
        ]
        if missing:
            log.warning("Skipping %s — missing required fields: %s", path.name, missing)
            skipped += 1
            continue

        doc_id = record.get("document_id")
        if doc_id and not dry_run and doc_id not in valid_doc_ids:
            log.warning(
                "%s: document_id '%s' not found in documents table — setting to NULL.",
                path.name,
                doc_id,
            )
            record["document_id"] = None
            nulled_doc_ids += 1

        docket_id_val = record.get("docket_id")
        if docket_id_val and not dry_run and docket_id_val not in valid_docket_ids:
            log.warning(
                "%s: docket_id '%s' not in dockets table — setting to NULL (add docket row to preserve FK).",
                path.name,
                docket_id_val,
            )
            record["docket_id"] = None
            nulled_docket_ids += 1

        batch.append(row_tuple(record))

        if len(batch) >= batch_size:
            flush(batch)
            batch.clear()

    if batch:
        flush(batch)

    if nulled_doc_ids:
        log.info(
            "%d comment(s) had document_id set to NULL due to missing documents row.",
            nulled_doc_ids,
        )
    if nulled_docket_ids:
        log.info(
            "%d comment(s) had docket_id set to NULL due to missing dockets row.",
            nulled_docket_ids,
        )

    processed = len(files) - skipped
    return processed, skipped


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Download comment JSON from S3 (optional) and ingest into Postgres."
    )
    p.add_argument(
        "--download-s3",
        metavar="DOCKET_ID",
        help="Download docket data from mirrulations S3 into --output-folder/DOCKET_ID/",
    )
    p.add_argument(
        "--output-folder",
        default=".",
        help="Parent directory for S3 download (default: current directory)",
    )
    p.add_argument(
        "--include-binary",
        action="store_true",
        help="With --download-s3: also download binary-* objects",
    )
    p.add_argument(
        "--s3-no-comments",
        action="store_true",
        help="With --download-s3: skip comments (and derived) in S3 — ingest will usually find nothing",
    )
    p.add_argument(
        "--download-only",
        action="store_true",
        help="Only S3 download, no DB (use with --download-s3 or run without --docket-dir to be prompted)",
    )
    p.add_argument(
        "--docket-dir",
        help="Path to docket folder with raw-data/comments/ (if unset and no --download-s3, prompts for docket ID)",
    )
    p.add_argument("--host", default=os.getenv("DB_HOST", "localhost"))
    p.add_argument("--port", type=int, default=int(os.getenv("DB_PORT", "5432")))
    p.add_argument("--dbname", default=os.getenv("DB_NAME", os.getenv("PGDATABASE", "mirrulations")))
    p.add_argument(
        "--user",
        default=os.getenv("DB_USER", os.getenv("PGUSER", os.getenv("USER", "postgres"))),
    )
    p.add_argument(
        "--password",
        default=os.getenv("DB_PASSWORD", os.getenv("PGPASSWORD", "")),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse JSON and validate only; no DB writes",
    )
    return p.parse_args()


def main():
    if load_dotenv:
        load_dotenv(Path(__file__).resolve().parent.parent / ".env")

    args = parse_args()

    if args.download_s3:
        docket_norm = _normalize_docket_id(args.download_s3)
        if docket_norm != args.download_s3.strip():
            log.info("Normalized docket id for S3: %s", docket_norm)
        download_docket_from_s3(
            docket_norm,
            output_folder=args.output_folder,
            include_binary=args.include_binary,
            no_comments=args.s3_no_comments,
        )
        docket_dir = Path(args.output_folder).resolve() / docket_norm
    elif args.docket_dir:
        docket_dir = Path(args.docket_dir).expanduser().resolve()
    else:
        raw = input("Enter docket ID (e.g. FAA-2025-0618): ").strip()
        if not raw:
            log.error("Docket ID cannot be empty.")
            sys.exit(1)
        docket_norm = _normalize_docket_id(raw)
        if docket_norm != raw:
            log.info("Normalized docket ID: %s", docket_norm)
        docket_dir = Path(args.output_folder).resolve() / docket_norm
        comments_dir = docket_dir / "raw-data" / "comments"
        have_local = comments_dir.is_dir() and bool(list(comments_dir.glob("*.json")))
        if args.download_only or not have_local:
            if args.download_only:
                log.info("Downloading from S3 (--download-only)…")
            elif not have_local:
                log.info("No comment JSON under %s — downloading from S3…", docket_dir)
            download_docket_from_s3(
                docket_norm,
                output_folder=args.output_folder,
                include_binary=args.include_binary,
                no_comments=args.s3_no_comments,
            )

    if args.download_only:
        if args.docket_dir:
            log.error("--download-only downloads from S3; omit --docket-dir.")
            sys.exit(1)
        log.info("Download complete (--download-only).")
        return

    files = find_json_files(docket_dir)
    if not files:
        log.error("No JSON files in %s/raw-data/comments/", docket_dir)
        sys.exit(1)

    if args.dry_run:
        log.info("DRY RUN — no database writes.")
        processed, skipped = ingest(files, conn=None, dry_run=True)
    else:
        log.info("Connecting to PostgreSQL at %s:%d/%s …", args.host, args.port, args.dbname)
        try:
            conn = psycopg2.connect(
                host=args.host,
                port=args.port,
                dbname=args.dbname,
                user=args.user,
                password=args.password or None,
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
