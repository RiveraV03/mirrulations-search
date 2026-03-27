#!/usr/bin/env python3

import os
import json
import logging
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host":        os.environ.get("DB_HOST"),
    "port":        int(os.environ.get("DB_PORT", 5432)),
    "dbname":      os.environ.get("DB_NAME"),
    "user":        os.environ.get("DB_USER"),
    "password":    os.environ.get("DB_PASSWORD"),
    "sslmode":     "verify-full",
    "sslrootcert": "/certs/global-bundle.pem",
}

DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/mnt/search-data/data"))

BATCH_SIZE = 500


def map_document(raw):
    try:
        data = raw["data"]
        attr = data["attributes"]
        links = data["links"]
    except KeyError as e:
        log.warning("Skipping malformed JSON — missing key: %s", e)
        return None

    document_id = data.get("id")
    docket_id   = attr.get("docketId")
    modify_date = attr.get("modifyDate")
    doc_type    = attr.get("documentType")

    if not all([document_id, docket_id, modify_date, doc_type]):
        log.warning("Skipping %s — missing required field(s)", document_id)
        return None

    return {
        "document_id":               document_id,
        "docket_id":                 docket_id,
        "document_api_link":         links.get("self"),
        "address1":                  attr.get("address1"),
        "address2":                  attr.get("address2"),
        "agency_id":                 attr.get("agencyId"),
        "is_late_comment":           attr.get("allowLateComments"),
        "author_date":               attr.get("authorDate"),
        "comment_category":          attr.get("category"),
        "city":                      attr.get("city"),
        "comment":                   attr.get("comment"),
        "comment_end_date":          attr.get("commentEndDate"),
        "comment_start_date":        attr.get("commentStartDate"),
        "country":                   attr.get("country"),
        "document_type":             doc_type,
        "effective_date":            attr.get("effectiveDate"),
        "email":                     attr.get("email"),
        "fax":                       attr.get("fax"),
        "flex_field1":               attr.get("field1"),
        "flex_field2":               attr.get("field2"),
        "first_name":                attr.get("firstName"),
        "submitter_gov_agency":      attr.get("govAgency"),
        "submitter_gov_agency_type": attr.get("govAgencyType"),
        "implementation_date":       attr.get("implementationDate"),
        "last_name":                 attr.get("lastName"),
        "modify_date":               modify_date,
        "is_open_for_comment":       attr.get("openForComment", False),
        "submitter_org":             attr.get("organization"),
        "phone":                     attr.get("phone"),
        "posted_date":               attr.get("postedDate"),
        "postmark_date":             attr.get("postmarkDate"),
        "reason_withdrawn":          attr.get("reasonWithdrawn"),
        "receive_date":              attr.get("receiveDate"),
        "reg_writer_instruction":    attr.get("regWriterInstruction"),
        "restriction_reason":        attr.get("restrictReason"),
        "restriction_reason_type":   attr.get("restrictReasonType"),
        "state_province_region":     attr.get("stateProvinceRegion"),
        "subtype":                   attr.get("subtype"),
        "document_title":            attr.get("title"),
        "topics":                    attr.get("topics"),
        "is_withdrawn":              attr.get("withdrawn", False),
        "postal_code":               attr.get("zip"),
        "frdocnum":                  attr.get("frDocNum"),
    }


def iter_documents(data_root):
    pattern = "**/documents/*.json"
    log.info("Scanning for JSON files under %s ...", data_root)

    for path in data_root.glob(pattern):
        try:
            with open(path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            doc = map_document(raw)
            if doc:
                yield doc
        except json.JSONDecodeError as e:
            log.warning("Skipping %s — invalid JSON: %s", path, e)
        except Exception as e:
            log.warning("Skipping %s — unexpected error: %s", path, e)


COLUMNS = [
    "document_id", "docket_id", "document_api_link", "address1", "address2",
    "agency_id", "is_late_comment", "author_date", "comment_category", "city",
    "comment", "comment_end_date", "comment_start_date", "country",
    "document_type", "effective_date", "email", "fax", "flex_field1",
    "flex_field2", "first_name", "submitter_gov_agency",
    "submitter_gov_agency_type", "implementation_date", "last_name",
    "modify_date", "is_open_for_comment", "submitter_org", "phone",
    "posted_date", "postmark_date", "reason_withdrawn", "receive_date",
    "reg_writer_instruction", "restriction_reason", "restriction_reason_type",
    "state_province_region", "subtype", "document_title", "topics",
    "is_withdrawn", "postal_code", "frdocnum",
]

INSERT_SQL = f"""
    INSERT INTO documentsWithFRdoc ({', '.join(COLUMNS)})
    VALUES %s
    ON CONFLICT (document_id) DO UPDATE SET
        modify_date          = EXCLUDED.modify_date,
        is_open_for_comment  = EXCLUDED.is_open_for_comment,
        is_withdrawn         = EXCLUDED.is_withdrawn,
        frdocnum             = COALESCE(EXCLUDED.frdocnum, documentsWithFRdoc.frdocnum),
        document_title       = EXCLUDED.document_title,
        topics               = EXCLUDED.topics,
        comment_end_date     = EXCLUDED.comment_end_date,
        comment_start_date   = EXCLUDED.comment_start_date,
        posted_date          = EXCLUDED.posted_date
"""


def insert_batch(cursor, batch):
    seen = {}
    for doc in batch:
        seen[doc["document_id"]] = doc
    rows = [tuple(doc[col] for col in COLUMNS) for doc in seen.values()]
    execute_values(cursor, INSERT_SQL, rows)


def main():
    log.info("Connecting to RDS at %s ...", DB_CONFIG["host"])
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cursor = conn.cursor()

    batch = []
    total_inserted = 0
    total_skipped  = 0
    total_processed = 0

    try:
        for doc in iter_documents(DATA_ROOT):
            batch.append(doc)
            total_processed += 1

            if total_processed % 10_000 == 0:
                log.info("Processed %d docs so far (inserted: %d)", total_processed, total_inserted)

            if len(batch) >= BATCH_SIZE:
                try:
                    insert_batch(cursor, batch)
                    conn.commit()
                    total_inserted += len(batch)
                    log.info("Inserted %d rows (total: %d)", len(batch), total_inserted)
                except Exception as e:
                    conn.rollback()
                    log.error("Batch insert failed, rolling back: %s", e)
                    total_skipped += len(batch)
                finally:
                    batch.clear()

        if batch:
            try:
                insert_batch(cursor, batch)
                conn.commit()
                total_inserted += len(batch)
            except Exception as e:
                conn.rollback()
                log.error("Final batch insert failed: %s", e)
                total_skipped += len(batch)

    finally:
        cursor.close()
        conn.close()

    log.info("Done. Inserted: %d | Skipped: %d", total_inserted, total_skipped)


if __name__ == "__main__":
    main()
