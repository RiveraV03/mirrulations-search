"""
Data reaper — deletes expired download jobs from S3 and Postgres.

Runs on a schedule via mirrulations-reaper.timer (systemd).

Environment variables:
  S3_BUCKET         S3 bucket name (required)
  DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD
"""
import logging
import os
import sys

import boto3
import psycopg2

from mirrsearch.db import DBLayer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


def _get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "your_db"),
        user=os.getenv("DB_USER", "your_user"),
        password=os.getenv("DB_PASSWORD", "your_password"),
    )


def _delete_from_s3(s3_path: str):
    """Delete a single object given a full s3://bucket/key URI."""
    if not s3_path or not s3_path.startswith("s3://"):
        log.warning("Skipping invalid s3_path: %r", s3_path)
        return
    without_prefix = s3_path[len("s3://"):]
    bucket, _, key = without_prefix.partition("/")
    if not bucket or not key:
        log.warning("Could not parse bucket/key from: %r", s3_path)
        return
    boto3.client("s3").delete_object(Bucket=bucket, Key=key)
    log.info("Deleted s3://%s/%s", bucket, key)


def main():
    log.info("Reaper starting")
    conn = _get_pg_conn()
    db = DBLayer(conn)

    expired = db.get_expired_download_jobs()
    if not expired:
        log.info("No expired jobs found")
        conn.close()
        return

    log.info("Found %d expired job(s)", len(expired))

    s3_errors = 0
    for job in expired:
        job_id = job["job_id"]
        s3_path = job["s3_path"]
        if s3_path:
            try:
                _delete_from_s3(s3_path)
            except Exception as exc:  # pylint: disable=broad-except
                log.error("Failed to delete S3 object for job %s: %s", job_id, exc)
                s3_errors += 1
        else:
            log.info("Job %s has no s3_path, skipping S3 delete", job_id)

    deleted = db.prune_expired_download_jobs()
    log.info("Deleted %d expired job(s) from Postgres (%d S3 error(s))", deleted, s3_errors)
    conn.close()


if __name__ == "__main__":
    main()
