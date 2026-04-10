#!/usr/bin/env python3
"""
Migrate (copy/upsert) all rows from `documentswithfrdoc` → `documents`.

Some deployments ended up with both tables, and `documentswithfrdoc` often has
newer/extra fields. This script makes `documents` match and then bulk-upserts
all rows server-side (no Python row loops).
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

EXTRA_COL_DEFS = [
    # Columns present on documentswithfrdoc but sometimes missing on documents
    # (types match db/schema-postgres.sql)
    ("frdocnum", "VARCHAR(50)"),
    ("attachments_self_link", "TEXT"),
    ("attachments_related_link", "TEXT"),
    ("file_formats", "JSONB"),
    ("display_properties", "JSONB"),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Upsert all documentswithfrdoc rows into documents (add missing columns first)."
    )
    p.add_argument("--env-file", default=".env", help="Path to .env (default: .env)")
    p.add_argument("--sslmode", default=None, help="Override sslmode (e.g. require, verify-full)")
    p.add_argument(
        "--sslrootcert",
        default=None,
        help="Path to CA bundle (used with sslmode=verify-full)",
    )
    p.add_argument("--dry-run", action="store_true", help="Print planned actions only.")
    return p.parse_args()


def connect(env_file: str, sslmode: str | None, sslrootcert: str | None):
    load_dotenv(Path(env_file))

    host = os.environ["DB_HOST"]
    kw = dict(
        host=host,
        port=os.environ.get("DB_PORT", "5432"),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )

    # Default for RDS: require TLS unless overridden
    if sslmode:
        kw["sslmode"] = sslmode
    elif os.environ.get("PGSSLMODE"):
        kw["sslmode"] = os.environ["PGSSLMODE"]
    elif ".rds.amazonaws.com" in host:
        kw["sslmode"] = "require"

    if sslrootcert:
        kw["sslrootcert"] = sslrootcert
    elif os.environ.get("PGSSLROOTCERT"):
        kw["sslrootcert"] = os.environ["PGSSLROOTCERT"]

    return psycopg2.connect(**kw)


def regclass(cur, name: str) -> str | None:
    cur.execute("SELECT to_regclass(%s)", (name,))
    return cur.fetchone()[0]


def existing_columns(cur, table_name: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table_name,),
    )
    return {r[0] for r in cur.fetchall()}


def main() -> None:
    args = parse_args()
    conn = connect(args.env_file, args.sslmode, args.sslrootcert)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            if regclass(cur, "public.documents") is None:
                raise SystemExit("Missing table public.documents")
            if regclass(cur, "public.documentswithfrdoc") is None:
                raise SystemExit("Missing table public.documentswithfrdoc")

            doc_cols = existing_columns(cur, "documents")
            docw_cols = existing_columns(cur, "documentswithfrdoc")

            # 1) Add missing "new" columns to documents
            alters: list[str] = []
            for col, coltype in EXTRA_COL_DEFS:
                if col in docw_cols and col not in doc_cols:
                    alters.append(f"ALTER TABLE documents ADD COLUMN IF NOT EXISTS {col} {coltype};")

            if alters:
                if args.dry_run:
                    print("Would run ALTERs:")
                    for stmt in alters:
                        print(stmt)
                else:
                    for stmt in alters:
                        cur.execute(stmt)
                    conn.commit()
                    print(f"Added/verified {len(alters)} extra column(s) on documents.")
            else:
                print("No schema changes needed on documents.")

            # Recompute columns after ALTER
            doc_cols = existing_columns(cur, "documents")
            docw_cols = existing_columns(cur, "documentswithfrdoc")

            # 2) Bulk upsert all shared columns (including newly added)
            common = sorted((doc_cols & docw_cols) - {"id"})
            if "document_id" not in common:
                raise SystemExit("Expected shared primary key column document_id.")

            insert_cols = ", ".join(common)
            select_cols = ", ".join(common)

            update_cols = [c for c in common if c != "document_id"]
            update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

            upsert_sql = f"""
                INSERT INTO documents ({insert_cols})
                SELECT {select_cols}
                FROM documentswithfrdoc
                ON CONFLICT (document_id) DO UPDATE
                SET {update_set}
            """

            if args.dry_run:
                print("\nWould run UPSERT SQL:")
                print(upsert_sql)
                return

            print("Starting UPSERT (this can take a while on millions of rows)...")
            cur.execute(upsert_sql)
            conn.commit()
            print("Done. documents now contains/upserts all documentswithfrdoc rows.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
