"""
eCFR Agencies to PostgreSQL Database
Fetches agencies from the eCFR API and stores regulation paths + URLs in a PostgreSQL database.

Commands:
  create       -- Fetch API & populate DB
  erase        -- Wipe the regulations table
  view         -- Display all DB rows
  view <name>  -- Search rows by name
  view id <id> -- Show full detail for one row
  exit         -- Quit
"""

import os
import sys
import json
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "mirrulations")
DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

ECFR_API_URL = "https://www.ecfr.gov/api/admin/v1/agencies.json"
BASE_URL     = "https://www.ecfr.gov/current"


# ─── Helpers ──────────────────────────────────────────────────────────────────

def fetch_agencies() -> list:
    """Fetch all agencies from the eCFR API."""
    print("Fetching agencies from eCFR API...")
    response = requests.get(ECFR_API_URL, headers={"accept": "application/json"}, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data.get("agencies", [])


def build_rows_from_agency(agency: dict, parent_name: str = None) -> list:
    """
    Recursively build (name, url) rows for an agency and its children.
    Each cfr_reference (title + chapter) on the agency becomes its own row.
    """
    rows = []

    agency_name = agency.get("display_name") or agency.get("name", "Unknown")
    # If this is a child agency, prefix with the parent name for clarity
    full_name = f"{parent_name} > {agency_name}" if parent_name else agency_name

    for ref in agency.get("cfr_references", []):
        title   = ref.get("title")
        chapter = ref.get("chapter")

        # Build the human-readable name
        parts = [full_name]
        if title:
            parts.append(f"Title-{title}")
        if chapter:
            parts.append(f"Chapter-{chapter}")
        name = " / ".join(parts)

        # Build the URL
        url_parts = [BASE_URL]
        if title:
            url_parts.append(f"title-{title}")
        if chapter:
            url_parts.append(f"chapter-{chapter}")
        url = "/".join(url_parts)

        rows.append((name, url))

    # Recurse into child agencies
    for child in agency.get("children", []):
        rows.extend(build_rows_from_agency(child, parent_name=full_name))

    return rows


def get_connection():
    """Create and return a PostgreSQL connection to the target DB."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def ensure_database():
    """Create the database if it does not already exist."""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname="postgres",
        user=DB_USER,
        password=DB_PASSWORD,
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (DB_NAME,))
        if not cur.fetchone():
            cur.execute(f'CREATE DATABASE "{DB_NAME}";')
            print(f"Database '{DB_NAME}' created successfully.")
        else:
            print(f"Database '{DB_NAME}' already exists.")
    conn.close()


def ensure_table(conn):
    """Create the regulations table and unique index if they don't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS regulations (
                id   SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                url  TEXT NOT NULL
            );
        """)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_regulations_url ON regulations (url);
        """)
    conn.commit()


def insert_rows(conn, rows: list):
    """Insert (name, url) rows, skipping duplicates."""
    with conn.cursor() as cur:
        inserted = 0
        skipped  = 0
        for name, url in rows:
            try:
                cur.execute(
                    "INSERT INTO regulations (name, url) VALUES (%s, %s) "
                    "ON CONFLICT (url) DO NOTHING;",
                    (name, url),
                )
                if cur.rowcount:
                    inserted += 1
                else:
                    skipped += 1
            except Exception as e:
                print(f"  Error inserting row: {e}")
                conn.rollback()
    conn.commit()
    print(f"Inserted {inserted} rows, skipped {skipped} duplicates.")


# ─── Commands ─────────────────────────────────────────────────────────────────

def cmd_create():
    """Create the DB (if needed), fetch agencies from the API, and populate the table."""
    print(f"Ensuring database '{DB_NAME}' exists...")
    try:
        ensure_database()
    except psycopg2.OperationalError as e:
        print(f"Could not connect to PostgreSQL: {e}")
        print("Make sure PostgreSQL is running and your .env credentials are correct.")
        return

    agencies = fetch_agencies()
    print(f"  -> {len(agencies)} top-level agencies retrieved.")

    if not agencies:
        print("No agencies found. Check the API response.")
        return

    # Flatten all agencies and children into (name, url) rows
    rows = []
    for agency in agencies:
        rows.extend(build_rows_from_agency(agency))

    print(f"  -> {len(rows)} total regulation references to insert.")

    print(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}/{DB_NAME}...")
    try:
        conn = get_connection()
    except psycopg2.OperationalError as e:
        print(f"Connection failed: {e}")
        return

    ensure_table(conn)
    print(f"Inserting rows into 'regulations' table...")
    insert_rows(conn, rows)
    conn.close()
    print("Done!")


def cmd_erase():
    """Drop and recreate the regulations table after user confirmation."""
    confirm = input(
        "WARNING: This will permanently delete all rows in the 'regulations' table.\n"
        "Type 'yes' to confirm: "
    ).strip().lower()

    if confirm != "yes":
        print("Erase cancelled.")
        return

    print(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}/{DB_NAME}...")
    try:
        conn = get_connection()
    except psycopg2.OperationalError as e:
        print(f"Connection failed: {e}")
        return

    with conn.cursor() as cur:
        cur.execute("DROP INDEX IF EXISTS idx_regulations_url;")
        cur.execute("DROP TABLE IF EXISTS regulations;")
    conn.commit()
    ensure_table(conn)
    conn.close()
    print("Table 'regulations' has been erased and recreated successfully.")


def cmd_view(name_filter=None):
    """
    Display rows in the regulations table.
    If name_filter is given, performs a case-insensitive partial match on the name column.
    """
    print(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}/{DB_NAME}...")
    try:
        conn = get_connection()
    except psycopg2.OperationalError as e:
        print(f"Connection failed: {e}")
        return

    with conn.cursor() as cur:
        if name_filter:
            cur.execute(
                "SELECT id, name, url FROM regulations WHERE name ILIKE %s ORDER BY id;",
                (f"%{name_filter}%",),
            )
            print(f'Searching for names matching: "{name_filter}"')
        else:
            cur.execute("SELECT id, name, url FROM regulations ORDER BY id;")
        rows = cur.fetchall()

    conn.close()

    if not rows:
        msg = f'No rows found matching "{name_filter}".' if name_filter else "The 'regulations' table is empty."
        print(msg)
        return

    col_id   = 6
    col_name = 70
    col_url  = 80

    header  = f"{'ID':<{col_id}} {'NAME':<{col_name}} {'URL':<{col_url}}"
    divider = "-" * len(header)

    print(f"\n{divider}")
    print(header)
    print(divider)
    for row_id, name, url in rows:
        print(
            f"{str(row_id):<{col_id}} "
            f"{name[:col_name]:<{col_name}} "
            f"{url[:col_url]:<{col_url}}"
        )
    print(divider)
    print(f"\nTotal rows: {len(rows)}")


def cmd_view_id(row_id):
    """Display the full untruncated details for a single row by ID."""
    print(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}/{DB_NAME}...")
    try:
        conn = get_connection()
    except psycopg2.OperationalError as e:
        print(f"Connection failed: {e}")
        return

    with conn.cursor() as cur:
        cur.execute("SELECT id, name, url FROM regulations WHERE id = %s;", (row_id,))
        row = cur.fetchone()

    conn.close()

    if not row:
        print(f"No record found with ID {row_id}.")
        return

    row_id, name, url = row
    divider = "-" * 80
    print(f"\n{divider}")
    print(f"  ID   : {row_id}")
    print(f"  Name : {name}")
    print(f"  URL  : {url}")
    print(divider)


# ─── Menu ─────────────────────────────────────────────────────────────────────

COMMANDS = {
    "create": cmd_create,
    "erase":  cmd_erase,
    "view":   cmd_view,
}

MENU = """
+----------------------------------------------------+
|            cfrurl -- eCFR Database                 |
+----------------------------------------------------+
|  create             -- Fetch API & populate DB     |
|  erase              -- Wipe the regulations table  |
|  view               -- Display all DB rows         |
|  view <name>        -- Search rows by name         |
|  view id <id>       -- Show full detail for one row|
|  exit               -- Quit                        |
+----------------------------------------------------+
"""


def interactive_menu():
    print(MENU)
    while True:
        try:
            raw = input("Enter command: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nGoodbye.")
            break

        parts  = raw.split(None, 1)
        choice = parts[0].lower() if parts else ""
        argument = parts[1] if len(parts) > 1 else None

        if choice in ("exit", "quit", "q"):
            print("Goodbye.")
            break
        elif choice == "view":
            print()
            if argument and argument.lower().startswith("id "):
                id_str = argument[3:].strip()
                if id_str.isdigit():
                    cmd_view_id(int(id_str))
                else:
                    print(f"Invalid ID '{id_str}'. Please provide a numeric ID, e.g.: view id 5")
            else:
                cmd_view(name_filter=argument)
            print()
        elif choice in COMMANDS:
            print()
            COMMANDS[choice]()
            print()
        else:
            print(f"Unknown command '{choice}'. Choose from: {', '.join(COMMANDS)} or exit.\n")


def main():
    # Allow direct CLI usage:
    #   python main.py create
    #   python main.py erase
    #   python main.py view
    #   python main.py view "Agriculture"
    #   python main.py view id 5
    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()
        if cmd == "view":
            if len(sys.argv) > 2 and sys.argv[2].lower() == "id":
                if len(sys.argv) > 3 and sys.argv[3].isdigit():
                    cmd_view_id(int(sys.argv[3]))
                else:
                    print("Usage: python main.py view id <numeric_id>")
                    sys.exit(1)
            else:
                name_filter = sys.argv[2] if len(sys.argv) > 2 else None
                cmd_view(name_filter=name_filter)
        elif cmd in COMMANDS:
            COMMANDS[cmd]()
        else:
            print(f"Unknown command '{cmd}'. Valid commands: {', '.join(COMMANDS)}")
            sys.exit(1)
    else:
        interactive_menu()


if __name__ == "__main__":
    main()