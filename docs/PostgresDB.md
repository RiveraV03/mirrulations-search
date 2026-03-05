# Postgres DB Setup

Prerequisites: Homebrew-installed PostgreSQL (or another PostgreSQL installation reachable from your shell).

## Create the database

**Option 1: Empty database (schema only)**  
Creates the `mirrulations` database with tables but no data. Prompts before overwriting if the DB already exists.

```bash
./db/create_empty_db.sh
```

**Option 2: Database with sample data**  
Creates the database, loads the schema, then loads sample dockets/documents/links/cfrparts. Prompts before overwriting if the DB already exists.

```bash
./db/create_sample_db.sh
```

**Option 3: Full setup (one command)**  
Same as Option 2 but with no overwrite prompt. Drops and recreates the DB every time.

```bash
./db/setup_postgres.sh
```

Use a different database name (e.g. for testing) by setting `DB_NAME`:

```bash
DB_NAME=mirrulations_test ./db/create_empty_db.sh
DB_NAME=mirrulations_test ./db/create_sample_db.sh
```

Non-interactive (e.g. in CI or verify script): `OVERWRITE_YES=1 ./db/create_sample_db.sh`

## Verify scripts (testing)

Before committing changes to the DB scripts, run the verification script. It creates `mirrulations_test`, runs both empty and sample scripts, checks row counts, then drops the test DB.

```bash
./db/verify_empty_and_sample.sh
```

## Quick reference

Start PostgreSQL service
```bash
brew services start postgresql
```

Create / drop the `mirrulations` database
```bash
# Drop the database (if needed)
dropdb mirrulations

# Create the database
createdb mirrulations
```

Initialize schema (run the SQL schema file provided in the repository)
```bash
psql -d mirrulations -f db/schema-postgres.sql
```

Open a psql session connected to `mirrulations`:
```bash
psql mirrulations
```

Example `INSERT` for the `documents` table (adjust values as needed):
```sql
INSERT INTO documents (
    document_id,
    docket_id,
    document_api_link,
    agency_id,
    document_type,
    modify_date,
    posted_date,
    document_title,
    comment_start_date,
    comment_end_date
)
VALUES (
    'CMS-2025-0242-0001',
    'CMS-2025-0242',
    'https://api.regulations.gov/v4/documents/CMS-2025-0242-0001',
    'CMS',
    'Proposed Rule',
    '2025-02-12 11:20:00+00',
    '2025-02-10 10:15:00+00',
    'ESRD Treatment Choices Model Updates',
    '2025-03-01 00:00:00+00',
    '2025-05-01 00:00:00+00'
);
```

**Common psql tips**
- Enable expanded display (easier to read wide rows): `\x`
- Show all rows from the `documents` table:
```sql
SELECT * FROM documents;
```
Exit psql
```sql
\q
```

Stop PostgreSQL service 
```bash
brew services stop postgresql
```
