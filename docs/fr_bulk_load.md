# Federal Register bulk load (FR-native schema)

Download the dataset: [`documents.json.zip`](https://drive.google.com/file/d/1htnOhjooaYgNRdobp1wj7et2e1UjlA7z/view?usp=sharing)

**1) Unzip to get `documents.json`**
```bash
unzip documents.json.zip -d extracted/
```

**2) Create the database and tables**
```bash
createdb mirrulations
psql -d mirrulations -f db/schema-postgres.sql
```

> Note: on macOS Homebrew installs, omit `-U <user>` — the default user is your system username.

**3) Run the loader**
```bash
.venv/bin/python db/cfr_and_fr/load_fr_bulk.py extracted/documents.json
```

The loader expects `documents.json` to be a top-level JSON array. Items without `document_number` are skipped. CFR references (`cfr_references`) are loaded into the `cfrparts` table keyed by `frdocnum`, `title`, and `cfrpart`.