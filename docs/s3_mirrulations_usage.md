## S3 bucket usage (`mirrulations`)

This document explains how we use the **public AWS Open Data S3 bucket** `mirrulations` to fetch docket data that ultimately powers search in this project.

The bucket itself is maintained as part of the Mirrulations project; this app only **reads** from it.

---

### High‑level overview

- **Bucket**: `mirrulations`
- **Access**: public / unsigned (**no AWS credentials required**)
- **Client**: `boto3` S3 client (`list_objects_v2` + `download_file`)
- **Organization**: objects grouped by **agency** and **docket id**

The actual S3 interaction lives in the Mirrulations fetch tooling (for example, `mirrulations_fetch/download_docket.py` in that repo). This app assumes those files have already been downloaded to disk.

---

### From docket id → S3 prefixes

Given a docket id such as `DEA-2024-0059`, the Mirrulations downloader derives:

- **Agency**: everything before the first `-` (sometimes further split on `_`), e.g. `DEA`
- **Base prefixes** under the bucket:
  - `raw-data/<agency>/<docket_id>/...`
  - `derived-data/<agency>/<docket_id>/...`

Those two top‑level trees hold the **raw text / binary content** and any **derived outputs** respectively.

---

### Bucket layout (key structure)

Conceptually the bucket looks like this:

```text
s3://mirrulations/
  raw-data/
    <agency>/
      <docket_id>/
        text-<docket_id>/
          docket/
            ...
          documents/
            ...
          comments/
            ...
        binary-<docket_id>/
          ...
  derived-data/
    <agency>/
      <docket_id>/
        ...
```

Where:

- **`<agency>`**: derived from the docket id (e.g. `DEA` from `DEA-2024-0059`)
- **`text-<docket_id>/...`**:
  - `docket/` – docket‑level JSON / metadata  
  - `documents/` – rule / notice / supporting document text  
  - `comments/` – public comments text
- **`binary-<docket_id>/...`**: original binary assets (PDFs, attachments, etc.)
- **`derived-data/<agency>/<docket_id>/...`**: data computed from the raw text (summaries, embeddings, etc.)

The downloader expects at minimum the **text tree** for a given docket.

---

### Expected prefixes per docket

For a single docket id, the Mirrulations downloader treats these prefixes as canonical:

- **Docket text**  
  `raw-data/<agency>/<docket_id>/text-<docket_id>/docket/`
- **Document text**  
  `raw-data/<agency>/<docket_id>/text-<docket_id>/documents/`
- **Comment text**  
  `raw-data/<agency>/<docket_id>/text-<docket_id>/comments/`
- **Derived data (optional)**  
  `derived-data/<agency>/<docket_id>/`
- **Binary data (optional)**  
  `raw-data/<agency>/<docket_id>/binary-<docket_id>/`

Before downloading, the CLI verifies that:

- At least one object exists under `raw-data/<agency>/<docket_id>/`
- At least one object exists under `raw-data/<agency>/<docket_id>/text-<docket_id>/`

If either check fails, the tool treats the docket as **not present** in S3 and exits with a “not found in S3 bucket” style error.

---

### Local download layout

The Mirrulations downloader mirrors the S3 structure into a local output directory, preserving relative paths under a per‑docket root.

For an output folder `<output-folder>` and docket `<docket_id>`:

```text
<output-folder>/
  <docket_id>/
    raw-data/
      <agency>/<docket_id>/text-<docket_id>/{docket,documents,comments}/...
      <agency>/<docket_id>/binary-<docket_id>/...   # if binary downloads enabled
    derived-data/
      <agency>/<docket_id>/...                     # if present and not explicitly skipped
```

Notes on common CLI flags (in the Mirrulations fetch tool):

- **`--no-comments`**  
  - Skips `.../comments/` under `text-<docket_id>/`
  - Often also skips `derived-data/...` when that data is comment‑driven
- **`--include-binary`**  
  - Adds `binary-<docket_id>/` if present in the bucket

This app can then ingest / index data from `<output-folder>/<docket_id>/` as part of its own pipeline.

---

### How downloads work (implementation details)

Implementation details from the Mirrulations downloader (for reference):

- **Listing**  
  - Uses `list_objects_v2` with a paginator to enumerate all objects under the relevant prefixes.
- **Downloading**  
  - Uses `s3_client.download_file(bucket, key, local_path)` for each object.
- **Concurrency**  
  - Runs downloads across a small pool of worker threads (up to 8).
- **Progress reporting**  
  - Prints counts and a rough ETA, typically tracking “Text” and “Bin” separately when binary download is enabled.

---

### Operational notes

- Because the bucket is public, **you should not add credentials** for normal use.
- If you see `AccessDenied` or similar S3 errors, likely causes are:
  - the key / prefix does not exist,
  - the bucket policy or AWS Open Data configuration changed, or
  - you are on a network (VPN, corporate proxy, etc.) that blocks S3 endpoints.

If you suspect a broader S3 or Open Data issue, you can confirm bucket visibility using the AWS CLI or the AWS Open Data registry page linked from `README.md`.

# S3 bucket usage (`mirrulations`)

This project downloads docket data from the **public AWS Open Data S3 bucket** named `mirrulations` (see the AWS Open Data registry page linked from `README.md`).

## Key points

- **Bucket**: `mirrulations` (hardcoded in `mirrulations_fetch/download_docket.py`)
- **Auth**: **unsigned / public access** (no AWS credentials required)
- **API**: `boto3` S3 client (`list_objects_v2`, paginated listing, and `download_file`)
- **How data is organized**: keys are grouped by **agency** and **docket id**

## How the CLI maps a docket id to S3 prefixes

Given a `docket_id` like `DEA-2024-0059`, the CLI derives:

- **Agency**: everything before the first `-` (with a small extra split on `_`), e.g. `DEA`
- **Base prefixes**:
  - `raw-data/<agency>/<docket_id>/...`
  - `derived-data/<agency>/<docket_id>/...`

## Bucket data structure (key layout)

At a high level, the bucket is organized like this:

```
s3://mirrulations/
  raw-data/
    <agency>/
      <docket_id>/
        text-<docket_id>/
          docket/
            ...
          documents/
            ...
          comments/
            ...
        binary-<docket_id>/
          ...
  derived-data/
    <agency>/
      <docket_id>/
        ...
```

Where:

- **`<agency>`**: derived from the docket id (e.g. `DEA` from `DEA-2024-0059`)
- **`text-<docket_id>/...`**: text/metadata organized into `docket/`, `documents/`, `comments/`
- **`binary-<docket_id>/...`**: original binary assets (only downloaded when `--include-binary`)
- **`derived-data/<agency>/<docket_id>/...`**: derived outputs (downloaded unless `--no-comments`)

The tool expects the following (text) layout for a docket:

- **Docket text**: `raw-data/<agency>/<docket_id>/text-<docket_id>/docket/`
- **Document text**: `raw-data/<agency>/<docket_id>/text-<docket_id>/documents/`
- **Comment text**: `raw-data/<agency>/<docket_id>/text-<docket_id>/comments/`
- **Derived data (optional)**: `derived-data/<agency>/<docket_id>/`
- **Binary (optional; when `--include-binary`)**: `raw-data/<agency>/<docket_id>/binary-<docket_id>/`

Before downloading, the CLI checks that:

- `raw-data/<agency>/<docket_id>/` exists (at least one object with that prefix), and
- `raw-data/<agency>/<docket_id>/text-<docket_id>/` exists

If either check fails, it exits with a “not found in S3 bucket” style error.

## What gets downloaded (and where it lands locally)

The CLI lists all objects under the relevant prefixes, then downloads each object to disk preserving its *relative path* under the logical root.

For an output folder `<output-folder>` and docket `<docket_id>`, the local structure is:

```
<output-folder>/
  <docket_id>/
    raw-data/
      <agency>/<docket_id>/text-<docket_id>/{docket,documents,comments}/...
      <agency>/<docket_id>/binary-<docket_id>/...          # (if --include-binary)
    derived-data/
      <agency>/<docket_id>/...                             # (if present and not --no-comments)
```

Notes:

- **`--no-comments`** skips both:
  - comment text (`.../comments/`), and
  - derived data (`derived-data/...`) (because it is comment-related in this project’s workflow)
- **`--include-binary`** adds `binary-<docket_id>/` if it exists

## How downloads work (implementation details)

In `mirrulations_fetch/download_docket.py`:

- **Listing**: uses `list_objects_v2` with a paginator to gather object keys and sizes.
- **Downloading**: uses `s3_client.download_file(bucket, key, local_path)`.
- **Concurrency**: downloads are executed across up to 8 worker threads.
- **Progress**: prints counts and an ETA based on objects completed, separately tracking “Text” and “Bin” when binary download is enabled.

## Operational notes

- Because the bucket is public, **you should not add credentials** for normal use.
- If you see `AccessDenied`, it usually indicates one of:
  - the key/prefix doesn’t exist,
  - the bucket policy changed, or
  - you’re on a network that blocks S3 endpoints.

