# Search overhaul: OpenSearch keyword merge

This document plans integrating OpenSearch full-text hits into the existing docket search pipeline **without** changing `app.py`, the return shape of `InternalLogic.search()`, or the signature of `DBLayer.search()`.

## Current behavior (baseline)

1. **`app.py`** — `/search/` calls `InternalLogic.search(...)` and returns its dict unchanged.
2. **`InternalLogic.search()`** — Calls `self.db_layer.search(query, docket_type_param, agency, cfr_part_param)`, then paginates the returned list in memory and maps `cfr_refs` → `cfrPart` on the current page only.
3. **`DBLayer.search()`** — If `self.conn is None`, returns `[]`; otherwise returns `_search_dockets_postgres(...)`.
4. **`_search_dockets_postgres()`** — Single SQL path: `d.docket_title ILIKE %keyword%`, optional filters (`docket_type`, `agency`, `cfr_part`), `ORDER BY d.modify_date DESC, ...`, **`LIMIT 50`**. Rows are folded through `_process_docket_row` into the same dict shape as today (including `cfr_refs` for later expansion).
5. **`text_match_terms(terms, opensearch_client=None)`** — Already implemented: queries OpenSearch `documents` and `comments` indices, returns `[{docket_id, document_match_count, comment_match_count}, ...]`. Uses `get_opensearch_connection()` when client is omitted. On failure, catches errors and returns `[]`.

## Target behavior

After the **existing** Postgres title search (still invoked only via `DBLayer.search()` with unchanged signature and internals):

1. Call **`self.db_layer.text_match_terms([query])`** with the same user `query` string the UI/API passed in (normalize consistently with how terms are intended—e.g. strip if you align with SQL’s `(query or '').strip().lower()` only where it matters for parity).
2. **Compare** OpenSearch docket IDs to the set of `docket_id` values from the SQL result list.
3. **New docket IDs** = OpenSearch IDs **not** in that set (full-text match without title match).
4. For those IDs only, load full docket rows from Postgres via a new **`DBLayer.get_dockets_by_ids(docket_ids: List[str]) -> List[Dict[str, Any]]`**.
5. **Merge**: SQL/title results **first** (preserve their order), then OpenSearch-only rows **second**. Recommend preserving **OpenSearch iteration order** for the appended block (order of `text_match_terms` output), unless product asks otherwise.
6. **Annotate** each dict:
   - **`match_source`**: `"title"` for every row originating from `db_layer.search()`; `"full_text"` for rows loaded only via `get_dockets_by_ids`.
   - For **`match_source == "full_text"`**, also set **`document_match_count`** and **`comment_match_count`** from the corresponding OpenSearch dict (same names as `text_match_terms` returns).
7. **Pagination** — Unchanged: `InternalLogic.search()` continues to compute `total_results`, `total_pages`, slice `page_results`, and transform `cfr_refs` → `cfrPart` exactly as today, but on the **merged** list.

## Files to touch

| File | Change |
|------|--------|
| **`internal_logic.py`** | Orchestrate: call `search()`, then `text_match_terms()`, diff IDs, call `get_dockets_by_ids()`, merge, set `match_source` (+ counts for full-text-only rows). |
| **`db.py`** | Add `get_dockets_by_ids` only. **Do not** change `DBLayer.search()` signature or `app.py`. |

## `get_dockets_by_ids` specification

- **Signature**: `get_dockets_by_ids(self, docket_ids: List[str]) -> List[Dict[str, Any]]`
- **Connection guard**: If `self.conn is None`, return `[]` (mirror `DBLayer.search()`).
- **Query**: Reuse the **same** `FROM` / `JOIN` structure as `_search_dockets_postgres` (`dockets` → `documents` → optional `cfrparts` / `links` joins) so row shape matches what `_process_docket_row` expects.
- **Filter**: `d.docket_id = ANY(%s)` (psycopg2: pass list/tuple for `ANY`).
- **Explicitly omit**: `docket_type`, `agency`, and `cfr_part` predicates—fetch purely by ID.
- **Processing**: Reuse **`_process_docket_row`** and the same list-comprehension pattern that builds `{**d, "cfr_refs": ...}` as `_search_dockets_postgres` so each docket dict matches the existing schema.
- **`ORDER BY`**: Not specified by product; use something deterministic and consistent with the rest of the app (e.g. same column order as title search: `d.modify_date DESC, d.docket_id, ...`) **or** reorder in `InternalLogic` to match OpenSearch order after fetch (if you need strict “OpenSearch order” for the tail, fetch then sort keyed by position in the OpenSearch list).
- **Empty input**: If `docket_ids` is empty, return `[]` without hitting the DB.

## Graceful degradation

- If **`text_match_terms`** returns **`[]`** (OpenSearch down, parse errors, no hits, etc.), **do not** call `get_dockets_by_ids` for merge purposes. Use **SQL-only** results.
- Still add **`match_source: "title"`** to those rows **if** you want a uniform schema for clients; if the API today omits `match_source`, adding it only when OpenSearch runs could be inconsistent—**recommend** always setting `match_source` on every item once this ships (`"title"` or `"full_text"`) so the contract is stable.

## Constraints checklist

- [ ] **No** `app.py` changes.
- [ ] **No** change to **`InternalLogic.search()`** return shape (still `results` + `pagination` with same keys).
- [ ] **No** change to **`DBLayer.search()`** signature (or behavior beyond what’s unavoidable—ideally none; merge stays in `InternalLogic`).
- [ ] **Do not** rewire `text_match_terms`; call it as-is with `[query]` (optional `opensearch_client` only if tests need injection).
- [ ] **`get_dockets_by_ids`** mirrors `conn is None` → `[]` like `search()`.

## Edge cases and notes

1. **SQL `LIMIT 50`**: Title matches remain capped by the existing `_search_dockets_postgres` query. The merged list length can exceed 50 when OpenSearch adds dockets. Pagination already handles arbitrary length; confirm with stakeholders that this is desired.
2. **Filters**: `docket_type` / `agency` / `cfr_part` apply only to the **title** branch (`db_layer.search()`). OpenSearch-only rows are **not** re-filtered by those params (per requirements). Document for API consumers that filters narrow title matches only; full-text tail is unfiltered by those facets unless you add a follow-up.
3. **Duplicates**: A docket in **both** SQL and OpenSearch appears **once**, from the title branch, with `match_source="title"`; do **not** attach OpenSearch counts to that row unless product asks (current spec: counts only on `full_text`-only rows).
4. **ID type**: Ensure comparison between SQL `docket_id` and OpenSearch `docket_id` is normalized (string vs string) if either source can return non-string types.
5. **Tests**: Add unit tests for merge order, empty OpenSearch, `conn is None` on `get_dockets_by_ids`, and empty `docket_ids`.

## Implementation order

1. Implement **`get_dockets_by_ids`** in `db.py` and test in isolation (mock connection or integration test).
2. Update **`InternalLogic.search()`**: build merged list and annotations; keep pagination and `cfrPart` mapping unchanged.
3. Manual or automated check: `/search/` response shape unchanged except new optional fields (`match_source`, and `document_match_count` / `comment_match_count` on full-text-only rows).

## Out of scope (this plan)

- Changing OpenSearch queries, indices, or `text_match_terms` internals.
- Moving merge logic into `DBLayer.search()` (would complicate “don’t change signature” / separation of concerns; spec places orchestration in `InternalLogic`).
- Applying facet filters to OpenSearch-only dockets.
