"""Internal logic module for search operations with pagination"""
from datetime import date, datetime
from typing import List

from mirrsearch.db import cfr_part_filter_patterns, get_db


def _correlation_score(row, support_k=10):
    """Compute ratio score with support bias toward larger denominator."""
    match_total = int(row.get("document_match_count", 0)) + int(row.get("comment_match_count", 0))
    total = int(row.get("document_total_count", 0)) + int(row.get("comment_total_count", 0))
    if total <= 0:
        return 0.0
    ratio = match_total / total
    support = total / (total + support_k)
    return ratio * support


def _row_docket_key(row):
    """Stable id for de-duping SQL vs OpenSearch (Postgres uses docket_id; some mocks use id)."""
    if "docket_id" in row:
        return str(row["docket_id"])
    return str(row["id"])


def _cfr_part_patterns_match_row(row, patterns):
    """True if any pattern matches any cfrPart value (Postgres: cp.cfrPart ILIKE %pattern%)."""
    if not patterns:
        return True
    cfr_refs = row.get("cfr_refs") or []
    for ref in cfr_refs:
        for part_key in (ref.get("cfrParts") or {}).keys():
            pk_l = str(part_key).lower()
            if any(pat in pk_l for pat in patterns):
                return True
    return False


def _docket_type_matches_filter(row, docket_type_param):
    """Postgres: d.docket_type = %s when filter set."""
    return not docket_type_param or row.get("docket_type") == docket_type_param


def _agency_matches_filter(row, agency):
    """Postgres: OR of d.agency_id ILIKE %s when agency list set."""
    if not agency:
        return True
    aid = (row.get("agency_id") or "").lower()
    return any((a or "").strip().lower() in aid for a in agency)


def _cfr_matches_filter(row, cfr_part_param):
    """Postgres: OR of cp.cfrPart ILIKE when CFR filter set."""
    if not cfr_part_param:
        return True
    patterns = cfr_part_filter_patterns(cfr_part_param)
    return not patterns or _cfr_part_patterns_match_row(row, patterns)


def _json_safe_scalar(value):
    """Convert DB/driver values that jsonify may not handle on all Flask/Python combos."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _sanitize_search_row_for_json(row):
    """In-place: e.g. Postgres returns datetime for modify_date."""
    if "modify_date" in row:
        row["modify_date"] = _json_safe_scalar(row["modify_date"])


def _row_matches_advanced_filters(row, docket_type_param, agency, cfr_part_param):
    """
    Same constraints as _search_dockets_postgres for full-text rows loaded via get_dockets_by_ids.
    Drops OpenSearch-only hits that fail advanced filters.
    """
    return (
        _docket_type_matches_filter(row, docket_type_param)
        and _agency_matches_filter(row, agency)
        and _cfr_matches_filter(row, cfr_part_param)
    )


class InternalLogic:  # pylint: disable=too-few-public-methods
    """Internal logic for search operations with pagination"""

    def __init__(self, database, db_layer=None):
        self.database = database
        self.db_layer = db_layer if db_layer is not None else get_db()

    def search(self, query, docket_type_param=None, agency=None,
               cfr_part_param=None, start_date=None, end_date=None, page=1, page_size=10):
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        """
        Search with pagination support.

        Args:
            query: Search query string
            docket_type_param: Filter by docket type
            agency: Filter by agency
            cfr_part_param: Filter by CFR part
            start_date: Filter by start date (YYYY-MM-DD)
            end_date: Filter by end date (YYYY-MM-DD)
            page: Page number (1-indexed)
            page_size: Number of results per page

        Returns:
            dict: Paginated response with metadata
        """
        sql_results = self.db_layer.search(
            query,
            docket_type_param,
            agency,
            cfr_part_param,
            start_date=start_date,
            end_date=end_date
        )
        title_rows = [{**r, "match_source": "title"} for r in sql_results]
        title_ids = {_row_docket_key(r) for r in sql_results}

        os_hits = self.db_layer.text_match_terms([(query or "").strip()])
        os_counts_by_id = {str(hit["docket_id"]): hit for hit in os_hits}

        new_ids_ordered = []
        seen_new = set()
        for hit in os_hits:
            did = str(hit["docket_id"])
            if did in title_ids or did in seen_new:
                continue
            seen_new.add(did)
            new_ids_ordered.append(did)

        # Attach numerators for title-matching cards (0 when OpenSearch doesn't have a hit)
        for row in title_rows:
            did = _row_docket_key(row)
            hit = os_counts_by_id.get(did)
            row["document_match_count"] = hit["document_match_count"] if hit else 0
            row["comment_match_count"] = hit["comment_match_count"] if hit else 0

        # Attach full-text-only cards
        full_text_rows = []
        if new_ids_ordered:
            fetched = self.db_layer.get_dockets_by_ids(new_ids_ordered)
            by_id = {str(r["docket_id"]): r for r in fetched}
            for did in new_ids_ordered:
                row = by_id.get(did)
                if row is None:
                    continue
                if not _row_matches_advanced_filters(
                        row, docket_type_param, agency, cfr_part_param):
                    continue
                h = os_counts_by_id.get(did, {})
                full_text_rows.append({
                    **row,
                    "match_source": "full_text",
                    "document_match_count": h.get("document_match_count", 0),
                    "comment_match_count": h.get("comment_match_count", 0),
                })

        all_results = title_rows + full_text_rows

        # Attach denominators for every returned docket card
        docket_ids_all = [_row_docket_key(r) for r in all_results]
        totals_map = self.db_layer.get_docket_document_comment_totals(docket_ids_all)
        for row in all_results:
            did = _row_docket_key(row)
            totals = totals_map.get(did, {})
            row["document_total_count"] = totals.get("document_total_count", 0)
            row["comment_total_count"] = totals.get("comment_total_count", 0)
            row["correlation_score"] = _correlation_score(row)

        # Rank by correlation score, then by raw match counts.
        all_results.sort(
            key=lambda r: (
                r.get("correlation_score", 0.0),
                int(r.get("document_match_count", 0)) + int(r.get("comment_match_count", 0)),
                int(r.get("document_total_count", 0)) + int(r.get("comment_total_count", 0)),
            ),
            reverse=True
        )

        total_results = len(all_results)
        total_pages = (total_results + page_size - 1) // page_size

        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size

        page_results = all_results[start_idx:end_idx]
        for result in page_results:
            _sanitize_search_row_for_json(result)
            cfr_refs = result.pop("cfr_refs", None)
            if cfr_refs is not None:
                result["cfrPart"] = [
                    {"title": ref.get("title"), "part": part, "link": link}
                    for ref in cfr_refs
                    for part, link in ref.get("cfrParts", {}).items()
                ]

        # Rename internal keys to user-facing numerator/denominator names
        key_map = {
            "document_match_count": "documentNumerator",
            "comment_match_count": "commentNumerator",
            "document_total_count": "documentDenominator",
            "comment_total_count": "commentDenominator",
        }
        for result in page_results:
            for old, new in key_map.items():
                if old in result:
                    result[new] = result.pop(old)

        return {
            "results": page_results,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_results": total_results,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            }
        }

    def get_agencies(self) -> List[str]:
        return self.db_layer.get_agencies()
