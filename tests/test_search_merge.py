# pylint: disable=too-few-public-methods,unused-argument
"""Tests for OpenSearch merge path in InternalLogic.search()."""
from mirrsearch.internal_logic import InternalLogic


class _FakeDbMerge:
    def __init__(self, sql_rows, os_hits, by_id_rows):
        self._sql_rows = sql_rows
        self._os_hits = os_hits
        self._by_id_rows = by_id_rows
        self.get_dockets_by_ids_calls = []

    def search(self, query, docket_type_param=None, agency=None, cfr_part_param=None):
        return list(self._sql_rows)

    def text_match_terms(self, terms, opensearch_client=None):
        return list(self._os_hits)

    def get_dockets_by_ids(self, docket_ids):
        self.get_dockets_by_ids_calls.append(list(docket_ids))
        return list(self._by_id_rows)

    def get_docket_document_comment_totals(self, docket_ids, opensearch_client=None):  # pylint: disable=unused-argument
        # Provide deterministic denominators for assertions.
        dids = [str(d) for d in docket_ids]
        totals = {
            "A": {"document_total_count": 10, "comment_total_count": 2},
            "B": {"document_total_count": 4, "comment_total_count": 5},
            "C": {"document_total_count": 7, "comment_total_count": 3},
        }
        return {d: totals[d] for d in dids if d in totals}


def test_merge_opensearch_empty_uses_sql_only_with_match_source():
    db = _FakeDbMerge(
        sql_rows=[{"docket_id": "A", "docket_title": "t", "cfr_refs": []}],
        os_hits=[],
        by_id_rows=[],
    )
    logic = InternalLogic("x", db_layer=db)
    out = logic.search("q", page=1, page_size=10)
    assert len(out["results"]) == 1
    assert out["results"][0]["match_source"] == "title"
    assert out["results"][0]["document_match_count"] == 0
    assert out["results"][0]["comment_match_count"] == 0
    assert out["results"][0]["document_total_count"] == 10
    assert out["results"][0]["comment_total_count"] == 2
    assert not db.get_dockets_by_ids_calls


def test_merge_appends_full_text_with_counts_and_order():  # pylint: disable=too-many-statements
    sql_rows = [{"docket_id": "A", "docket_title": "ta", "cfr_refs": []}]
    os_hits = [
        {"docket_id": "A", "document_match_count": 9, "comment_match_count": 1},
        {"docket_id": "B", "document_match_count": 2, "comment_match_count": 3},
        {"docket_id": "C", "document_match_count": 1, "comment_match_count": 0},
    ]
    by_id_rows = [
        {"docket_id": "C", "docket_title": "tc", "cfr_refs": []},
        {"docket_id": "B", "docket_title": "tb", "cfr_refs": []},
    ]
    db = _FakeDbMerge(sql_rows, os_hits, by_id_rows)
    logic = InternalLogic("x", db_layer=db)
    out = logic.search("q", page=1, page_size=10)
    merged = out["results"]
    assert [r["docket_id"] for r in merged] == ["A", "B", "C"]
    assert merged[0]["match_source"] == "title"
    assert merged[0]["document_match_count"] == 9
    assert merged[0]["comment_match_count"] == 1
    assert merged[0]["document_total_count"] == 10
    assert merged[0]["comment_total_count"] == 2
    assert merged[1]["match_source"] == "full_text"
    assert merged[1]["document_match_count"] == 2
    assert merged[1]["comment_match_count"] == 3
    assert merged[1]["document_total_count"] == 4
    assert merged[1]["comment_total_count"] == 5
    assert merged[2]["match_source"] == "full_text"
    assert merged[2]["document_match_count"] == 1
    assert merged[2]["document_total_count"] == 7
    assert db.get_dockets_by_ids_calls == [["B", "C"]]


def test_merge_skips_os_docket_missing_in_postgres():
    sql_rows = [{"docket_id": "A", "docket_title": "ta", "cfr_refs": []}]
    os_hits = [{"docket_id": "B", "document_match_count": 1, "comment_match_count": 0}]
    db = _FakeDbMerge(sql_rows, os_hits, by_id_rows=[])
    logic = InternalLogic("x", db_layer=db)
    out = logic.search("q", page=1, page_size=10)
    assert len(out["results"]) == 1
    assert out["results"][0]["docket_id"] == "A"
    assert out["results"][0]["document_match_count"] == 0
    assert out["results"][0]["comment_match_count"] == 0
    assert out["results"][0]["document_total_count"] == 10
    assert out["results"][0]["comment_total_count"] == 2


def test_row_docket_key_accepts_id_for_mocks():
    db = _FakeDbMerge(
        sql_rows=[{"id": 1, "title": "x", "cfr_refs": []}],
        os_hits=[],
        by_id_rows=[],
    )
    logic = InternalLogic("x", db_layer=db)
    out = logic.search("q", page=1, page_size=2)
    assert out["results"][0]["match_source"] == "title"
    assert out["results"][0]["document_match_count"] == 0
    assert out["results"][0]["comment_match_count"] == 0
    assert out["results"][0]["document_total_count"] == 0
    assert out["results"][0]["comment_total_count"] == 0


def test_merge_os_hits_all_title_matches_falls_back_to_title_only():
    """OpenSearch returns hits but none are new vs title search → no get_dockets_by_ids."""
    sql_rows = [{"docket_id": "A", "docket_title": "ta", "cfr_refs": []}]
    os_hits = [{"docket_id": "A", "document_match_count": 9, "comment_match_count": 1}]
    db = _FakeDbMerge(sql_rows, os_hits, by_id_rows=[])
    logic = InternalLogic("x", db_layer=db)
    out = logic.search("q", page=1, page_size=10)
    assert len(out["results"]) == 1
    assert out["results"][0]["match_source"] == "title"
    assert out["results"][0]["document_match_count"] == 9
    assert out["results"][0]["comment_match_count"] == 1
    assert out["results"][0]["document_total_count"] == 10
    assert out["results"][0]["comment_total_count"] == 2
    assert not db.get_dockets_by_ids_calls
