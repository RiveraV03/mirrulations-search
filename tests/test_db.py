"""
Tests for the database layer (db.py)

Only tests DBLayer wiring, the postgres branch, and module-level
factory functions. Dummy-data behavior tests live in test_mock.py.
"""
# pylint: disable=redefined-outer-name,protected-access
import pytest
import mirrsearch.db as db_module
from mirrsearch.db import DBLayer, get_db


# --- DBLayer instantiation ---

def test_db_layer_creation():
    """Test that DBLayer can be instantiated"""
    db = DBLayer()
    assert db is not None
    assert isinstance(db, DBLayer)


def test_db_layer_is_frozen():
    """Test that DBLayer is a frozen dataclass (immutable)"""
    db = DBLayer()
    with pytest.raises(Exception):  # FrozenInstanceError
        db.new_attribute = "test"


def test_db_layer_no_conn_returns_empty():
    """DBLayer with no connection returns empty list from search"""
    db = DBLayer()
    assert db.search("anything") == []


def test_get_db_returns_dblayer():
    """Test the get_db factory function returns a DBLayer"""
    db = get_db()
    assert isinstance(db, DBLayer)


# --- Fake postgres helpers ---

class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.executed = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self.executed = (sql, params)

    def fetchall(self):
        return self._rows

    def close(self):
        return None


class _FakeConn:
    def __init__(self, rows):
        self.cursor_obj = _FakeCursor(rows)

    def cursor(self):
        return self.cursor_obj

    def close(self):
        return None


# --- _search_dockets_postgres filter tests ---

def test_search_dockets_postgres_agency_filter():
    """Agency filter adds ILIKE clause and wraps value with wildcards"""
    db = DBLayer(conn=_FakeConn([]))
    db._search_dockets_postgres("", agency=["CMS"])
    sql, params = db.conn.cursor_obj.executed
    assert "agency_id ILIKE %s" in sql
    assert params == ["%%", "%CMS%"]


def test_search_dockets_postgres_agency_multi_filter():
    """Multiple agencies produce OR'd ILIKE clauses"""
    db = DBLayer(conn=_FakeConn([]))
    db._search_dockets_postgres("", agency=["CMS", "EPA"])
    sql, params = db.conn.cursor_obj.executed
    assert sql.count("agency_id ILIKE %s") == 2
    assert "%CMS%" in params
    assert "%EPA%" in params


def test_search_dockets_postgres_docket_type_filter():
    """Docket type filter adds exact match clause"""
    db = DBLayer(conn=_FakeConn([]))
    db._search_dockets_postgres("", docket_type_param="Rulemaking")
    sql, params = db.conn.cursor_obj.executed
    assert "d.docket_type = %s" in sql
    assert params == ["%%", "Rulemaking"]


def test_search_dockets_postgres_agency_and_docket_type_filter():
    """Both filters add their clauses and params in order"""
    db = DBLayer(conn=_FakeConn([]))
    db._search_dockets_postgres("renal", docket_type_param="Rulemaking", agency=["CMS"])
    sql, params = db.conn.cursor_obj.executed
    assert "d.docket_type = %s" in sql
    assert "agency_id ILIKE %s" in sql
    assert params == ["%renal%", "Rulemaking", "%CMS%"]


def test_get_cfr_docket_ids_single_part():
    """Single CFR part queries federal_register_documents with title+part ILIKE clauses"""
    db = DBLayer(conn=_FakeConn([("CMS-2025-0304",)]))
    result = db._get_cfr_docket_ids([{"title": "Title 42", "part": "413"}])
    sql, params = db.conn.cursor_obj.executed
    assert "federal_register_documents" in sql
    assert "cfr_title ILIKE %s" in sql
    assert "cfr_part ILIKE %s" in sql
    assert "%Title 42%" in params
    assert "%413%" in params
    assert result == {"CMS-2025-0304"}


def test_get_cfr_docket_ids_multi_part():
    """Multiple CFR parts produce OR'd title+part ILIKE clauses
    against federal_register_documents"""
    db = DBLayer(conn=_FakeConn([("CMS-2025-0304",), ("CMS-2025-0240",)]))
    result = db._get_cfr_docket_ids([
        {"title": "Title 42", "part": "413"},
        {"title": "Title 42", "part": "512"},
    ])
    sql, params = db.conn.cursor_obj.executed
    assert "federal_register_documents" in sql
    assert sql.count("cfr_part ILIKE %s") == 2
    assert "%413%" in params
    assert "%512%" in params
    assert result == {"CMS-2025-0304", "CMS-2025-0240"}


def test_search_dockets_postgres_no_filter_no_extra_clauses():
    """Without filters, SQL has no extra AND clauses beyond docket_title"""
    db = DBLayer(conn=_FakeConn([]))
    db._search_dockets_postgres("abc")
    sql, params = db.conn.cursor_obj.executed
    assert "d.docket_type = %s" not in sql
    assert "agency_id ILIKE %s" not in sql
    assert params == ["%abc%"]


# --- _search_dockets_postgres tests ---

def test_search_dockets_postgres_empty_results():
    """No rows returns an empty list"""
    db = DBLayer(conn=_FakeConn([]))
    results = db._search_dockets_postgres("anything")
    assert results == []


def test_search_dockets_postgres_single_docket_single_cfr():
    """Single row returns one docket with one cfr_ref"""
    rows = [("DOC-001", "Test Docket", "CMS", "Rulemaking",
             "2024-01-01", "Title 42", "42", "http://link")]
    db = DBLayer(conn=_FakeConn(rows))

    results = db._search_dockets_postgres("test")

    assert len(results) == 1
    assert results[0]["docket_id"] == "DOC-001"
    assert results[0]["docket_title"] == "Test Docket"
    assert results[0]["agency_id"] == "CMS"
    assert results[0]["docket_type"] == "Rulemaking"
    assert results[0]["modify_date"] == "2024-01-01"
    assert len(results[0]["cfr_refs"]) == 1
    assert results[0]["cfr_refs"][0]["title"] == "Title 42"
    assert results[0]["cfr_refs"][0]["cfrParts"] == {"42": "http://link"}


def test_search_dockets_postgres_multiple_cfr_parts_same_title():
    """Multiple rows for same docket+title aggregate cfrParts without duplicates"""
    rows = [
        ("DOC-001", "Test Docket", "CMS", "Rulemaking",
         "2024-01-01", "Title 42", "42", "http://link"),
        ("DOC-001", "Test Docket", "CMS", "Rulemaking",
         "2024-01-01", "Title 42", "43", "http://link"),
    ]
    db = DBLayer(conn=_FakeConn(rows))

    results = db._search_dockets_postgres("test")

    assert len(results) == 1
    cfr_ref = results[0]["cfr_refs"][0]
    assert cfr_ref["title"] == "Title 42"
    assert "42" in cfr_ref["cfrParts"]
    assert "43" in cfr_ref["cfrParts"]
    assert len(cfr_ref["cfrParts"]) == 2


def test_search_dockets_postgres_multiple_titles_same_docket():
    """Multiple cfr titles for the same docket produce multiple cfr_refs"""
    rows = [
        ("DOC-001", "Test Docket", "CMS", "Rulemaking",
         "2024-01-01", "Title 42", "42", "http://link42"),
        ("DOC-001", "Test Docket", "CMS", "Rulemaking",
         "2024-01-01", "Title 45", "45", "http://link45"),
    ]
    db = DBLayer(conn=_FakeConn(rows))

    results = db._search_dockets_postgres("test")

    assert len(results) == 1
    titles = {ref["title"] for ref in results[0]["cfr_refs"]}
    assert titles == {"Title 42", "Title 45"}


def test_search_dockets_postgres_multiple_dockets():
    """Rows for different dockets produce separate docket entries"""
    rows = [
        ("DOC-001", "First Docket", "CMS", "Rulemaking",
         "2024-01-01", "Title 42", "42", "http://a"),
        ("DOC-002", "Second Docket", "EPA", "Rulemaking",
         "2024-02-01", "Title 40", "40", "http://b"),
    ]
    db = DBLayer(conn=_FakeConn(rows))

    results = db._search_dockets_postgres("docket")

    assert len(results) == 2
    ids = {r["docket_id"] for r in results}
    assert ids == {"DOC-001", "DOC-002"}


def test_search_dockets_postgres_none_cfr_fields_ignored():
    """Rows with None title or None cfrPart do not add entries to cfr_refs"""
    rows = [
        ("DOC-001", "Test Docket", "CMS", "Rulemaking", "2024-01-01", None, None, None),
    ]
    db = DBLayer(conn=_FakeConn(rows))

    results = db._search_dockets_postgres("test")

    assert len(results) == 1
    assert results[0]["cfr_refs"] == []


def test_search_dockets_postgres_duplicate_cfr_part_not_repeated():
    """Same cfrPart appearing in multiple rows is only stored once"""
    rows = [
        ("DOC-001", "Test Docket", "CMS", "Rulemaking",
         "2024-01-01", "Title 42", "42", "http://link"),
        ("DOC-001", "Test Docket", "CMS", "Rulemaking",
         "2024-01-01", "Title 42", "42", "http://link"),
    ]
    db = DBLayer(conn=_FakeConn(rows))

    results = db._search_dockets_postgres("test")

    assert results[0]["cfr_refs"][0]["cfrParts"] == {"42": "http://link"}


def test_search_dockets_postgres_query_param_formatting():
    """Query string is wrapped with %...% wildcards in params"""
    db = DBLayer(conn=_FakeConn([]))
    db._search_dockets_postgres("clean air")
    _, params = db.conn.cursor_obj.executed
    assert params == ["%clean air%"]


def test_search_dockets_postgres_empty_query_uses_wildcard():
    """Empty query string results in a %% wildcard param"""
    db = DBLayer(conn=_FakeConn([]))
    db._search_dockets_postgres("")
    _, params = db.conn.cursor_obj.executed
    assert params == ["%%"]


# --- _search_dockets_by_title tests ---

def test_search_dockets_by_title_returns_matching_ids():
    """Returns a set of docket_ids whose titles match the query"""
    rows = [("DOC-001",), ("DOC-002",)]
    db = DBLayer(conn=_FakeConn(rows))
    result = db._search_dockets_by_title("clean air")
    assert result == {"DOC-001", "DOC-002"}


def test_search_dockets_by_title_no_matches_returns_empty_set():
    """Returns an empty set when no titles match"""
    db = DBLayer(conn=_FakeConn([]))
    result = db._search_dockets_by_title("nonexistent")
    assert result == set()


def test_search_dockets_by_title_query_wrapped_with_wildcards():
    """Query is wrapped with %...% wildcards in the SQL params"""
    db = DBLayer(conn=_FakeConn([]))
    db._search_dockets_by_title("water")
    sql, params = db.conn.cursor_obj.executed
    assert "dockets" in sql
    assert "docket_title ILIKE %s" in sql
    assert params == ["%water%"]


# --- _search_dockets_by_cfr tests ---

def test_search_dockets_by_cfr_returns_matching_ids():
    """Returns a set of docket_ids from federal_register_documents matching cfr params"""
    rows = [("DOC-001",), ("DOC-002",)]
    db = DBLayer(conn=_FakeConn(rows))
    result = db._search_dockets_by_cfr([{"title": "Title 42", "part": "413"}])
    assert result == {"DOC-001", "DOC-002"}


def test_search_dockets_by_cfr_empty_param_returns_empty_set():
    """Returns an empty set without querying when cfr_part_param is empty"""
    db = DBLayer(conn=_FakeConn([]))
    result = db._search_dockets_by_cfr([])
    assert result == set()


def test_search_dockets_by_cfr_deduplicates_docket_ids():
    """Multiple CFR parts matching the same docket produce only one entry in the set"""
    rows = [("DOC-001",), ("DOC-001",), ("DOC-001",)]
    db = DBLayer(conn=_FakeConn(rows))
    result = db._search_dockets_by_cfr([{"title": "Title 42", "part": "413"}])
    assert result == {"DOC-001"}
    assert len(result) == 1


def test_search_dockets_by_cfr_queries_federal_register_documents():
    """SQL targets federal_register_documents with cfr_title and cfr_part ILIKE clauses"""
    db = DBLayer(conn=_FakeConn([]))
    db._search_dockets_by_cfr([{"title": "Title 42", "part": "413"}])
    sql, params = db.conn.cursor_obj.executed
    assert "federal_register_documents" in sql
    assert "cfr_title ILIKE %s" in sql
    assert "cfr_part ILIKE %s" in sql
    assert "%Title 42%" in params
    assert "%413%" in params


# --- _search_dockets_by_document_title tests ---

def test_search_dockets_by_document_title_returns_matching_ids():
    """Returns a set of docket_ids whose documents match the query"""
    rows = [("DOC-001",), ("DOC-002",)]
    db = DBLayer(conn=_FakeConn(rows))
    result = db._search_dockets_by_document_title("clean air")
    assert result == {"DOC-001", "DOC-002"}


def test_search_dockets_by_document_title_no_matches_returns_empty_set():
    """Returns an empty set when no document titles match"""
    db = DBLayer(conn=_FakeConn([]))
    result = db._search_dockets_by_document_title("nonexistent")
    assert result == set()


def test_search_dockets_by_document_title_deduplicates_docket_ids():
    """Multiple document title matches under the same docket produce only one id in the set"""
    rows = [("DOC-001",), ("DOC-001",), ("DOC-001",)]
    db = DBLayer(conn=_FakeConn(rows))
    result = db._search_dockets_by_document_title("water")
    assert result == {"DOC-001"}
    assert len(result) == 1


def test_search_dockets_by_document_title_queries_documents_table():
    """SQL targets documents table with document_title ILIKE and wildcard-wrapped query"""
    db = DBLayer(conn=_FakeConn([]))
    db._search_dockets_by_document_title("water quality")
    sql, params = db.conn.cursor_obj.executed
    assert "documents" in sql
    assert "document_title ILIKE %s" in sql
    assert params == ["%water quality%"]


# --- _join_results tests ---

def test_join_results_combines_all_three_sets():
    """Union of three disjoint sets contains all docket ids"""
    db = DBLayer()
    result = db._join_results({"DOC-001"}, {"DOC-002"}, {"DOC-003"})
    assert result == {"DOC-001", "DOC-002", "DOC-003"}


def test_join_results_deduplicates_across_sets():
    """Docket ids appearing in multiple sets are only included once"""
    db = DBLayer()
    result = db._join_results({"DOC-001", "DOC-002"}, {"DOC-002", "DOC-003"},
                              {"DOC-001", "DOC-003"})
    assert result == {"DOC-001", "DOC-002", "DOC-003"}
    assert len(result) == 3


def test_join_results_with_empty_sets():
    """Empty sets are handled gracefully, returning only ids from non-empty sets"""
    db = DBLayer()
    result = db._join_results({"DOC-001"}, set(), set())
    assert result == {"DOC-001"}


def test_join_results_all_empty_returns_empty_set():
    """All empty sets returns an empty set"""
    db = DBLayer()
    result = db._join_results(set(), set(), set())
    assert result == set()


# --- _search_dockets tests ---

def test_search_dockets_returns_empty_when_no_ids_found(monkeypatch):
    """Returns [] immediately when all three helpers return empty sets"""
    db = DBLayer(conn=_FakeConn([]))
    monkeypatch.setattr(DBLayer, "_search_dockets_by_title", lambda self, q: set())
    monkeypatch.setattr(DBLayer, "_search_dockets_by_cfr", lambda self, c: set())
    monkeypatch.setattr(DBLayer, "_search_dockets_by_document_title", lambda self, q: set())
    assert db._search_dockets("anything") == []


def test_search_dockets_returns_docket_details_for_matched_ids(monkeypatch):
    """Returns processed docket details for ids returned by the helpers"""
    rows = [("DOC-001", "Test Docket", "CMS", "Rulemaking", "2024-01-01", None, None, None)]
    db = DBLayer(conn=_FakeConn(rows))
    monkeypatch.setattr(DBLayer, "_search_dockets_by_title", lambda self, q: {"DOC-001"})
    monkeypatch.setattr(DBLayer, "_search_dockets_by_cfr", lambda self, c: set())
    monkeypatch.setattr(DBLayer, "_search_dockets_by_document_title", lambda self, q: set())
    results = db._search_dockets("test")
    assert len(results) == 1
    assert results[0]["docket_id"] == "DOC-001"
    assert results[0]["docket_title"] == "Test Docket"


def test_search_dockets_applies_docket_type_filter(monkeypatch):
    """docket_type_param is added as a filter clause in the details query"""
    db = DBLayer(conn=_FakeConn([]))
    monkeypatch.setattr(DBLayer, "_search_dockets_by_title", lambda self, q: {"DOC-001"})
    monkeypatch.setattr(DBLayer, "_search_dockets_by_cfr", lambda self, c: set())
    monkeypatch.setattr(DBLayer, "_search_dockets_by_document_title", lambda self, q: set())
    db._search_dockets("test", docket_type_param="Rulemaking")
    sql, params = db.conn.cursor_obj.executed
    assert "d.docket_type = %s" in sql
    assert "Rulemaking" in params


def test_search_dockets_applies_agency_filter(monkeypatch):
    """agency filter is added as an ILIKE clause in the details query"""
    db = DBLayer(conn=_FakeConn([]))
    monkeypatch.setattr(DBLayer, "_search_dockets_by_title", lambda self, q: {"DOC-001"})
    monkeypatch.setattr(DBLayer, "_search_dockets_by_cfr", lambda self, c: set())
    monkeypatch.setattr(DBLayer, "_search_dockets_by_document_title", lambda self, q: set())
    db._search_dockets("test", agency=["CMS"])
    sql, params = db.conn.cursor_obj.executed
    assert "agency_id ILIKE %s" in sql
    assert "%CMS%" in params


# --- Factory function tests ---

def test_get_postgres_connection_uses_env_and_dotenv(monkeypatch):
    called = {"dotenv": False}

    def fake_load():
        called["dotenv"] = True

    captured = {}

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return "conn"

    monkeypatch.setattr(db_module, "LOAD_DOTENV", fake_load)
    monkeypatch.setattr(db_module.psycopg2, "connect", fake_connect)
    monkeypatch.setenv("DB_HOST", "dbhost")
    monkeypatch.setenv("DB_PORT", "5433")
    monkeypatch.setenv("DB_NAME", "dbname")
    monkeypatch.setenv("DB_USER", "dbuser")
    monkeypatch.setenv("DB_PASSWORD", "dbpass")

    db = db_module.get_postgres_connection()

    assert isinstance(db, DBLayer)
    assert db.conn == "conn"
    assert called["dotenv"] is True
    assert captured == {
        "host": "dbhost",
        "port": "5433",
        "database": "dbname",
        "user": "dbuser",
        "password": "dbpass",
    }


def test_get_postgres_connection_uses_aws_secrets(monkeypatch):
    """USE_AWS_SECRETS=true uses boto3 to get credentials"""
    fake_creds = {
        "host": "aws-host",
        "port": "5432",
        "db": "aws-db",
        "username": "aws-user",
        "password": "aws-pass",
    }

    class FakeClient:  # pylint: disable=too-few-public-methods
        def get_secret_value(self, **_kwargs):  # pylint: disable=unused-argument
            return {"SecretString": __import__("json").dumps(fake_creds)}

        def describe_secret(self, **_kwargs):  # pylint: disable=unused-argument
            return {}

    fake_boto3 = type("boto3", (), {"client": staticmethod(lambda *a, **kw: FakeClient())})()
    captured = {}

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return "aws-conn"

    monkeypatch.setattr(db_module, "boto3", fake_boto3)
    monkeypatch.setattr(db_module.psycopg2, "connect", fake_connect)
    monkeypatch.setenv("USE_AWS_SECRETS", "true")

    db = db_module.get_postgres_connection()

    assert isinstance(db, DBLayer)
    assert db.conn == "aws-conn"
    assert captured["host"] == "aws-host"
    assert captured["database"] == "aws-db"


def test_get_secrets_from_aws_raises_without_boto3(monkeypatch):
    """_get_secrets_from_aws raises ImportError when boto3 is None"""
    monkeypatch.setattr(db_module, "boto3", None)
    with pytest.raises(ImportError):
        db_module._get_secrets_from_aws()


def test_get_db_uses_postgres_when_env_set(monkeypatch):
    sentinel = DBLayer(conn="conn")
    monkeypatch.setattr(db_module, "get_postgres_connection", lambda: sentinel)

    db = get_db()

    assert db is sentinel


def test_get_opensearch_connection(monkeypatch):
    captured = {}

    def fake_opensearch(**kwargs):
        captured.update(kwargs)
        return "client"

    monkeypatch.setattr(db_module, "OpenSearch", fake_opensearch)

    client = db_module.get_opensearch_connection()

    assert client == "client"
    assert captured["hosts"] == [{"host": "localhost", "port": 9200}]
    assert captured["use_ssl"] is False
    assert captured["verify_certs"] is False

# --- OpenSearch text_match_terms tests ---

class _FakeOpenSearch:  # pylint: disable=too-few-public-methods
    """Fake OpenSearch client that returns canned responses for multiple indices"""
    def __init__(self, doc_buckets, comment_buckets, extracted_buckets):
        self.doc_buckets = doc_buckets
        self.comment_buckets = comment_buckets
        self.extracted_buckets = extracted_buckets
        self.searches = []

    def search(self, index, body):
        self.searches.append((index, body))

        if index == "documents":
            return {
                "aggregations": {
                    "by_docket": {
                        "buckets": self.doc_buckets
                    }
                }
            }
        if index == "comments":
            return {
                "aggregations": {
                    "by_docket": {
                        "buckets": self.comment_buckets
                    }
                }
            }
        if index == "comments_extracted_text":
            return {
                "aggregations": {
                    "by_docket": {
                        "buckets": self.extracted_buckets
                    }
                }
            }
        return {"aggregations": {"by_docket": {"buckets": []}}}


def test_text_match_terms_searches_comments_and_extracted():
    """Test text_match_terms searches comments and extracted text"""
    doc_buckets = []
    comment_buckets = [
        {"key": "CMS-2025-0240", "matching_comments": {"doc_count": 2}}
    ]
    extracted_buckets = [
        {"key": "CMS-2025-0240", "matching_extracted": {"doc_count": 4}}
    ]

    fake_client = _FakeOpenSearch(doc_buckets, comment_buckets, extracted_buckets)
    db = DBLayer()

    results = db.text_match_terms(["medicare"], opensearch_client=fake_client)

    # Should have searched all three indices
    assert len(fake_client.searches) == 3
    assert fake_client.searches[0][0] == "documents"
    assert fake_client.searches[1][0] == "comments"
    assert fake_client.searches[2][0] == "comments_extracted_text"

    # Should combine comment sources: 6 comments (2 + 4)
    assert len(results) == 1
    assert results[0]["docket_id"] == "CMS-2025-0240"
    assert results[0]["comment_match_count"] == 6


def test_text_match_terms_combines_comment_sources():
    """Test that comments and extracted text are both counted as comments"""
    doc_buckets = []
    comment_buckets = [
        {"key": "DEA-2024-0059", "matching_comments": {"doc_count": 1}}
    ]
    extracted_buckets = [
        {"key": "DEA-2024-0059", "matching_extracted": {"doc_count": 1}}
    ]

    fake_client = _FakeOpenSearch(doc_buckets, comment_buckets, extracted_buckets)
    db = DBLayer()

    results = db.text_match_terms(["cannabis"], opensearch_client=fake_client)

    assert len(results) == 1
    assert results[0]["docket_id"] == "DEA-2024-0059"
    assert results[0]["comment_match_count"] == 2  # 1 comment + 1 extracted


def test_text_match_terms_multiple_dockets_comments():
    """Test searching comments across multiple dockets"""
    doc_buckets = []
    comment_buckets = [
        {"key": "CMS-2025-0240", "matching_comments": {"doc_count": 2}},
        {"key": "DEA-2024-0059", "matching_comments": {"doc_count": 1}}
    ]
    extracted_buckets = [
        {"key": "CMS-2025-0240", "matching_extracted": {"doc_count": 4}}
    ]

    fake_client = _FakeOpenSearch(doc_buckets, comment_buckets, extracted_buckets)
    db = DBLayer()

    results = db.text_match_terms(["test"], opensearch_client=fake_client)

    assert len(results) == 2

    cms = next(r for r in results if r["docket_id"] == "CMS-2025-0240")
    assert cms["comment_match_count"] == 6  # 2 + 4

    dea = next(r for r in results if r["docket_id"] == "DEA-2024-0059")
    assert dea["comment_match_count"] == 1


def test_text_match_terms_uses_filtered_aggregations():
    """Verify the OpenSearch queries use filtered aggregations"""
    fake_client = _FakeOpenSearch([], [], [])
    db = DBLayer()

    db.text_match_terms(["medicare", "medicaid"], opensearch_client=fake_client)

    # Check all three queries were made
    assert len(fake_client.searches) == 3

    # Check comments query structure
    comment_index, comment_body = fake_client.searches[1]
    assert comment_index == "comments"
    assert comment_body["size"] == 0
    assert "aggs" in comment_body
    assert "matching_comments" in comment_body["aggs"]["by_docket"]["aggs"]
    assert "filter" in comment_body["aggs"]["by_docket"]["aggs"]["matching_comments"]

    # Check extracted text query structure
    extracted_index, extracted_body = fake_client.searches[2]
    assert extracted_index == "comments_extracted_text"
    assert "matching_extracted" in extracted_body["aggs"]["by_docket"]["aggs"]


def test_text_match_terms_returns_correct_structure():
    """Verify each result has the required fields"""
    doc_buckets = []
    comment_buckets = [{"key": "TEST-001", "matching_comments": {"doc_count": 5}}]
    extracted_buckets = []

    fake_client = _FakeOpenSearch(doc_buckets, comment_buckets, extracted_buckets)
    db = DBLayer()

    results = db.text_match_terms(["test"], opensearch_client=fake_client)

    assert len(results) == 1
    assert "docket_id" in results[0]
    assert "document_match_count" in results[0]
    assert "comment_match_count" in results[0]
    assert isinstance(results[0]["docket_id"], str)
    assert isinstance(results[0]["document_match_count"], int)
    assert isinstance(results[0]["comment_match_count"], int)


def test_text_match_terms_handles_empty_results():
    """When OpenSearch returns no buckets, return empty list"""
    fake_client = _FakeOpenSearch([], [], [])
    db = DBLayer()

    results = db.text_match_terms(["nonexistent"], opensearch_client=fake_client)

    assert not results


def test_text_match_terms_only_returns_comment_matches():
    """Only dockets with comment match_count > 0 are included"""
    doc_buckets = []
    comment_buckets = [
        {"key": "HAS-MATCH", "matching_comments": {"doc_count": 5}},
        {"key": "NO-MATCH", "matching_comments": {"doc_count": 0}}
    ]
    extracted_buckets = []

    fake_client = _FakeOpenSearch(doc_buckets, comment_buckets, extracted_buckets)
    db = DBLayer()

    results = db.text_match_terms(["test"], opensearch_client=fake_client)

    assert len(results) == 1
    assert results[0]["docket_id"] == "HAS-MATCH"


def test_text_match_terms_docket_only_in_comments():
    """When a docket only has matching comment text"""
    doc_buckets = []
    comment_buckets = [{"key": "COMMENT-ONLY", "matching_comments": {"doc_count": 10}}]
    extracted_buckets = []

    fake_client = _FakeOpenSearch(doc_buckets, comment_buckets, extracted_buckets)
    db = DBLayer()

    results = db.text_match_terms(["test"], opensearch_client=fake_client)

    assert len(results) == 1
    assert results[0]["docket_id"] == "COMMENT-ONLY"
    assert results[0]["comment_match_count"] == 10


def test_text_match_terms_docket_only_in_extracted():
    """When a docket only has matching extracted text"""
    doc_buckets = []
    comment_buckets = []
    extracted_buckets = [{"key": "EXTRACTED-ONLY", "matching_extracted": {"doc_count": 3}}]

    fake_client = _FakeOpenSearch(doc_buckets, comment_buckets, extracted_buckets)
    db = DBLayer()

    results = db.text_match_terms(["test"], opensearch_client=fake_client)

    assert len(results) == 1
    assert results[0]["docket_id"] == "EXTRACTED-ONLY"
    assert results[0]["comment_match_count"] == 3
