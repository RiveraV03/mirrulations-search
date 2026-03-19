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


def test_search_dockets_postgres_cfr_part_filter():
    """CFR part filter adds ILIKE clause"""
    db = DBLayer(conn=_FakeConn([]))
    db._search_dockets_postgres("", cfr_part_param=["42"])
    sql, params = db.conn.cursor_obj.executed
    assert "cp.cfrPart ILIKE %s" in sql
    assert params == ["%%", "%42%"]


def test_search_dockets_postgres_cfr_part_multi_filter():
    """Multiple CFR parts produce OR'd ILIKE clauses"""
    db = DBLayer(conn=_FakeConn([]))
    db._search_dockets_postgres("", cfr_part_param=["42", "45"])
    sql, params = db.conn.cursor_obj.executed
    assert sql.count("cp.cfrPart ILIKE %s") == 2
    assert "%42%" in params
    assert "%45%" in params


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
    def __init__(self, doc_buckets, comment_buckets):
        self.doc_buckets = doc_buckets
        self.comment_buckets = comment_buckets
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
        elif index == "comments":
            return {
                "aggregations": {
                    "by_docket": {
                        "buckets": self.comment_buckets
                    }
                }
            }
        return {"aggregations": {"by_docket": {"buckets": []}}}


def test_text_match_terms_with_fake_opensearch():
    """Test text_match_terms searches both indices and combines results"""
    doc_buckets = [
        {"key": "DEA-2024-0059", "doc_count": 3},
        {"key": "CMS-2025-0240", "doc_count": 2}
    ]
    comment_buckets = [
        {"key": "DEA-2024-0059", "doc_count": 2},
        {"key": "CMS-2025-0240", "doc_count": 4}
    ]
    
    fake_client = _FakeOpenSearch(doc_buckets, comment_buckets)
    db = DBLayer()
    
    results = db.text_match_terms(["meaningful use"], opensearch_client=fake_client)
    
    # Should have searched both indices
    assert len(fake_client.searches) == 2
    assert fake_client.searches[0][0] == "documents"
    assert fake_client.searches[1][0] == "comments"
    
    # Should combine results from both
    assert len(results) == 2
    
    dea_result = next(r for r in results if r["docket_id"] == "DEA-2024-0059")
    assert dea_result["document_match_count"] == 3
    assert dea_result["comment_match_count"] == 2
    
    cms_result = next(r for r in results if r["docket_id"] == "CMS-2025-0240")
    assert cms_result["document_match_count"] == 2
    assert cms_result["comment_match_count"] == 4


def test_text_match_terms_updates_query():
    """Test that searching for 'updates' returns correct counts from dummy data"""
    doc_buckets = [
        {"key": "CMS-2025-0240", "doc_count": 2}
    ]
    comment_buckets = [
        {"key": "CMS-2025-0240", "doc_count": 1}
    ]
    
    fake_client = _FakeOpenSearch(doc_buckets, comment_buckets)
    db = DBLayer()
    
    results = db.text_match_terms(["updates"], opensearch_client=fake_client)
    
    assert len(results) == 1
    assert results[0]["docket_id"] == "CMS-2025-0240"
    assert results[0]["document_match_count"] == 2
    assert results[0]["comment_match_count"] == 1


def test_text_match_terms_meaningful_use_query():
    """Test that searching for 'meaningful use' returns correct counts"""
    doc_buckets = [
        {"key": "DEA-2024-0059", "doc_count": 3}
    ]
    comment_buckets = [
        {"key": "DEA-2024-0059", "doc_count": 2}
    ]
    
    fake_client = _FakeOpenSearch(doc_buckets, comment_buckets)
    db = DBLayer()
    
    results = db.text_match_terms(["meaningful use"], opensearch_client=fake_client)
    
    assert len(results) == 1
    assert results[0]["docket_id"] == "DEA-2024-0059"
    assert results[0]["document_match_count"] == 3
    assert results[0]["comment_match_count"] == 2


def test_text_match_terms_builds_correct_queries():
    """Verify the OpenSearch queries are structured correctly for both indices"""
    fake_client = _FakeOpenSearch([], [])
    db = DBLayer()
    
    db.text_match_terms(["medicare", "medicaid"], opensearch_client=fake_client)
    
    # Should have made 2 searches
    assert len(fake_client.searches) == 2
    
    # Check documents query
    doc_index, doc_body = fake_client.searches[0]
    assert doc_index == "documents"
    assert doc_body["size"] == 0
    assert "query" in doc_body
    assert "aggs" in doc_body
    assert doc_body["aggs"]["by_docket"]["terms"]["field"] == "docketId.keyword"
    
    # Check comments query
    comment_index, comment_body = fake_client.searches[1]
    assert comment_index == "comments"
    assert comment_body["size"] == 0
    assert "query" in comment_body
    assert "aggs" in comment_body
    assert comment_body["aggs"]["by_docket"]["terms"]["field"] == "docketId.keyword"


def test_text_match_terms_returns_correct_structure():
    """Verify each result has the required fields"""
    doc_buckets = [{"key": "TEST-001", "doc_count": 1}]
    comment_buckets = [{"key": "TEST-001", "doc_count": 0}]
    
    fake_client = _FakeOpenSearch(doc_buckets, comment_buckets)
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
    fake_client = _FakeOpenSearch([], [])
    db = DBLayer()
    
    results = db.text_match_terms(["nonexistent"], opensearch_client=fake_client)
    
    assert not results


def test_text_match_terms_docket_only_in_documents():
    """When a docket only has matching documents (no matching comments)"""
    doc_buckets = [{"key": "DOC-ONLY", "doc_count": 5}]
    comment_buckets = []
    
    fake_client = _FakeOpenSearch(doc_buckets, comment_buckets)
    db = DBLayer()
    
    results = db.text_match_terms(["test"], opensearch_client=fake_client)
    
    assert len(results) == 1
    assert results[0]["docket_id"] == "DOC-ONLY"
    assert results[0]["document_match_count"] == 5
    assert results[0]["comment_match_count"] == 0


def test_text_match_terms_docket_only_in_comments():
    """When a docket only has matching comments (no matching documents)"""
    doc_buckets = []
    comment_buckets = [{"key": "COMMENT-ONLY", "doc_count": 10}]
    
    fake_client = _FakeOpenSearch(doc_buckets, comment_buckets)
    db = DBLayer()
    
    results = db.text_match_terms(["test"], opensearch_client=fake_client)
    
    assert len(results) == 1
    assert results[0]["docket_id"] == "COMMENT-ONLY"
    assert results[0]["document_match_count"] == 0
    assert results[0]["comment_match_count"] == 10