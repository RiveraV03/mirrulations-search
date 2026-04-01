"""
Tests for the Flask app endpoints - Header-based pagination (returns list)
"""
import tempfile
import os
from unittest.mock import patch, MagicMock
import pytest
from mock_db import MockDBLayer
from mirrsearch.app import create_app
from mirrsearch.db import get_postgres_connection, get_opensearch_connection

# pylint: disable=duplicate-code
class MockOAuthHandler:
    """Mock OAuth handler that always authenticates as a test user"""
    def get_authorization_url(self):
        return "http://mock-auth-url", None

    def validate_jwt_token(self, token):  # pylint: disable=unused-argument
        return "Test User|test@example.com"

    def exchange_code_for_user_info(self, code):  # pylint: disable=unused-argument
        return {"name": "Test User", "email": "test@example.com"}

    def create_jwt_token(self, user_id):  # pylint: disable=unused-argument
        return "mock-token"


@pytest.fixture
def app(tmp_path):
    """Create and configure a test app instance"""
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "index.html").write_text("<html></html>")
    test_app = create_app(
        dist_dir=str(dist), db_layer=MockDBLayer(), oauth_handler=MockOAuthHandler()
    )
    test_app.config['TESTING'] = True
    return test_app


@pytest.fixture
def client(app):  # pylint: disable=redefined-outer-name
    """Create a test client for the app"""
    c = app.test_client()
    c.set_cookie("jwt_token", "mock-token")
    return c


def test_search_endpoint_exists(client):  # pylint: disable=redefined-outer-name
    """Test that the search endpoint exists and returns 200"""
    response = client.get('/search/')
    assert response.status_code == 200


def test_search_returns_list(client):  # pylint: disable=redefined-outer-name
    """Test that search endpoint returns a list (not dict)"""
    response = client.get('/search/')
    assert response.status_code == 200
    assert response.is_json
    data = response.get_json()
    assert isinstance(data, list)


def test_search_has_pagination_headers(client):  # pylint: disable=redefined-outer-name
    """Test that pagination metadata is in HTTP headers"""
    response = client.get('/search/')

    assert 'X-Page' in response.headers
    assert 'X-Page-Size' in response.headers
    assert 'X-Total-Results' in response.headers
    assert 'X-Total-Pages' in response.headers
    assert 'X-Has-Next' in response.headers
    assert 'X-Has-Prev' in response.headers


def test_search_with_query_parameter(client):  # pylint: disable=redefined-outer-name
    """Test search endpoint with query parameter"""
    response = client.get('/search/?str=ESRD')
    data = response.get_json()
    assert isinstance(data, list)
    assert len(data) > 0
    assert any('ESRD' in item['title'] for item in data)


def test_search_with_different_query_parameters(client):  # pylint: disable=redefined-outer-name
    """Test search endpoint with various query parameters"""
    data1 = client.get('/search/?str=CMS-2025-024').get_json()
    assert isinstance(data1, list)
    assert len(data1) > 0
    assert all(item['docket_id'].startswith('CMS-2025-024') for item in data1)

    data2 = client.get('/search/?str=ESRD').get_json()
    assert isinstance(data2, list)
    assert len(data2) > 0
    assert any('ESRD' in item['title'] for item in data2)

    data3 = client.get('/search/?str=CMS').get_json()
    assert isinstance(data3, list)
    assert len(data3) > 0
    assert all(item['agency_id'] == 'CMS' for item in data3)


def test_search_without_filter_returns_all_matches(client):  # pylint: disable=redefined-outer-name
    """Search without filter returns all matching documents"""
    response = client.get('/search/?str=renal')
    assert response.status_code == 200
    data = response.get_json()
    assert isinstance(data, list)
    assert len(data) > 0


def test_search_with_valid_filter_returns_matching_docket_type(client):  # pylint: disable=redefined-outer-name
    """Filter param restricts results to the specified docket_type"""
    response = client.get('/search/?str=renal&docket_type=Proposed Rule')
    assert response.status_code == 200
    data = response.get_json()
    assert isinstance(data, list)
    assert len(data) > 0
    for doc in data:
        assert doc['document_type'] == 'Proposed Rule'



def test_search_with_filter_only_affects_docket_type(client):  # pylint: disable=redefined-outer-name
    """Filter only restricts docket_type; other fields are unaffected"""
    response = client.get('/search/?str=ESRD&docket_type=Proposed Rule')
    assert response.status_code == 200
    data = response.get_json()
    assert isinstance(data, list)
    assert len(data) > 0
    for item in data:
        assert 'ESRD' in item['title'] or 'esrd' in item['title'].lower()
        assert item['document_type'] == 'Proposed Rule'


def test_search_with_nonexistent_filter_returns_empty_list(client):  # pylint: disable=redefined-outer-name
    """A filter value matching no docket_type returns an empty list"""
    response = client.get('/search/?str=renal&docket_type=Final Rule')
    assert response.status_code == 200
    data = response.get_json()
    assert isinstance(data, list)
    assert len(data) == 0


def test_search_filter_without_query_string_uses_default(client):  # pylint: disable=redefined-outer-name
    """If str is missing, defaults to 'example_query' which matches nothing"""
    response = client.get('/search/?docket_type=Proposed Rule')
    assert response.status_code == 200
    data = response.get_json()
    assert isinstance(data, list)
    assert len(data) == 0


def test_search_filter_result_structure(client):  # pylint: disable=redefined-outer-name
    """Filtered results have all required fields"""
    response = client.get('/search/?str=CMS&docket_type=Proposed Rule')
    assert response.status_code == 200
    data = response.get_json()
    assert isinstance(data, list)
    assert len(data) > 0
    required_fields = ['docket_id', 'title', 'cfrPart', 'agency_id', 'document_type']
    for item in data:
        for field in required_fields:
            assert field in item, f"Result missing field: {field}"


def test_search_with_agency_filter(client):  # pylint: disable=redefined-outer-name
    """Agency param restricts results to the specified agency_id"""
    response = client.get('/search/?str=renal&agency=CMS')
    assert response.status_code == 200
    data = response.get_json()
    assert isinstance(data, list)
    assert len(data) > 0
    for doc in data:
        assert doc['agency_id'] == 'CMS'


def test_search_with_multiple_agency_filters(client):  # pylint: disable=redefined-outer-name
    """Multiple agency params return results matching any of them"""
    response = client.get('/search/?str=renal&agency=CMS&agency=EPA')
    assert response.status_code == 200
    data = response.get_json()
    assert isinstance(data, list)
    assert len(data) > 0
    for doc in data:
        assert doc['agency_id'] in ('CMS', 'EPA')


def test_search_with_nonexistent_agency_returns_empty_list(client):  # pylint: disable=redefined-outer-name
    """An agency value matching no agency_id returns an empty list"""
    response = client.get('/search/?str=renal&agency=FDA')
    assert response.status_code == 200
    data = response.get_json()
    assert isinstance(data, list)
    assert len(data) == 0


def test_search_with_multiple_cfr_part_filters(client):  # pylint: disable=redefined-outer-name
    """Multiple cfr_part params return results matching any of them"""
    response = client.get('/search/?str=renal&cfr_part=42 CFR Parts 413 and 512:413'
                          '&cfr_part=42 CFR Parts 413 and 512:512')
    assert response.status_code == 200
    data = response.get_json()
    assert isinstance(data, list)
    assert len(data) > 0


def test_search_with_agency_and_filter(client):  # pylint: disable=redefined-outer-name
    """Both agency and filter params can be combined"""
    response = client.get('/search/?str=renal&agency=CMS&docket_type=Proposed Rule')
    assert response.status_code == 200
    data = response.get_json()
    assert isinstance(data, list)
    assert len(data) > 0
    for doc in data:
        assert doc['agency_id'] == 'CMS'
        assert doc['document_type'] == 'Proposed Rule'


def test_search_returns_401_without_cookie(app):  # pylint: disable=redefined-outer-name
    """Search endpoint returns 401 when no JWT cookie is present"""
    anon = app.test_client()
    response = anon.get('/search/')
    assert response.status_code == 401


def test_login_route_redirects(app):  # pylint: disable=redefined-outer-name
    """Login route redirects to Google authorization URL"""
    anon = app.test_client()
    response = anon.get('/login')
    assert response.status_code == 302
    assert "mock-auth-url" in response.headers['Location']


def test_logout_route_clears_cookie(app):  # pylint: disable=redefined-outer-name
    """Logout route redirects to home and clears jwt_token cookie"""
    anon = app.test_client()
    anon.set_cookie("jwt_token", "mock-token")
    response = anon.get('/logout')
    assert response.status_code == 302
    assert any(
        'jwt_token' in h and 'expires' in h.lower()
        for h in response.headers.getlist('Set-Cookie')
    )


def test_auth_status_logged_in(client):  # pylint: disable=redefined-outer-name
    """Auth status returns logged_in true when valid cookie present"""
    response = client.get('/auth/status')
    assert response.status_code == 200
    data = response.get_json()
    assert data['logged_in'] is True
    assert data['name'] == 'Test User'
    assert data['email'] == 'test@example.com'


def test_auth_status_not_logged_in(app):  # pylint: disable=redefined-outer-name
    """Auth status returns logged_in false when no cookie"""
    anon = app.test_client()
    response = anon.get('/auth/status')
    assert response.status_code == 200
    data = response.get_json()
    assert data['logged_in'] is False


def test_invalid_cookie_treated_as_unauthenticated(tmp_path):
    """An invalid JWT cookie results in 401 on search"""
    from mirrsearch.oauth_handler import TokenInvalidError  # pylint: disable=import-outside-toplevel

    class RejectingOAuthHandler:  # pylint: disable=too-few-public-methods
        """OAuth handler that always rejects tokens"""
        def validate_jwt_token(self, token):  # pylint: disable=unused-argument
            raise TokenInvalidError("bad token")

    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "index.html").write_text("<html></html>")
    test_app = create_app(
        dist_dir=str(dist), db_layer=MockDBLayer(), oauth_handler=RejectingOAuthHandler()
    )
    test_app.config['TESTING'] = True
    anon = test_app.test_client()
    anon.set_cookie("jwt_token", "invalid-token")
    response = anon.get('/search/')
    assert response.status_code == 401


def test_home_route_with_oauth_code_redirects(app):  # pylint: disable=redefined-outer-name
    """Home route exchanges OAuth code and redirects"""
    anon = app.test_client()
    response = anon.get('/?code=valid-code')
    assert response.status_code == 302
    assert 'jwt_token' in response.headers.get('Set-Cookie', '')


def test_home_route_with_bad_oauth_code_redirects(tmp_path):
    """Home route redirects to / when OAuth code exchange fails"""
    class FailingOAuthHandler:
        """OAuth handler that always fails code exchange"""
        def exchange_code_for_user_info(self, code):  # pylint: disable=unused-argument
            from mirrsearch.oauth_handler import OAuthCodeError  # pylint: disable=import-outside-toplevel
            raise OAuthCodeError("bad code")

        def validate_jwt_token(self, token):  # pylint: disable=unused-argument
            from mirrsearch.oauth_handler import TokenInvalidError  # pylint: disable=import-outside-toplevel
            raise TokenInvalidError("invalid")

    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "index.html").write_text("<html></html>")
    test_app = create_app(
        dist_dir=str(dist), db_layer=MockDBLayer(), oauth_handler=FailingOAuthHandler()
    )
    test_app.config['TESTING'] = True
    anon = test_app.test_client()
    response = anon.get('/?code=bad-code')
    assert response.status_code == 302


def test_home_route_with_index_html():
    """Test home route serves index.html"""
    with tempfile.TemporaryDirectory() as tmpdir:
        index_path = os.path.join(tmpdir, 'index.html')
        with open(index_path, 'w', encoding='utf-8') as f:
            f.write('<html><body>Home</body></html>')

        test_app = create_app(dist_dir=tmpdir, db_layer=MockDBLayer())
        test_client = test_app.test_client()

        response = test_client.get('/')
        assert response.status_code == 200
        assert b'Home' in response.data

# --- Collections ---

def test_get_collections_returns_empty_list(client):  # pylint: disable=redefined-outer-name
    """GET /collections returns empty list when user has no collections"""
    response = client.get('/collections')
    assert response.status_code == 200
    assert response.get_json() == []


def test_get_collections_requires_auth(app):  # pylint: disable=redefined-outer-name
    """GET /collections returns 401 without cookie"""
    response = app.test_client().get('/collections')
    assert response.status_code == 401


def test_create_collection_returns_id(client):  # pylint: disable=redefined-outer-name
    """POST /collections creates a collection and returns its id"""
    response = client.post('/collections', json={"name": "My Collection"})
    assert response.status_code == 201
    data = response.get_json()
    assert "collection_id" in data
    assert isinstance(data["collection_id"], int)


def test_create_collection_requires_name(client):  # pylint: disable=redefined-outer-name
    """POST /collections returns 400 when name is missing"""
    response = client.post('/collections', json={})
    assert response.status_code == 400


def test_create_collection_requires_auth(app):  # pylint: disable=redefined-outer-name
    """POST /collections returns 401 without cookie"""
    response = app.test_client().post('/collections', json={"name": "Test"})
    assert response.status_code == 401


def test_delete_collection(client):  # pylint: disable=redefined-outer-name
    """DELETE /collections/<id> deletes an existing collection"""
    collection_id = client.post(
        '/collections', json={"name": "To Delete"}
    ).get_json()["collection_id"]
    response = client.delete(f'/collections/{collection_id}')
    assert response.status_code == 204


def test_delete_collection_not_found(client):  # pylint: disable=redefined-outer-name
    """DELETE /collections/<id> returns 404 for nonexistent collection"""
    response = client.delete('/collections/9999')
    assert response.status_code == 404


def test_delete_collection_requires_auth(app):  # pylint: disable=redefined-outer-name
    """DELETE /collections/<id> returns 401 without cookie"""
    response = app.test_client().delete('/collections/1')
    assert response.status_code == 401


def test_add_docket_to_collection(client):  # pylint: disable=redefined-outer-name
    """POST /collections/<id>/dockets adds a docket to a collection"""
    collection_id = client.post(
        '/collections', json={"name": "My List"}
    ).get_json()["collection_id"]
    response = client.post(
        f'/collections/{collection_id}/dockets', json={"docket_id": "CMS-2025-0240"}
    )
    assert response.status_code == 204


def test_add_docket_requires_docket_id(client):  # pylint: disable=redefined-outer-name
    """POST /collections/<id>/dockets returns 400 when docket_id is missing"""
    collection_id = client.post(
        '/collections', json={"name": "My List"}
    ).get_json()["collection_id"]
    response = client.post(f'/collections/{collection_id}/dockets', json={})
    assert response.status_code == 400


def test_add_docket_collection_not_found(client):  # pylint: disable=redefined-outer-name
    """POST /collections/<id>/dockets returns 404 for nonexistent collection"""
    response = client.post('/collections/9999/dockets', json={"docket_id": "CMS-2025-0240"})
    assert response.status_code == 404


def test_add_docket_requires_auth(app):  # pylint: disable=redefined-outer-name
    """POST /collections/<id>/dockets returns 401 without cookie"""
    response = app.test_client().post(
        '/collections/1/dockets', json={"docket_id": "CMS-2025-0240"}
    )
    assert response.status_code == 401


def test_remove_docket_from_collection(client):  # pylint: disable=redefined-outer-name
    """DELETE /collections/<id>/dockets/<docket_id> removes a docket"""
    collection_id = client.post(
        '/collections', json={"name": "My List"}
    ).get_json()["collection_id"]
    client.post(f'/collections/{collection_id}/dockets', json={"docket_id": "CMS-2025-0240"})
    response = client.delete(f'/collections/{collection_id}/dockets/CMS-2025-0240')
    assert response.status_code == 204


def test_remove_docket_collection_not_found(client):  # pylint: disable=redefined-outer-name
    """DELETE /collections/<id>/dockets/<docket_id> returns 404 for nonexistent collection"""
    response = client.delete('/collections/9999/dockets/CMS-2025-0240')
    assert response.status_code == 404


def test_remove_docket_requires_auth(app):  # pylint: disable=redefined-outer-name
    """DELETE /collections/<id>/dockets/<docket_id> returns 401 without cookie"""
    response = app.test_client().delete('/collections/1/dockets/CMS-2025-0240')
    assert response.status_code == 401


def test_get_collections_shows_added_dockets(client):  # pylint: disable=redefined-outer-name
    """GET /collections reflects dockets added to a collection"""
    collection_id = client.post(
        '/collections', json={"name": "My List"}
    ).get_json()["collection_id"]
    client.post(f'/collections/{collection_id}/dockets', json={"docket_id": "CMS-2025-0240"})
    data = client.get('/collections').get_json()
    match = next(c for c in data if c["collection_id"] == collection_id)
    assert "CMS-2025-0240" in match["docket_ids"]


def test_agencies_returns_list(client): # pylint: disable=redefined-outer-name
    """Test that agencies endpoint returns a JSON list"""
    response = client.get('/agencies')
    assert response.status_code == 200
    assert response.is_json
    data = response.get_json()
    assert isinstance(data, list)


@patch('mirrsearch.db.psycopg2.connect')
def test_get_postgres_connection(mock_connect):
    """Test postgres connection"""
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    with patch.dict(os.environ, {
        'DB_HOST': 'localhost',
        'DB_PORT': '5432',
        'DB_NAME': 'test',
        'DB_USER': 'test',
        'DB_PASSWORD': 'test'
    }):
        result = get_postgres_connection()
        assert result.conn == mock_conn
        mock_connect.assert_called_once()


@patch('mirrsearch.db.OpenSearch')
def test_get_opensearch_connection(mock_opensearch):
    """Test opensearch connection"""
    get_opensearch_connection()
    mock_opensearch.assert_called_once()
