"""Tests for OAuthHandler JWT creation and validation"""
import datetime
import pytest
import jwt
from mirrsearch.oauth_handler import (
    OAuthHandler,
    TokenExpiredError,
    TokenInvalidError,
)


@pytest.fixture
def handler():
    """Create an OAuthHandler with test credentials"""
    return OAuthHandler(
        base_url="http://localhost",
        google_client_id="test-client-id",
        google_client_secret="test-client-secret",
        jwt_secret="test-secret"
    )


def test_create_jwt_token(handler):  # pylint: disable=redefined-outer-name
    """create_jwt_token returns a valid JWT string"""
    token = handler.create_jwt_token("Alice|alice@example.com")
    assert isinstance(token, str)
    payload = jwt.decode(token, "test-secret", algorithms=["HS256"])
    assert payload["user_id"] == "Alice|alice@example.com"


def test_validate_jwt_token_returns_user_id(handler):  # pylint: disable=redefined-outer-name
    """validate_jwt_token returns user_id from a valid token"""
    token = handler.create_jwt_token("Alice|alice@example.com")
    result = handler.validate_jwt_token(token)
    assert result == "Alice|alice@example.com"


def test_validate_jwt_token_expired(handler):  # pylint: disable=redefined-outer-name
    """validate_jwt_token raises TokenExpiredError for an expired token"""
    now = datetime.datetime.now(datetime.timezone.utc)
    payload = {
        "user_id": "Alice|alice@example.com",
        "exp": now - datetime.timedelta(seconds=1),
        "iat": now - datetime.timedelta(days=1),
    }
    token = jwt.encode(payload, "test-secret", algorithm="HS256")
    with pytest.raises(TokenExpiredError):
        handler.validate_jwt_token(token)


def test_validate_jwt_token_invalid(handler):  # pylint: disable=redefined-outer-name
    """validate_jwt_token raises TokenInvalidError for a bad token"""
    with pytest.raises(TokenInvalidError):
        handler.validate_jwt_token("not.a.valid.token")


def test_validate_jwt_token_wrong_secret(handler):  # pylint: disable=redefined-outer-name
    """validate_jwt_token raises TokenInvalidError when signed with wrong secret"""
    token = jwt.encode({"user_id": "x"}, "wrong-secret", algorithm="HS256")
    with pytest.raises(TokenInvalidError):
        handler.validate_jwt_token(token)


def test_get_authorization_url(handler):  # pylint: disable=redefined-outer-name
    """get_authorization_url returns a Google auth URL and state"""
    url, _ = handler.get_authorization_url()
    assert "accounts.google.com" in url
    assert "test-client-id" in url
