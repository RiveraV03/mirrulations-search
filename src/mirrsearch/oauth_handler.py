import datetime
import jwt
from google_auth_oauthlib.flow import Flow
from google.oauth2 import id_token
import google.auth.transport.requests
from google.auth import exceptions as google_auth_exceptions
from oauthlib.oauth2.rfc6749 import errors as oauth2_errors


class TokenExpiredError(Exception):
    """Raised when JWT token has expired"""


class TokenInvalidError(Exception):
    """Raised when JWT token is invalid (malformed, wrong secret, etc.)"""


class OAuthCodeError(Exception):
    """Raised when OAuth authorization code is invalid or expired"""


class OAuthVerificationError(Exception):
    """Raised when OAuth token verification fails"""


class OAuthHandler:
    """Handles Google OAuth authentication and JWT token management"""

    def __init__(self, base_url, google_client_id, google_client_secret, jwt_secret):
        """
        Initialize OAuth handler.

        Args:
            base_url: Base URL of the application (e.g., "http://localhost:8000" or
                     "https://colemanbcostsharing.moraviancs.click")
            google_client_id: Google OAuth client ID
            google_client_secret: Google OAuth client secret
            jwt_secret: Secret key for JWT token signing
        """
        self.redirect_uri = base_url
        self.google_client_id = google_client_id
        self.google_client_secret = google_client_secret
        self.jwt_secret = jwt_secret

        # Cache client config (includes redirect_uri)
        self._client_config = {
            "web": {
                "client_id": self.google_client_id,
                "client_secret": self.google_client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": [self.redirect_uri]
            }
        }

        # Cache scopes (don't change)
        self._scopes = [
            "openid",
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/userinfo.profile"  # provides name of user
        ]

    def _create_oauth_flow(self):
        """
        Create a new OAuth flow instance.
        Note: Flow objects maintain state, so we create fresh for each request.

        Returns:
            Flow object configured for Google OAuth
        """
        return Flow.from_client_config(
            self._client_config,
            scopes=self._scopes,
            redirect_uri=self.redirect_uri,
            autogenerate_code_verifier=False
        )

    def get_authorization_url(self):
        """
        Get Google OAuth authorization URL.

        Returns:
            Tuple of (authorization_url, state)
        """
        flow = self._create_oauth_flow()
        flow.code_verifier = None
        authorization_url, state = flow.authorization_url(
            access_type='offline',
            include_granted_scopes='true'
        )
        return authorization_url, state

    # Not testable because it makes a network request to Google
    def exchange_code_for_user_info(self, oauth_code): # pragma: no cover
        """
        Exchange OAuth authorization code for user information.

        Args:
            oauth_code: Authorization code from Google OAuth callback

        Returns:
            Dict with keys: email, name

        Raises:
            OAuthCodeError: If authorization code is invalid or expired
            OAuthVerificationError: If token verification fails
        """
        try:
            flow = self._create_oauth_flow()
            flow.fetch_token(code=oauth_code)

            # Verify the ID token
            credentials = flow.credentials
            request_obj = google.auth.transport.requests.Request()
            try:
                id_info = id_token.verify_oauth2_token(
                    credentials.id_token,
                    request_obj,
                    self.google_client_id
                )
            except (ValueError, google_auth_exceptions.GoogleAuthError) as e:
                raise OAuthVerificationError(f"Token verification failed: {str(e)}") from e

            # Extract required fields from verified token
            # These should always be present in a valid Google ID token
            if 'email' not in id_info or 'name' not in id_info:
                raise OAuthVerificationError(
                    "Token missing required fields (email or name)"
                )

            return {
                "email": id_info['email'],
                "name": id_info['name']
            }
        except oauth2_errors.OAuth2Error as e:
            raise OAuthCodeError(f"Invalid or expired authorization code: {str(e)}") from e
        except ValueError as e:
            # ValueError from fetch_token (code format issues)
            raise OAuthCodeError(f"Invalid authorization code: {str(e)}") from e

    def create_jwt_token(self, user_id, expiration_days=7):
        """
        Create a JWT token for authenticated user.

        Args:
            user_id: User ID to encode in token
            expiration_days: Number of days until token expires (default: 7)

        Returns:
            JWT token string
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        payload = {
            'user_id': user_id,
            'exp': now + datetime.timedelta(days=expiration_days),
            'iat': now
        }
        return jwt.encode(
            payload,
            self.jwt_secret,
            algorithm='HS256'
        )

    def validate_jwt_token(self, token):
        """
        Validate JWT token and extract user ID.

        Args:
            token: JWT token string

        Returns:
            user_id from token

        Raises:
            TokenExpiredError: If token has expired
            TokenInvalidError: If token is invalid (malformed, wrong secret, etc.)
        """
        try:
            payload = jwt.decode(
                token,
                self.jwt_secret,
                algorithms=['HS256']
            )
            return payload['user_id']
        except jwt.ExpiredSignatureError as e:
            raise TokenExpiredError("Token has expired") from e
        except jwt.InvalidTokenError as e:
            raise TokenInvalidError(f"Invalid token: {str(e)}") from e
