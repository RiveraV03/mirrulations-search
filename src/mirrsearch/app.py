"""Flask application with pagination via HTTP headers"""
import os
from flask import Flask, request, jsonify, send_from_directory, redirect, make_response
from mirrsearch.internal_logic import InternalLogic
from mirrsearch.oauth_handler import OAuthHandler, OAuthCodeError, OAuthVerificationError
from mirrsearch.oauth_handler import TokenExpiredError, TokenInvalidError


def _get_search_params():
    """Extract and validate search parameters from the request."""
    cfr_parts_raw = [v for v in request.args.getlist('cfr_part') if v]
    cfr_parts_parsed = None

    if cfr_parts_raw:
        cfr_parts_parsed = []
        for cfr_str in cfr_parts_raw:
            # Ignore malformed values instead of crashing request parsing.
            if ':' not in cfr_str:
                continue
            title, part = cfr_str.split(':', 1)
            if not title or not part:
                continue
            cfr_parts_parsed.append({'title': title, 'part': part})

    return {
        'search_input': request.args.get('str') or 'example_query',
        'docket_type': request.args.get('docket_type'),
        'agency': [v for v in request.args.getlist('agency') if v] or None,
        'cfr_part': cfr_parts_parsed,
        'start_date': request.args.get('start_date') or None,
        'end_date': request.args.get('end_date') or None,
    }


def _get_pagination_params():
    """Extract and validate pagination parameters from the request."""
    page = max(request.args.get('page', default=1, type=int), 1)
    page_size = request.args.get('page_size', default=10, type=int)
    if page_size < 1 or page_size > 100:
        page_size = 10
    return page, page_size


def _build_paginated_response(results, pagination):
    """Build a JSON response with pagination metadata in HTTP headers."""
    response = jsonify(results)
    response.headers['X-Page'] = str(pagination['page'])
    response.headers['X-Page-Size'] = str(pagination['page_size'])
    response.headers['X-Total-Results'] = str(pagination['total_results'])
    response.headers['X-Total-Pages'] = str(pagination['total_pages'])
    response.headers['X-Has-Next'] = str(pagination['has_next']).lower()
    response.headers['X-Has-Prev'] = str(pagination['has_prev']).lower()
    return response


def _make_oauth_handler():
    """Create OAuthHandler from environment variables or AWS Secrets Manager."""
    use_aws = os.getenv("USE_AWS_SECRETS", "").lower() in {"1", "true", "yes", "on"}
    if use_aws:
        return _make_oauth_handler_from_aws()
    return OAuthHandler(
        base_url=os.getenv("BASE_URL", "http://localhost:80"),
        google_client_id=os.getenv("GOOGLE_CLIENT_ID", ""),
        google_client_secret=os.getenv("GOOGLE_CLIENT_SECRET", ""),
        jwt_secret=os.getenv("JWT_SECRET", "dev-secret")
    )


def _make_oauth_handler_from_aws():
    """Create OAuthHandler from AWS Secrets Manager."""
    import boto3  # pylint: disable=import-outside-toplevel
    import json  # pylint: disable=import-outside-toplevel
    client = boto3.client(
        "secretsmanager",
        region_name=os.getenv("AWS_REGION", "us-east-1")
    )
    secret = json.loads(
        client.get_secret_value(
            SecretId=os.getenv("OAUTH_SECRET_NAME", "mirrulations/oauth")
        )["SecretString"]
    )
    return OAuthHandler(
        base_url=secret.get("base_url", ""),
        google_client_id=secret.get("google_client_id", ""),
        google_client_secret=secret.get("google_client_secret", ""),
        jwt_secret=secret.get("jwt_secret", "dev-secret")
    )


def _get_user_from_cookie(oauth_handler):
    """Extract and validate user info from JWT cookie. Returns dict or None."""
    token = request.cookies.get("jwt_token")
    if not token:
        return None
    try:
        user_id = oauth_handler.validate_jwt_token(token)
        name, email = user_id.split("|", 1)
        return {"name": name, "email": email}
    except (TokenExpiredError, TokenInvalidError, ValueError):
        return None


def _handle_oauth_callback(handler):
    """Exchange OAuth code for JWT cookie response. Returns response or None."""
    code = request.args.get("code")
    if not code:
        return None
    try:
        user_info = handler.exchange_code_for_user_info(code)
        user_id = f"{user_info['name']}|{user_info['email']}"
        token = handler.create_jwt_token(user_id)
        response = make_response(redirect("/"))
        response.set_cookie("jwt_token", token, httponly=True, samesite="Lax", path="/")
        return response
    except (OAuthCodeError, OAuthVerificationError):
        return redirect("/")


def create_app(dist_dir=None, db_layer=None, oauth_handler=None):  # pylint: disable=too-many-locals,too-many-statements
    """Create and configure Flask application"""
    if dist_dir is None:
        project_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '..', '..')
        )
        dist_dir = os.path.join(project_root, 'frontend', 'dist')

    flask_app = Flask(__name__, static_folder=dist_dir, static_url_path='')
    flask_app.secret_key = os.getenv("JWT_SECRET", "dev-secret")

    @flask_app.route("/")
    def home():
        handler = oauth_handler or _make_oauth_handler()
        callback_response = _handle_oauth_callback(handler)
        if callback_response:
            return callback_response
        return send_from_directory(dist_dir, "index.html")

    @flask_app.route("/login")
    def login():
        handler = oauth_handler or _make_oauth_handler()
        authorization_url, _ = handler.get_authorization_url()
        return redirect(authorization_url)

    @flask_app.route("/logout")
    def logout():
        response = make_response(redirect("/"))
        response.delete_cookie("jwt_token")
        return response

    @flask_app.route("/auth/status")
    def auth_status():
        handler = oauth_handler or _make_oauth_handler()
        user = _get_user_from_cookie(handler)
        if user:
            return jsonify({"logged_in": True, "name": user["name"], "email": user["email"]})
        return jsonify({"logged_in": False})

    @flask_app.route("/search/")
    def search():
        handler = oauth_handler or _make_oauth_handler()
        if not _get_user_from_cookie(handler):
            return jsonify({"error": "Unauthorized"}), 401

        params = _get_search_params()
        page, page_size = _get_pagination_params()

        logic = InternalLogic("sample_database", db_layer=db_layer)
        result = logic.search(
            params['search_input'],
            params['docket_type'],
            params['agency'],
            params['cfr_part'],
            params['start_date'],
            params['end_date'],
            page=page,
            page_size=page_size
        )

        return _build_paginated_response(result['results'], result['pagination'])

    @flask_app.route("/agencies")
    def agencies():
        result = InternalLogic("sample_database", db_layer=db_layer).get_agencies()
        return jsonify(result)

    return flask_app


app = create_app()

if __name__ == '__main__':
    app.run(port=80, debug=True)
