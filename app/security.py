"""Shared-secret HTTP Basic Auth gate applied to every request except health checks."""

from __future__ import annotations

import base64
import hmac

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

_REALM = "Medical Audit"
_EXEMPT_PATHS = frozenset({"/health", "/health/db"})
_BASIC_PREFIX = "Basic "
_ADMIN_USERNAME = "admin"


class ApiKeyAuthMiddleware(BaseHTTPMiddleware):
    """Requires HTTP Basic Auth with a fixed username and a shared password on every request.

    Basic Auth (not a bearer token) is used deliberately so browsers cache the
    credential after one prompt, protecting both page loads and JS fetch() calls
    without any frontend code.
    """

    def __init__(self, app, app_password: str) -> None:
        super().__init__(app)
        self._app_password = app_password

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if request.url.path in _EXEMPT_PATHS:
            return await call_next(request)

        header = request.headers.get("authorization", "")
        username = ""
        password = ""
        if header.startswith(_BASIC_PREFIX):
            try:
                decoded = base64.b64decode(header[len(_BASIC_PREFIX):]).decode("utf-8")
                username, _, password = decoded.partition(":")
            except (ValueError, UnicodeDecodeError):
                username = ""
                password = ""

        username_ok = bool(username) and hmac.compare_digest(username, _ADMIN_USERNAME)
        password_ok = bool(password) and hmac.compare_digest(password, self._app_password)
        if username_ok and password_ok:
            return await call_next(request)

        return Response(
            status_code=401,
            headers={"WWW-Authenticate": f'Basic realm="{_REALM}"'},
        )
