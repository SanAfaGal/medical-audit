"""Tests for the shared-secret HTTP Basic Auth middleware (app/security.py)."""

from __future__ import annotations

import base64

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from app.security import ApiKeyAuthMiddleware


def _make_app(app_password: str) -> Starlette:
    async def ping(request):
        return PlainTextResponse("pong")

    async def health(request):
        return PlainTextResponse("ok")

    return Starlette(
        routes=[Route("/ping", ping), Route("/health", health)],
        middleware=[Middleware(ApiKeyAuthMiddleware, app_password=app_password)],
    )


def _basic_header(password: str, user: str = "admin") -> dict:
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


class TestApiKeyAuthMiddleware:
    def test_rejects_missing_credentials(self):
        client = TestClient(_make_app("secret123"))
        resp = client.get("/ping")
        assert resp.status_code == 401
        assert resp.headers["www-authenticate"].startswith("Basic")

    def test_rejects_wrong_password(self):
        client = TestClient(_make_app("secret123"))
        resp = client.get("/ping", headers=_basic_header("wrong"))
        assert resp.status_code == 401

    def test_rejects_wrong_username(self):
        client = TestClient(_make_app("secret123"))
        resp = client.get("/ping", headers=_basic_header("secret123", user="whoever"))
        assert resp.status_code == 401

    def test_accepts_correct_username_and_password(self):
        client = TestClient(_make_app("secret123"))
        resp = client.get("/ping", headers=_basic_header("secret123", user="admin"))
        assert resp.status_code == 200
        assert resp.text == "pong"

    def test_health_path_is_exempt(self):
        client = TestClient(_make_app("secret123"))
        resp = client.get("/health")
        assert resp.status_code == 200
