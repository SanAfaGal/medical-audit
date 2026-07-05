"""Tests for global exception handlers registered in app/main.py."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from sqlalchemy.exc import IntegrityError


class TestIntegrityErrorHandler:
    async def test_returns_409_with_generic_message(self):
        from app.main import integrity_error_handler

        fake_request = MagicMock()
        fake_request.url.path = "/api/institutions/administrators/1"
        exc = IntegrityError("DELETE FROM administrators ...", {}, Exception("FK violation"))

        response = await integrity_error_handler(fake_request, exc)

        assert response.status_code == 409
        body = json.loads(response.body)
        assert "en uso" in body["detail"]
