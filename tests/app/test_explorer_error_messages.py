"""Server errors from filesystem operations must not leak raw OSError text
(absolute paths, OS-specific detail) to the HTTP client."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from app.routers.api.explorer import _delete_path


class TestDeletePathErrorMessage:
    def test_oserror_is_logged_not_leaked(self, tmp_path, caplog):
        sandbox = tmp_path / "sandbox"
        sandbox.mkdir()
        target = sandbox / "file.pdf"
        target.write_bytes(b"%PDF-1.4")

        sensitive_detail = f"[WinError 32] El proceso no tiene acceso al archivo: '{target}'"

        with patch.object(Path, "unlink", side_effect=OSError(sensitive_detail)):
            with caplog.at_level("ERROR"):
                with pytest.raises(HTTPException) as exc_info:
                    _delete_path(target, sandbox)

        assert str(target) not in str(exc_info.value.detail)
        assert any(str(target) in record.message for record in caplog.records)
