"""The one exception the explorer services raise."""

from __future__ import annotations


class ExplorerError(Exception):
    """An operation the user cannot do (bad path, missing file, name clash, ...).

    ``status_code`` is the HTTP status the API answers with; ``detail`` is safe to show to the user (never raw
    OS error text: log that instead).
    """

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail
