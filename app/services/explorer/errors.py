"""The error the explorer services raise."""

from __future__ import annotations

from app.services.errors import ServiceError


class ExplorerError(ServiceError):
    """A file operation of the explorer the user cannot do (see :class:`ServiceError`)."""
