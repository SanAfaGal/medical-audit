"""File operations behind the PDF explorer: sandboxed paths, folder/file actions and PDF page tools.

The functions here know nothing about HTTP: they raise :class:`ExplorerError` (carrying the status the API should
answer with) and the router in ``app/routers/api/explorer.py`` translates it.
"""

from app.services.explorer.errors import ExplorerError

__all__ = ["ExplorerError"]
