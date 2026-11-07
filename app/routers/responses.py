"""Shared HTTP response helpers for the routers."""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from pathlib import Path
from urllib.parse import quote

import aiofiles
from fastapi.responses import StreamingResponse

_CHUNK = 1024 * 1024


def content_disposition(filename: str, *, inline: bool = False) -> str:
    """Build a Content-Disposition value that survives quotes, newlines and non-ASCII names.

    Always sends an ASCII-safe ``filename`` plus the RFC 5987 ``filename*`` with the real name, so a
    file called ``a"b\\n.pdf`` can't break out of the header.
    """
    ascii_name = "".join(c if c.isascii() and c.isprintable() and c not in '"\\/;' else "_" for c in filename) or "file"
    kind = "inline" if inline else "attachment"
    return f"{kind}; filename=\"{ascii_name}\"; filename*=UTF-8''{quote(filename, safe='')}"


def temp_file_response(path: Path, filename: str, media_type: str) -> StreamingResponse:
    """Stream a temporary file to the client and delete it when the response ends.

    The file is removed in a ``finally`` around the stream itself, so it is cleaned up even when the
    client disconnects halfway (a ``BackgroundTask`` does not reliably run in that case, and would
    leave a multi-GB package on disk).
    """
    size = os.path.getsize(path)

    async def _stream() -> AsyncGenerator[bytes, None]:
        try:
            async with aiofiles.open(path, "rb") as fh:
                while chunk := await fh.read(_CHUNK):
                    yield chunk
        finally:
            path.unlink(missing_ok=True)

    return StreamingResponse(
        _stream(),
        media_type=media_type,
        headers={"Content-Disposition": content_disposition(filename), "Content-Length": str(size)},
    )
