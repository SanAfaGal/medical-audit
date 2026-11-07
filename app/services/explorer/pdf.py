"""PDF page tools of the explorer (thumbnails, merge, split, reorder). Blocking: call from a worker thread.

Every public function takes the sandbox and the client's relative paths and validates them itself.
"""

from __future__ import annotations

import base64
import logging
from collections.abc import Sequence
from pathlib import Path

from app.services.explorer.errors import ExplorerError
from app.services.explorer.sandbox import relative, require_pdf, resolve_in

logger = logging.getLogger(__name__)


def render_thumbnails(sandbox: Path, rel: str) -> list[str]:
    """Every page as a half-size PNG, base64 encoded."""
    import fitz  # PyMuPDF

    path = resolve_in(sandbox, rel)
    require_pdf(path, "Archivo PDF no encontrado")
    matrix = fitz.Matrix(0.5, 0.5)
    with fitz.open(str(path)) as doc:
        return [base64.b64encode(page.get_pixmap(matrix=matrix).tobytes("png")).decode() for page in doc]


def merge_files(sandbox: Path, rels: Sequence[str], output_name: str) -> str:
    """Join PDFs into ``output_name`` (next to the first one). Returns the final file name."""
    import fitz

    if len(rels) < 2:
        raise ExplorerError(400, "Se necesitan al menos 2 PDFs para unir")
    paths = [resolve_in(sandbox, rel) for rel in rels]
    for path in paths:
        if not path.exists() or path.suffix.lower() != ".pdf":
            raise ExplorerError(404, f"PDF no encontrado: {path.name}")

    name = output_name if output_name.endswith(".pdf") else output_name + ".pdf"
    output = paths[0].parent / name
    resolve_in(sandbox, relative(sandbox, output))  # the output must stay inside the sandbox too

    merged = fitz.open()
    for path in paths:
        with fitz.open(str(path)) as doc:
            merged.insert_pdf(doc)
    merged.save(str(output))
    merged.close()
    return name


def parse_ranges(ranges: str, total_pages: int) -> list[list[int]]:
    """``'1-3, 5'`` (1-based) to lists of 0-based page indexes. Raises ValueError on malformed input."""
    groups: list[list[int]] = []
    for raw in ranges.split(","):
        token = raw.strip()
        if not token:
            continue
        if "-" in token:
            first, last = token.split("-")[:2]
            start, end = int(first.strip()) - 1, int(last.strip()) - 1
            groups.append(list(range(max(0, start), min(total_pages - 1, end) + 1)))
        else:
            index = int(token) - 1
            if 0 <= index < total_pages:
                groups.append([index])
    return groups


def split_groups(ranges: str | None, total_pages: int) -> list[list[int]]:
    """Page groups to split into: by ``ranges`` when given, otherwise one page per group."""
    return parse_ranges(ranges, total_pages) if ranges else [[i] for i in range(total_pages)]


def _groups_for(path: Path, ranges: str | None) -> list[list[int]]:
    import fitz

    with fitz.open(str(path)) as doc:
        total = doc.page_count
    try:
        return split_groups(ranges, total)
    except (ValueError, IndexError) as exc:
        raise ExplorerError(400, f"Rango de páginas inválido: {exc}") from exc


def preview_split(sandbox: Path, rel: str, ranges: str | None) -> list[list[int]]:
    """The page groups a split would produce, without writing anything."""
    path = resolve_in(sandbox, rel)
    require_pdf(path)
    return _groups_for(path, ranges)


def split(sandbox: Path, rel: str, ranges: str | None) -> int:
    """Write ``<name>_parteNNN.pdf`` next to the original for every group; returns how many were created."""
    import fitz

    path = resolve_in(sandbox, rel)
    require_pdf(path)
    groups = _groups_for(path, ranges)
    with fitz.open(str(path)) as doc:
        for i, pages in enumerate(groups, start=1):
            part = fitz.open()
            if pages == list(range(pages[0], pages[-1] + 1)):
                part.insert_pdf(doc, from_page=pages[0], to_page=pages[-1])
            else:  # non-contiguous selection: copy page by page
                for page in pages:
                    part.insert_pdf(doc, from_page=page, to_page=page)
            part.save(str(path.parent / f"{path.stem}_parte{i:03d}.pdf"))
            part.close()
    return len(groups)


def reorder(sandbox: Path, rel: str, order: Sequence[int]) -> int:
    """Rewrite the PDF with only the pages in ``order``, in that order. Returns how many pages were dropped."""
    import fitz

    path = resolve_in(sandbox, rel)
    require_pdf(path)
    pages = list(order)
    with fitz.open(str(path)) as doc:
        page_count: int = doc.page_count
        if not pages:
            raise ExplorerError(400, "Debe quedar al menos 1 página")
        if len(pages) != len(set(pages)):
            raise ExplorerError(400, "No puede haber páginas repetidas")
        for index in pages:
            if index < 0 or index >= page_count:
                raise ExplorerError(400, f"Índice de página inválido: {index} (rango válido 0–{page_count - 1})")
        doc.select(pages)
        # tobytes() avoids saving over the file that is still open (a problem on Windows)
        data = doc.tobytes(garbage=4, deflate=True)
    try:
        path.write_bytes(data)
    except OSError as exc:
        logger.error("Error al reordenar %s: %s", path, exc)
        raise ExplorerError(500, "Error al reordenar el PDF") from exc
    return page_count - len(pages)
