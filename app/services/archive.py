"""Safe extraction and packaging of ZIP / 7z / RAR archives (streamed through disk, not memory).

Blocking and disk heavy: call these through ``asyncio.to_thread`` so they never stall the event
loop (the app runs a single uvicorn worker).

There are **no size limits by default** - the app runs locally and audits can be many GB. Operators
who want a safety net can set ``MAX_ARCHIVE_BYTES`` / ``MAX_UNCOMPRESSED_BYTES`` /
``MAX_ARCHIVE_MEMBERS`` (0 = unlimited). Protections that cost nothing in size are always on:

* path traversal ("zip slip") - every member is resolved with ``safe_join``;
* symbolic links inside 7z/RAR archives are rejected, so extracted content can't point at (and be
  used to read) files outside the destination.
"""

from __future__ import annotations

import io
import shutil
import tempfile
import zipfile
from collections.abc import Iterable
from pathlib import Path
from typing import BinaryIO

from app.config import settings
from app.paths import safe_join

_COPY_CHUNK = 1024 * 1024
# PDFs are already compressed: deflating them again burns CPU for ~0 gain on multi-GB packages.
_STORED_SUFFIXES = frozenset({".pdf", ".zip", ".7z", ".rar", ".jpg", ".jpeg", ".png"})


class ArchiveError(ValueError):
    """The archive is invalid, unsafe, over a configured limit or in an unsupported format."""


class ArchiveToolMissing(RuntimeError):
    """The optional library needed for this format (py7zr / rarfile) is not installed."""


def _over(value: int, limit: int) -> bool:
    """True when ``limit`` is enabled (> 0) and ``value`` exceeds it."""
    return limit > 0 and value > limit


def extract_archive(source: bytes | BinaryIO, filename: str, dest: Path) -> list[str]:
    """Extract ``source`` (named ``filename``) into ``dest``; returns the extracted relative paths.

    ``source`` may be raw bytes or a seekable file object (e.g. ``UploadFile.file``), which
    avoids holding a multi-GB upload in memory.
    """
    fileobj: BinaryIO = io.BytesIO(source) if isinstance(source, (bytes, bytearray)) else source
    ext = Path(filename).suffix.lower()
    if ext == ".zip":
        return _extract_zip(fileobj, dest)
    if ext == ".7z":
        return _extract_7z(fileobj, dest)
    if ext == ".rar":
        return _extract_rar(fileobj, dest)
    raise ArchiveError(f"Formato no soportado: '{ext}'. Use .zip, .7z o .rar")


def _check_declared(total_bytes: int, members: int) -> None:
    if _over(members, settings.max_archive_members):
        raise ArchiveError(f"El archivo contiene demasiados elementos ({members} > {settings.max_archive_members})")
    if _over(total_bytes, settings.max_uncompressed_bytes):
        raise ArchiveError("El contenido descomprimido es demasiado grande")


def _extract_zip(fileobj: BinaryIO, dest: Path) -> list[str]:
    extracted: list[str] = []
    try:
        zf = zipfile.ZipFile(fileobj)
    except zipfile.BadZipFile as exc:
        raise ArchiveError("El archivo ZIP está dañado o no es un ZIP válido") from exc
    with zf:
        members = [m for m in zf.infolist() if not m.filename.endswith("/")]
        _check_declared(sum(m.file_size for m in members), len(members))
        # Solo eliminar el directorio raíz si TODOS los archivos comparten exactamente un
        # directorio de nivel superior (el ZIP tiene una carpeta envolvente). Si hay varias
        # carpetas de nivel superior (estructura real de facturas), se preserva la ruta completa.
        top_levels = {Path(m.filename).parts[0] for m in members if Path(m.filename).parts}
        has_single_wrapper = len(top_levels) == 1
        written_total = 0
        for member in members:
            parts = Path(member.filename).parts
            rel = Path(*parts[1:]) if has_single_wrapper and len(parts) > 1 else Path(*parts)
            try:
                target = safe_join(dest, str(rel))
            except ValueError as exc:
                raise ArchiveError(f"Ruta insegura en ZIP: {member.filename}") from exc
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(member) as src, target.open("wb") as out:
                while chunk := src.read(_COPY_CHUNK):
                    written_total += len(chunk)
                    if _over(written_total, settings.max_uncompressed_bytes):  # declared sizes can lie
                        raise ArchiveError("El contenido descomprimido es demasiado grande")
                    out.write(chunk)
            extracted.append(str(rel))
    return extracted


def _reject_symlinks(root: Path) -> None:
    for p in root.rglob("*"):
        if p.is_symlink():
            raise ArchiveError(f"El archivo contiene un enlace simbólico no permitido: {p.name}")


def _check_actual_size(root: Path) -> None:
    """Verify what really landed on disk (headers can lie) before copying it anywhere."""
    if settings.max_uncompressed_bytes <= 0 and settings.max_archive_members <= 0:
        return
    total = 0
    count = 0
    for p in root.rglob("*"):
        if p.is_file():
            count += 1
            total += p.stat().st_size
            if _over(total, settings.max_uncompressed_bytes) or _over(count, settings.max_archive_members):
                raise ArchiveError("El contenido descomprimido es demasiado grande")


def _merge_into(tmp_root: Path, top_levels: set[str], dest: Path) -> list[str]:
    """Copy extracted content into ``dest``, dropping a single wrapper folder if there is one."""
    src_root = tmp_root
    if len(top_levels) == 1:
        wrapper = tmp_root / next(iter(top_levels))
        if wrapper.is_dir():
            src_root = wrapper
    for item in src_root.iterdir():
        target = dest / item.name
        if item.is_dir():
            shutil.copytree(str(item), str(target), dirs_exist_ok=True)
        else:
            shutil.copy2(str(item), str(target))
    return [str(p) for p in dest.rglob("*") if p.is_file()]


def _extract_7z(fileobj: BinaryIO, dest: Path) -> list[str]:
    try:
        import py7zr
    except ImportError as exc:
        raise ArchiveToolMissing("py7zr no está instalado (soporte .7z no disponible)") from exc

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            with py7zr.SevenZipFile(fileobj, mode="r") as szf:
                entries = szf.list()
                _check_declared(sum(int(e.uncompressed or 0) for e in entries if not e.is_directory), len(entries))
                for entry in entries:  # reject before writing anything to disk
                    if entry.is_symlink:
                        raise ArchiveError(f"El archivo contiene un enlace simbólico no permitido: {entry.filename}")
                top_levels = {Path(e.filename).parts[0] for e in entries if Path(e.filename).parts}
                szf.extractall(path=tmpdir)
        except py7zr.Bad7zFile as exc:
            raise ArchiveError("El archivo 7z está dañado o no es válido") from exc
        # py7zr bounds each stream by the sizes in the header, so a lying header can't inflate the
        # output; still verify the real size and links on disk before merging.
        _check_actual_size(Path(tmpdir))
        _reject_symlinks(Path(tmpdir))
        return _merge_into(Path(tmpdir), top_levels, dest)


def _extract_rar(fileobj: BinaryIO, dest: Path) -> list[str]:
    try:
        import rarfile
    except ImportError as exc:
        raise ArchiveToolMissing("rarfile no está instalado (soporte .rar no disponible)") from exc

    with tempfile.TemporaryDirectory() as tmpdir:
        rar_path = Path(tmpdir) / "upload.rar"
        out_dir = Path(tmpdir) / "out"
        out_dir.mkdir()
        with rar_path.open("wb") as fh:  # rarfile needs a path; stream, don't hold it in memory
            shutil.copyfileobj(fileobj, fh, _COPY_CHUNK)
        try:
            with rarfile.RarFile(str(rar_path)) as rf:
                infos = rf.infolist()
                _check_declared(sum(i.file_size for i in infos), len(infos))
                top_levels = {Path(i.filename).parts[0] for i in infos if Path(i.filename).parts}
                # Stream member by member and count the bytes actually produced: RAR headers can
                # understate the real size, and extractall() would write it all unchecked.
                written = 0
                for info in infos:
                    if info.is_dir():
                        continue
                    if info.is_symlink():
                        raise ArchiveError(f"El archivo contiene un enlace simbólico no permitido: {info.filename}")
                    try:
                        target = safe_join(out_dir, info.filename.replace("\\", "/"))
                    except ValueError as exc:
                        raise ArchiveError(f"Ruta insegura en RAR: {info.filename}") from exc
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with rf.open(info) as src, target.open("wb") as out:
                        while chunk := src.read(_COPY_CHUNK):
                            written += len(chunk)
                            if _over(written, settings.max_uncompressed_bytes):
                                raise ArchiveError("El contenido descomprimido es demasiado grande")
                            out.write(chunk)
        except rarfile.Error as exc:
            raise ArchiveError(f"No se pudo leer el archivo RAR: {exc}") from exc
        return _merge_into(out_dir, top_levels, dest)


def build_zip_file(entries: Iterable[tuple[Path, str | Path]]) -> Path:
    """Write ``(file, name_in_zip)`` pairs to a temporary ZIP on disk and return its path.

    Streams file by file, so a multi-GB package never sits in memory. The caller owns the file and
    must delete it (e.g. as a ``BackgroundTask`` after sending it).
    """
    tmp = tempfile.NamedTemporaryFile(prefix="audit_", suffix=".zip", delete=False)
    tmp.close()
    path = Path(tmp.name)
    try:
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED, compresslevel=6, allowZip64=True) as zf:
            for file, arcname in entries:
                stored = file.suffix.lower() in _STORED_SUFFIXES
                zf.write(file, arcname, compress_type=zipfile.ZIP_STORED if stored else zipfile.ZIP_DEFLATED)
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    return path
