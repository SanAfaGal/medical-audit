"""Safe extraction of uploaded ZIP / 7z / RAR archives into a destination folder.

Blocking and CPU/disk heavy: call :func:`extract_archive` through ``asyncio.to_thread`` so it
never stalls the event loop (the app runs a single uvicorn worker).

Defences against hostile archives:
* path traversal ("zip slip") - every member is resolved with ``safe_join``;
* decompression bombs - declared and actually-written sizes are capped;
* symbolic links inside 7z/RAR archives are rejected, so extracted content can't point at (and be
  used to read) files outside the destination.
"""

from __future__ import annotations

import shutil
import tempfile
import zipfile
from pathlib import Path

from app.paths import safe_join

MAX_ARCHIVE_BYTES = 1024**3  # 1 GiB compressed upload
MAX_UNCOMPRESSED_BYTES = 5 * 1024**3  # 5 GiB once extracted
MAX_MEMBERS = 200_000
_COPY_CHUNK = 1024 * 1024


class ArchiveError(ValueError):
    """The archive is invalid, unsafe, too large or in an unsupported format (client error)."""


class ArchiveToolMissing(RuntimeError):
    """The optional library needed for this format (py7zr / rarfile) is not installed."""


def extract_archive(data: bytes, filename: str, dest: Path) -> list[str]:
    """Extract ``data`` (named ``filename``) into ``dest``; returns the extracted relative paths."""
    ext = Path(filename).suffix.lower()
    if ext == ".zip":
        return _extract_zip(data, dest)
    if ext == ".7z":
        return _extract_7z(data, dest)
    if ext == ".rar":
        return _extract_rar(data, dest)
    raise ArchiveError(f"Formato no soportado: '{ext}'. Use .zip, .7z o .rar")


def _check_declared(total_bytes: int, members: int) -> None:
    if members > MAX_MEMBERS:
        raise ArchiveError(f"El archivo contiene demasiados elementos ({members} > {MAX_MEMBERS})")
    if total_bytes > MAX_UNCOMPRESSED_BYTES:
        raise ArchiveError("El contenido descomprimido es demasiado grande")


def _extract_zip(data: bytes, dest: Path) -> list[str]:
    import io

    extracted: list[str] = []
    try:
        zf = zipfile.ZipFile(io.BytesIO(data))
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
                    if written_total > MAX_UNCOMPRESSED_BYTES:  # declared sizes can lie
                        raise ArchiveError("El contenido descomprimido es demasiado grande")
                    out.write(chunk)
            extracted.append(str(rel))
    return extracted


def _reject_symlinks(root: Path) -> None:
    for p in root.rglob("*"):
        if p.is_symlink():
            raise ArchiveError(f"El archivo contiene un enlace simbólico no permitido: {p.name}")


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


def _extract_7z(data: bytes, dest: Path) -> list[str]:
    import io

    try:
        import py7zr
    except ImportError as exc:
        raise ArchiveToolMissing("py7zr no está instalado (soporte .7z no disponible)") from exc

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            with py7zr.SevenZipFile(io.BytesIO(data), mode="r") as szf:
                names = szf.getnames()
                _check_declared(int(szf.archiveinfo().uncompressed), len(names))
                top_levels = {Path(n).parts[0] for n in names if Path(n).parts}
                szf.extractall(path=tmpdir)
        except py7zr.Bad7zFile as exc:
            raise ArchiveError("El archivo 7z está dañado o no es válido") from exc
        _reject_symlinks(Path(tmpdir))
        return _merge_into(Path(tmpdir), top_levels, dest)


def _extract_rar(data: bytes, dest: Path) -> list[str]:
    try:
        import rarfile
    except ImportError as exc:
        raise ArchiveToolMissing("rarfile no está instalado (soporte .rar no disponible)") from exc

    with tempfile.TemporaryDirectory() as tmpdir:
        rar_path = Path(tmpdir) / "upload.rar"
        out_dir = Path(tmpdir) / "out"
        out_dir.mkdir()
        rar_path.write_bytes(data)
        try:
            with rarfile.RarFile(str(rar_path)) as rf:
                infos = rf.infolist()
                _check_declared(sum(i.file_size for i in infos), len(infos))
                top_levels = {Path(n).parts[0] for n in rf.namelist() if Path(n).parts}
                rf.extractall(path=str(out_dir))
        except rarfile.Error as exc:
            raise ArchiveError(f"No se pudo leer el archivo RAR: {exc}") from exc
        _reject_symlinks(out_dir)
        return _merge_into(out_dir, top_levels, dest)
