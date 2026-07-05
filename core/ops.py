"""File and folder manipulation: delete, move, rename, and copy operations."""

import logging
import re
import shutil
from pathlib import Path
from typing import Literal, TypedDict

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

_RE_NIT: re.Pattern = re.compile(r"_(\d+)_")
_FOLDER_NAME_PART_INDEX: int = 2

# Supported image formats for PDF conversion
IMAGE_EXTENSIONS: frozenset[str] = frozenset({"jpg", "jpeg", "png", "tiff", "tif"})


def convert_image_to_pdf(image_path: Path) -> Path:
    """Convert an image file to PDF (same folder, same stem, .pdf extension).

    Deletes the original image on success.
    Returns the path of the newly created PDF.

    Raises:
        fitz.FileDataError: if the image cannot be opened.
        OSError: if writing the output PDF fails.
    """
    import fitz  # PyMuPDF — already a project dependency

    output_path = image_path.with_suffix(".pdf")
    img_doc = fitz.open(str(image_path))
    pdf_bytes = img_doc.convert_to_pdf()
    img_doc.close()
    output_path.write_bytes(pdf_bytes)
    image_path.unlink()
    return output_path


class _TransferResult(TypedDict):
    success: int
    failed: int
    not_found: int
    errors: list[str]


class DocumentOps:
    """Performs file and folder manipulation operations on the filesystem.

    Args:
        base_dir: Root directory for all operations.
        id_prefix: Invoice identifier prefix used to build directory-name regexes.
    """

    def __init__(self, base_dir: Path, id_prefix: str = "") -> None:
        self.base_dir = Path(base_dir)
        _esc = re.escape(id_prefix)
        self._re_dir_id_loose = re.compile(rf"({_esc})[^a-zA-Z]*?(\d+)", re.IGNORECASE)

    def remove_files(self, files: list[Path]) -> int:
        """Delete a list of files and return the count of successful deletions."""
        count = 0
        for f in files:
            try:
                f.unlink()
                count += 1
            except OSError as exc:
                logger.error("Could not delete file %s: %s", f, exc)
        return count

    def relocate_misplaced(self, source_dir: Path, dry_run: bool = True) -> None:
        """Move files from the source directory to their correct folder."""
        for f in source_dir.rglob("*"):
            if not f.is_file():
                continue
            parts = f.stem.split("_")
            if len(parts) <= _FOLDER_NAME_PART_INDEX:
                logger.warning(
                    "File name has insufficient parts to determine destination: %s",
                    f,
                )
                continue
            folder_name = parts[_FOLDER_NAME_PART_INDEX]
            destination = self.base_dir / folder_name
            if dry_run:
                logger.info("Dry-run: %s -> %s", f, destination)
            elif destination.exists():
                try:
                    shutil.move(str(f), str(destination))
                except (shutil.Error, OSError) as exc:
                    logger.error("Could not move %s to %s: %s", f, destination, exc)
            else:
                logger.warning("Destination folder does not exist, skipping: %s", destination)

    def apply_prefix_renames(
        self,
        prefix_map: dict[str, str],
        files: list[Path] | None = None,
    ) -> int:
        """Rename files by replacing their prefix according to a mapping."""
        if not files:
            return 0

        count = 0
        for f in files:
            if not f.is_file():
                logger.warning("Path is not a file: %s", f)
                continue
            parts = f.name.split("_", 1)
            if len(parts) > 1:
                current = parts[0].upper()
                if current in prefix_map:
                    new_name = f"{prefix_map[current]}_{parts[1]}"
                    try:
                        f.rename(f.with_name(new_name))
                        count += 1
                    except OSError as exc:
                        logger.error("Could not rename %s: %s", f, exc)
        return count

    def correct_nit_in_names(self, files: list[Path], correct_nit: str) -> int:
        """Rename files to use the correct NIT number."""
        count = 0
        for f in files:
            try:
                current_nit = self.parse_nit_from_filename(f.name)
                if current_nit and current_nit != correct_nit:
                    parts = f.name.split("_", 2)
                    if len(parts) == 3:
                        new_name = f"{parts[0]}_{correct_nit}_{parts[2]}"
                        f.rename(f.with_name(new_name))
                        count += 1
            except OSError as exc:
                logger.error("Could not rename %s: %s", f, exc)
        return count

    def move_or_copy_dirs(
        self,
        dir_names: list[str],
        source_dir: Path | str,
        destination_dir: Path | str,
        action: Literal["copy", "move"] = "copy",
    ) -> _TransferResult:
        """Copy or move named directories from source to destination."""
        source_dir = Path(source_dir)
        destination_dir = Path(destination_dir)

        result: _TransferResult = {
            "success": 0,
            "failed": 0,
            "not_found": 0,
            "errors": [],
        }

        if not source_dir.is_dir():
            logger.error("Invalid source directory: %s", source_dir)
            result["errors"].append(f"Invalid source: {source_dir}")
            return result

        try:
            destination_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            logger.error("Cannot create destination %s: %s", destination_dir, exc)
            result["errors"].append(f"Cannot create destination: {exc}")
            return result

        for name in dir_names:
            src = source_dir / name
            dst = destination_dir / name

            if not src.is_dir():
                logger.warning("Source folder not found: %s", src)
                result["not_found"] += 1
                result["errors"].append(f"Folder not found: {name}")
                continue

            try:
                if dst.exists():
                    logger.warning("Destination already exists, skipping: %s", dst)
                    result["failed"] += 1
                    result["errors"].append(f"Already exists: {name}")
                elif action == "copy":
                    shutil.copytree(src, dst)
                    result["success"] += 1
                elif action == "move":
                    shutil.move(str(src), str(dst))
                    result["success"] += 1
                else:
                    raise ValueError(f"Invalid action: {action}")
            except (shutil.Error, OSError) as exc:
                logger.error("Error processing %s: %s", name, exc)
                result["failed"] += 1
                result["errors"].append(f"Error on {name}: {exc}")

        return result

    def tag_dirs_missing_cufe(self, files: list[Path]) -> int:
        """Append ' CUFE' to parent folders of files that are missing a CUFE code."""
        count = 0
        seen: set[Path] = set()

        for f in files:
            parent = f.parent
            if parent in seen:
                continue
            seen.add(parent)

            if parent.name.upper().endswith(" CUFE"):
                continue

            new_path = parent.parent / (f"{parent.name} CUFE")
            if new_path.exists():
                logger.warning(
                    "Cannot tag %s: target already exists: %s",
                    parent.name,
                    new_path.name,
                )
                continue

            try:
                parent.rename(new_path)
                count += 1
            except OSError as exc:
                logger.error("Could not rename directory %s: %s", parent, exc)

        return count

    def standardize_dir_names(self, dirs: list[Path]) -> int:
        """Rename directories to their canonical identifier, stripping extra text."""
        count = 0
        for dir_path in dirs:
            match = self._re_dir_id_loose.search(dir_path.name)
            if not match:
                logger.warning("Cannot extract canonical name from: %s", dir_path.name)
                continue
            canonical = (match.group(1) + match.group(2)).upper()
            new_path = dir_path.parent / canonical
            if new_path == dir_path:
                continue
            if new_path.exists():
                logger.warning(
                    "Cannot rename %s: target already exists: %s",
                    dir_path.name,
                    canonical,
                )
                continue
            try:
                dir_path.rename(new_path)
                count += 1
            except OSError as exc:
                logger.error("Could not rename directory %s: %s", dir_path, exc)
        return count

    @staticmethod
    def parse_nit_from_filename(filename: str) -> str | None:
        """Extract the NIT number embedded between underscores in a file name."""
        match = _RE_NIT.search(filename)
        return match.group(1) if match else None
