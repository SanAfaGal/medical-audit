"""File and folder manipulation: delete, move, rename, and copy operations."""

import logging
import re
from pathlib import Path

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
