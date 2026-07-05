"""Cross-cutting utility functions for text normalisation, file I/O, and reporting."""

import logging
import shutil
import unicodedata
from pathlib import Path

logger = logging.getLogger(__name__)


def remove_accents(text: str | object) -> str:
    """Strip accent marks and normalise text to plain ASCII-equivalent.

    Safe for non-string inputs such as ``None``, ``float`` (NaN), or ``int``.

    Args:
        text: Input value to normalise.

    Returns:
        Accent-free string, or an empty string for non-string inputs.
    """
    if not isinstance(text, str):
        return ""
    normalised = unicodedata.normalize("NFD", text)
    return "".join(c for c in normalised if unicodedata.category(c) != "Mn")


def safe_move(src: Path, dest: Path) -> bool:
    """Move a file or directory with pre-flight safety checks.

    Args:
        src: Source path to move.
        dest: Destination path.

    Returns:
        True if the move succeeded, False otherwise.
    """
    try:
        if dest.exists():
            logger.error("Destination already exists, cannot move: %s", dest)
            return False
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dest))
        return True
    except (shutil.Error, OSError) as exc:
        logger.error("Error moving %s: %s", src, exc)
        return False


def read_lines_from_file(file_path: str | Path) -> list[str]:
    """Read a text file and return a list of non-empty stripped lines.

    Args:
        file_path: Path to the ``.txt`` file.

    Returns:
        Stripped, non-empty lines. Returns an empty list on any error.
    """
    path = Path(file_path)
    if not path.exists():
        logger.error("File does not exist: %s", path)
        return []
    try:
        with path.open("r", encoding="utf-8") as fh:
            return [line.strip() for line in fh if line.strip()]
    except (PermissionError, OSError, UnicodeDecodeError) as exc:
        logger.error("Error reading file %s: %s", path, exc)
        return []


def flatten_prefixes(prefixes_dict: dict[str, str | list[str]]) -> list[str]:
    """Flatten a prefix dictionary into a single deduplicated list.

    Args:
        prefixes_dict: Mapping of document type labels to prefix strings or lists.

    Returns:
        Deduplicated list of all prefix strings.
    """
    flat: list[str] = []
    for value in prefixes_dict.values():
        if isinstance(value, list):
            flat.extend(value)
        else:
            flat.append(str(value))
    return list(set(flat))
