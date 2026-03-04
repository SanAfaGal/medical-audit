"""Folder-level auditing and validation for healthcare document hierarchies."""

import logging
import re
from pathlib import Path

from config.settings import Settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level compiled regex constants
# ---------------------------------------------------------------------------

_id_prefix = Settings.hospital["INVOICE_IDENTIFIER_PREFIX"]
_RE_DIR_NAME = re.compile(rf"{_id_prefix}\d+$", re.IGNORECASE)
_RE_DIR_PATTERN = re.compile(rf"{_id_prefix}.\d+", re.IGNORECASE)
_RE_FOLDER_SUFFIX = re.compile(rf"({_id_prefix}\d+)$", re.IGNORECASE)
_VOID_MARKER: str = "ANULAR"


class FolderInspector:
    """Audits folder structures and validates directory naming conventions.

    Args:
        base_dir: Root directory for all inspection operations.
    """

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = Path(base_dir)

    def find_malformed_dirs(
        self, skip: list[Path] | None = None
    ) -> list[Path]:
        """Return directories whose names do not match the expected invoice pattern.

        Args:
            skip: Directory paths to exclude from the result.

        Returns:
            List of directories with non-conforming names.
        """
        skip_set = set(skip) if skip else set()
        return [
            path
            for path in self.base_dir.iterdir()
            if path.is_dir()
            and path not in skip_set
            and not _RE_DIR_NAME.match(path.name.upper())
        ]

    def resolve_dir_paths(self, dir_names: list[str]) -> list[Path]:
        """Return paths for the named directories found directly under base_dir.

        Args:
            dir_names: Directory names to locate.

        Returns:
            Paths of matching directories.
        """
        return [
            path
            for path in self.base_dir.iterdir()
            if path.is_dir() and path.name in dir_names
        ]

    def find_missing_dirs(self, expected_dirs: list[str]) -> list[str]:
        """Compare expected directory IDs against the directories on disk.

        Args:
            expected_dirs: Expected directory identifiers.

        Returns:
            Identifiers that are absent from the filesystem.
        """
        on_disk: set[str] = set()
        for path in self.base_dir.iterdir():
            if path.is_dir():
                match = _RE_DIR_PATTERN.search(path.name)
                if match:
                    on_disk.add(match.group())
        return [name for name in expected_dirs if name not in on_disk]

    def find_void_dirs(self) -> list[Path]:
        """Return directories whose names contain the void marker (``ANULAR``).

        Returns:
            Directories marked for cancellation.
        """
        return [
            d
            for d in self.base_dir.iterdir()
            if d.is_dir() and _VOID_MARKER in d.name.upper()
        ]

    def find_mismatched_files(
        self, skip_dirs: list[Path] | None = None
    ) -> list[Path]:
        """Return files whose invoice suffix does not match their parent folder name.

        Args:
            skip_dirs: Directories to exclude from the scan.

        Returns:
            Files where the trailing invoice identifier differs from the parent
            folder name.
        """
        skip_set = set(skip_dirs) if skip_dirs else set()
        mismatched: list[Path] = []

        for folder in self.base_dir.iterdir():
            if not folder.is_dir() or folder in skip_set:
                continue
            for file in folder.iterdir():
                if file.is_file():
                    match = _RE_FOLDER_SUFFIX.search(file.stem)
                    if match and match.group(1).upper() != folder.name.upper():
                        mismatched.append(file)

        return mismatched

    def find_dirs_missing_file(
        self,
        prefixes: str | list[str],
        skip: list[Path] | None = None,
        target_dirs: list[Path] | None = None,
    ) -> list[Path]:
        """Return directories that do not contain a file with the given prefix(es).

        Args:
            prefixes: Prefix or list of prefixes to search for.
            skip: Directories to exclude from the scan.
            target_dirs: Specific directories to check. If None, all
                subdirectories of base_dir are scanned.

        Returns:
            Directories missing at least one file with the required prefix.
        """
        skip_set = set(skip) if skip else set()
        dirs_to_scan = (
            target_dirs
            if target_dirs is not None
            else [p for p in self.base_dir.rglob("*") if p.is_dir()]
        )

        criteria: str | tuple[str, ...]
        if isinstance(prefixes, list):
            criteria = tuple(p.upper() for p in prefixes)
        else:
            criteria = prefixes.upper()

        return [
            d
            for d in dirs_to_scan
            if d.is_dir()
            and d not in skip_set
            and not any(
                f.is_file() and f.name.upper().startswith(criteria)
                for f in d.iterdir()
            )
        ]
