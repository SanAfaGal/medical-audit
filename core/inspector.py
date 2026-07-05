"""Folder-level auditing and validation for healthcare document hierarchies."""

import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

_VOID_MARKER: str = "ANULAR"
_UNKNOWN_MARKER: str = "(DESCONOCIDO)"


class FolderInspector:
    """Audits folder structures and validates directory naming conventions.

    Args:
        base_dir: Root directory for all inspection operations.
        id_prefix: Invoice identifier prefix used to build directory-name regexes.
    """

    def __init__(self, base_dir: Path, id_prefix: str = "") -> None:
        self.base_dir = Path(base_dir)
        self._id_prefix = id_prefix.upper()
        _esc = re.escape(id_prefix)
        # When there is no prefix the separator token is meaningless — omitting it
        # prevents the wildcard from consuming the first digit of a numeric-only ID.
        _sep = r"[^a-zA-Z0-9]?" if id_prefix else ""
        self._re_dir_name = re.compile(rf"^{_esc}\d+$", re.IGNORECASE)
        self._re_dir_pattern = re.compile(rf"{_esc}{_sep}(\d+)", re.IGNORECASE)
        self._re_folder_suffix = re.compile(rf"({_esc}{_sep}\d+)$", re.IGNORECASE)

    def find_malformed_dirs(self, skip: list[Path] | None = None) -> list[Path]:
        """Return directories whose names do not match the expected invoice pattern.

        Args:
            skip: Directory paths to exclude from the result.

        Returns:
            List of directories with non-conforming names.
        """
        skip_set = set(skip) if skip else set()
        try:
            entries = list(self.base_dir.iterdir())
        except PermissionError as exc:
            logger.error("No se puede leer el directorio %s: %s", self.base_dir, exc)
            return []
        return [
            path
            for path in entries
            if path.is_dir() and path not in skip_set and not self._re_dir_name.match(path.name.upper())
        ]

    def resolve_dir_paths(self, dir_names: list[str]) -> list[Path]:
        """Return paths for the named directories found directly under base_dir.

        Args:
            dir_names: Directory names to locate.

        Returns:
            Paths of matching directories.
        """
        return [path for path in self.base_dir.iterdir() if path.is_dir() and path.name in dir_names]

    def find_missing_dirs(self, expected_dirs: list[str]) -> list[str]:
        """Compare expected directory IDs against the directories on disk.

        Args:
            expected_dirs: Expected directory identifiers.

        Returns:
            Identifiers that are absent from the filesystem.
        """
        on_disk: set[str] = set()
        try:
            entries = list(self.base_dir.iterdir())
        except PermissionError as exc:
            logger.error("No se puede leer el directorio %s: %s", self.base_dir, exc)
            return list(expected_dirs)
        for path in entries:
            if path.is_dir():
                match = self._re_dir_pattern.search(path.name)
                if match:
                    # DB stores only the numeric portion (e.g. "12345"), not the full
                    # folder name which may include a prefix (e.g. "HSL12345").
                    on_disk.add(match.group(1))
        return [name for name in expected_dirs if name not in on_disk]

    def find_unknown_dirs(self, known_numbers: set[str]) -> list[Path]:
        """Return folders that match the invoice pattern but are absent from *known_numbers*.

        Folders already prefixed with ``_UNKNOWN_MARKER`` are skipped so the
        operation is idempotent when re-run.

        Args:
            known_numbers: Invoice numbers present in the database for this period.

        Returns:
            Paths of on-disk folders whose extracted invoice number has no
            matching record in *known_numbers*.
        """
        result: list[Path] = []
        for path in self.base_dir.iterdir():
            if not path.is_dir():
                continue
            if path.name.startswith(_UNKNOWN_MARKER):
                continue
            match = self._re_dir_pattern.search(path.name)
            if match and match.group(1) not in known_numbers:
                result.append(path)
        return result

    def extract_invoice_number(self, folder_name: str) -> str | None:
        """Extract the invoice number digits from a folder name.

        Args:
            folder_name: Directory name to parse.

        Returns:
            The digit-only portion of the invoice identifier, or None if no
            match is found.
        """
        match = self._re_dir_pattern.search(folder_name)
        return match.group(1) if match else None

    def find_void_dirs(self) -> list[Path]:
        """Return directories whose names contain the void marker (``ANULAR``).

        Returns:
            Directories marked for cancellation.
        """
        return [d for d in self.base_dir.iterdir() if d.is_dir() and _VOID_MARKER in d.name.upper()]

    def find_mismatched_files(self, skip_dirs: list[Path] | None = None) -> list[Path]:
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
                    match = self._re_folder_suffix.search(file.stem)
                    if match and match.group(1).upper() != folder.name.upper():
                        mismatched.append(file)

        return mismatched

    def check_required_docs(
        self,
        folder: Path,
        required_prefixes: dict[str, list[str]],
    ) -> list[str]:
        """Return document codes missing from a folder based on required prefixes.

        Args:
            folder: Path to the invoice folder to inspect.
            required_prefixes: Mapping of document_type code → list of filename
                prefixes to look for (e.g. ``{"FIRMA": ["CRC"], "HISTORIA": ["EPI","HEV"]}``).

        Returns:
            List of document codes whose files were not found in the folder.
            Codes with empty prefix lists are silently skipped (they are content
            checks, not file-presence checks).
        """
        if not folder.is_dir():
            return list(required_prefixes.keys())

        files_upper = [f.name.upper() for f in folder.iterdir() if f.is_file()]
        missing: list[str] = []
        for doc_code, prefixes in required_prefixes.items():
            if not prefixes:
                continue
            criteria = tuple(p.upper() for p in prefixes)
            if not any(fname.startswith(criteria) for fname in files_upper):
                missing.append(doc_code)
        return missing

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
        dirs_to_scan = target_dirs if target_dirs is not None else [p for p in self.base_dir.rglob("*") if p.is_dir()]

        criteria: str | tuple[str, ...]
        criteria = tuple(p.upper() for p in prefixes) if isinstance(prefixes, list) else prefixes.upper()

        return [
            d
            for d in dirs_to_scan
            if d not in skip_set and not any(f.is_file() and f.name.upper().startswith(criteria) for f in d.iterdir())
        ]
