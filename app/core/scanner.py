"""Filesystem listing and file naming validation for healthcare documents."""

import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)


class DocumentScanner:
    """Finds and validates files on the filesystem using pathlib and regex.

    Args:
        base_dir: Root directory for all scan operations.
    """

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = Path(base_dir)

    def find_by_extension(self, ext: str = "pdf") -> list[Path]:
        """Return all files under base_dir with the given extension.

        Args:
            ext: File extension to search for (without leading dot).

        Returns:
            List of matching file paths.
        """
        return list(self.base_dir.rglob(f"*.{ext}"))

    def find_by_prefix(self, prefixes: str | list[str]) -> list[Path]:
        """Return files whose names start with one or more given prefixes.

        Args:
            prefixes: Single prefix string or list of prefix strings.

        Returns:
            Matching file paths.
        """
        upper = [p.upper() for p in prefixes] if isinstance(prefixes, list) else prefixes.upper()
        criteria = tuple(upper) if isinstance(upper, list) else upper
        return [f for f in self.base_dir.rglob("*.pdf") if f.name.upper().startswith(criteria)]

    @staticmethod
    def _build_name_pattern(valid_prefixes: list[str], suffix: str, nit: str) -> re.Pattern[str]:
        """Compile the expected filename regex from hospital-specific parameters.

        Args:
            valid_prefixes: Allowed document type prefixes (e.g. ``["FEV", "CRC"]``).
            suffix: Invoice identifier prefix (e.g. ``"HSL"``).
            nit: Hospital NIT number.

        Returns:
            Compiled regex pattern for ``{PREFIX}_{NIT}_{SUFFIX}{digits}.pdf``.
        """
        prefixes_group = "|".join(re.escape(p) for p in valid_prefixes)
        return re.compile(
            rf"^({prefixes_group})_{re.escape(nit)}_{re.escape(suffix)}\d+\.pdf$",
            re.IGNORECASE,
        )

    def find_invalid_names(self, valid_prefixes: list[str], suffix: str, nit: str) -> list[Path]:
        """Return PDF files that do not match the expected naming pattern.

        Expected pattern: ``{PREFIX}_{NIT}_{SUFFIX}{digits}.pdf``

        Args:
            valid_prefixes: Allowed document type prefixes (e.g. ``["FEV", "CRC"]``).
            suffix: Invoice identifier prefix (e.g. ``"HSL"``).
            nit: Hospital NIT number.

        Returns:
            Files that fail the naming validation.
        """
        pattern = self._build_name_pattern(valid_prefixes, suffix, nit)
        return [f for f in self.find_by_extension("pdf") if not pattern.match(f.name)]
