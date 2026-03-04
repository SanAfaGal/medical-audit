"""File normaliser: renames PDFs to the hospital's canonical naming standard."""

import logging
import re
from dataclasses import dataclass
from pathlib import Path

from config.settings import Settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Compiled regex constants
# ---------------------------------------------------------------------------

_id_prefix = Settings.hospital["INVOICE_IDENTIFIER_PREFIX"]
_RE_INVOICE_ID_STRICT = re.compile(rf"{_id_prefix}_?(\d+)", re.IGNORECASE)
_RE_INVOICE_ID_LOOSE = re.compile(rf"{_id_prefix}[-_ ]?(\d+)", re.IGNORECASE)
_RE_PREFIX = re.compile(r"^([a-zA-Z]+)")


@dataclass
class RenameResult:
    """Result record for a single file rename attempt."""

    original_path: str
    new_name: str
    status: str   # SUCCESS, REJECTED, or ERROR
    reason: str


class FilenameStandardizer:
    """Normalise PDF files to the standard: ``{PREFIX}_{NIT}_{SUFFIX}{ID}.pdf``.

    Applies deep-cleaning and cross-validates the invoice ID with the parent
    directory name.

    Args:
        nit: Hospital NIT number.
        valid_prefixes: Allowed document type prefixes.
        suffix_const: Invoice identifier prefix used as the name suffix.
        prefix_map: Maps misnamed prefixes to their correct counterparts.
    """

    def __init__(
        self,
        nit: str,
        valid_prefixes: list[str],
        suffix_const: str,
        prefix_map: dict[str, str],
    ) -> None:
        self.nit = nit
        self.valid_prefixes = valid_prefixes
        self.suffix_const = suffix_const
        self.prefix_map = prefix_map

    def _extract_id_from_path(self, file_path: Path) -> str:
        """Extract the invoice ID from the file path.

        Prioritises the parent folder name as the source of truth.

        Args:
            file_path: Path to the PDF file.

        Returns:
            The ID string, or an empty string if not found.
        """
        folder_match = _RE_INVOICE_ID_STRICT.search(file_path.parent.name)
        if folder_match:
            return folder_match.group(1)

        file_match = _RE_INVOICE_ID_LOOSE.search(file_path.name)
        if file_match:
            return file_match.group(1)

        return ""

    def _sanitize_prefix(self, raw_name: str) -> str:
        """Clean and map the leading alphabetic prefix of a file name.

        Args:
            raw_name: Raw file name string.

        Returns:
            Mapped (or original) prefix in upper case, or empty string if none found.
        """
        match = _RE_PREFIX.match(raw_name)
        if not match:
            return ""
        prefix = match.group(1).upper()
        return self.prefix_map.get(prefix, prefix)

    def build_canonical_name(self, file_path: Path) -> tuple[str | None, str]:
        """Attempt to build the canonical name for a PDF file.

        Args:
            file_path: Path to the file to normalise.

        Returns:
            Tuple of ``(new_name, reason)``. ``new_name`` is None on failure.
        """
        file_id = self._extract_id_from_path(file_path)
        if not file_id:
            return None, "Could not find a valid invoice ID."

        prefix = self._sanitize_prefix(file_path.name)
        if prefix not in self.valid_prefixes:
            return None, "Prefix '%s' is not recognised or invalid." % prefix

        name = "%s_%s_%s%s.pdf" % (prefix, self.nit, self.suffix_const, file_id)
        return name, "Ok"

    def run(self, files: list[Path]) -> list[RenameResult]:
        """Normalise a list of PDF files and return a report for each.

        Args:
            files: PDF files to process.

        Returns:
            List of :class:`RenameResult` records (one per file).
        """
        results: list[RenameResult] = []

        for f in files:
            if not f.is_file():
                continue

            try:
                new_name, reason = self.build_canonical_name(f)

                if new_name is None:
                    logger.warning("Skipped %s: %s", f.name, reason)
                    results.append(RenameResult(str(f), "N/A", "REJECTED", reason))
                    continue

                if f.name == new_name:
                    continue

                target = f.with_name(new_name)
                if target.exists():
                    logger.warning(
                        "Skipped %s: destination already exists", f.name
                    )
                    results.append(
                        RenameResult(
                            str(f),
                            new_name,
                            "REJECTED",
                            "Destination file already exists",
                        )
                    )
                else:
                    f.rename(target)
                    logger.info("Renamed %s -> %s", f.name, new_name)
                    results.append(
                        RenameResult(str(f), new_name, "SUCCESS", "Renamed successfully")
                    )

            except OSError as exc:
                logger.error("Could not rename %s: %s", f, exc)
                results.append(RenameResult(str(f), "N/A", "ERROR", str(exc)))

        return results
