"""Folder organisation services for healthcare document auditing."""

import logging
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import NamedTuple

import pandas as pd

from core.helpers import safe_move

logger = logging.getLogger(__name__)


class FolderCopier:
    """Handles physical folder copy operations into a target root directory.

    Args:
        target_root: Root directory where folders will be copied.
    """

    def __init__(self, target_root: Path) -> None:
        self.target_root = Path(target_root)
        self.target_root.mkdir(parents=True, exist_ok=True)

    def move_folders(self, folders: list[Path], use_prefix: bool = True) -> None:
        """Move a list of folders into the target root.

        Uses shutil.move which resolves to an O(1) rename when source and
        destination are on the same filesystem volume.

        Args:
            folders: Source folder paths to move.
            use_prefix: When True, prefix the destination name with the parent
                folder name to avoid name collisions.
        """
        for folder in folders:
            dest = self._build_destination(folder, use_prefix)
            try:
                shutil.move(str(folder), dest)
                logger.info("Moved %s to %s", folder.name, dest.name)
            except (shutil.Error, OSError) as exc:
                logger.error("Failed to move %s: %s", folder.name, exc)

    def _build_destination(self, folder: Path, use_prefix: bool) -> Path:
        """Build the destination path, optionally prefixing with the parent name.

        Args:
            folder: Source folder.
            use_prefix: Whether to prepend the parent directory name.

        Returns:
            Full destination path inside target_root.
        """
        name = (
            f"{folder.parent.name}_{folder.name}"
            if use_prefix
            else folder.name
        )
        return self.target_root / name


class LeafFolderFinder:
    """Scans the filesystem and identifies folders that directly contain files."""

    @staticmethod
    def has_files(path: Path) -> bool:
        """Return True if the directory directly contains at least one file.

        Args:
            path: Directory path to inspect.

        Returns:
            True if the directory exists and contains at least one file.
        """
        if not path.is_dir():
            return False
        return any(item.is_file() for item in path.iterdir())

    def find_leaf_folders(self, source_root: Path) -> list[Path]:
        """Scan a source root recursively and return directories containing files.

        Args:
            source_root: Root directory to scan.

        Returns:
            List of directories that directly contain at least one file.
        """
        return [
            folder
            for folder in Path(source_root).rglob("*")
            if folder.is_dir() and self.has_files(folder)
        ]


@dataclass
class _OrganizeStats:
    """Mutable counters accumulated during a single organize run."""

    moved: int = 0
    failed: int = 0
    not_found: int = 0
    errors: list[str] = field(default_factory=list)
    moved_ids: list[str] = field(default_factory=list)


class TransferSummary(NamedTuple):
    """Summary of a folder organisation run."""

    moved: int
    failed: int
    not_found: int
    errors: list[str]
    moved_ids: list[str]


class InvoiceOrganizer:
    """Orchestrates the movement of invoice folders to their final hierarchy.

    Optimised for large datasets via a single up-front index of the staging area
    rather than repeated directory scans.

    Args:
        df: DataFrame indexed by invoice ID with a ``Ruta`` column.
        staging_dir: Directory containing the downloaded invoice folders.
        archive_dir: Root directory for the final organised hierarchy.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        staging_dir: Path,
        archive_dir: Path,
    ) -> None:
        self.df = df
        self.staging_dir = Path(staging_dir)
        self.archive_dir = Path(archive_dir)
        self._staging_cache: dict[str, Path] = {}

    def _index_staging_area(self) -> None:
        """Scan the staging folder once and map invoice IDs to physical paths."""
        logger.info("Indexing staging area at %s", self.staging_dir)
        for folder in self.staging_dir.iterdir():
            if folder.is_dir():
                self._staging_cache[folder.name] = folder

    def _find_source_in_staging(self, invoice_id: str) -> Path | None:
        """Return the staging path for the given invoice ID, or None if not found.

        Tries an exact key match first, then falls back to an ``endswith``
        suffix match to tolerate folder names with extra prefixes.

        Args:
            invoice_id: Invoice identifier to look up.

        Returns:
            Matching :class:`~pathlib.Path`, or ``None``.
        """
        key = invoice_id.upper()
        if key in self._staging_cache:
            return self._staging_cache[key]
        return next(
            (
                path
                for name, path in self._staging_cache.items()
                if name.upper().endswith(key) or name.upper().startswith(key + " ")
            ),
            None,
        )

    def _move_single_invoice(
        self,
        invoice_id: str,
        source_path: Path,
        destination_path: Path,
        dry_run: bool,
        stats: _OrganizeStats,
    ) -> None:
        """Move a single invoice folder from staging to its final location.

        Args:
            invoice_id: Invoice identifier (used for logging).
            source_path: Current location of the folder in staging.
            destination_path: Target location in the final hierarchy.
            dry_run: When True, only log the intended move without executing it.
            stats: Mutable statistics accumulator updated in place.
        """
        if dry_run:
            logger.info("[DRY RUN] %s -> %s", source_path.name, destination_path)
            stats.moved += 1
            stats.moved_ids.append(invoice_id)
            return

        if safe_move(source_path, destination_path):
            stats.moved += 1
            stats.moved_ids.append(invoice_id)
            logger.info("Moved invoice %s successfully", invoice_id)
        else:
            stats.failed += 1
            stats.errors.append(f"Failed to move {invoice_id}")

    def organize(self, dry_run: bool = False) -> TransferSummary:
        """Execute the migration of invoice folders to the final hierarchy.

        Args:
            dry_run: When True, log intended moves without modifying the filesystem.

        Returns:
            :class:`TransferSummary` with counts of moved, failed, and not-found folders.
        """
        self._index_staging_area()
        stats = _OrganizeStats()

        for invoice_id, row in self.df.iterrows():
            source_path = self._find_source_in_staging(str(invoice_id))

            if not source_path:
                logger.warning("Invoice not found in staging: %s", invoice_id)
                stats.not_found += 1
                continue

            destination_path = self.archive_dir / row["Ruta"]
            self._move_single_invoice(
                str(invoice_id), source_path, destination_path, dry_run, stats
            )

        return TransferSummary(
            moved=stats.moved,
            failed=stats.failed,
            not_found=stats.not_found,
            errors=stats.errors,
            moved_ids=stats.moved_ids,
        )
