"""Folder organisation services for healthcare document auditing."""

import logging
import shutil
from pathlib import Path


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
        """Move a list of folders into the target root."""
        for folder in folders:
            dest = self._build_destination(folder, use_prefix)
            try:
                shutil.move(str(folder), dest)
                logger.info("Moved %s to %s", folder.name, dest.name)
            except (shutil.Error, OSError) as exc:
                logger.error("Failed to move %s: %s", folder.name, exc)

    def _build_destination(self, folder: Path, use_prefix: bool) -> Path:
        """Build the destination path, optionally prefixing with the parent name."""
        name = f"{folder.parent.name}_{folder.name}" if use_prefix else folder.name
        return self.target_root / name


class LeafFolderFinder:
    """Scans the filesystem and identifies folders that directly contain files."""

    @staticmethod
    def has_files(path: Path) -> bool:
        """Return True if the directory directly contains at least one file."""
        if not path.is_dir():
            return False
        return any(item.is_file() for item in path.iterdir())

    def find_leaf_folders(self, source_root: Path) -> list[Path]:
        """Scan a source root recursively and return directories containing files."""
        return [folder for folder in Path(source_root).rglob("*") if folder.is_dir() and self.has_files(folder)]
