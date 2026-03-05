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
        self._re_dir_id_loose = re.compile(
            rf"({id_prefix})[^a-zA-Z]*?(\d+)", re.IGNORECASE
        )

    def remove_files(self, files: list[Path]) -> int:
        """Delete a list of files and return the count of successful deletions.

        Args:
            files: Files to remove from disk.

        Returns:
            Number of files successfully deleted.
        """
        count = 0
        for f in files:
            try:
                f.unlink()
                count += 1
            except OSError as exc:
                logger.error("Could not delete file %s: %s", f, exc)
        return count

    def relocate_misplaced(
        self, source_dir: Path, dry_run: bool = True
    ) -> None:
        """Move files from the source directory to their correct folder.

        Files are matched to destination folders by extracting the folder
        identifier from position 2 when the file name is split by ``_``.

        Args:
            source_dir: Directory containing misplaced files.
            dry_run: When True, log intended moves without executing them.
        """
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
                    logger.error(
                        "Could not move %s to %s: %s", f, destination, exc
                    )
            else:
                logger.warning(
                    "Destination folder does not exist, skipping: %s", destination
                )

    def apply_prefix_renames(
        self,
        prefix_map: dict[str, str],
        files: list[Path] | None = None,
    ) -> int:
        """Rename files by replacing their prefix according to a mapping.

        Args:
            prefix_map: Current prefix -> new prefix mapping.
            files: Files to process. Returns 0 if empty or None.

        Returns:
            Number of files successfully renamed.
        """
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
                    new_name = "%s_%s" % (prefix_map[current], parts[1])
                    try:
                        f.rename(f.with_name(new_name))
                        count += 1
                    except OSError as exc:
                        logger.error("Could not rename %s: %s", f, exc)
        return count

    def correct_nit_in_names(self, files: list[Path], correct_nit: str) -> int:
        """Rename files to use the correct NIT number.

        Args:
            files: Files whose NIT portion may be incorrect.
            correct_nit: The authoritative NIT to apply.

        Returns:
            Number of files successfully renamed.
        """
        count = 0
        for f in files:
            try:
                current_nit = self.parse_nit_from_filename(f.name)
                if current_nit and current_nit != correct_nit:
                    parts = f.name.split("_", 2)
                    if len(parts) == 3:
                        new_name = "%s_%s_%s" % (parts[0], correct_nit, parts[2])
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
        """Copy or move named directories from source to destination.

        Args:
            dir_names: Names of directories to process.
            source_dir: Directory containing the source folders.
            destination_dir: Directory where folders will land.
            action: ``"copy"`` to copy, ``"move"`` to move.

        Returns:
            Dict with keys ``success``, ``failed``, ``not_found``, and ``errors``.
        """
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
            result["errors"].append("Invalid source: %s" % source_dir)
            return result

        try:
            destination_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            logger.error("Cannot create destination %s: %s", destination_dir, exc)
            result["errors"].append("Cannot create destination: %s" % exc)
            return result

        for name in dir_names:
            src = source_dir / name
            dst = destination_dir / name

            if not src.is_dir():
                logger.warning("Source folder not found: %s", src)
                result["not_found"] += 1
                result["errors"].append("Folder not found: %s" % name)
                continue

            try:
                if dst.exists():
                    logger.warning("Destination already exists, skipping: %s", dst)
                    result["failed"] += 1
                    result["errors"].append("Already exists: %s" % name)
                elif action == "copy":
                    shutil.copytree(src, dst)
                    result["success"] += 1
                elif action == "move":
                    shutil.move(str(src), str(dst))
                    result["success"] += 1
                else:
                    raise ValueError("Invalid action: %s" % action)
            except (shutil.Error, OSError) as exc:
                logger.error("Error processing %s: %s", name, exc)
                result["failed"] += 1
                result["errors"].append("Error on %s: %s" % (name, exc))

        return result

    def tag_dirs_missing_cufe(self, files: list[Path]) -> int:
        """Append ' CUFE' to parent folders of files that are missing a CUFE code.

        Deduplicates parent directories so each folder is renamed at most once.
        Folders already ending with ' CUFE' are skipped.

        Args:
            files: Invoice PDFs found to be missing a valid CUFE.

        Returns:
            Number of directories successfully renamed.
        """
        count = 0
        seen: set[Path] = set()

        for f in files:
            parent = f.parent
            if parent in seen:
                continue
            seen.add(parent)

            if parent.name.upper().endswith(" CUFE"):
                continue

            new_path = parent.parent / ("%s CUFE" % parent.name)
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
        """Rename directories to their canonical identifier, stripping extra text.

        Extracts the first occurrence of ``{id_prefix}\\d+`` from the name and
        renames the directory to that value (uppercased). Directories already
        matching or without a parseable identifier are skipped.

        Args:
            dirs: Directories with non-canonical names.

        Returns:
            Number of directories successfully renamed.
        """
        count = 0
        for dir_path in dirs:
            match = self._re_dir_id_loose.search(dir_path.name)
            if not match:
                logger.warning(
                    "Cannot extract canonical name from: %s", dir_path.name
                )
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
        """Extract the NIT number embedded between underscores in a file name.

        Args:
            filename: File name string to parse.

        Returns:
            Extracted NIT string, or None if not found.
        """
        match = _RE_NIT.search(filename)
        return match.group(1) if match else None
