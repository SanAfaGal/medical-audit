"""Google Drive API client for folder search and recursive file downloads."""

import io
import logging
import time
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Drive API constants
# ---------------------------------------------------------------------------

_DRIVE_FOLDER_MIME: str = "application/vnd.google-apps.folder"
_DRIVE_SCOPES: list[str] = ["https://www.googleapis.com/auth/drive.readonly"]
_DRIVE_SEARCH_PAGE_SIZE: int = 1000
_DRIVE_SINGLE_PAGE_SIZE: int = 1

# Transient HTTP status codes that warrant a retry (5xx server errors + 429 rate-limit).
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
_MAX_RETRIES: int = 4
_RETRY_BACKOFF_BASE: float = 2.0  # seconds; doubles on each attempt


class DriveSync:
    """Client for the Google Drive API.

    Supports global folder searches and recursive directory downloads.

    Args:
        credentials_path: Path to the service-account JSON credentials file.
    """

    def __init__(self, credentials_path: Path) -> None:
        self.creds = service_account.Credentials.from_service_account_file(
            str(credentials_path), scopes=_DRIVE_SCOPES
        )
        self.service = build("drive", "v3", credentials=self.creds)

    def _execute_with_retry(self, request) -> dict:
        """Execute a Drive API request, retrying on transient server errors.

        Uses exponential backoff for HTTP 429 / 5xx responses.

        Args:
            request: A googleapiclient HttpRequest object (not yet executed).

        Returns:
            The parsed JSON response dict.

        Raises:
            HttpError: Re-raised after all retries are exhausted, or immediately
                for non-retryable status codes (4xx other than 429).
        """
        delay = _RETRY_BACKOFF_BASE
        for attempt in range(_MAX_RETRIES + 1):
            try:
                return request.execute()
            except HttpError as exc:
                if exc.status_code not in _RETRYABLE_STATUS or attempt == _MAX_RETRIES:
                    raise
                logger.warning(
                    "Drive API HTTP %s — retrying in %.0fs (attempt %d/%d)…",
                    exc.status_code, delay, attempt + 1, _MAX_RETRIES,
                )
                time.sleep(delay)
                delay *= 2
        raise RuntimeError("unreachable")  # pragma: no cover

    def find_folders_by_name(self, folder_name: str) -> list[dict]:
        """Search Drive for folders whose names contain the given string.

        Args:
            folder_name: Substring to match against folder names.

        Returns:
            List of Drive file resource dicts (id, name, parents).
        """
        query = (
            f"name contains '{folder_name}' "
            f"and mimeType = '{_DRIVE_FOLDER_MIME}' "
            "and trashed = false"
        )
        request = self.service.files().list(
            q=query,
            fields="files(id, name, parents)",
            pageSize=_DRIVE_SEARCH_PAGE_SIZE,
        )
        results = self._execute_with_retry(request)
        return results.get("files", [])

    def download_file(
        self, file_id: str, file_name: str, local_dir: Path
    ) -> None:
        """Download a single file from Drive to the local filesystem.

        Skips download if the file already exists locally.

        Args:
            file_id: Drive file identifier.
            file_name: Name to use for the local file.
            local_dir: Directory where the file will be saved.
        """
        local_dir.mkdir(parents=True, exist_ok=True)
        file_path = local_dir / file_name

        if file_path.exists():
            logger.info("Skipping already-downloaded file: %s", file_name)
            return

        try:
            request = self.service.files().get_media(fileId=file_id)
            with io.FileIO(str(file_path), "wb") as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    _status, done = downloader.next_chunk()
            logger.info("Downloaded file: %s", file_name)
        except (OSError, HttpError) as exc:
            logger.error("Failed to download file %s: %s", file_name, exc)

    def _list_folder_contents(
        self, folder_id: str, page_token: str | None
    ) -> dict:
        """Fetch one page of a Drive folder's children.

        Args:
            folder_id: Drive folder identifier.
            page_token: Pagination token from the previous request, or None.

        Returns:
            Raw Drive API response dict.
        """
        query = f"'{folder_id}' in parents and trashed = false"
        return (
            self.service.files()
            .list(
                q=query,
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token,
                pageSize=_DRIVE_SEARCH_PAGE_SIZE,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )

    def _process_drive_item(
        self, item: dict, local_path: Path, depth: int
    ) -> None:
        """Route a single Drive item to download or recurse into a sub-folder.

        Args:
            item: Drive file resource dict (id, name, mimeType).
            local_path: Local directory corresponding to the current Drive folder.
            depth: Current recursion depth (used for logging).
        """
        if item["mimeType"] == _DRIVE_FOLDER_MIME:
            self._sync_folder_tree(item["id"], local_path / item["name"], depth + 1)
        elif "google-apps" not in item["mimeType"]:
            self.download_file(item["id"], item["name"], local_path)
        else:
            logger.info("Skipping Google native file: %s", item["name"])

    def _sync_folder_tree(
        self, folder_id: str, local_path: Path, depth: int = 0
    ) -> None:
        """Recursively download the contents of a Drive folder.

        Args:
            folder_id: Drive folder identifier.
            local_path: Local directory where files will be saved.
            depth: Current recursion depth.
        """
        logger.info("Processing folder %s (depth %d)", local_path.name, depth)

        page_token: str | None = None
        has_items = False

        while True:
            results = self._list_folder_contents(folder_id, page_token)
            items = results.get("files", [])

            if items:
                has_items = True

            for item in items:
                self._process_drive_item(item, local_path, depth)

            page_token = results.get("nextPageToken")
            if not page_token:
                break

        if not has_items and depth == 0:
            logger.warning("Folder appears to be empty in Drive: %s", local_path.name)

    def download_missing_dirs(
        self, dir_names: list[str], local_root: Path
    ) -> list[str]:
        """Search Drive for a list of folder names and download each one found.

        Args:
            dir_names: Folder names to search for.
            local_root: Local root directory where folders will be downloaded.

        Returns:
            Names of folders that were found in Drive and downloaded.
            Folders absent from this list were not found in Drive.
        """
        downloaded: list[str] = []
        for target in dir_names:
            logger.info("Searching Drive for folder: %s", target)
            found = self.find_folders_by_name(target)
            logger.info("Search results for %s: %d found", target, len(found))

            if not found:
                logger.warning("Folder not found in Drive: %s", target)
                continue

            for folder in found:
                dest = local_root / folder["name"]
                self._sync_folder_tree(folder["id"], dest)

            downloaded.append(target)

        logger.info(
            "Drive download complete: %d/%d folders found",
            len(downloaded), len(dir_names),
        )
        return downloaded

    def download_specific_files(
        self, file_names: list[str], local_root: Path
    ) -> None:
        """Search for specific files by name in Drive and download them.

        Args:
            file_names: Exact file names to search for.
            local_root: Local directory where found files will be saved.
        """
        logger.info("Starting search for %d specific files", len(file_names))
        not_found: set[str] = set()

        for name in file_names:
            query = (
                f"name = '{name}' "
                f"and mimeType != '{_DRIVE_FOLDER_MIME}' "
                "and trashed = false"
            )
            results = (
                self.service.files()
                .list(q=query, fields="files(id, name)", pageSize=_DRIVE_SINGLE_PAGE_SIZE)
                .execute()
            )
            files = results.get("files", [])

            if not files:
                logger.info("File not found in Drive: %s", name)
                not_found.add(name)
                continue

            file_info = files[0]
            self.download_file(file_info["id"], file_info["name"], local_root)

        if not_found:
            logger.warning("Files not found in Drive: %s", sorted(not_found))
