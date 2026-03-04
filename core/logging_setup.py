"""Centralised logging configuration for the application."""

import logging
from pathlib import Path


def configure_logging(output_dir: str = "logs", level: str = "INFO") -> None:
    """Set up file and console log handlers on the root logger.

    Safe to call multiple times — existing handlers are cleared first.

    Args:
        output_dir: Directory where ``app.log`` will be written.
        level: Logging level name (e.g. ``"INFO"``, ``"DEBUG"``).
    """
    log_dir = Path(output_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "app.log"

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    if root.hasHandlers():
        root.handlers.clear()

    file_handler = logging.FileHandler(str(log_file), encoding="utf-8")
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(message)s"))

    root.addHandler(file_handler)
    root.addHandler(console_handler)
