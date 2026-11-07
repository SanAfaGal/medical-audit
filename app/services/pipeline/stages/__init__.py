"""Stage handlers, grouped by phase. Importing this package registers every stage."""

from app.services.pipeline.stages import downloads, ingest, packaging, staging, verification

__all__ = ["downloads", "ingest", "packaging", "staging", "verification"]
