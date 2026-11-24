"""Stage handlers, grouped by phase. Importing this package registers every stage."""

from app.services.pipeline.stages import (
    downloads,
    findings,
    folder_checks,
    ingest,
    invoice_checks,
    packaging,
    pdf_quality,
    staging,
)

__all__ = ["downloads", "findings", "folder_checks", "ingest", "invoice_checks", "packaging", "pdf_quality", "staging"]
