"""Rule behind CHECK_REQUIRED_DOCS: which required documents each invoice folder lacks."""

from __future__ import annotations

import threading
from collections.abc import Mapping, Sequence
from pathlib import Path

from app.core.helpers import ProgressFn
from app.core.inspector import FolderInspector


def find_missing_documents(
    stage_path: Path,
    id_prefix: str,
    invoices: Sequence[tuple[int, str, int | None]],
    service_docs: Mapping[int, Sequence[int]],
    prefixes: Mapping[int, list[str]],
    on_progress: ProgressFn,
    cancel_event: threading.Event | None,
) -> dict[int, tuple[str, list[int]]]:
    """``{invoice_id: (invoice_number, [missing document type ids])}`` for the invoices that lack something.

    ``invoices`` is ``(id, number, service_type_id)``; ``service_docs`` maps a service type to the document types
    it requires, and ``prefixes`` a document type to the filename prefixes that satisfy it. Plain data in, plain
    data out, so it can run in a worker thread.
    """
    inspector = FolderInspector(stage_path, id_prefix)
    missing: dict[int, tuple[str, list[int]]] = {}
    for index, (invoice_id, number, service_type_id) in enumerate(invoices, 1):
        if cancel_event is not None and cancel_event.is_set():
            break
        on_progress(index, len(invoices), number)
        required = service_docs.get(service_type_id, []) if service_type_id is not None else []
        if not required:
            continue
        required_prefixes = {str(dt_id): prefixes.get(dt_id, []) for dt_id in required}
        missing_codes = inspector.check_required_docs(stage_path / (id_prefix + number), required_prefixes)
        if missing_codes:
            missing[invoice_id] = (number, [int(code) for code in missing_codes])
    return missing
