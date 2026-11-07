"""Rules behind REVISAR_SOBRANTES: which files in an invoice folder are surplus, and which missing document each
one most likely is. Pure functions over plain data (no ORM objects), so they run in a worker thread and are easy to test.
"""

from __future__ import annotations

import threading
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypedDict

from app.core.helpers import ProgressFn
from app.services.pipeline.logfmt import plog

if TYPE_CHECKING:
    from app.models.invoice import Invoice

#: A surplus file is only suggested as a missing document when the name is at least this similar to its prefix.
MIN_SIMILARITY = 0.3


class Faltante(TypedDict):
    """A required document still missing for an invoice (an open finding)."""

    doc_type_id: int
    code: str
    description: str
    prefix: str | None


class InvoiceInfo(TypedDict):
    """The slice of an invoice this review needs."""

    id: int
    invoice_number: str
    service_type_id: int | None
    admin_type: str | None
    service_type: str | None
    faltantes: list[Faltante]


class Suggestion(TypedDict):
    suggested_doc_type_id: int | None
    suggested_code: str | None
    suggested_prefix: str | None
    confidence: str | None


NO_SUGGESTION: Suggestion = {
    "suggested_doc_type_id": None,
    "suggested_code": None,
    "suggested_prefix": None,
    "confidence": None,
}


@dataclass
class SurplusScan:
    items: list[dict[str, Any]] = field(default_factory=list)
    log_lines: list[str] = field(default_factory=list)
    total: int = 0  # surplus files across all folders


def snapshot_invoices(invoices: Iterable[Invoice]) -> dict[str, InvoiceInfo]:
    """Plain-data copy of loaded invoices, keyed by upper-cased invoice number (ORM objects can't cross threads)."""
    snapshot: dict[str, InvoiceInfo] = {}
    for inv in invoices:
        snapshot[inv.invoice_number.upper()] = {
            "id": inv.id,
            "invoice_number": inv.invoice_number,
            "service_type_id": inv.service_type_id,
            "admin_type": inv.agreement.contract_type.name if inv.agreement and inv.agreement.contract_type else None,
            "service_type": inv.service_type.display_name if inv.service_type else None,
            "faltantes": [
                {
                    "doc_type_id": mf.doc_type.id,
                    "code": mf.doc_type.code,
                    "description": mf.doc_type.description,
                    "prefix": mf.doc_type.prefix,
                }
                for mf in inv.missing_files
                if mf.resolved_at is None and mf.doc_type
            ],
        }
    return snapshot


def match_invoice(folder_name: str, invoices: Mapping[str, InvoiceInfo]) -> InvoiceInfo | None:
    """The invoice a folder belongs to: exact name, or the number followed by extra text, or ending with it."""
    key = folder_name.upper()
    exact = invoices.get(key)
    if exact is not None:
        return exact
    return next(
        (
            info
            for number, info in invoices.items()
            if key.startswith(number + "_") or key.startswith(number + " ") or key.endswith(number)
        ),
        None,
    )


def surplus_files(folder: Path, required_prefixes: Sequence[str]) -> list[Path]:
    """Files in ``folder`` whose name starts with none of the required document prefixes."""
    prefixes = [p.upper() for p in required_prefixes]
    return [f for f in folder.iterdir() if f.is_file() and not any(f.name.upper().startswith(p) for p in prefixes)]


def suggest_documents(filenames: Sequence[str], faltantes: Sequence[Faltante]) -> dict[str, Suggestion]:
    """Guess which missing document each surplus file is.

    One surplus file and one missing document: a high-confidence match. Otherwise each file gets the missing
    document whose prefix its name resembles most (``difflib`` ratio above :data:`MIN_SIMILARITY`), low confidence.
    """
    if not faltantes:
        return {}
    if len(filenames) == 1 and len(faltantes) == 1:
        return {filenames[0]: _suggestion(faltantes[0], "high")}

    suggestions: dict[str, Suggestion] = {}
    for name in filenames:
        best_score, best = 0.0, None
        for faltante in faltantes:
            prefix = faltante["prefix"]
            if not prefix:
                continue
            score = SequenceMatcher(None, name.upper(), prefix.upper()).ratio()
            if score > best_score:
                best_score, best = score, faltante
        if best is not None and best_score > MIN_SIMILARITY:
            suggestions[name] = _suggestion(best, "low")
    return suggestions


def _suggestion(faltante: Faltante, confidence: str) -> Suggestion:
    return {
        "suggested_doc_type_id": faltante["doc_type_id"],
        "suggested_code": faltante["code"],
        "suggested_prefix": faltante["prefix"],
        "confidence": confidence,
    }


def scan_surplus(
    stage_path: Path,
    invoices: Mapping[str, InvoiceInfo],
    service_docs: Mapping[int, Sequence[int]],
    prefixes: Mapping[int, str | None],
    on_progress: ProgressFn,
    cancel_event: threading.Event | None,
) -> SurplusScan:
    """Review every folder in STAGE: the payload the UI shows (surplus files + suggestions) and the log lines.

    ``service_docs`` maps a service type to the document types it requires; ``prefixes`` maps a document type to its
    filename prefix.
    """
    scan = SurplusScan()
    folders = sorted(d for d in stage_path.iterdir() if d.is_dir())
    for index, folder in enumerate(folders, 1):
        if cancel_event is not None and cancel_event.is_set():
            break
        on_progress(index, len(folders), folder.name)

        info = match_invoice(folder.name, invoices)
        if info is None:
            continue
        required = service_docs.get(info["service_type_id"], []) if info["service_type_id"] is not None else []
        sobrantes = surplus_files(folder, [prefix for dt_id in required if (prefix := prefixes.get(dt_id))])
        if not sobrantes:
            continue

        suggestions = suggest_documents([f.name for f in sobrantes], info["faltantes"])
        scan.items.append(
            {
                "invoice_id": info["id"],
                "invoice_number": info["invoice_number"],
                "admin_type": info["admin_type"],
                "service_type": info["service_type"],
                "folder_path": str(folder),
                "sobrantes": [{"filename": f.name, **suggestions.get(f.name, NO_SUGGESTION)} for f in sobrantes],
                "faltantes": [
                    {"doc_type_id": ft["doc_type_id"], "code": ft["code"], "description": ft["description"]}
                    for ft in info["faltantes"]
                ],
            }
        )
        scan.total += len(sobrantes)
        scan.log_lines.append(
            plog(
                "INFO",
                f"{len(sobrantes)} sobrante(s), {len(info['faltantes'])} faltante(s)",
                folder=folder.name,
                contract_type=info["admin_type"],
            )
        )
    return scan
