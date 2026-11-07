"""Log line formatting for pipeline stages."""

from __future__ import annotations


def plog(
    level: str,
    msg: str,
    *,
    folder: str | None = None,
    contract_type: str | None = None,
) -> str:
    """Format a pipeline log line.

    Usage:
        plog("INFO", "Facturas cargadas: 154")
        plog("WARN", "PDF sin texto", folder="FAC-001234", contract_type="CAPITACIÓN")
        plog("ERROR", "Directorio no existe")
    """
    parts = [f"[{level}]"]
    if contract_type:
        parts.append(f"[{contract_type}]")
    if folder:
        parts.append(f"[{folder}]")
    parts.append(msg)
    return " ".join(parts)
