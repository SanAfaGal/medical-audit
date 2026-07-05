"""BillingIngester: load SIHOS Excel and upsert invoices into PostgreSQL."""

from __future__ import annotations

import io
import logging

import pandas as pd
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.institution import Administrator, Agreement, Contract, Institution, Service
from app.models.invoice import Invoice
from app.repositories.institution_repo import InstitutionRepo
from app.repositories.rules_repo import RulesRepo

logger = logging.getLogger(__name__)

# Columns expected in the new SIHOS Excel export (uppercase format)
_SIHOS_COLUMNS = [
    "FACTURA",
    "FECHA",
    "DOCUMENTO",
    "NUMERO",
    "PACIENTE",
    "ADMINISTRADORA",
    "CONTRATO",
    "SERVICIO",
    "OPERARIO",
]

_DEFAULT_FOLDER_STATUS = "PRESENTE"


def load_excel(file_bytes: bytes) -> pd.DataFrame:
    """Read SIHOS Excel from raw bytes into a DataFrame."""
    buf = io.BytesIO(file_bytes)
    raw_df = pd.read_excel(buf, dtype=str)
    available = [c for c in _SIHOS_COLUMNS if c in raw_df.columns]
    df = raw_df[available].copy()
    if "ADMISION" in raw_df.columns:
        df["ADMISION"] = raw_df["ADMISION"]
    return df


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Strip blanks and validate the FACTURA column."""
    factura = df["FACTURA"].str.strip().str.upper()
    mask = factura.notna() & factura.ne("") & df["ADMINISTRADORA"].notna()
    df = df[mask].copy()
    df["FACTURA"] = factura[mask]
    df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce").dt.date
    return df


async def ingest(
    file_bytes: bytes,
    institution: Institution,
    period_id: int,
    db: AsyncSession,
    scan_only: bool = False,
    save_mappings_only: bool = False,
) -> dict:
    """
    Full ingestion pipeline for SIHOS Excel (new uppercase format).

    Optimized to minimize DB round-trips:
    1. Parse + normalize Excel
    2. Pre-load existing administrators/contracts/services into memory
    3. Bulk-insert only the NEW administrators/contracts (globally)
    4. Load/create agreements for new (admin, contract) pairs
    5. Build invoice_map entirely in memory
    6. Bulk-insert all invoices in one statement
    """
    inst_repo = InstitutionRepo(db)
    rules_repo = RulesRepo(db)

    default_fs = await rules_repo.get_folder_status_by_status(_DEFAULT_FOLDER_STATUS)
    if not default_fs:
        raise RuntimeError(f"Folder status '{_DEFAULT_FOLDER_STATUS}' not found — run seeds first.")

    raw_df = load_excel(file_bytes)
    df = _normalize(raw_df)
    skipped = int(df["FECHA"].isna().sum())

    # ── Phase 1: collect unique raw strings from the Excel ────────────────
    def col(name):
        return df[name].str.strip().dropna() if name in df.columns else pd.Series(dtype=str)

    raw_admins: set[str] = set(col("ADMINISTRADORA").unique()) - {""}
    raw_contracts: set[str] = set(col("CONTRATO").unique()) - {""}
    raw_services: set[str] = set(col("SERVICIO").unique()) - {""}

    # ── Phase 2: pre-load existing global rows ────────────────────────────
    adm_cache: dict[str, Administrator] = {a.raw_name: a for a in await inst_repo.get_all_administrators()}
    ctr_cache: dict[str, Contract] = {c.raw_name: c for c in await inst_repo.get_all_contracts()}
    svc_cache: dict[str, Service] = {s.raw_service: s for s in await inst_repo.get_services(institution.id)}

    # ── Phase 3: bulk-insert only new global entries ──────────────────────
    new_admins = raw_admins - adm_cache.keys()
    new_contracts = raw_contracts - ctr_cache.keys()
    new_services = raw_services - svc_cache.keys()

    if new_admins:
        await db.execute(
            pg_insert(Administrator)
            .values([{"raw_name": r} for r in new_admins])
            .on_conflict_do_nothing(index_elements=["raw_name"])
        )
        await db.flush()
        adm_cache = {a.raw_name: a for a in await inst_repo.get_all_administrators()}

    if new_contracts:
        await db.execute(
            pg_insert(Contract)
            .values([{"raw_name": r} for r in new_contracts])
            .on_conflict_do_nothing(index_elements=["raw_name"])
        )
        await db.flush()
        ctr_cache = {c.raw_name: c for c in await inst_repo.get_all_contracts()}

    if new_services:
        await db.execute(
            pg_insert(Service)
            .values(
                [{"institution_id": institution.id, "raw_service": r, "service_type_id": None} for r in new_services]
            )
            .on_conflict_do_nothing()
        )
        await db.flush()
        svc_cache = {s.raw_service: s for s in await inst_repo.get_services(institution.id)}

    # Track unknowns for the summary report
    unknown_admins = [r for r in raw_admins if adm_cache.get(r) and adm_cache[r].canonical_name is None]
    unknown_contracts = [r for r in raw_contracts if ctr_cache.get(r) and ctr_cache[r].canonical_name is None]
    unknown_services = [r for r in raw_services if svc_cache.get(r) and svc_cache[r].service_type_id is None]

    if scan_only:
        if save_mappings_only:
            await db.commit()
            logger.info(
                "ingest (mappings-only): institution=%s saved raw administrators/contracts/services",
                institution.name,
            )
        return {
            "scan_only": True,
            "save_mappings_only": save_mappings_only,
            "inserted": 0,
            "skipped": skipped,
            "unknown_admins": unknown_admins,
            "unknown_contracts": unknown_contracts,
            "unknown_services": unknown_services,
        }

    # ── Phase 3b: load/create agreements — canonical-aware ────────────────
    ic_list = await inst_repo.get_agreements()

    # Cache por raw pair para lookup rápido
    ic_cache: dict[tuple[str, str], Agreement] = {
        (ic.administrator.raw_name, ic.contract.raw_name): ic for ic in ic_list
    }

    # Cache canónico para deduplicación: prefiere el que ya tiene contract_type_id
    ic_cache_canonical: dict[tuple[str, str], Agreement] = {}
    for ag in ic_list:
        ak = (ag.administrator.canonical_name or ag.administrator.raw_name).strip().upper()
        ck = (ag.contract.canonical_name or ag.contract.raw_name).strip().upper()
        key = (ak, ck)
        if key not in ic_cache_canonical or (ag.contract_type_id and not ic_cache_canonical[key].contract_type_id):
            ic_cache_canonical[key] = ag

    # Recolectar pares únicos del Excel
    raw_pairs: set[tuple[str, str]] = set()
    for _, row in df.iterrows():
        raw_admin = str(row.get("ADMINISTRADORA", "") or "").strip()
        raw_contract = str(row.get("CONTRATO", "") or "").strip()
        if raw_admin:
            raw_pairs.add((raw_admin, raw_contract))

    # Por cada par raw, verificar si ya existe un agreement canónico equivalente
    new_pairs_to_create: list[tuple[str, str]] = []
    for raw_admin, raw_contract in raw_pairs:
        if (raw_admin, raw_contract) in ic_cache:
            continue  # raw pair exacto ya existe

        admin_obj = adm_cache.get(raw_admin)
        contract_obj = ctr_cache.get(raw_contract)
        if admin_obj and contract_obj:
            ak = (admin_obj.canonical_name or raw_admin).strip().upper()
            ck = (contract_obj.canonical_name or raw_contract).strip().upper()
            if (ak, ck) in ic_cache_canonical:
                # Reutilizar el agreement canónico existente para este raw pair
                ic_cache[(raw_admin, raw_contract)] = ic_cache_canonical[(ak, ck)]
                continue
        new_pairs_to_create.append((raw_admin, raw_contract))

    if new_pairs_to_create:
        new_ic_values = [
            {
                "administrator_id": adm_cache[a].id,
                "contract_id": ctr_cache[c].id,
                "contract_type_id": None,
            }
            for a, c in new_pairs_to_create
            if a in adm_cache and c in ctr_cache
        ]
        if new_ic_values:
            await db.execute(
                pg_insert(Agreement)
                .values(new_ic_values)
                .on_conflict_do_nothing(index_elements=["administrator_id", "contract_id"])
            )
            await db.flush()
            ic_list = await inst_repo.get_agreements()
            ic_cache = {(ic.administrator.raw_name, ic.contract.raw_name): ic for ic in ic_list}

    # ── Phase 4: build invoice_map entirely in memory ────────────────────
    service_types = await rules_repo.get_service_types()
    priority_map: dict[int, int] = {st.id: st.priority for st in service_types}

    invoice_map: dict[str, dict] = {}

    for _, row in df.iterrows():
        invoice_date = row.get("FECHA")
        if not invoice_date:
            continue

        invoice_number = str(row["FACTURA"])
        raw_admin = str(row.get("ADMINISTRADORA", "") or "").strip()
        raw_contract = str(row.get("CONTRATO", "") or "").strip()
        raw_service = str(row.get("SERVICIO", "") or "").strip()

        ic: Agreement | None = ic_cache.get((raw_admin, raw_contract)) if raw_admin else None
        service = svc_cache.get(raw_service) if raw_service else None
        service_type_id = service.service_type_id if service else None

        if invoice_number not in invoice_map:

            def _str(val, maxlen=None):
                """Convert a pandas cell to str, returning None for NaN/empty."""
                if val is None or (not isinstance(val, str) and pd.isna(val)):
                    return None
                s = str(val).strip()
                return (s[:maxlen] if maxlen else s) or None

            invoice_map[invoice_number] = {
                "date": invoice_date,
                "id_type": _str(row.get("DOCUMENTO"), 10),
                "id_number": _str(row.get("NUMERO"), 50),
                "patient_name": _str(row.get("PACIENTE"), 300),
                "employee": _str(row.get("OPERARIO"), 200),
                "admission": _str(row.get("ADMISION")),
                "agreement_id": ic.id if ic else None,
                "folder_status_id": default_fs.id,
                "_service_type_ids": set(),
            }

        if service_type_id is not None:
            invoice_map[invoice_number]["_service_type_ids"].add(service_type_id)

    # ── Phase 5: bulk-insert all invoices in one statement ───────────────
    invoice_values = []
    for invoice_number, data in invoice_map.items():
        st_ids: set[int] = data.pop("_service_type_ids")
        best_st_id = max(st_ids, key=lambda sid: priority_map.get(sid, 0)) if st_ids else None
        data["service_type_id"] = best_st_id
        invoice_values.append(
            {
                "audit_period_id": period_id,
                "invoice_number": invoice_number,
                **data,
            }
        )

    inserted = 0
    if invoice_values:
        await db.execute(
            pg_insert(Invoice)
            .values(invoice_values)
            .on_conflict_do_nothing(index_elements=["audit_period_id", "invoice_number"])
        )
        inserted = len(invoice_values)

    await db.commit()
    logger.info(
        "ingest: institution=%s period_id=%s inserted=%d skipped=%d",
        institution.name,
        period_id,
        inserted,
        skipped,
    )
    return {
        "scan_only": False,
        "inserted": inserted,
        "skipped": skipped,
        "unknown_admins": unknown_admins,
        "unknown_contracts": unknown_contracts,
        "unknown_services": unknown_services,
    }
