"""API router for institutions CRUD (mounted at /api/institutions)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

_ALLOWED_MIME = {"image/png", "image/jpeg", "image/webp", "image/avif", "image/gif"}
_MAX_LOGO_SIZE = 5 * 1024 * 1024  # 5 MB

from app import crypto  # noqa: E402
from app.database import get_db  # noqa: E402
from app.models.institution import Administrator, Agreement, Contract, Institution, Service  # noqa: E402
from app.repositories.institution_repo import InstitutionRepo  # noqa: E402
from app.schemas.institution import (  # noqa: E402
    AdministratorCreate,
    AdministratorOut,
    AdministratorUpdate,
    AgreementCreate,
    AgreementOut,
    AgreementUpdate,
    ContractCreate,
    ContractOut,
    ContractTypeCreate,
    ContractTypeOut,
    ContractTypeUpdate,
    ContractUpdate,
    InstitutionCreate,
    InstitutionOut,
    InstitutionUpdate,
    ServiceCreate,
    ServiceOut,
    ServiceUpdate,
)
from app.schemas.rules import ServiceTypeDocumentOut, ServiceTypeDocumentCreate  # noqa: E402

router = APIRouter(prefix="/institutions", tags=["institutions"])


def _encrypt_sensitive(data: dict) -> dict:
    """Encrypt sihos_password and drive_credentials_json in-place, return cleaned dict."""
    out = dict(data)
    if out.get("sihos_password"):
        out["sihos_password"] = crypto.encrypt(out["sihos_password"])
    else:
        out.pop("sihos_password", None)
    if out.get("drive_credentials_json"):
        out["drive_credentials_enc"] = crypto.encrypt(out.pop("drive_credentials_json"))
    else:
        out.pop("drive_credentials_json", None)
    return out


# ------------------------------------------------------------------
# Institutions
# ------------------------------------------------------------------


@router.get("", response_model=list[InstitutionOut])
async def list_institutions(db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    institutions = await repo.get_all()
    out = []
    for inst in institutions:
        item = InstitutionOut.model_validate(inst)
        item.logo_url = f"/api/institutions/{inst.id}/logo" if inst.logo_content_type else None
        item.has_drive_credentials = bool(inst.drive_credentials_enc)
        item.has_sihos_password = bool(inst.sihos_password)
        out.append(item)
    return out


@router.get("/{institution_id}/logo")
async def serve_logo(institution_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Institution.logo_bytes, Institution.logo_content_type).where(Institution.id == institution_id)
    )
    row = result.first()
    if not row or not row.logo_bytes:
        raise HTTPException(status_code=404, detail="Sin logo")
    return Response(
        content=row.logo_bytes,
        media_type=row.logo_content_type,
        headers={"Cache-Control": "public, max-age=86400"},
    )


@router.post("/{institution_id}/drive-credentials", status_code=200)
async def upload_drive_credentials(
    institution_id: int,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    """Upload a Google Drive service-account JSON file and store it encrypted."""
    import json as _json

    institution = await db.get(Institution, institution_id)
    if institution is None:
        raise HTTPException(status_code=404, detail="Institución no encontrada")

    _MAX_SIZE = 64 * 1024
    raw = await file.read(_MAX_SIZE + 1)
    if len(raw) > _MAX_SIZE:
        raise HTTPException(status_code=413, detail="Archivo demasiado grande (máx 64 KB)")
    try:
        data = _json.loads(raw)
    except _json.JSONDecodeError:
        raise HTTPException(status_code=422, detail="El archivo no es JSON válido")
    if not {"type", "project_id", "private_key", "client_email"}.issubset(data):
        raise HTTPException(status_code=422, detail="No es un service account de Google válido")

    institution.drive_credentials_enc = crypto.encrypt(raw.decode("utf-8"))
    await db.commit()
    return {"ok": True}


@router.post("/{institution_id}/logo")
async def upload_logo(
    institution_id: int,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    institution = await db.get(Institution, institution_id)
    if institution is None:
        raise HTTPException(status_code=404, detail="Institution not found")

    content_type = (file.content_type or "").split(";")[0].strip()
    if content_type not in _ALLOWED_MIME:
        raise HTTPException(status_code=422, detail=f"Tipo no soportado: {content_type}")

    content = await file.read(_MAX_LOGO_SIZE + 1)
    if len(content) > _MAX_LOGO_SIZE:
        raise HTTPException(status_code=413, detail="Logo demasiado grande (máx 5 MB)")

    institution.logo_bytes = content
    institution.logo_content_type = content_type
    await db.commit()

    return {"logo_url": f"/api/institutions/{institution_id}/logo"}


@router.post("", response_model=InstitutionOut, status_code=201)
async def create_institution(data: InstitutionCreate, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    obj = _encrypt_sensitive(data.model_dump())
    institution = await repo.create(obj)
    await db.commit()
    return institution


# ------------------------------------------------------------------
# ContractTypes (global)
# ------------------------------------------------------------------


@router.get("/contract-types", response_model=list[ContractTypeOut])
async def list_contract_types(db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    return await repo.get_all_contract_types()


@router.post("/contract-types", response_model=ContractTypeOut, status_code=201)
async def create_contract_type(data: ContractTypeCreate, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    ct = await repo.create_contract_type(data.name, data.description)
    await db.commit()
    return ct


@router.patch("/contract-types/{ct_id}", response_model=ContractTypeOut)
async def update_contract_type(ct_id: int, data: ContractTypeUpdate, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    ct = await repo.update_contract_type(ct_id, data.name, data.description)
    if not ct:
        raise HTTPException(404, "Tipo de contrato no encontrado")
    await db.commit()
    return ct


@router.delete("/contract-types/{ct_id}", status_code=204)
async def delete_contract_type(ct_id: int, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    deleted = await repo.delete_contract_type(ct_id)
    if not deleted:
        raise HTTPException(404, "Tipo de contrato no encontrado")
    await db.commit()


# ------------------------------------------------------------------
# Administrators (global)
# ------------------------------------------------------------------


@router.get("/administrators", response_model=list[AdministratorOut])
async def list_administrators(pending_only: bool = False, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    if pending_only:
        return await repo.get_pending_administrators()
    return await repo.get_all_administrators()


@router.post("/administrators", response_model=AdministratorOut, status_code=201)
async def create_administrator(data: AdministratorCreate, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    admin = await repo.create_administrator(data.raw_name, data.canonical_name)
    await db.commit()
    return admin


@router.patch("/administrators/{administrator_id}", response_model=AdministratorOut)
async def update_administrator(administrator_id: int, data: AdministratorUpdate, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    await repo.set_administrator_canonical(administrator_id, data.canonical_name)
    await db.commit()
    admin = await db.get(Administrator, administrator_id)
    if not admin:
        raise HTTPException(404, "Administradora no encontrada")
    return admin


@router.delete("/administrators/{administrator_id}", status_code=204)
async def delete_administrator(administrator_id: int, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    deleted = await repo.delete_administrator(administrator_id)
    if not deleted:
        raise HTTPException(404, "Administradora no encontrada")
    await db.commit()


# ------------------------------------------------------------------
# Contracts (global)
# ------------------------------------------------------------------


@router.get("/contracts", response_model=list[ContractOut])
async def list_contracts(pending_only: bool = False, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    if pending_only:
        return await repo.get_pending_contracts()
    return await repo.get_all_contracts()


@router.post("/contracts", response_model=ContractOut, status_code=201)
async def create_contract(data: ContractCreate, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    contract = await repo.create_contract(data.raw_name, data.canonical_name)
    await db.commit()
    return contract


@router.patch("/contracts/{contract_id}", response_model=ContractOut)
async def update_contract(contract_id: int, data: ContractUpdate, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    await repo.set_contract_canonical(contract_id, data.canonical_name)
    await db.commit()
    contract = await db.get(Contract, contract_id)
    if not contract:
        raise HTTPException(404, "Contrato no encontrado")
    return contract


@router.delete("/contracts/{contract_id}", status_code=204)
async def delete_contract(contract_id: int, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    deleted = await repo.delete_contract(contract_id)
    if not deleted:
        raise HTTPException(404, "Contrato no encontrado")
    await db.commit()


# ------------------------------------------------------------------
# Agreements (global)
# ------------------------------------------------------------------


@router.post("/agreements/consolidate")
async def consolidate_agreements(db: AsyncSession = Depends(get_db)):
    """Fusiona agreements con el mismo par canónico (admin, contrato)."""
    repo = InstitutionRepo(db)
    result = await repo.consolidate_agreements()
    await db.commit()
    return result


@router.get("/agreements", response_model=list[AgreementOut])
async def list_agreements(pending_only: bool = False, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    if pending_only:
        return await repo.get_pending_agreements()
    return await repo.get_agreements()


@router.post("/agreements", response_model=AgreementOut, status_code=201)
async def create_agreement(data: AgreementCreate, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    ag = await repo.create_agreement(data.administrator_id, data.contract_id, data.contract_type_id)
    await db.commit()
    await db.refresh(ag)
    return ag


@router.patch("/agreements/{agreement_id}", response_model=AgreementOut)
async def update_agreement(agreement_id: int, data: AgreementUpdate, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    await repo.set_agreement_contract_type(agreement_id, data.contract_type_id)
    await db.commit()
    result = await db.execute(
        select(Agreement)
        .where(Agreement.id == agreement_id)
        .options(
            selectinload(Agreement.administrator),
            selectinload(Agreement.contract),
            selectinload(Agreement.contract_type),
        )
    )
    ag = result.scalar_one_or_none()
    if not ag:
        raise HTTPException(404, "Acuerdo no encontrado")
    return ag


@router.delete("/agreements/{agreement_id}", status_code=204)
async def delete_agreement(agreement_id: int, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    deleted = await repo.delete_agreement(agreement_id)
    if not deleted:
        raise HTTPException(404, "Acuerdo no encontrado")
    await db.commit()


# ------------------------------------------------------------------
# Services sub-resource
# ------------------------------------------------------------------


@router.get("/{institution_id}/services", response_model=list[ServiceOut])
async def list_services(institution_id: int, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    institution = await repo.get_by_id(institution_id)
    if not institution:
        raise HTTPException(404, "Institución no encontrada")
    return await repo.get_services(institution_id)


@router.patch("/services/{service_id}", response_model=ServiceOut)
async def update_service(service_id: int, data: ServiceUpdate, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    await repo.set_service_type(service_id, data.service_type_id)
    await db.commit()
    service = await db.get(Service, service_id)
    if not service:
        raise HTTPException(404, "Servicio no encontrado")
    return service


# ------------------------------------------------------------------
# ServiceTypeDocuments sub-resource
# ------------------------------------------------------------------


@router.get("/{institution_id}/service-type-documents", response_model=list[ServiceTypeDocumentOut])
async def list_service_type_documents(institution_id: int, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    institution = await repo.get_by_id(institution_id)
    if not institution:
        raise HTTPException(404, "Institución no encontrada")
    return await repo.get_service_type_documents(institution_id)


@router.post(
    "/{institution_id}/service-type-documents",
    response_model=ServiceTypeDocumentOut,
    status_code=201,
)
async def create_service_type_document(
    institution_id: int,
    data: ServiceTypeDocumentCreate,
    db: AsyncSession = Depends(get_db),
):
    repo = InstitutionRepo(db)
    institution = await repo.get_by_id(institution_id)
    if not institution:
        raise HTTPException(404, "Institución no encontrada")
    obj = await repo.upsert_service_type_document(institution_id, data.service_type_id, data.doc_type_id)
    await db.commit()
    return obj


@router.delete(
    "/{institution_id}/service-type-documents/{service_type_id}/{doc_type_id}",
    status_code=204,
)
async def delete_service_type_document(
    institution_id: int,
    service_type_id: int,
    doc_type_id: int,
    db: AsyncSession = Depends(get_db),
):
    repo = InstitutionRepo(db)
    await repo.delete_service_type_document(institution_id, service_type_id, doc_type_id)
    await db.commit()


# ------------------------------------------------------------------
# Service create / delete
# ------------------------------------------------------------------


@router.post("/{institution_id}/services", response_model=ServiceOut, status_code=201)
async def create_service(institution_id: int, data: ServiceCreate, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    inst = await repo.get_by_id(institution_id)
    if not inst:
        raise HTTPException(404, "Institución no encontrada")
    svc = await repo.create_service(institution_id, data.raw_service, data.service_type_id or None)
    await db.commit()
    return svc


@router.delete("/services/{service_id}", status_code=204)
async def delete_service(service_id: int, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    deleted = await repo.delete_service(service_id)
    if not deleted:
        raise HTTPException(404, "Servicio no encontrado")
    await db.commit()


# ------------------------------------------------------------------
# Institution get / update / delete  (must come AFTER all fixed-path routes)
# ------------------------------------------------------------------


@router.get("/{institution_id}", response_model=InstitutionOut)
async def get_institution(institution_id: int, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    institution = await repo.get_by_id(institution_id)
    if not institution:
        raise HTTPException(404, "Institución no encontrada")
    return institution


@router.put("/{institution_id}", response_model=InstitutionOut)
async def update_institution(institution_id: int, data: InstitutionUpdate, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    obj = _encrypt_sensitive({k: v for k, v in data.model_dump().items() if v is not None})
    updated = await repo.update(institution_id, obj)
    if not updated:
        raise HTTPException(404, "Institución no encontrada")
    await db.commit()
    return updated


@router.delete("/{institution_id}", status_code=204)
async def delete_institution(institution_id: int, db: AsyncSession = Depends(get_db)):
    repo = InstitutionRepo(db)
    deleted = await repo.delete_institution(institution_id)
    if not deleted:
        raise HTTPException(404, "Institución no encontrada")
    await db.commit()
