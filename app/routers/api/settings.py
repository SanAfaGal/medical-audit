"""API router for business rules settings (service types, doc types, folder statuses)."""

from __future__ import annotations

from collections.abc import Awaitable

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.repositories.rules_repo import RulesRepo
from app.schemas.rules import (
    DocTypeCreate,
    DocTypeOut,
    DocTypeUpdate,
    FolderStatusCreate,
    FolderStatusOut,
    FolderStatusUpdate,
    PrefixCorrectionCreate,
    PrefixCorrectionOut,
    PrefixCorrectionUpdate,
    ServiceTypeCreate,
    ServiceTypeOut,
    ServiceTypeUpdate,
)

router = APIRouter(prefix="/settings", tags=["settings"])


# ------------------------------------------------------------------
# Service types
# ------------------------------------------------------------------


async def _delete_or_409(db: AsyncSession, delete_call: Awaitable[bool], *, not_found: str, in_use: str) -> None:
    """Run a repo delete: 404 when the row is gone, 409 with a specific message when other rows still use it.

    SQLAlchemy reports the database's foreign-key error as ``IntegrityError`` (not asyncpg's own exception).
    """
    try:
        if not await delete_call:
            raise HTTPException(404, not_found)
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(409, in_use) from exc


@router.get("/service-types", response_model=list[ServiceTypeOut])
async def list_service_types(db: AsyncSession = Depends(get_db)):
    return await RulesRepo(db).get_service_types()


@router.post("/service-types", response_model=ServiceTypeOut, status_code=201)
async def create_service_type(data: ServiceTypeCreate, db: AsyncSession = Depends(get_db)):
    repo = RulesRepo(db)
    obj = await repo.upsert_service_type(data.model_dump())
    await db.commit()
    return obj


@router.patch("/service-types/{service_type_id}", response_model=ServiceTypeOut)
async def update_service_type(service_type_id: int, data: ServiceTypeUpdate, db: AsyncSession = Depends(get_db)):
    repo = RulesRepo(db)
    obj = await repo.update_service_type(service_type_id, data.model_dump(exclude_none=True))
    if not obj:
        raise HTTPException(404, "Tipo de servicio no encontrado")
    await db.commit()
    return obj


# ------------------------------------------------------------------
# Doc types
# ------------------------------------------------------------------


@router.get("/doc-types", response_model=list[DocTypeOut])
async def list_doc_types(db: AsyncSession = Depends(get_db)):
    return await RulesRepo(db).get_doc_types()


@router.post("/doc-types", response_model=DocTypeOut, status_code=201)
async def create_doc_type(data: DocTypeCreate, db: AsyncSession = Depends(get_db)):
    repo = RulesRepo(db)
    obj = await repo.upsert_doc_type(data.model_dump())
    await db.commit()
    return obj


@router.patch("/doc-types/{doc_type_id}", response_model=DocTypeOut)
async def update_doc_type(doc_type_id: int, data: DocTypeUpdate, db: AsyncSession = Depends(get_db)):
    repo = RulesRepo(db)
    obj = await repo.update_doc_type(doc_type_id, data.model_dump(exclude_none=True))
    if not obj:
        raise HTTPException(404, "Tipo de documento no encontrado")
    await db.commit()
    return obj


# ------------------------------------------------------------------
# Folder statuses
# ------------------------------------------------------------------


@router.get("/folder-statuses", response_model=list[FolderStatusOut])
async def list_folder_statuses(db: AsyncSession = Depends(get_db)):
    return await RulesRepo(db).get_folder_statuses()


# ------------------------------------------------------------------
# Service type delete
# ------------------------------------------------------------------


@router.delete("/service-types/{service_type_id}", status_code=204)
async def delete_service_type(service_type_id: int, db: AsyncSession = Depends(get_db)):
    await _delete_or_409(
        db,
        RulesRepo(db).delete_service_type(service_type_id),
        not_found="Tipo de servicio no encontrado",
        in_use="No se puede eliminar: hay facturas o servicios que usan este tipo",
    )


# ------------------------------------------------------------------
# Doc type delete
# ------------------------------------------------------------------


@router.delete("/doc-types/{doc_type_id}", status_code=204)
async def delete_doc_type(doc_type_id: int, db: AsyncSession = Depends(get_db)):
    await _delete_or_409(
        db,
        RulesRepo(db).delete_doc_type(doc_type_id),
        not_found="Tipo de documento no encontrado",
        in_use="No se puede eliminar: hay hallazgos que usan este tipo",
    )


# ------------------------------------------------------------------
# Folder status create / update / delete
# ------------------------------------------------------------------


@router.post("/folder-statuses", response_model=FolderStatusOut, status_code=201)
async def create_folder_status(data: FolderStatusCreate, db: AsyncSession = Depends(get_db)):
    obj = await RulesRepo(db).create_folder_status(data.status)
    await db.commit()
    return obj


@router.patch("/folder-statuses/{fs_id}", response_model=FolderStatusOut)
async def update_folder_status(fs_id: int, data: FolderStatusUpdate, db: AsyncSession = Depends(get_db)):
    obj = await RulesRepo(db).update_folder_status_obj(fs_id, data.status)
    if not obj:
        raise HTTPException(404, "Estado no encontrado")
    await db.commit()
    return obj


@router.delete("/folder-statuses/{fs_id}", status_code=204)
async def delete_folder_status(fs_id: int, db: AsyncSession = Depends(get_db)):
    await _delete_or_409(
        db,
        RulesRepo(db).delete_folder_status(fs_id),
        not_found="Estado no encontrado",
        in_use="No se puede eliminar: hay facturas que usan este estado",
    )


# ------------------------------------------------------------------
# Prefix corrections
# ------------------------------------------------------------------


@router.get("/prefix-corrections", response_model=list[PrefixCorrectionOut])
async def list_prefix_corrections(db: AsyncSession = Depends(get_db)):
    return await RulesRepo(db).get_prefix_corrections()


@router.post("/prefix-corrections", response_model=PrefixCorrectionOut, status_code=201)
async def create_prefix_correction(data: PrefixCorrectionCreate, db: AsyncSession = Depends(get_db)):
    obj = await RulesRepo(db).create_prefix_correction(data.model_dump())
    await db.commit()
    return obj


@router.patch("/prefix-corrections/{correction_id}", response_model=PrefixCorrectionOut)
async def update_prefix_correction(
    correction_id: int, data: PrefixCorrectionUpdate, db: AsyncSession = Depends(get_db)
):
    obj = await RulesRepo(db).update_prefix_correction(correction_id, data.model_dump(exclude_none=True))
    if not obj:
        raise HTTPException(404, "Corrección no encontrada")
    await db.commit()
    return obj


@router.delete("/prefix-corrections/{correction_id}", status_code=204)
async def delete_prefix_correction(correction_id: int, db: AsyncSession = Depends(get_db)):
    deleted = await RulesRepo(db).delete_prefix_correction(correction_id)
    if not deleted:
        raise HTTPException(404, "Corrección no encontrada")
    await db.commit()
