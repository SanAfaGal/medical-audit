"""Run: python seeds/seed_data.py — Populates default service types, doc types, and folder statuses."""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select
from app.database import AsyncSessionLocal
from app.models.rules import DocType, FolderStatus, ServiceType

DEFAULT_SERVICE_TYPES = [
    {"code": "GENERAL", "display_name": "General", "priority": 0},
    {"code": "SOAT", "display_name": "SOAT", "priority": 10},
    {"code": "LABORATORIO", "display_name": "Laboratorio", "priority": 20},
    {"code": "ECG", "display_name": "Electrocardiograma", "priority": 20},
    {"code": "RADIOGRAFIA", "display_name": "Radiografía", "priority": 20},
    {"code": "ODONTOLOGIA", "display_name": "Odontología", "priority": 20},
    {"code": "POLICLINICA", "display_name": "Policlínica", "priority": 20},
    {"code": "URGENCIAS", "display_name": "Urgencias", "priority": 20},
    {"code": "AMBULANCIA", "display_name": "Ambulancia", "priority": 30},
]

DEFAULT_DOC_TYPES = [
    {"code": "FACTURA", "description": "Factura", "prefix": "FEV"},
    {"code": "FIRMA", "description": "Firma", "prefix": "CRC"},
    {"code": "HISTORIA", "description": "Historia clínica", "prefix": "EPI"},
    {"code": "VALIDACION", "description": "Validación", "prefix": "OPF"},
    {"code": "RESULTADOS", "description": "Resultados", "prefix": "PDX"},
    {"code": "BITACORA", "description": "Bitácora", "prefix": "TAP"},
    {"code": "RESOLUCION", "description": "Resolución", "prefix": "LDP"},
    {"code": "MEDICAMENTOS", "description": "Medicamentos", "prefix": "HAM"},
    {"code": "AUTORIZACION", "description": "Autorización", "prefix": "PDE"},
    {"code": "CARPETA", "description": "Carpeta", "prefix": None},
    {"code": "ORDEN", "description": "Orden médica", "prefix": None},
    {"code": "FURIPS", "description": "FURIPS", "prefix": None},
    {"code": "CUFE", "description": "CUFE", "prefix": None},
]

DEFAULT_FOLDER_STATUSES = [
    {"status": "PRESENTE"},
    {"status": "PENDIENTE"},
    {"status": "FALTANTE"},
    {"status": "AUDITADA"},
    {"status": "ANULAR"},
    {"status": "REVISAR"},
]


async def seed() -> None:
    async with AsyncSessionLocal() as db:
        # Service types — upsert por code
        existing_codes = set((await db.execute(select(ServiceType.code))).scalars().all())
        added = [st for st in DEFAULT_SERVICE_TYPES if st["code"] not in existing_codes]
        for st in added:
            db.add(ServiceType(**st))
        print(f"ServiceTypes: +{len(added)} nuevos, {len(existing_codes)} ya existían.")

        # Doc types — upsert por code
        existing_codes = set((await db.execute(select(DocType.code))).scalars().all())
        added = [dt for dt in DEFAULT_DOC_TYPES if dt["code"] not in existing_codes]
        for dt in added:
            db.add(DocType(**dt))
        print(f"DocTypes:     +{len(added)} nuevos, {len(existing_codes)} ya existían.")

        # Folder statuses — upsert por status
        existing_statuses = set((await db.execute(select(FolderStatus.status))).scalars().all())
        added = [fs for fs in DEFAULT_FOLDER_STATUSES if fs["status"] not in existing_statuses]
        for fs in added:
            db.add(FolderStatus(**fs))
        print(f"FolderStatus: +{len(added)} nuevos, {len(existing_statuses)} ya existían.")

        await db.commit()
        print("Seed complete.")


if __name__ == "__main__":
    asyncio.run(seed())
