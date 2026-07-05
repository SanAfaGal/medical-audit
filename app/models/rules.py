"""ORM models for dynamic business rules: service types, doc types, folder statuses."""

from __future__ import annotations

from sqlalchemy import Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class ServiceType(Base):
    __tablename__ = "service_types"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(String(200), nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=10)


class DocType(Base):
    __tablename__ = "doc_types"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    description: Mapped[str] = mapped_column(String(200), nullable=False)
    prefix: Mapped[str | None] = mapped_column(String(20))  # single prefix, not JSON
    notes: Mapped[str | None] = mapped_column(Text)


class FolderStatus(Base):
    __tablename__ = "folder_statuses"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    status: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    # values: PRESENTE | FALTANTE | AUDITADA | ANULAR | REVISAR | PENDIENTE


class PrefixCorrection(Base):
    """Maps a wrong file-name prefix to the canonical correct one.

    Used by the NORMALIZE_FILES pipeline stage to fix mis-prefixed PDFs
    before the main standardization pass (e.g. OPD → OPF, FVE → FEV).
    """

    __tablename__ = "prefix_corrections"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    wrong_prefix: Mapped[str] = mapped_column(String(20), unique=True, nullable=False)
    correct_prefix: Mapped[str] = mapped_column(String(20), nullable=False)
    notes: Mapped[str | None] = mapped_column(String(200))


