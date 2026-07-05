"""ORM models for institutions, administrators, contracts, contract types, services, and service-type-document mappings."""

from __future__ import annotations

import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, LargeBinary, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class Institution(Base):
    __tablename__ = "institutions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(String(100), nullable=False)
    nit: Mapped[str] = mapped_column(String(20), unique=True, nullable=False)
    invoice_id_prefix: Mapped[str | None] = mapped_column(String(20), nullable=True)
    sihos_base_url: Mapped[str | None] = mapped_column(String(500))
    sihos_doc_code: Mapped[str | None] = mapped_column(String(20))
    sihos_user: Mapped[str | None] = mapped_column(String(200))
    sihos_password: Mapped[str | None] = mapped_column(String(200))  # store encrypted via crypto.py
    drive_credentials_enc: Mapped[str | None] = mapped_column(String(10000))  # JSON encrypted via crypto.py
    logo_bytes: Mapped[bytes | None] = mapped_column(LargeBinary, deferred=True, default=None)
    logo_content_type: Mapped[str | None] = mapped_column(String(50), default=None)

    services: Mapped[list[Service]] = relationship(back_populates="institution", cascade="all, delete-orphan")
    periods: Mapped[list[AuditPeriod]] = relationship(back_populates="institution", cascade="all, delete-orphan")
    service_type_documents: Mapped[list[ServiceTypeDocument]] = relationship(
        back_populates="institution", cascade="all, delete-orphan"
    )


class ContractType(Base):
    """Global contract type (EPS, SOAT, ARL, etc.) — assigned manually by user."""

    __tablename__ = "contract_types"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(String(200))


class Administrator(Base):
    """Global administrator — same company shared across institutions."""

    __tablename__ = "administrators"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    raw_name: Mapped[str] = mapped_column(String(300), unique=True, nullable=False)
    canonical_name: Mapped[str | None] = mapped_column(String(300))


class Contract(Base):
    """Global contract — same contract identifier shared across institutions."""

    __tablename__ = "contracts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    raw_name: Mapped[str] = mapped_column(String(300), unique=True, nullable=False)
    canonical_name: Mapped[str | None] = mapped_column(String(300))


class Agreement(Base):
    """Links an administrator+contract pair with an optional contract type."""

    __tablename__ = "agreements"
    __table_args__ = (UniqueConstraint("administrator_id", "contract_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    administrator_id: Mapped[int] = mapped_column(ForeignKey("administrators.id"))
    contract_id: Mapped[int] = mapped_column(ForeignKey("contracts.id"))
    contract_type_id: Mapped[int | None] = mapped_column(ForeignKey("contract_types.id"), nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime, server_default=func.now())

    administrator: Mapped[Administrator] = relationship()
    contract: Mapped[Contract] = relationship()
    contract_type: Mapped[ContractType | None] = relationship()


class Service(Base):
    """Raw SIHOS service strings mapped to a ServiceType."""

    __tablename__ = "services"
    __table_args__ = (UniqueConstraint("institution_id", "raw_service"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    institution_id: Mapped[int] = mapped_column(ForeignKey("institutions.id", ondelete="CASCADE"))
    raw_service: Mapped[str] = mapped_column(String(300), nullable=False)
    service_type_id: Mapped[int | None] = mapped_column(ForeignKey("service_types.id"), nullable=True)

    institution: Mapped[Institution] = relationship(back_populates="services")


class ServiceTypeDocument(Base):
    """Links a service type to a required document type, scoped to an institution."""

    __tablename__ = "service_type_documents"
    __table_args__ = (UniqueConstraint("institution_id", "service_type_id", "doc_type_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    institution_id: Mapped[int] = mapped_column(ForeignKey("institutions.id", ondelete="CASCADE"))
    service_type_id: Mapped[int] = mapped_column(ForeignKey("service_types.id"))
    doc_type_id: Mapped[int] = mapped_column(ForeignKey("doc_types.id"))

    institution: Mapped[Institution] = relationship(back_populates="service_type_documents")


# Deferred import to avoid circular reference at module load
from app.models.period import AuditPeriod  # noqa: E402
