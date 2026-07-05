"""ORM model for invoices (facturas)."""

from __future__ import annotations

import datetime

from sqlalchemy import Date, DateTime, ForeignKey, Index, Integer, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class Invoice(Base):
    __tablename__ = "invoices"
    __table_args__ = (
        UniqueConstraint("audit_period_id", "invoice_number"),
        Index("ix_invoices_audit_period_id", "audit_period_id"),
        Index("ix_invoices_folder_status_id", "folder_status_id"),
        Index("ix_invoices_service_type_id", "service_type_id"),
        Index("ix_invoices_agreement_id", "agreement_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    audit_period_id: Mapped[int] = mapped_column(ForeignKey("audit_periods.id", ondelete="CASCADE"))
    invoice_number: Mapped[str] = mapped_column(String(50), nullable=False)
    date: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    id_type: Mapped[str] = mapped_column(String(10), nullable=False)  # CC, TI, RC, CE
    id_number: Mapped[str] = mapped_column(String(50), nullable=False)
    patient_name: Mapped[str] = mapped_column(String(300), nullable=False)
    agreement_id: Mapped[int | None] = mapped_column(ForeignKey("agreements.id"), nullable=True)
    service_type_id: Mapped[int | None] = mapped_column(ForeignKey("service_types.id"), nullable=True)
    employee: Mapped[str | None] = mapped_column(String(200))
    admission: Mapped[str | None] = mapped_column(String(50), nullable=True)
    folder_status_id: Mapped[int] = mapped_column(ForeignKey("folder_statuses.id"), nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    period: Mapped[AuditPeriod] = relationship(back_populates="invoices")
    folder_status: Mapped[FolderStatus] = relationship()
    service_type: Mapped[ServiceType | None] = relationship()
    agreement: Mapped[Agreement | None] = relationship()
    missing_files: Mapped[list[MissingFile]] = relationship(back_populates="invoice", cascade="all, delete-orphan")


from app.models.period import AuditPeriod  # noqa: E402
from app.models.finding import MissingFile  # noqa: E402
from app.models.rules import FolderStatus, ServiceType  # noqa: E402
from app.models.institution import Agreement  # noqa: E402
