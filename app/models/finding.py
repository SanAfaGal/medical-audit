"""ORM model for missing files (audit findings)."""

from __future__ import annotations

import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class MissingFile(Base):
    __tablename__ = "missing_files"
    __table_args__ = (UniqueConstraint("invoice_id", "doc_type_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    invoice_id: Mapped[int] = mapped_column(ForeignKey("invoices.id", ondelete="CASCADE"))
    doc_type_id: Mapped[int] = mapped_column(ForeignKey("doc_types.id"))
    expected_path: Mapped[str] = mapped_column(String(500), nullable=False)
    detected_at: Mapped[datetime.datetime] = mapped_column(DateTime, server_default=func.now())
    resolved_at: Mapped[datetime.datetime | None] = mapped_column(DateTime)

    invoice: Mapped[Invoice] = relationship(back_populates="missing_files")
    doc_type: Mapped[DocType] = relationship()


from app.models.invoice import Invoice  # noqa: E402
from app.models.rules import DocType  # noqa: E402
