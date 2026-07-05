"""ORM model for audit periods."""

from __future__ import annotations

import datetime

from sqlalchemy import Date, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class AuditPeriod(Base):
    __tablename__ = "audit_periods"
    __table_args__ = (UniqueConstraint("institution_id", "date_from", "date_to", "period_label"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    institution_id: Mapped[int] = mapped_column(ForeignKey("institutions.id", ondelete="CASCADE"))
    date_from: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    date_to: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    period_label: Mapped[str] = mapped_column(String(50), nullable=False)

    institution: Mapped[Institution] = relationship(back_populates="periods")
    invoices: Mapped[list[Invoice]] = relationship(back_populates="period", cascade="all, delete-orphan")


from app.models.institution import Institution  # noqa: E402
from app.models.invoice import Invoice  # noqa: E402
