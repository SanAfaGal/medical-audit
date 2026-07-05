"""Pydantic schemas for missing files (audit findings)."""

from __future__ import annotations

import datetime

from pydantic import BaseModel


class MissingFileOut(BaseModel):
    model_config = {"from_attributes": True}

    id: int
    invoice_id: int
    doc_type_id: int
    expected_path: str
    detected_at: datetime.datetime
    resolved_at: datetime.datetime | None


class MissingFileCreate(BaseModel):
    invoice_id: int
    doc_type_id: int
    expected_path: str


class MissingFileResolve(BaseModel):
    resolved_at: datetime.datetime
