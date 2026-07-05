"""Pydantic schemas for business rules (service types, doc types, folder statuses)."""

from pydantic import BaseModel


class ServiceTypeOut(BaseModel):
    model_config = {"from_attributes": True}

    id: int
    code: str
    display_name: str
    priority: int


class ServiceTypeCreate(BaseModel):
    code: str
    display_name: str
    priority: int = 10


class ServiceTypeUpdate(BaseModel):
    code: str | None = None
    display_name: str | None = None
    priority: int | None = None


class DocTypeOut(BaseModel):
    model_config = {"from_attributes": True}

    id: int
    code: str
    description: str
    prefix: str | None
    notes: str | None


class DocTypeCreate(BaseModel):
    code: str
    description: str
    prefix: str | None = None
    notes: str | None = None


class DocTypeUpdate(BaseModel):
    code: str | None = None
    description: str | None = None
    prefix: str | None = None
    notes: str | None = None


class FolderStatusOut(BaseModel):
    model_config = {"from_attributes": True}

    id: int
    status: str


class ServiceTypeDocumentOut(BaseModel):
    model_config = {"from_attributes": True}

    id: int
    institution_id: int
    service_type_id: int
    doc_type_id: int


class ServiceTypeDocumentCreate(BaseModel):
    institution_id: int
    service_type_id: int
    doc_type_id: int


class FolderStatusCreate(BaseModel):
    status: str


class FolderStatusUpdate(BaseModel):
    status: str | None = None


class PrefixCorrectionOut(BaseModel):
    model_config = {"from_attributes": True}

    id: int
    wrong_prefix: str
    correct_prefix: str
    notes: str | None


class PrefixCorrectionCreate(BaseModel):
    wrong_prefix: str
    correct_prefix: str
    notes: str | None = None


class PrefixCorrectionUpdate(BaseModel):
    correct_prefix: str | None = None
    notes: str | None = None


