"""Pydantic schemas for institutions, administrators, contracts, contract_types, agreements, and services."""

from pydantic import BaseModel, Field

_SAFE_NAME_PATTERN = r"^[A-Za-z0-9_\-]+$"


class InstitutionCreate(BaseModel):
    name: str = Field(..., pattern=_SAFE_NAME_PATTERN, max_length=100)  # internal unique key, e.g. "SANTA_LUCIA"
    display_name: str  # human-readable, e.g. "Hospital Santa Lucia"
    nit: str
    invoice_id_prefix: str | None = None
    sihos_base_url: str | None = None
    sihos_doc_code: str | None = None
    sihos_user: str | None = None
    sihos_password: str | None = None  # plaintext — encrypted before DB insert
    drive_credentials_json: str | None = None  # plaintext JSON — encrypted before DB insert


class InstitutionUpdate(BaseModel):
    name: str | None = Field(None, pattern=_SAFE_NAME_PATTERN, max_length=100)
    display_name: str | None = None
    nit: str | None = None
    invoice_id_prefix: str | None = None
    sihos_base_url: str | None = None
    sihos_doc_code: str | None = None
    sihos_user: str | None = None
    sihos_password: str | None = None  # plaintext — encrypted before DB insert
    drive_credentials_json: str | None = None  # plaintext JSON — encrypted before DB insert


class InstitutionOut(BaseModel):
    model_config = {"from_attributes": True}

    id: int
    name: str
    display_name: str
    nit: str
    invoice_id_prefix: str | None
    sihos_base_url: str | None
    sihos_doc_code: str | None
    sihos_user: str | None
    logo_url: str | None = None
    has_drive_credentials: bool = False
    has_sihos_password: bool = False
    # NOTE: sihos_password and drive_credentials_enc are never exposed


# ------------------------------------------------------------------
# ContractType
# ------------------------------------------------------------------


class ContractTypeOut(BaseModel):
    model_config = {"from_attributes": True}

    id: int
    name: str
    description: str | None


class ContractTypeCreate(BaseModel):
    name: str
    description: str | None = None


class ContractTypeUpdate(BaseModel):
    name: str | None = None
    description: str | None = None


# ------------------------------------------------------------------
# Administrator (global)
# ------------------------------------------------------------------


class AdministratorOut(BaseModel):
    model_config = {"from_attributes": True}

    id: int
    raw_name: str
    canonical_name: str | None


class AdministratorCreate(BaseModel):
    raw_name: str
    canonical_name: str | None = None


class AdministratorUpdate(BaseModel):
    canonical_name: str | None = None


# ------------------------------------------------------------------
# Contract (global)
# ------------------------------------------------------------------


class ContractOut(BaseModel):
    model_config = {"from_attributes": True}

    id: int
    raw_name: str
    canonical_name: str | None


class ContractCreate(BaseModel):
    raw_name: str
    canonical_name: str | None = None


class ContractUpdate(BaseModel):
    canonical_name: str | None = None


# ------------------------------------------------------------------
# Agreement
# ------------------------------------------------------------------


class AgreementOut(BaseModel):
    model_config = {"from_attributes": True}

    id: int
    administrator_id: int
    contract_id: int
    contract_type_id: int | None
    administrator: AdministratorOut | None = None
    contract: ContractOut | None = None
    contract_type: ContractTypeOut | None = None


class AgreementCreate(BaseModel):
    administrator_id: int
    contract_id: int
    contract_type_id: int | None = None


class AgreementUpdate(BaseModel):
    contract_type_id: int | None = None


# ------------------------------------------------------------------
# Service
# ------------------------------------------------------------------


class ServiceOut(BaseModel):
    model_config = {"from_attributes": True}

    id: int
    institution_id: int
    raw_service: str
    service_type_id: int | None


class ServiceUpdate(BaseModel):
    service_type_id: int | None = None


class ServiceCreate(BaseModel):
    raw_service: str
    service_type_id: int | None = None
