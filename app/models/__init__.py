"""Import all ORM models so Alembic autogenerate can detect them."""

from app.models.base import Base  # noqa: F401
from app.models.rules import ServiceType, DocType, FolderStatus  # noqa: F401
from app.models.institution import (  # noqa: F401
    Institution,
    Administrator,
    Contract,
    ContractType,
    Agreement,
    Service,
    ServiceTypeDocument,
)
from app.models.period import AuditPeriod  # noqa: F401
from app.models.invoice import Invoice  # noqa: F401
from app.models.finding import MissingFile  # noqa: F401
