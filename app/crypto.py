"""Fernet-based encryption/decryption for secrets stored in the database."""

import base64

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from app.config import settings

_fernet: Fernet | None = None

# Static salt — changing this invalidates all stored credentials.
_KDF_SALT = b"medical-audit-v2"
_KDF_ITERATIONS = 100_000


def _derive_key(secret: str) -> bytes:
    """Derive a 32-byte Fernet key from the application secret via PBKDF2-HMAC-SHA256."""
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=_KDF_SALT, iterations=_KDF_ITERATIONS)
    return base64.urlsafe_b64encode(kdf.derive(secret.encode()))


def _get_fernet() -> Fernet:
    global _fernet
    if _fernet is None:
        _fernet = Fernet(_derive_key(settings.secret_key))
    return _fernet


def encrypt(plaintext: str) -> str:
    """Encrypt plaintext using Fernet symmetric encryption."""
    return _get_fernet().encrypt(plaintext.encode()).decode()


def decrypt(token: str) -> str:
    """Decrypt a Fernet token back to plaintext."""
    return _get_fernet().decrypt(token.encode()).decode()
