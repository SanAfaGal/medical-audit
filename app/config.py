"""Application configuration loaded from .env via pydantic-settings."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str
    secret_key: str
    log_level: str = "INFO"
    host: str = "0.0.0.0"
    port: int = 8000
    # Disable Swagger UI and ReDoc in production (set DOCS_ENABLED=false)
    docs_enabled: bool = True
    # Root directory where audit data is stored.
    # Dev (Windows native): absolute path to the shared folder, e.g. C:\Users\...\Carpeta compartida
    # Prod (Docker): /audit_data  (volume mount — set in docker-compose.prod.yml)
    audit_data_root: str = "/audit_data"
    # Optional safety caps for uploads/archives, in bytes. 0 = unlimited (default): the app runs
    # locally and audits can be many GB. Set them only if the app is ever exposed beyond a trusted host.
    max_archive_bytes: int = 0  # compressed archive uploaded to "load drive zip"
    max_uncompressed_bytes: int = 0  # total size once extracted
    max_archive_members: int = 0  # number of files inside an archive
    max_upload_bytes: int = 0  # per file uploaded through the explorer


settings = Settings()  # type: ignore[call-arg]
