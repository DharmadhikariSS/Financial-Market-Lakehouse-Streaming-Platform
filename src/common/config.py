"""Strongly-typed application configuration powered by Pydantic Settings."""

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Base workspace directory
BASE_DIR = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Execution Mode
    DB_ENGINE: Literal["duckdb", "postgres"] = Field(
        default="duckdb",
        description="Target database engine: 'duckdb' for zero-RAM local execution, 'postgres' for Docker.",
    )

    # DuckDB Configuration
    DUCKDB_PATH: str = Field(
        default="data/market_ledger.duckdb",
        description="File path for persistent embedded DuckDB storage.",
    )

    # PostgreSQL Configuration
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "market_db"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres_secure_pass"

    # Pipeline Ingestion Tuning
    BATCH_SIZE: int = 25000
    LOG_LEVEL: str = "INFO"

    # MinIO / Object Storage
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_BUCKET: str = "market-lakehouse"

    @property
    def postgres_connection_uri(self) -> str:
        """Constructs a psycopg3 compatible SQLAlchemy connection URI."""
        return (
            f"postgresql+psycopg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def resolved_duckdb_path(self) -> Path:
        """Returns the absolute path to the DuckDB file, ensuring parent dirs exist."""
        p = Path(self.DUCKDB_PATH)
        if not p.is_absolute():
            p = BASE_DIR / p
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def BASE_DIR(self) -> Path:
        return BASE_DIR


# Singleton settings instance
settings = Settings()


def get_settings() -> Settings:
    """Return the global Settings singleton instance."""
    return settings
