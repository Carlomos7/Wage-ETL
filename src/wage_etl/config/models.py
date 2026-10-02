"""Configuration models for the application."""

import json
from pathlib import Path
from typing import Literal
from urllib.parse import quote_plus

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, SecretStr, field_validator, model_validator


def project_root() -> Path:
    """Find the repository root by walking up to pyproject.toml."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "pyproject.toml").is_file():
            return parent
    raise RuntimeError(f"pyproject.toml not found above {here}")


PROJECT_ROOT = project_root()
CONFIG_DIR = PROJECT_ROOT / "config"
STATE_FIPS_PATH = CONFIG_DIR / "state_fips.json"


class Frozen(BaseModel):
    """Read-only model. Unknown keys are errors."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class HttpClientConfig(Frozen):
    """Base configuration for HTTP clients (API and scraping)."""

    base_url: HttpUrl
    max_retries: int = 3
    timeout_seconds: int = 30
    cache_ttl_days: int = 30
    ssl_verify: bool = True
    proxies: dict[str, str] | None = None


class ApiConfig(HttpClientConfig):
    """Census API configuration."""

    dataset: str
    variables: list[str]
    county: list[str]


class ScrapingConfig(HttpClientConfig):
    """Web scraping configuration."""

    min_delay_seconds: float = 1.0
    max_delay_seconds: float = 3.0

    @model_validator(mode="after")
    def check_delay_range(self) -> "ScrapingConfig":
        """Ensure max_delay >= min_delay, including when min_delay is 0."""
        if self.max_delay_seconds < self.min_delay_seconds:
            raise ValueError(
                f"max_delay_seconds ({self.max_delay_seconds}) must be >= "
                f"min_delay_seconds ({self.min_delay_seconds})"
            )
        return self


class PipelineConfig(Frozen):
    """Pipeline configuration for the ETL process."""

    min_success_rate: float = Field(0.8, ge=0, le=1)
    target_states: list[str] = Field(default=["*"])

    @field_validator("target_states", mode="before")
    @classmethod
    def normalize_states(cls, value):
        """
        Normalize input so:
        - "*" becomes ["*"]
        - "NJ" becomes ["NJ"]
        - ["NJ", "NY"] stays as ["NJ", "NY"]
        - None or not provided becomes ["*"]
        """
        if value is None:
            return ["*"]
        if isinstance(value, str):
            value = value.strip()
            return ["*"] if value == "*" else [value.upper()]
        if isinstance(value, list):
            return [item.upper() for item in value]
        raise ValueError("target_states must be a string or list of strings")


class DatabaseSettings(Frozen):
    """PostgreSQL connection settings."""

    host: str
    port: int = 5432
    name: str
    user: str
    password: SecretStr

    @property
    def dsn(self) -> str:
        """Connection URL with the user and password percent-encoded."""
        user = quote_plus(self.user)
        password = quote_plus(self.password.get_secret_value())
        return f"postgresql://{user}:{password}@{self.host}:{self.port}/{self.name}"


class LoggingSettings(Frozen):
    """Logging settings."""

    name: str = "wage_etl"
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    to_file: bool = True
    config_file: Path = CONFIG_DIR / "logging_conf.json"


class PathSettings(Frozen):
    """Directories the pipeline reads and writes."""

    data_dir: Path = PROJECT_ROOT / "data"
    log_dir: Path = PROJECT_ROOT / "logs"

    @property
    def cache_dir(self) -> Path:
        """Cache directory follows data_dir."""
        return self.data_dir / "cache"

    def ensure_dirs(self) -> None:
        """Create required directories if they don't exist."""
        for directory in (self.data_dir, self.cache_dir, self.log_dir):
            directory.mkdir(parents=True, exist_ok=True)


class StateCodes(Frozen):
    """State abbreviation to FIPS code mapping loaded from JSON."""

    fips_map: dict[str, str]

    @classmethod
    def from_json(cls, path: Path) -> "StateCodes":
        """Load a FIPS map from a JSON object."""
        with open(path, encoding="utf-8") as handle:
            return cls(fips_map=json.load(handle))

    @field_validator("fips_map")
    @classmethod
    def fips_map_not_empty(cls, value: dict[str, str]) -> dict[str, str]:
        """Validate the FIPS map is not empty."""
        if not value:
            raise ValueError("FIPS map cannot be empty")
        return value


__all__ = [
    "ApiConfig",
    "CONFIG_DIR",
    "DatabaseSettings",
    "HttpClientConfig",
    "LoggingSettings",
    "PROJECT_ROOT",
    "PathSettings",
    "PipelineConfig",
    "STATE_FIPS_PATH",
    "ScrapingConfig",
    "StateCodes",
]
