"""Application settings module."""
from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from config.models import AppConfig, StateConfig, ApiConfig, ScrapingConfig, PipelineConfig

CONFIG_DIR = Path(__file__).parent
PROJECT_ROOT = CONFIG_DIR.parent


class Settings(BaseSettings):
    """Application configuration."""

    app_name: str = "wage_etl"

    # Logging
    log_level: str = "INFO"
    log_to_file: bool = True

    # Directories
    data_dir: Path = PROJECT_ROOT / "data"
    cache_dir: Path = data_dir / "cache"
    log_dir: Path = PROJECT_ROOT / "logs"
    logging_config_file: Path = CONFIG_DIR / "logging_conf.json"

    # Database
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str

    # App config from files
    app_config: AppConfig = Field(
        default_factory=lambda: AppConfig.from_yaml(CONFIG_DIR / "config.yaml")
    )
    state_config: StateConfig = Field(
        default_factory=lambda: StateConfig.from_json(
            CONFIG_DIR / "state_fips.json")
    )

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def api(self) -> ApiConfig:
        return self.app_config.api

    @property
    def scraping(self) -> ScrapingConfig:
        return self.app_config.scraping

    @property
    def pipeline(self) -> PipelineConfig:
        return self.app_config.pipeline

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v not in allowed:
            raise ValueError(
                f"Invalid log level: {v}. Must be one of {allowed}")
        return v

    def ensure_dirs(self) -> None:
        """Create required directories if they don't exist."""
        for directory in [self.data_dir, self.cache_dir, self.log_dir]:
            directory.mkdir(parents=True, exist_ok=True)


@lru_cache
def get_settings() -> Settings:
    """Get cached application settings."""
    settings = Settings()
    settings.ensure_dirs()
    return settings
