"""Application settings module."""

import os
from functools import lru_cache
from typing import Any

from dotenv import dotenv_values
from pydantic.fields import FieldInfo
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)

from wage_etl.config.models import (
    CONFIG_DIR,
    PROJECT_ROOT,
    ApiConfig,
    DatabaseSettings,
    LoggingSettings,
    PathSettings,
    PipelineConfig,
    ScrapingConfig,
)

# Flat names kept so an existing .env, Compose, and CI keep working.
# Prefixed names (WAGE_ETL_DB__HOST and so on) are the canonical ones and win.
LEGACY_FLAT_ENV: dict[str, tuple[str, ...]] = {
    "DB_HOST": ("db", "host"),
    "DB_PORT": ("db", "port"),
    "DB_NAME": ("db", "name"),
    "DB_USER": ("db", "user"),
    "DB_PASSWORD": ("db", "password"),
    "LOG_LEVEL": ("logging", "level"),
    "LOG_TO_FILE": ("logging", "to_file"),
    "DATA_DIR": ("paths", "data_dir"),
    "LOG_DIR": ("paths", "log_dir"),
    "LOGGING_CONFIG_FILE": ("logging", "config_file"),
    "APP_NAME": ("logging", "name"),
}


def _assign_nested(data: dict[str, Any], path: tuple[str, ...], value: str) -> None:
    node = data
    for key in path[:-1]:
        child = node.get(key)
        if not isinstance(child, dict):
            child = {}
            node[key] = child
        node = child
    node[path[-1]] = value


class LegacyFlatEnvSource(PydanticBaseSettingsSource):
    """Map the previous flat environment names onto nested settings."""

    def get_field_value(self, field: FieldInfo, field_name: str) -> tuple[Any, str, bool]:
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        values: dict[str, str] = {}
        env_file = self.config.get("env_file")
        if env_file:
            paths = env_file if isinstance(env_file, (list, tuple)) else [env_file]
            for path in paths:
                if path and os.path.isfile(path):
                    for key, raw in dotenv_values(path).items():
                        if key is not None and raw is not None:
                            values[key.upper()] = raw
        for key, raw in os.environ.items():
            values[key.upper()] = raw

        nested: dict[str, Any] = {}
        for name, path in LEGACY_FLAT_ENV.items():
            if name in values:
                _assign_nested(nested, path, values[name])
        return nested


class Settings(BaseSettings):
    """Application configuration."""

    db: DatabaseSettings
    logging: LoggingSettings = LoggingSettings()
    paths: PathSettings = PathSettings()
    api: ApiConfig
    scraping: ScrapingConfig
    pipeline: PipelineConfig

    model_config = SettingsConfigDict(
        env_prefix="WAGE_ETL_",
        env_nested_delimiter="__",
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        yaml_file=CONFIG_DIR / "config.yaml",
        yaml_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
        nested_model_default_partial_update=True,
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            LegacyFlatEnvSource(settings_cls),
            YamlConfigSettingsSource(settings_cls),
            file_secret_settings,
        )

    def ensure_dirs(self) -> None:
        """Create data, cache, and log directories."""
        self.paths.ensure_dirs()


@lru_cache
def get_settings() -> Settings:
    """Get cached application settings. Does not touch the filesystem."""
    return Settings()
