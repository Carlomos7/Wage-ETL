"""Application settings module."""

from functools import lru_cache

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
