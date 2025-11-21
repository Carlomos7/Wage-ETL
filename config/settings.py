'''
This module contains the settings for the application.
'''

from functools import lru_cache
from pathlib import Path
from typing import Type
from pydantic import Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)
from config.models import ApiConfig, ScrapingConfig, TargetStateConfig

# Default paths: TODO: Consider using environment variables to override these paths
_DEFAULT_YAML_FILE = Path(__file__).parent / "config.yaml"
_DEFAULT_ENV_FILE = Path(__file__).parent.parent / ".env"

class Settings(BaseSettings):
    '''
    This class contains the configurations for the application.
    '''

    model_config = SettingsConfigDict(
        env_file=str(_DEFAULT_ENV_FILE),
        env_file_encoding="utf-8",
        yaml_file=str(_DEFAULT_YAML_FILE),
    )

    # Application settings
    app_name: str = "wage-etl"
    app_version: str = "0.1.0"
    log_level: str = "INFO"

    # Base Destination Paths
    base_dir: Path = Path(__file__).parent.parent
    data_dir: Path = base_dir / "data"
    raw_dir: Path = data_dir / "raw"
    processed_dir: Path = data_dir / "processed"

    # Database settings
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    db_driver: str = "psycopg2"

    # Configurations from YAML
    api: ApiConfig = Field(default_factory=ApiConfig)
    scraping: ScrapingConfig = Field(default_factory=ScrapingConfig)
    target_state: TargetStateConfig = Field(default_factory=TargetStateConfig)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        '''
        Customize settings source priority.
        '''
        if not _DEFAULT_YAML_FILE.exists():
            raise FileNotFoundError(f"YAML file not found: {_DEFAULT_YAML_FILE}")
        
        return (
            dotenv_settings,
            env_settings,
            YamlConfigSettingsSource(settings_cls, _DEFAULT_YAML_FILE),
            init_settings,
        )

    def ensure_dirs(self):
        '''
        Ensure all required directories exist.
        '''
        for dir in [self.raw_dir, self.processed_dir]:
            dir.mkdir(parents=True, exist_ok=True)
    
    @property
    def db_url(self) -> str:
        '''
        Get the database URL.
        '''
        return f"{self.db_driver}://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"


@lru_cache
def get_settings() -> Settings:
    '''
    Get the settings for the application.
    '''
    settings = Settings()
    settings.ensure_dirs()
    return settings
