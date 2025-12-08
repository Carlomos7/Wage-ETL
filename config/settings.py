'''
This module contains the settings for the application.
'''
import json
from functools import lru_cache
from pathlib import Path
from typing import Type
from pydantic import Field, field_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)
from config.models import ApiConfig, ScrapingConfig, StateConfig, PipelineConfig

# Default paths:
# TODO: Consider using environment variables to override these paths
_DEFAULT_YAML_FILE = Path(__file__).parent / "config.yaml"
_DEFAULT_ENV_FILE = Path(__file__).parent.parent / ".env"
_DEFAULT_STATE_JSON_FILE = Path(__file__).parent / "state_fips.json"


def _load_state_config() -> StateConfig:
    '''
    Load StateConfig from the JSON file.
    '''
    if not _DEFAULT_STATE_JSON_FILE.exists():
        raise FileNotFoundError(
            f"State FIPS file not found: {_DEFAULT_STATE_JSON_FILE}")
    
    with open(_DEFAULT_STATE_JSON_FILE, 'r', encoding='utf-8') as f:
        fips_map = json.load(f)
    
    return StateConfig(fips_map=fips_map)


class Settings(BaseSettings):
    '''
    This class contains the configurations for the application.
    '''
    # Application settings
    app_name: str = Field(default="wage_etl")
    
    # Environment settings
    environment: str = Field(default='production')

    # Logging
    log_level: str = Field(default="INFO")
    log_dir: Path = Path(__file__).parent.parent / "logs"
    log_to_file: bool = Field(default=True)
    logging_config_file: Path = Path(__file__).parent / "logging_conf.json"

    # Base Destination Paths
    base_dir: Path = Path(__file__).parent.parent
    data_dir: Path = base_dir / "data"
    raw_dir: Path = data_dir / "raw"
    processed_dir: Path = data_dir / "processed"
    cache_dir: Path = data_dir / "cache"

    # Database settings
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    db_driver: str = "psycopg2"

    # Configurations from YAML
    api: ApiConfig
    scraping: ScrapingConfig
    pipeline: PipelineConfig
    state_config: StateConfig = Field(default_factory=_load_state_config)
    

    # Customize settings source priority
    model_config = SettingsConfigDict(
        env_file=str(_DEFAULT_ENV_FILE),
        env_file_encoding="utf-8",
        yaml_file=str(_DEFAULT_YAML_FILE),
        extra="ignore",
    )

    @field_validator('environment')
    @classmethod
    def validate_environment(cls, v: str) -> str:
        '''Validate environment is allowed.'''
        allowed = {'development', 'testing', 'production'}
        if v not in allowed:
            raise ValueError(f"Invalid environment: {v}. Must be one of {allowed}")
        return v
    
    @field_validator('log_level')
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        '''Validate log level is allowed.'''
        allowed = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
        if v not in allowed:
            raise ValueError(f"Invalid log level: {v}. Must be one of {allowed}")
        return v

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
            raise FileNotFoundError(
                f"YAML file not found: {_DEFAULT_YAML_FILE}")

        return (
            dotenv_settings,
            env_settings,
            YamlConfigSettingsSource(settings_cls, _DEFAULT_YAML_FILE),
            init_settings,
        )

    @property
    def db_url(self) -> str:
        '''
        Get the database URL.
        '''
        return f"{self.db_driver}://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

    def ensure_dirs(self):
        '''
        Ensure all required directories exist.
        '''
        for dir in [self.raw_dir, self.processed_dir, self.log_dir, self.cache_dir]:
            dir.mkdir(parents=True, exist_ok=True)
    
    def __init__(self, **data):
        super().__init__(**data)
        
        # Auto-adjust settings based on environment
        if self.environment == 'development':
            if 'log_level' not in data:
                self.log_level = 'DEBUG'
            if 'log_to_file' not in data:
                self.log_to_file = False
                
        elif self.environment == 'testing':
            if 'log_level' not in data:
                self.log_level = 'WARNING'
            if 'log_to_file' not in data:
                self.log_to_file = False
            if 'raw_dir' not in data:
                self.raw_dir = self.data_dir / "test" / "raw"
            if 'processed_dir' not in data:
                self.processed_dir = self.data_dir / "test" / "processed"
    
@lru_cache
def get_settings() -> Settings:
    '''Get the settings for the application.'''
    settings = Settings()
    settings.ensure_dirs()
    return settings