'''
Configuration models for the application.
'''

from pydantic import BaseModel, field_validator
from typing import Optional


class HttpClientConfig(BaseModel):
    '''
    Base configuration for HTTP clients (API and scraping).
    '''
    base_url: str
    max_retries: int = 3
    timeout_seconds: int = 30
    cache_ttl_days: int = 30
    ssl_verify: bool = True
    proxies: Optional[dict] = None

    @field_validator('base_url')
    @classmethod
    def url_not_empty(cls, v: str) -> str:
        """Validate base URL is not empty."""
        if not v:
            raise ValueError("Base URL cannot be empty")
        return v


class ApiConfig(HttpClientConfig):
    '''
    Census API configuration.
    '''
    cache_ttl_days: int = 90


class ScrapingConfig(HttpClientConfig):
    '''
    Web scraping configuration.
    '''
    min_delay_seconds: float = 1.0
    max_delay_seconds: float = 3.0

    @field_validator('max_delay_seconds')
    @classmethod
    def validate_delay_range(cls, v: float, info) -> float:
        """Ensure max_delay >= min_delay."""
        min_delay = info.data.get('min_delay_seconds')
        if min_delay and v < min_delay:
            raise ValueError(
                f"max_delay_seconds ({v}) must be >= min_delay_seconds ({min_delay})")
        return v


class PipelineConfig(BaseModel):
    """
    ETL pipeline orchestration configuration.
    """
    min_success_rate: float = 0.8

    @field_validator('min_success_rate')
    @classmethod
    def validate_success_rate(cls, v: float) -> float:
        """Ensure success rate is between 0 and 1."""
        if not 0 <= v <= 1:
            raise ValueError(
                f"min_success_rate must be between 0 and 1, got {v}")
        return v


class TargetStateConfig(BaseModel):
    '''
    Target state configuration.
    '''
    state_abbr: str

    @field_validator('state_abbr')
    def state_abbr_not_empty(cls, v: str) -> str:
        '''Validate the state abbreviation is not empty.'''
        if not v:
            raise ValueError("State abbreviation cannot be empty")
        return v


class StateConfig(BaseModel):
    '''
    State configuration with FIPS mappingg from a JSON file.
    '''
    fips_map: dict[str, str]

    @field_validator('fips_map')
    def fips_map_not_empty(cls, v: dict[str, str]) -> dict[str, str]:
        '''Validate the FIPS map is not empty.'''
        if not v:
            raise ValueError("FIPS map cannot be empty")
        return v


__all__ = [
    "HttpClientConfig",
    "ApiConfig",
    "ScrapingConfig",
    "PipelineConfig",
    "TargetStateConfig",
    "StateConfig",
]