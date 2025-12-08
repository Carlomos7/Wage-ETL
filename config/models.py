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
    dataset: str
    variables: list[str]
    county: list[str]


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
    '''
    Pipeline configuration for the ETL process.
    '''
    min_success_rate: float = 0.8
    target_states: list[str] | str = "*"  # Allow list OR single string

    @field_validator("min_success_rate")
    @classmethod
    def validate_success_rate(cls, v: float) -> float:
        if not 0 <= v <= 1:
            raise ValueError("min_success_rate must be between 0 and 1")
        return v

    @field_validator("target_states")
    @classmethod
    def normalize_states(cls, v):
        """
        Normalize input so:
        - "*" becomes ["*"]
        - "NJ" becomes ["NJ"]
        - ["NJ", "NY"] stays as ["NJ", "NY"]
        """
        if isinstance(v, str):
            v = v.strip()
            return ["*"] if v == "*" else [v.upper()]

        if isinstance(v, list):
            return [item.upper() for item in v]

        raise ValueError("target_states must be a string or list of strings")


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
    "StateConfig",
]
