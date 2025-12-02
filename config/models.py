'''
Configuration models for the application.
'''

from pydantic import BaseModel, field_validator


class ApiConfig(BaseModel):
    '''
    Census API configuration.
    '''
    base_url: str
    max_retries: int = 3
    timeout_seconds: int = 30
    rate_limit_delay: float = 1.0
    cache_ttl_days: int = 90

    @field_validator('base_url')
    def api_not_empty(cls, v: str) -> str:
        '''Validate the API base URL is not empty.'''
        if not v:
            raise ValueError("API base URL cannot be empty")
        return v


class ScrapingConfig(BaseModel):
    '''
    Web scraping configuration.
    '''
    base_url: str
    max_retries: int = 3
    timeout_seconds: int = 30
    min_delay_seconds: float = 1.0
    max_delay_seconds: float = 3.0
    min_success_rate: float = 0.8
    cache_ttl_days: int = 30

    @field_validator('base_url')
    def scrape_url_not_empty(cls, v: str) -> str:
        '''Validate the scraping base URL is not empty.'''
        if not v:
            raise ValueError("Scraping base URL cannot be empty")
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
