'''
Configuration models for the application.
'''

from pydantic import BaseModel, field_validator

class ApiConfig(BaseModel):
    '''
    Census API configuration.
    '''
    base_url: str

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
    state_fips: str
    @field_validator('state_fips')
    def state_fips_not_empty(cls, v: str) -> str:
        '''Validate the state FIPS code is not empty.'''
        if not v:
            raise ValueError("State FIPS code cannot be empty")
        return v