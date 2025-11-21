'''
Configuration models for the application.
'''

from pydantic import BaseModel, Field

class ApiConfig(BaseModel):
    '''
    Census API configuration.
    '''
    base_url: str

class ScrapingConfig(BaseModel):
    '''
    Web scraping configuration.
    '''
    base_url: str

class TargetStateConfig(BaseModel):
    '''
    Target state configuration.
    '''
    state_fips: str