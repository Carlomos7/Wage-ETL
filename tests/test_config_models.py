import pytest
from pydantic import ValidationError
from config.models import ApiConfig, ScrapingConfig, TargetStateConfig

def test_api_config_valid_data():
    '''Test the API configuration with valid data.'''
    test_url = "https://api.census.gov/data/2023/acs/acs5"  
    api_config = ApiConfig(base_url=test_url)
    assert api_config.base_url == test_url

def test_api_config_invalid_data():
    '''Test the API configuration with invalid data.'''
    with pytest.raises(ValidationError):
        ApiConfig(base_url="")

def test_scraping_config_valid_data():
    '''Test the scraping configuration with valid data.'''
    test_url = "https://livingwage.mit.edu"
    scraping_config = ScrapingConfig(base_url=test_url)
    assert scraping_config.base_url == test_url

def test_scraping_config_invalid_data():
    '''Test the scraping configuration with invalid data.'''
    with pytest.raises(ValidationError):
        ScrapingConfig(base_url="")

def test_target_state_config_valid_data():
    '''Test the target state configuration with valid data.'''
    test_fips = "34"
    target_state_config = TargetStateConfig(state_fips=test_fips)
    assert target_state_config.state_fips == test_fips