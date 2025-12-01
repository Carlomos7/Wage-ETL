'''
Logging utilities for the extract module.
'''
from typing import Optional


def format_log_with_metadata(message: str, year: int, state_fips: str, county_fips: str) -> str:
    '''
    Format log message with structured metadata for provenance tracking.
    
    Args:
        message: The log message
        year: Year of the scrape
        state_fips: State FIPS code
        county_fips: County FIPS code (will be zero-padded)
    
    Returns:
        Formatted message with metadata prefix
    '''
    county_fips_str = str(county_fips).zfill(3)
    metadata = f"[year={year}][state={state_fips}][county={county_fips_str}]"
    return f"{metadata} {message}"

