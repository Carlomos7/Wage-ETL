"""
Data extraction from web sources and APIs.
"""
from src.extract.cache import ResponseCache
from src.extract.http import HttpClient
from src.extract.census_api import CensusExtractor
from src.extract.wage_scraper import WageExtractor
from src.extract.extract_ops import (
    ScrapeResult,
    scrape_county,
    scrape_county_with_extractor,
    scrape_state_counties,
    get_states,
    get_all_counties,
    get_county_codes,
    get_county_codes_for_state,
)

__all__ = [
    # Classes
    "ResponseCache",
    "HttpClient",
    "CensusExtractor",
    "WageExtractor",
    # Types
    "ScrapeResult",
    # Functions
    "scrape_county",
    "scrape_county_with_extractor",
    "scrape_state_counties",
    "get_states",
    "get_all_counties",
    "get_county_codes",
    "get_county_codes_for_state",
]