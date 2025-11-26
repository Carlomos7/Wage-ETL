'''
This module contains the extract functions for the application.
'''
from src.extract.web_scraper import scrape_county, scrape_all_counties
from src.extract.api_extractor import get_county_codes

__all__ = ["scrape_county", "scrape_all_counties", "get_county_codes"]
