'''
Scraper implementations for different data sources.
'''
from src.extract.scrapers.base import WebScraperBase
from src.extract.scrapers.wage_scraper import WageScraper

__all__ = ["WebScraperBase", "WageScraper"]