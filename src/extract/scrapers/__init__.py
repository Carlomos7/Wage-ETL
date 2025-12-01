'''
Scraper implementations for different data sources.
'''
from src.extract.scrapers.base import BaseScraper
from src.extract.scrapers.wage_scraper import WageScraper
from src.extract.scrapers.scraper_cache import ScraperCache

__all__ = ["BaseScraper", "WageScraper", "ScraperCache"]