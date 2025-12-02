'''
Abstract base class for scrapers.
'''
from abc import ABC, abstractmethod
from typing import Any, Dict
import requests
from requests.exceptions import RequestException, HTTPError, Timeout, ConnectionError
import time

from config.logging import get_logger
from config.settings import get_settings
from src.extract.scrapers.scraper_cache import ScraperCache

HEADERS = {
    'User-Agent': 'MIT-WageETL/1.0 (Educational Project)',
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'en-US,en;q=0.9',
}

settings = get_settings()
logger = get_logger(module=__name__)


class BaseScraper(ABC):
    '''
    Abstract base class for scrapers.
    '''

    def __init__(self, use_cache: bool = True):
        self.settings = get_settings()
        self.logger = get_logger(module=self.__class__.__name__)
        self.use_cache = use_cache
        self._cache = ScraperCache() if use_cache else None
        self._session = requests.Session()
        self._session.headers.update(HEADERS)

    def fetch_with_retry(self, url: str) -> requests.Response:
        '''
        Fetch URL with exponential backoff retry logic.
        '''
        scrape_config = self.settings.scraping
        max_retries = scrape_config.max_retries
        timeout = scrape_config.timeout_seconds

        for attempt in range(max_retries):
            try:
                self.logger.debug(
                    f'Attempt {attempt + 1} of {max_retries} to fetch {url}')
                response = self._session.get(url, timeout=timeout)
                response.raise_for_status()
                self.logger.info(
                    f'Successfully fetched data (Status: {response.status_code})')
                return response
            except (Timeout, ConnectionError) as e:
                wait_time = 2 ** attempt
                self.logger.warning(
                    f'Request timed out or connection error. Retrying in {wait_time} seconds...')
                if attempt < max_retries - 1:
                    time.sleep(wait_time)
            except HTTPError as e:
                status_code = e.response.status_code
                match status_code:
                    case 404:
                        self.logger.error(f'Resource not found: {url}')
                        raise
                    case 429:
                        wait_time = 2 ** (attempt + 2)
                        self.logger.error(f'Rate limit exceeded: {url}')
                        if attempt < max_retries - 1:
                            time.sleep(wait_time)
                    case _ if status_code >= 500:
                        wait_time = 2 ** (attempt + 2)
                        self.logger.error(f'Server error {status_code}: {url}')
                        if attempt < max_retries - 1:
                            time.sleep(wait_time)
                    case _:
                        self.logger.error(f'HTTP error {status_code}: {url}')
                        raise
            except RequestException as e:
                self.logger.error(f'Request failed for {url}: {e}')
                raise
        raise RequestException(
            f"Failed to fetch {url} after {max_retries} attempts")

    def get_page(self, url: str) -> requests.Response:
        '''
        Get the page from the URL, using cache if available.
        '''
        self.logger.info(f'Fetching data from {url}')

        # Check cache first
        if self._cache:
            cached_content = self._cache.get(url)
            if cached_content:
                self.logger.info(f'Using cached response for {url}')
                # Create a structured response object that mimics a real network response
                response = requests.Response()
                response._content = cached_content
                response.status_code = 200
                response.url = url
                response.encoding = 'utf-8'
                response.reason = "OK"
                response.headers['Content-Type'] = 'text/html; charset=utf-8'
                return response

        # Fetch from network
        response = self.fetch_with_retry(url)

        # Cache the response
        if self._cache:
            self._cache.set(url, response.content)

        return response
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc, tb):
        self._session.close()

    @abstractmethod
    def parse(self, response: requests.Response, **kwargs) -> Dict[str, Any]:
        '''
        Parse the response and return the extracted data.
        '''
        pass

    @abstractmethod
    def build_url(self, **kwargs) -> str:
        '''
        Build the URL for a specific resource.
        '''
        pass
