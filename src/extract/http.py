'''
HTTP client for making requests to APIs and web pages.
'''
import time
from typing import Any, Optional
import requests
from requests.compat import urlencode
from requests.exceptions import ConnectionError, Timeout, HTTPError, RequestException
from config.settings import get_settings
from config.logging import get_logger
from src.extract.cache import ResponseCache

logger = get_logger(module=__name__)

DEFAULT_HEADERS = {
    "User-Agent": "Wage-ETL/1.0 (Educational Project)",
    "Accept": "text/html,application/json",
    "Accept-Language": "en-US,en;q=0.9",
}


class HttpClient:
    '''
    HTTP client for making requests to APIs and web pages.
    '''

    def __init__(
        self,
        base_url: str,
        headers: Optional[dict[str, str]] = None,
        timeout: int = 30,
        max_retries: int = 3,
        ssl_verify: bool = True,
        proxies: Optional[dict[str, str]] = None,
        cache: Optional[ResponseCache] = None,
    ) -> None:
        '''
        Initialize the HTTP client.
        '''
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.cache = cache
        self._request_counter = 0

        # Configure session
        self._session = requests.Session()
        self._session.verify = ssl_verify

        merged_headers = DEFAULT_HEADERS.copy()
        if headers:
            merged_headers.update(headers)
        self._session.headers.update(merged_headers)

        if proxies:
            self._session.proxies.update(proxies)

    @property
    def request_count(self) -> int:
        '''
        Get the number of requests made.
        '''
        return self._request_counter

    def _build_url(self, endpoint: str) -> str:
        '''
        Build the full URL for the given endpoint.
        '''
        if endpoint.startswith(('http://', 'https://')):
            url = endpoint
        else:
            url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.debug(f"Resolved URL: {url}")
        return url

    def _build_cache_key(self, endpoint: str, params: Optional[dict[str, Any]] = None) -> str:
        '''
        Build a cache key for the given endpoint and kwargs.
        '''
        if params:
            sorted_params = sorted(params.items())
            param_str = urlencode(sorted_params)
            return f"{endpoint}?{param_str}"
        return endpoint

    def _wait(self, attempt: int, base_delay: int) -> None:
        '''
        Exponential backoff with retries.
        '''
        if attempt < self.max_retries - 1:
            wait_time = base_delay * (2 ** attempt)
            time.sleep(wait_time)

    def _fetch(self, url: str, params: Optional[dict[str, Any]] = None) -> bytes:
        '''
        Single request wrapper. Raise on failure.
        '''
        response = self._session.get(url, params=params, timeout=self.timeout)
        self._request_counter += 1
        response.raise_for_status()
        return response.content

    def _fetch_with_retry(self, url: str, params: Optional[dict[str, Any]] = None) -> bytes:
        '''
        Fetch the content from the given URL with retry logic.
        '''
        last_exception = None
        for attempt in range(self.max_retries):
            try:
                return self._fetch(url, params)
            except (Timeout, ConnectionError) as e:
                last_exception = e
                logger.warning(
                    f"Network timeout/connection issue on attempt {attempt + 1}: {e}")
                self._wait(attempt, base_delay=1)
            except HTTPError as e:
                status_code = e.response.status_code
                last_exception = e

                match status_code:
                    case 404:
                        logger.error(
                            f"Resource not found ({status_code}): {url}")
                        raise
                    case 429:
                        logger.warning(f"Rate limited ({status_code}): {url}")
                        self._wait(attempt, base_delay=4)
                    case code if code <= 500 and code <= 599:
                        logger.warning(f"Server error ({code}): {url}")
                        self._wait(attempt, base_delay=4)
                    case _:
                        logger.error(f"HTTP error {status_code}: {e}")
                        raise

            except RequestException as e:
                last_exception = e
                logger.error(f"Request failure (non-retryable): {e}")
                raise

        logger.error(f"Request failed after {self.max_retries} attempts")
        raise last_exception

    def get(self, endpoint: str, params: Optional[dict[str, Any]] = None, use_cache: bool = True) -> bytes:
        '''
        Make a GET request to the given endpoint and return the content as bytes.
        '''
        url = self._build_url(endpoint)
        cache_key = self._build_cache_key(endpoint, params)

        if use_cache and self.cache:
            cached_content = self.cache.get(cache_key)
            if cached_content is not None:
                logger.debug(f"Cache hit for {cache_key}")
                return cached_content

        # Fetch from source
        content = self._fetch_with_retry(url, params)

        # Store in cache
        if use_cache and self.cache:
            self.cache.store(cache_key, content)

        return content

    def __enter__(self):
        '''
        Enter the context manager.
        '''
        return self

    def __exit__(self, exec_type, exec_val, exec_tb):
        '''
        Exit the context manager.
        '''
        self._session.close()
