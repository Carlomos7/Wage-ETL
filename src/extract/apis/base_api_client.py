"""
Abstract base class for API clients.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple
import time
import requests
from requests.exceptions import RequestException, HTTPError, Timeout, ConnectionError

from config.logging import get_logger
from config.settings import get_settings
from src.extract.caching.api_cache import ApiCache

HEADERS = {
    "User-Agent": "Wage-ETL/1.0 (Educational Project)",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}


class BaseApiClient(ABC):
    """
    Abstract base class for API clients.
    """

    def __init__(self, use_cache: bool = True):
        self.settings = get_settings()
        self.logger = get_logger(module=self.__class__.__name__)
        self.use_cache = use_cache
        self._cache = ApiCache() if use_cache else None

        self._session = requests.Session()
        self._session.headers.update(HEADERS)

    def fetch_with_retry(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        """
        Fetch with exponential backoff and structured retry logic.
        """
        cfg = self.settings.api
        max_retries = cfg.max_retries
        timeout = cfg.timeout_seconds

        for attempt in range(max_retries):
            try:
                self.logger.debug(
                    f"[Attempt {attempt + 1}/{max_retries}] GET {url} params={params}"
                )

                response = self._session.get(url, params=params, timeout=timeout)
                response.raise_for_status()

                self.logger.info(f"Fetched successfully (status={response.status_code})")
                return response

            except (Timeout, ConnectionError) as e:
                self._log_and_wait(
                    msg="Network issue",
                    error=e,
                    attempt=attempt,
                    max_retries=max_retries,
                    base_delay=1,
                )

            except HTTPError as e:
                status = e.response.status_code

                match status:
                    # Not found - fail fast
                    case 404:
                        self.logger.error(f"Resource not found: {url}")
                        raise

                    # Rate limit - 429
                    case 429:
                        self._log_and_wait(
                            msg="Rate limited",
                            error=e,
                            attempt=attempt,
                            max_retries=max_retries,
                            base_delay=4,
                        )

                    # 5xx server errors
                    case code if 500 <= code <= 599:
                        self._log_and_wait(
                            msg=f"Server error ({code})",
                            error=e,
                            attempt=attempt,
                            max_retries=max_retries,
                            base_delay=4,
                        )

                    # Other HTTP errors - do not retry
                    case _:
                        self.logger.error(f"HTTP error {status}: {e}")
                        raise
            except RequestException as e:
                self.logger.error(f"Request failure (non-retryable): {e}")
                raise

        # If all retries exhausted:
        raise RequestException(
            f"Failed to fetch {url} after {max_retries} attempts"
        )

    def _log_and_wait(self, msg: str, error: Exception, attempt: int, max_retries: int, base_delay: int) -> None:
        """Shared retry logic for all retryable errors."""
        retry_num = attempt + 1
        wait_time = base_delay * (2 ** attempt)

        self.logger.warning(
            f"{msg} (attempt {retry_num}/{max_retries}) — {error}. "
            f"Retrying in {wait_time}s..."
        )

        if attempt < max_retries - 1:
            time.sleep(wait_time)
    def _validate_response(self, http_response: requests.Response) -> Dict[str, Any]:
        """
        Validate the response from the API.

        Args:
            response: The response from the API.

        Raises:
            ValueError: If the response is not valid.
        """
        try:
            response_data = http_response.json()
            self.logger.debug(f'Successfully parsed JSON response')
            return response_data
        except ValueError as e:
            self.logger.error(f'Failed to parse JSON response: {e}')
            raise ValueError(f'Invalid JSON response: {e}')

    def get_data(self, url: str, params: Dict[str, str]) -> Dict[str, Any]:
        """
        Get data from API endpoint, using cache if available.
        
        Args:
            url: Base URL for the API endpoint
            params: Query parameters as dictionary
            
        Returns:
            Parsed API response data as dictionary
        """
        # Building complete API URL for cache key
        cache_key = f'{url}?{requests.compat.urlencode(sorted(params.items()))}'

        # Check cache first
        if self._cache:
            cached_data = self._cache.get(cache_key)
            if cached_data:
                self.logger.info(f'Using cached data for {cache_key}')
                return cached_data
        
        # Fetch from network
        http_response = self.fetch_with_retry(url, params)
        response_data = self._validate_response(http_response)

        # Cache the response
        if self._cache:
            self._cache.store(cache_key, response_data)

        return response_data

    
    def __enter__(self):
        """
        Context manager entry point.
        """
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        """
        Context manager exit point.
        """
        self._session.close()
    
    @abstractmethod
    def build_request_url_and_params(self, resource: str, **kwargs) -> Tuple[str, Dict[str, str]]:
        """
        Build the request URL and parameters for a specific API resource.
        """
        pass

    @abstractmethod
    def parse(self, response_data: Dict[str, Any], **kwargs) -> Any:
        """
        Parse the API response data into structured format.
        """
        pass