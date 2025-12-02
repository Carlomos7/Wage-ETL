'''
Abstract base class for caching implementations.
'''
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional
from datetime import datetime, timedelta
import hashlib

from config.logging import get_logger
from config.settings import get_settings

logger = get_logger(module=__name__)


class CacheInterface(ABC):
    '''
    Abstract base class for caching implementations.
    '''

    def __init__(self, cache_dir: Optional[Path] = None, ttl_days: Optional[int] = None):
        '''
        Initialize the cache with a directory and a time to live.

        Args:
            cache_dir: The directory to store the cache.
            ttl_days: The time to live for the cache in days.
        '''
        settings = get_settings()
        self.cache_dir = cache_dir or settings.cache_dir
        self.ttl_days = ttl_days

        logger.debug(
            f'{self.__class__.__name__} initialized in {self.cache_dir} with TTL of {self.ttl_days} days')

    def _compute_cache_key(self, identifier: str) -> str:
        '''
        Generate a cache key from a given identifier.

        Args:
            identifier: The identifier to generate a cache key from.

        Returns:
            MD5 hash of the identifier.
        '''
        return hashlib.md5(identifier.encode()).hexdigest()

    def _resolve_cache_path(self, identifier: str) -> Path:
        '''
        Get the cache path for a given identifier.

        Args:
            identifier: The identifier to get the cache path for.

        Returns:
            Path to the cache file.
        '''
        cache_key = self._compute_cache_key(identifier)
        return self.cache_dir / f"{cache_key}.json"

    def _is_expired(self, timestamp: datetime) -> bool:
        '''
        Check if a cache entry is expired based on the time to live.

        Args:
            timestamp: The timestamp of the cache entry.
        Returns:
            True if the cache entry is expired, False otherwise.
        '''
        return datetime.now() - timestamp > timedelta(days=self.ttl_days)

    @abstractmethod
    def get(self, identifier: str) -> Optional[Any]:
        '''
        Get the cached content for a given identifier.
        '''
        pass

    @abstractmethod
    def store(self, identifier: str, content: Any, **kwargs) -> None:
        '''
        Store the cached content for a given identifier.
        '''
        pass

    def clear(self) -> None:
        '''
        Clear all cached content.
        '''
        count = 0
        for cache_file in self.cache_dir.glob("*.json"):
            cache_file.unlink()
            count += 1
        logger.info(f"Cleared {count} cached content")
        return count
    
    @abstractmethod
    def clear_expired(self) -> int:
        '''
        Clear all expired cached content.
        '''
        pass