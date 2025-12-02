'''
Caching layer for scrapers.
'''
import hashlib
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

from config.logging import get_logger
from config.settings import get_settings

settings = get_settings()
logger = get_logger(module=__name__)


class ScraperCache:
    '''
    File-based cache for scrapers storing raw HTML responses.
    '''

    def __init__(self, cache_dir: Optional[Path] = None, ttl_days: Optional[int] = None):
        self.cache_dir = cache_dir or settings.cache_dir
        self.ttl_days = ttl_days or settings.scraping.cache_ttl_days
        logger.debug(
            f'Cache initialized in {self.cache_dir} with TTL of {self.ttl_days} days')

    def _get_cache_key(self, url: str) -> str:
        '''Generate a cache key from URL.'''
        return hashlib.md5(url.encode()).hexdigest()

    def _get_cache_path(self, url: str) -> Path:
        '''Get the file path for a cached URL.'''
        cache_key = self._get_cache_key(url)
        return self.cache_dir / f"{cache_key}.json"

    def get(self, url: str) -> Optional[bytes]:
        '''
        Retrieve cached content for a given URL.

        Returns:
            Cached content as bytes, or None if not cached or expired.
        '''
        cache_path = self._get_cache_path(url)

        if not cache_path.exists():
            logger.debug(f'Cache miss for {url}')
            return None

        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cached_data = json.load(f)

            # Check expiration
            cache_time = datetime.fromisoformat(cached_data['timestamp'])
            if datetime.now() - cache_time > timedelta(days=self.ttl_days):
                logger.debug(f'Cache expired for {url}')
                return None

            logger.debug(f'Cache hit for {url}')
            return cached_data['content'].encode('utf-8')
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Invalid cache file for {url}: {e}")
            cache_path.unlink()
            return None

    def set(self, url: str, content: bytes) -> None:
        '''
        Store content in cache for a given URL.
        '''
        cache_path = self._get_cache_path(url)
        cache_data = {
            'url': url,
            'timestamp': datetime.now().isoformat(),
            'content': content.decode('utf-8')
        }
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f)

    def clear(self) -> int:
        '''
        Clear all cached files.

        Returns:
            Number of files deleted.
        '''
        count = 0
        for cache_file in self.cache_dir.glob("*.json"):
            cache_file.unlink()
            count += 1

        logger.info(f"Cleared {count} cached files")
        return count

    def clear_expired(self) -> int:
        '''
        Clear only expired cached files.

        Returns:
            Number of files deleted.
        '''
        count = 0
        for cache_file in self.cache_dir.glob("*.json"):
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cached_data = json.load(f)

                cached_time = datetime.fromisoformat(cached_data['timestamp'])
                if datetime.now() - cached_time > timedelta(days=self.ttl_days):
                    cache_file.unlink()
                    count += 1

            except (json.JSONDecodeError, KeyError):
                cache_file.unlink()
                count += 1

        logger.info(f"Cleared {count} expired cache files")
        return count
