'''
Response caching for HTTP requests.
'''
import base64
import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from config.settings import get_settings
from config.logging import get_logger

logger = get_logger(module=__name__)


class ResponseCache:
    '''
    File-based cache for HTTP responses.
    '''

    def __init__(self, cache_dir: Optional[Path] = None, ttl_days: int = 30):
        settings = get_settings()
        self.cache_dir = cache_dir if cache_dir is not None else settings.cache_dir
        self.ttl_days = ttl_days

    def _hash_key(self, key: str) -> str:
        '''
        Generate MD5 hash of the key.
        '''
        return hashlib.md5(key.encode()).hexdigest()

    def _cache_path(self, key: str) -> Path:
        '''
        Get the filepath for a cached key.
        '''
        return self.cache_dir / f"{self._hash_key(key)}.json"

    def _is_expired(self, timestamp: datetime) -> bool:
        '''
        Check if a timestamp is expired.
        '''
        return datetime.now() - timestamp > timedelta(days=self.ttl_days)

    def get(self, key: str) -> Optional[dict]:
        '''
        Get a cached item by key.
        '''
        cache_path = self._cache_path(key)
        if not cache_path.exists():
            return None
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cached_data = json.load(f)
            timestamp = datetime.fromisoformat(cached_data['timestamp'])
            if self._is_expired(timestamp):
                return None
            return base64.b64decode(cached_data['content'])
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.warning(f"Error loading cached data for key {key}: {e}")
            cache_path.unlink()
            return None

    def store(self, key: str, content: bytes) -> None:
        '''
        Store a new item in the cache.
        '''
        cache_path = self._cache_path(key)
        cached = {
            'key': key,
            'timestamp': datetime.now().isoformat(),
            'content': base64.b64encode(content).decode('utf-8')
        }
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(cached, f)

    def clear_expired(self) -> int:
        '''
        Clear all expired items from the cache.
        '''
        count = 0
        for cache_file in self.cache_dir.glob('*.json'):
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cached_data = json.load(f)
                timestamp = datetime.fromisoformat(cached_data['timestamp'])
                if self._is_expired(timestamp):
                    cache_file.unlink()
                    count += 1
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logger.warning(
                    f"Error clearing expired cache file {cache_file.name}: {e}")
                cache_file.unlink()
                count += 1
        return count

    def clear_all(self) -> int:
        '''
        Remove all items from the cache.
        '''
        count = 0
        for cache_file in self.cache_dir.glob("*.json"):
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cached_data = json.load(f)

                cached_time = datetime.fromisoformat(cached_data['timestamp'])
                if self._is_expired(cached_time):
                    cache_file.unlink()
                    count += 1
            except (json.JSONDecodeError, KeyError):
                # Invalid cache file, remove it
                cache_file.unlink()
                count += 1
        logger.debug(f"Cleared {count} cache files")
        return count
