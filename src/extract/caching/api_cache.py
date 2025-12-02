'''
Cache implementation for API JSON responses.
'''
import json
from datetime import datetime
from typing import Optional, Dict, Any

from config.logging import get_logger
from src.extract.caching.cache_interface import CacheInterface

logger = get_logger(module=__name__)

class ApiCache(CacheInterface):
    '''
    File-based cache for API JSON responses
    '''

    def get(self, identifier: str) -> Optional[Dict[str, Any]]:
        '''
        Get the cached content for a given identifier.

        Args:
            identifier: The identifier of the resource to retrieve.
        
        Returns:
            Dictionary of cached content, or None if not cached or expired.
        '''
        cache_path = self._resolve_cache_path(identifier)

        if not cache_path.exists():
            logger.debug(f'Cache miss for {identifier}')
            return None

        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cached_data = json.load(f)

            # Check expiration
            cache_time = datetime.fromisoformat(cached_data['timestamp'])
            if self._is_expired(cache_time):
                logger.debug(f'Cache expired for {identifier}')
                return None
            
            logger.debug(f'Cache hit for {identifier}')
            return {
                'data': cached_data['response_data'],
                'status_code': cached_data.get('status_code', 200)
            }
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Invalid cache file for {identifier}: {e}")
            cache_path.unlink()
            return None
        
    def store(self, identifier: str, content: Dict[str, Any]) -> None:
        '''
        Store the cached content for a given identifier.
        
        Args:
            identifier: The identifier of the resource to store.
            content: The content to store as a dictionary.
        '''
        cache_path = self._resolve_cache_path(identifier)
        cache_data = {
            'url': identifier,
            'timestamp': datetime.now().isoformat(),
            'response_data': content['data'],
            'status_code': content.get('status_code', 200)
        }
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f)
        
        logger.debug(f'Cached API response for {identifier}')

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
                if self._is_expired(cached_time):
                    cache_file.unlink()
                    count += 1
            except (json.JSONDecodeError, KeyError):
                # Invalid cache file, remove it
                cache_file.unlink()
                count += 1
        logger.info(f"Cleared {count} expired cache files")
        return count