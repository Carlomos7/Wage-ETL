import pytest
from datetime import datetime, timedelta
import json

from src.extract.caching.scraper_cache import ScraperCache


class TestScraperCache:
    """Tests for the ScraperCache class."""

    @pytest.fixture
    def temp_cache(self, tmp_path):
        """Create a cache with a temporary directory."""
        return ScraperCache(cache_dir=tmp_path, ttl_days=7)

    def test_cache_miss_returns_none(self, temp_cache):
        """Should return None for uncached URL."""
        result = temp_cache.get("http://example.com/not-cached")
        assert result is None

    def test_cache_set_and_get(self, temp_cache):
        """Should store and retrieve cached content."""
        url = "http://example.com/test"
        content = b"<html>Test content</html>"

        temp_cache.store(url, content)
        result = temp_cache.get(url)

        assert result == content

    def test_cache_expiration(self, temp_cache):
        """Should return None for expired cache."""
        url = "http://example.com/expired"
        content = b"<html>Old content</html>"
        
        # Manually create an expired cache entry
        cache_path = temp_cache._resolve_cache_path(url)
        expired_time = datetime.now() - timedelta(days=10)
        
        cached_data = {
            'url': url,
            'timestamp': expired_time.isoformat(),
            'content': content.decode('utf-8')
        }
        
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(cached_data, f)

        result = temp_cache.get(url)
        assert result is None

    def test_clear_removes_all_files(self, temp_cache):
        """Should remove all cached files."""
        # Add some cached items
        temp_cache.store("http://example.com/1", b"content1")
        temp_cache.store("http://example.com/2", b"content2")
        
        count = temp_cache.clear()
        
        assert count == 2
        assert temp_cache.get("http://example.com/1") is None
        assert temp_cache.get("http://example.com/2") is None

    def test_cache_key_is_consistent(self, temp_cache):
        """Same URL should always produce same cache key."""
        url = "http://example.com/test"
        
        key1 = temp_cache._compute_cache_key(url)
        key2 = temp_cache._compute_cache_key(url)
        
        assert key1 == key2

    def test_different_urls_have_different_keys(self, temp_cache):
        """Different URLs should have different cache keys."""
        key1 = temp_cache._compute_cache_key("http://example.com/page1")
        key2 = temp_cache._compute_cache_key("http://example.com/page2")
        
        assert key1 != key2