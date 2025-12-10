"""
Tests for response caching functionality.
"""
import json
import base64
import pytest
from datetime import datetime, timedelta
from pathlib import Path

from src.extract.cache import ResponseCache


class TestResponseCache:
    """Tests for ResponseCache."""

    @pytest.fixture
    def cache(self, tmp_path):
        """Create a ResponseCache instance for testing."""
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        return ResponseCache(cache_dir=cache_dir, ttl_days=7)

    def test_store_and_get(self, cache):
        """Test storing and retrieving cached content."""
        key = "test/key"
        content = b"response content"
        
        cache.store(key, content)
        result = cache.get(key)
        
        assert result == content

    def test_get_miss(self, cache):
        """Test getting non-existent cache entry."""
        result = cache.get("nonexistent/key")
        assert result is None

    def test_get_expired(self, cache):
        """Test that expired cache entries return None."""
        key = "expired/key"
        cache_path = cache._cache_path(key)
        
        # Create expired cache file
        cached_data = {
            "key": key,
            "timestamp": (datetime.now() - timedelta(days=10)).isoformat(),
            "content": base64.b64encode(b"old data").decode('utf-8')
        }
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(cached_data, f)
        
        result = cache.get(key)
        assert result is None

    def test_clear_expired(self, cache):
        """Test clearing expired cache entries."""
        # Create expired entry
        expired_key = "expired/entry"
        expired_path = cache._cache_path(expired_key)
        expired_data = {
            "key": expired_key,
            "timestamp": (datetime.now() - timedelta(days=10)).isoformat(),
            "content": base64.b64encode(b"old").decode('utf-8')
        }
        with open(expired_path, 'w', encoding='utf-8') as f:
            json.dump(expired_data, f)
        
        # Create fresh entry
        fresh_key = "fresh/entry"
        cache.store(fresh_key, b"new")
        
        count = cache.clear_expired()
        
        assert count == 1
        assert not expired_path.exists()
        assert cache.get(fresh_key) == b"new"
