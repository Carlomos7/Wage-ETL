"""
Tests for HTTP client functionality.
"""
import pytest
from unittest.mock import Mock, patch
from requests.exceptions import Timeout, HTTPError

from src.extract.http import HttpClient
from src.extract.cache import ResponseCache


class TestHttpClient:
    """Tests for HttpClient."""

    def test_init(self):
        """Test HttpClient initialization."""
        client = HttpClient(base_url="https://example.com")
        assert client.base_url == "https://example.com"
        assert client.timeout == 30
        assert client.max_retries == 3

    def test_build_url(self):
        """Test URL building."""
        client = HttpClient(base_url="https://example.com")
        assert client._build_url("api/data") == "https://example.com/api/data"
        assert client._build_url("https://other.com") == "https://other.com"

    @patch('src.extract.http.HttpClient._fetch_with_retry')
    def test_get_with_cache_hit(self, mock_fetch):
        """Test GET request with cache hit."""
        cache = Mock(spec=ResponseCache)
        cache.get.return_value = b"cached content"
        
        client = HttpClient(base_url="https://example.com", cache=cache)
        result = client.get("api/data")
        
        assert result == b"cached content"
        mock_fetch.assert_not_called()

    @patch('src.extract.http.HttpClient._fetch_with_retry')
    def test_get_with_cache_miss(self, mock_fetch):
        """Test GET request with cache miss."""
        cache = Mock(spec=ResponseCache)
        cache.get.return_value = None
        mock_fetch.return_value = b"fresh content"
        
        client = HttpClient(base_url="https://example.com", cache=cache)
        result = client.get("api/data")
        
        assert result == b"fresh content"
        cache.store.assert_called_once()

    @patch('src.extract.http.HttpClient._fetch')
    @patch('src.extract.http.HttpClient._wait')
    def test_retry_on_timeout(self, mock_wait, mock_fetch):
        """Test retry logic on timeout."""
        mock_fetch.side_effect = [Timeout("Connection timeout"), b"success"]
        
        client = HttpClient(base_url="https://example.com", max_retries=3)
        result = client._fetch_with_retry("https://example.com/api")
        
        assert result == b"success"
        assert mock_fetch.call_count == 2

    @patch('src.extract.http.HttpClient._fetch')
    def test_no_retry_on_404(self, mock_fetch):
        """Test that 404 errors are not retried."""
        mock_response = Mock()
        mock_response.status_code = 404
        http_error = HTTPError("Not found")
        http_error.response = mock_response
        mock_fetch.side_effect = http_error
        
        client = HttpClient(base_url="https://example.com")
        
        with pytest.raises(HTTPError):
            client._fetch_with_retry("https://example.com/api")
        
        assert mock_fetch.call_count == 1
