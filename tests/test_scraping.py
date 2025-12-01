import pytest
from unittest.mock import patch, Mock
from requests.exceptions import Timeout, HTTPError
from src.extract.web_scraper import fetch_with_retry, scrape_county, ScrapeResult


class TestFetchWithRetry:
    """Tests for the fetch_with_retry function."""

    @patch('src.extract.web_scraper.requests.get')
    def test_successful_fetch(self, mock_get):
        """Should return response on successful fetch."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        result = fetch_with_retry("http://example.com")
        
        assert result == mock_response
        mock_get.assert_called_once()

    @patch('src.extract.web_scraper.time.sleep')
    @patch('src.extract.web_scraper.requests.get')
    def test_retry_on_timeout(self, mock_get, mock_sleep):
        """Should retry on timeout and succeed on second attempt."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_get.side_effect = [Timeout(), mock_response]
        
        result = fetch_with_retry("http://example.com")
        
        assert result == mock_response
        assert mock_get.call_count == 2
        mock_sleep.assert_called_once_with(1)  # 2^0 = 1

    @patch('src.extract.web_scraper.requests.get')
    def test_no_retry_on_404(self, mock_get):
        """Should not retry on 404 errors."""
        mock_response = Mock()
        mock_response.status_code = 404
        http_error = HTTPError(response=mock_response)
        mock_get.side_effect = http_error
        
        with pytest.raises(HTTPError):
            fetch_with_retry("http://example.com")
        
        mock_get.assert_called_once()


class TestScrapeCounty:
    """Tests for the scrape_county function."""

    @patch('src.extract.web_scraper.get_page')
    @patch('pathlib.Path.exists')
    def test_returns_scrape_result_on_failure(self, mock_exists, mock_get_page):
        """Should return ScrapeResult with error on failure."""
        from requests.exceptions import RequestException
        # Mock file existence to return False so we don't skip scraping
        mock_exists.return_value = False
        mock_get_page.side_effect = RequestException("Connection failed")
        
        result = scrape_county("34", "001")
        
        assert isinstance(result, ScrapeResult)
        assert result.success is False
        assert result.fips_code == "34001"
        assert "Connection failed" in result.error