import pytest
from unittest.mock import patch, Mock, MagicMock
from requests.exceptions import Timeout, HTTPError, RequestException
from src.extract.scraping import scrape_county, ScrapeResult
from src.extract.scrapers import WageScraper


class TestWageScraperFetchWithRetry:
    """Tests for the WageScraper.fetch_with_retry method."""

    def test_successful_fetch(self):
        """Should return response on successful fetch."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status = Mock()
        
        scraper = WageScraper()
        scraper._session.get = Mock(return_value=mock_response)
        
        result = scraper.fetch_with_retry("http://example.com")

        assert result == mock_response
        scraper._session.get.assert_called_once()

    @patch('src.extract.scrapers.base_scraper.time.sleep')
    def test_retry_on_timeout(self, mock_sleep):
        """Should retry on timeout and succeed on second attempt."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status = Mock()
        
        scraper = WageScraper()
        scraper._session.get = Mock(side_effect=[Timeout(), mock_response])

        result = scraper.fetch_with_retry("http://example.com")

        assert result == mock_response
        assert scraper._session.get.call_count == 2
        mock_sleep.assert_called_once_with(1)  # 2^0 = 1

    def test_no_retry_on_404(self):
        """Should not retry on 404 errors."""
        mock_response = Mock()
        mock_response.status_code = 404
        http_error = HTTPError(response=mock_response)
        
        scraper = WageScraper()
        scraper._session.get = Mock(side_effect=http_error)

        with pytest.raises(HTTPError):
            scraper.fetch_with_retry("http://example.com")

        scraper._session.get.assert_called_once()


class TestScrapeCounty:
    """Tests for the scrape_county function."""

    @patch('src.extract.scraping.WageScraper')
    @patch('src.extract.caching.scraper_cache.ScraperCache.get')
    def test_returns_scrape_result_on_failure(self, mock_get, mock_scraper_class):
        """Should return ScrapeResult with error on failure."""
        mock_get.return_value = None
        
        mock_scraper = MagicMock()
        mock_scraper.build_url.return_value = "http://example.com"
        mock_scraper.get_page.side_effect = RequestException("Connection failed")
        # Make __enter__ return the same mock_scraper instance
        mock_scraper.__enter__.return_value = mock_scraper
        mock_scraper_class.return_value = mock_scraper

        result = scrape_county("34", "001")

        assert isinstance(result, ScrapeResult)
        assert result.success is False
        assert result.fips_code == "34001"
        assert "Connection failed" in result.error