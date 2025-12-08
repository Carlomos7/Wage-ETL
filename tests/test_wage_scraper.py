"""
Tests for wage scraper functionality.
"""
import pytest
from unittest.mock import Mock, patch
from bs4 import BeautifulSoup

from src.extract.wage_scraper import WageExtractor
from src.extract.http import HttpClient


class TestWageExtractor:
    """Tests for WageExtractor."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock HttpClient."""
        return Mock(spec=HttpClient)

    @pytest.fixture
    def sample_html(self):
        """Sample HTML with wage tables."""
        return """
        <html>
            <body>
                <table class="results_table">
                    <thead>
                        <tr>
                            <th colspan="2">1 Adult</th>
                        </tr>
                    </thead>
                    <thead>
                        <tr>
                            <td>Category</td>
                            <td>0 Children</td>
                            <td>1 Child</td>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>Housing</td>
                            <td>$1000</td>
                            <td>$1200</td>
                        </tr>
                    </tbody>
                </table>
                <table class="results_table">
                    <thead>
                        <tr>
                            <th colspan="1">1 Adult</th>
                        </tr>
                    </thead>
                    <thead>
                        <tr>
                            <td>Category</td>
                            <td>0 Children</td>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>Wage</td>
                            <td>$15.00</td>
                        </tr>
                    </tbody>
                </table>
            </body>
        </html>
        """

    @patch('src.extract.wage_scraper.HttpClient')
    @patch('src.extract.wage_scraper.get_settings')
    def test_init(self, mock_get_settings, mock_http_client):
        """Test WageExtractor initialization."""
        mock_settings = Mock()
        mock_settings.scraping.base_url = "https://example.com"
        mock_settings.scraping.timeout_seconds = 30
        mock_settings.scraping.max_retries = 3
        mock_settings.scraping.ssl_verify = True
        mock_settings.scraping.proxies = None
        mock_get_settings.return_value = mock_settings
        
        extractor = WageExtractor(use_cache=False)
        assert extractor._client is not None

    def test_get_county_data(self, mock_client, sample_html):
        """Test getting county data."""
        mock_client.get.return_value = sample_html.encode('utf-8')
        
        extractor = WageExtractor.__new__(WageExtractor)
        extractor._client = mock_client
        
        result = extractor.get_county_data("01", "001")
        
        assert "wages_data" in result
        assert "expenses_data" in result
        # Verify endpoint is passed as keyword argument
        mock_client.get.assert_called_once_with(endpoint="counties/01001")

    def test_parse_page(self, sample_html):
        """Test parsing HTML page."""
        extractor = WageExtractor.__new__(WageExtractor)
        content = sample_html.encode('utf-8')
        result = extractor._parse_page(content, "001")
        
        assert "wages_data" in result
        assert "expenses_data" in result
        assert isinstance(result["wages_data"], list)
        assert isinstance(result["expenses_data"], list)

    def test_parse_page_insufficient_tables(self):
        """Test parsing page with insufficient tables raises error."""
        html = """
        <html>
            <body>
                <table class="results_table">
                    <tbody><tr><td>Only one table</td></tr></tbody>
                </table>
            </body>
        </html>
        """
        extractor = WageExtractor.__new__(WageExtractor)
        
        with pytest.raises(ValueError, match="Expected at least 2 tables"):
            extractor._parse_page(html.encode('utf-8'), "001")
