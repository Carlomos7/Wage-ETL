"""
Tests for Census API extractor functionality.
"""
import pytest
import json
from unittest.mock import Mock, patch

from src.extract.census_api import CensusExtractor
from src.extract.http import HttpClient


class TestCensusExtractor:
    """Tests for CensusExtractor."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock HttpClient."""
        return Mock(spec=HttpClient)

    @pytest.fixture
    def sample_county_data(self):
        """Sample Census API county response."""
        return [
            ["NAME", "state", "county"],
            ["Alabama County, Alabama", "01", "001"],
            ["Baldwin County, Alabama", "01", "003"],
        ]

    @pytest.fixture
    def sample_state_data(self):
        """Sample Census API state response."""
        return [
            ["NAME", "state"],
            ["Alabama", "01"],
            ["Alaska", "02"],
        ]

    @patch('src.extract.census_api.HttpClient')
    @patch('src.extract.census_api.get_settings')
    def test_init(self, mock_get_settings, mock_http_client):
        """Test CensusExtractor initialization."""
        mock_settings = Mock()
        mock_settings.api.base_url = "https://api.census.gov"
        mock_settings.api.timeout_seconds = 30
        mock_settings.api.max_retries = 3
        mock_settings.api.ssl_verify = True
        mock_settings.api.proxies = None
        mock_get_settings.return_value = mock_settings
        
        extractor = CensusExtractor(use_cache=False)
        assert extractor._client is not None

    def test_get_counties(self, mock_client, sample_county_data):
        """Test getting counties for a state."""
        mock_client.get.return_value = json.dumps(sample_county_data).encode('utf-8')
        
        extractor = CensusExtractor.__new__(CensusExtractor)
        extractor._client = mock_client
        
        result = extractor.get_counties("01")
        
        assert len(result) == 2
        assert result[0]["county_name"] == "Alabama County"
        assert result[0]["state_fips"] == "01"
        assert result[0]["county_fips"] == "001"
        assert result[0]["full_fips"] == "01001"

    def test_get_states(self, mock_client, sample_state_data):
        """Test getting all states."""
        mock_client.get.return_value = json.dumps(sample_state_data).encode('utf-8')
        
        extractor = CensusExtractor.__new__(CensusExtractor)
        extractor._client = mock_client
        
        with patch('src.extract.census_api.get_settings') as mock_get_settings:
            mock_settings = Mock()
            mock_settings.state_config.fips_map = {"AL": "01", "AK": "02"}
            mock_get_settings.return_value = mock_settings
            
            result = extractor.get_states()
        
        assert len(result) == 2
        assert result[0]["state_name"] == "Alabama"
        assert result[0]["state_fips"] == "01"
        assert result[0]["state_abbr"] == "AL"

    def test_get_county_codes(self, mock_client, sample_county_data):
        """Test getting just county FIPS codes."""
        mock_client.get.return_value = json.dumps(sample_county_data).encode('utf-8')
        
        extractor = CensusExtractor.__new__(CensusExtractor)
        extractor._client = mock_client
        
        result = extractor.get_county_codes("01")
        assert result == ["001", "003"]

    def test_parse_counties(self, sample_county_data):
        """Test parsing county data."""
        extractor = CensusExtractor.__new__(CensusExtractor)
        result = extractor._parse_counties(sample_county_data)
        
        assert len(result) == 2
        assert result[0]["county_name"] == "Alabama County"
        assert result[0]["state_fips"] == "01"
        assert result[0]["county_fips"] == "001"
