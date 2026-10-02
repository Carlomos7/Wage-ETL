"""
Tests for Census API extractor functionality.
"""
import pytest
import json
from unittest.mock import Mock

from wage_etl.config.models import ApiConfig
from wage_etl.extract.census_api import CensusExtractor
from wage_etl.extract.http import HttpClient


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

    def test_init(self, mock_client):
        """Test CensusExtractor initialization."""
        api_config = ApiConfig(
            base_url="https://api.census.gov/data",
            dataset="2023/acs/acs5",
            variables=["NAME"],
            county=["*"],
            cache_ttl_days=90,
        )
        fips_map = {"AL": "01", "AK": "02", "NJ": "34"}

        extractor = CensusExtractor(mock_client, api_config, fips_map, ["*"])

        assert extractor._client is mock_client
        assert extractor._api_config == api_config
        assert extractor.state_fips_list == ["01", "02", "34"]

    def test_get_counties(self, mock_client, sample_county_data):
        """Test getting counties for a state."""
        mock_client.get.return_value = json.dumps(
            sample_county_data).encode('utf-8')

        extractor = CensusExtractor.__new__(CensusExtractor)
        extractor._client = mock_client
        mock_api_config = Mock()
        mock_api_config.dataset = "2023/acs/acs5"
        mock_api_config.variables = ["NAME"]
        mock_api_config.county = ["*"]
        extractor._api_config = mock_api_config
        
        # Set up state_fips_list to include state 01
        extractor.state_fips_list = ["01"]
        # Set up _state_fips_map for the comparison in get_counties
        extractor._state_fips_map = {"AL": "01", "AK": "02", "NJ": "34"}

        result = extractor.get_counties()

        # Verify the endpoint (dataset) and variables were used
        mock_client.get.assert_called_once()
        call_args = mock_client.get.call_args
        # endpoint is a keyword argument
        assert call_args.kwargs['endpoint'] == "2023/acs/acs5"
        # Verify variables are in params
        assert "NAME" in call_args.kwargs['params']['get']
        # Verify state filter is in params
        assert call_args.kwargs['params']['in'] == "state:01"
        assert len(result) == 2
        assert result[0]["county_name"] == "Alabama County"
        assert result[0]["state_fips"] == "01"
        assert result[0]["county_fips"] == "001"
        assert result[0]["full_fips"] == "01001"

    def test_get_states(self, mock_client, sample_state_data):
        """Test getting all states."""
        mock_client.get.return_value = json.dumps(
            sample_state_data).encode('utf-8')

        extractor = CensusExtractor.__new__(CensusExtractor)
        extractor._client = mock_client
        mock_api_config = Mock()
        mock_api_config.dataset = "2023/acs/acs5"
        mock_api_config.variables = ["NAME"]
        extractor._api_config = mock_api_config
        
        # Set up _state_fips_map attribute
        extractor._state_fips_map = {"AL": "01", "AK": "02"}

        result = extractor.get_states()

        assert len(result) == 2
        assert result[0]["state_name"] == "Alabama"
        assert result[0]["state_fips"] == "01"
        assert result[0]["state_abbr"] == "AL"

    def test_get_county_codes(self, mock_client, sample_county_data):
        """Test getting just county FIPS codes."""
        mock_client.get.return_value = json.dumps(
            sample_county_data).encode('utf-8')

        extractor = CensusExtractor.__new__(CensusExtractor)
        extractor._client = mock_client
        mock_api_config = Mock()
        mock_api_config.dataset = "2023/acs/acs5"
        mock_api_config.variables = ["NAME"]
        mock_api_config.county = ["*"]
        extractor._api_config = mock_api_config
        
        # Set up state_fips_list to include state 01
        extractor.state_fips_list = ["01"]
        
        # Mock get_counties to return parsed data
        extractor.get_counties = Mock(return_value=[
            {"county_name": "Alabama County", "state_fips": "01", "county_fips": "001", "full_fips": "01001"},
            {"county_name": "Baldwin County", "state_fips": "01", "county_fips": "003", "full_fips": "01003"},
        ])

        result = extractor.get_county_codes()
        assert result == ["001", "003"]

    def test_parse_counties(self, sample_county_data):
        """Test parsing county data."""
        extractor = CensusExtractor.__new__(CensusExtractor)
        result = extractor._parse_counties(sample_county_data)

        assert len(result) == 2
        assert result[0]["county_name"] == "Alabama County"
        assert result[0]["state_fips"] == "01"
        assert result[0]["county_fips"] == "001"
