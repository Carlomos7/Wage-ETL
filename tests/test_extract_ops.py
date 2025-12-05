"""
Tests for extraction operations.
"""
import pytest
from unittest.mock import Mock, patch

from src.extract.extract_ops import (
    ScrapeResult,
    scrape_county,
    scrape_county_with_extractor,
    scrape_state_counties,
    get_states,
    get_counties,
    get_county_codes,
)
from src.extract.wage_scraper import WageExtractor
from src.extract.census_api import CensusExtractor


class TestScrapeResult:
    """Tests for ScrapeResult."""

    def test_success(self):
        """Test ScrapeResult for successful scrape."""
        result = ScrapeResult(
            fips_code="01001",
            success=True,
            wages_data=[{"category": "Housing"}],
            expenses_data=[{"category": "Food"}],
        )
        assert result.fips_code == "01001"
        assert result.success is True
        assert result.error is None

    def test_failure(self):
        """Test ScrapeResult for failed scrape."""
        result = ScrapeResult(
            fips_code="01001",
            success=False,
            error="Network timeout",
        )
        assert result.fips_code == "01001"
        assert result.success is False
        assert result.error == "Network timeout"


class TestScrapeCounty:
    """Tests for scrape_county."""

    @patch('src.extract.extract_ops.WageExtractor')
    def test_success(self, mock_wage_extractor_class):
        """Test successful county scrape."""
        mock_extractor = Mock(spec=WageExtractor)
        mock_extractor.get_county_data.return_value = {
            "wages_data": [{"category": "Housing"}],
            "expenses_data": [{"category": "Food"}],
        }
        mock_wage_extractor_class.return_value.__enter__.return_value = mock_extractor
        
        result = scrape_county("01", "001")
        
        assert result.success is True
        assert result.fips_code == "01001"

    @patch('src.extract.extract_ops.WageExtractor')
    def test_failure(self, mock_wage_extractor_class):
        """Test failed county scrape."""
        mock_extractor = Mock(spec=WageExtractor)
        mock_extractor.get_county_data.side_effect = Exception("Network error")
        mock_wage_extractor_class.return_value.__enter__.return_value = mock_extractor
        
        result = scrape_county("01", "001")
        
        assert result.success is False
        assert result.error == "Network error"


class TestScrapeCountyWithExtractor:
    """Tests for scrape_county_with_extractor."""

    def test_success(self):
        """Test successful scrape with existing extractor."""
        mock_extractor = Mock(spec=WageExtractor)
        mock_extractor.get_county_data.return_value = {
            "wages_data": [],
            "expenses_data": [],
        }
        
        result = scrape_county_with_extractor(mock_extractor, "01", "001")
        
        assert result.success is True
        assert result.fips_code == "01001"


class TestScrapeStateCounties:
    """Tests for scrape_state_counties."""

    @patch('src.extract.extract_ops.WageExtractor')
    def test_scrape_multiple_counties(self, mock_wage_extractor_class):
        """Test scraping multiple counties."""
        mock_extractor = Mock(spec=WageExtractor)
        mock_extractor.get_county_data.return_value = {
            "wages_data": [],
            "expenses_data": [],
        }
        mock_wage_extractor_class.return_value.__enter__.return_value = mock_extractor
        
        county_codes = ["001", "003"]
        results = list(scrape_state_counties("01", county_codes))
        
        assert len(results) == 2
        assert all(r.success for r in results)
        assert results[0].fips_code == "01001"


class TestCensusLookups:
    """Tests for Census lookup functions."""

    @patch('src.extract.extract_ops.CensusExtractor')
    def test_get_states(self, mock_census_extractor_class):
        """Test getting all states."""
        mock_extractor = Mock(spec=CensusExtractor)
        mock_extractor.get_states.return_value = [
            {"state_name": "Alabama", "state_fips": "01", "state_abbr": "AL"},
        ]
        mock_census_extractor_class.return_value.__enter__.return_value = mock_extractor
        
        result = get_states()
        assert len(result) == 1
        assert result[0]["state_name"] == "Alabama"

    @patch('src.extract.extract_ops.CensusExtractor')
    def test_get_counties(self, mock_census_extractor_class):
        """Test getting counties for a state."""
        mock_extractor = Mock(spec=CensusExtractor)
        mock_extractor.get_counties.return_value = [
            {"county_name": "Alabama County", "state_fips": "01", "county_fips": "001"},
        ]
        mock_census_extractor_class.return_value.__enter__.return_value = mock_extractor
        
        result = get_counties("01")
        assert len(result) == 1
        assert result[0]["county_name"] == "Alabama County"

    @patch('src.extract.extract_ops.CensusExtractor')
    def test_get_county_codes(self, mock_census_extractor_class):
        """Test getting county FIPS codes."""
        mock_extractor = Mock(spec=CensusExtractor)
        mock_extractor.get_county_codes.return_value = ["001", "003"]
        mock_census_extractor_class.return_value.__enter__.return_value = mock_extractor
        
        result = get_county_codes("01")
        assert result == ["001", "003"]
