"""
Tests for extraction operations.
"""
from unittest.mock import Mock

from wage_etl.extract.extract_ops import (
    ScrapeResult,
    scrape_county,
    scrape_county_with_extractor,
    scrape_state_counties,
    get_states,
    get_all_counties,
    get_counties_for_state,
    get_county_codes_for_state,
    get_county_codes,
)
from wage_etl.extract.wage_scraper import WageExtractor
from wage_etl.extract.census_api import CensusExtractor


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

    def test_success(self):
        """Test successful county scrape."""
        from datetime import datetime
        mock_extractor = Mock(spec=WageExtractor)
        mock_extractor.get_county_data.return_value = {
            "wages_data": [{"category": "Housing"}],
            "expenses_data": [{"category": "Food"}],
            "page_updated_at": datetime(2024, 1, 15),
        }

        result = scrape_county(mock_extractor, "01", "001")

        assert result.success is True
        assert result.fips_code == "01001"

    def test_failure(self):
        """Test failed county scrape."""
        mock_extractor = Mock(spec=WageExtractor)
        mock_extractor.get_county_data.side_effect = Exception("Network error")

        result = scrape_county(mock_extractor, "01", "001")

        assert result.success is False
        assert result.error == "Network error"


class TestScrapeCountyWithExtractor:
    """Tests for scrape_county_with_extractor."""

    def test_success(self):
        """Test successful scrape with existing extractor."""
        from datetime import datetime
        mock_extractor = Mock(spec=WageExtractor)
        mock_extractor.get_county_data.return_value = {
            "wages_data": [],
            "expenses_data": [],
            "page_updated_at": datetime(2024, 1, 15),
        }

        result = scrape_county_with_extractor(mock_extractor, "01", "001")

        assert result.success is True
        assert result.fips_code == "01001"


class TestScrapeStateCounties:
    """Tests for scrape_state_counties."""

    def test_scrape_multiple_counties(self):
        """Test scraping multiple counties."""
        from datetime import datetime
        mock_extractor = Mock(spec=WageExtractor)
        mock_extractor.get_county_data.return_value = {
            "wages_data": [],
            "expenses_data": [],
            "page_updated_at": datetime(2024, 1, 15),
        }

        county_codes = ["001", "003"]
        results = list(scrape_state_counties(mock_extractor, "01", county_codes))

        assert len(results) == 2
        assert all(r.success for r in results)
        assert results[0].fips_code == "01001"


class TestCensusLookups:
    """Tests for Census lookup functions."""

    def test_get_states(self):
        """Test getting all states."""
        mock_extractor = Mock(spec=CensusExtractor)
        mock_extractor.get_states.return_value = [
            {"state_name": "Alabama", "state_fips": "01", "state_abbr": "AL"},
        ]

        result = get_states(mock_extractor)
        assert len(result) == 1
        assert result[0]["state_name"] == "Alabama"

    def test_get_all_counties(self):
        """Test getting all counties for target states."""
        mock_extractor = Mock(spec=CensusExtractor)
        mock_extractor.get_counties.return_value = [
            {"county_name": "Alabama County", "state_fips": "01",
                "county_fips": "001", "full_fips": "01001"},
            {"county_name": "Texas County", "state_fips": "48",
                "county_fips": "001", "full_fips": "48001"},
        ]

        result = get_all_counties(mock_extractor)

        assert len(result) == 2
        assert result[0]["county_name"] == "Alabama County"
        assert result[1]["county_name"] == "Texas County"

        assert mock_extractor.get_counties.called

    def test_get_counties_for_state(self):
        """Test getting counties for a specific state."""
        mock_extractor = Mock(spec=CensusExtractor)
        mock_extractor.get_counties.return_value = [
            {"county_name": "Alabama County", "state_fips": "01",
                "county_fips": "001", "full_fips": "01001"},
            {"county_name": "Texas County", "state_fips": "48",
                "county_fips": "001", "full_fips": "48001"},
        ]

        result = get_counties_for_state(mock_extractor, "01")
        assert len(result) == 1
        assert result[0]["county_name"] == "Alabama County"
        assert result[0]["state_fips"] == "01"

    def test_get_county_codes_for_state(self):
        """Test getting county FIPS codes for a state."""
        mock_extractor = Mock(spec=CensusExtractor)
        mock_extractor.get_counties.return_value = [
            {"county_name": "Alabama County", "state_fips": "01",
                "county_fips": "001", "full_fips": "01001"},
            {"county_name": "Another County", "state_fips": "01",
                "county_fips": "003", "full_fips": "01003"},
        ]

        result = get_county_codes_for_state(mock_extractor, "01")
        assert result == ["001", "003"]

    def test_get_county_codes(self):
        """Test getting all county FIPS codes for all states."""
        mock_extractor = Mock(spec=CensusExtractor)
        mock_extractor.get_county_codes.return_value = ["001", "003", "005"]

        result = get_county_codes(mock_extractor)
        assert result == ["001", "003", "005"]
