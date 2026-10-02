"""
Extraction operations and result types.
"""
from dataclasses import dataclass
from typing import Optional, Generator
from datetime import datetime
from src.extract.census_api import CensusExtractor
from src.extract.wage_scraper import WageExtractor


@dataclass
class ScrapeResult:
    """Result of a single county scrape."""
    fips_code: str
    success: bool
    wages_data: Optional[list[dict]] = None
    expenses_data: Optional[list[dict]] = None
    page_updated_at: Optional[datetime] = None
    error: Optional[str] = None

# --- Wage scraping ---


def scrape_county(state_fips: str, county_fips: str) -> ScrapeResult:
    """Scrape a single county. Creates its own session."""
    with WageExtractor() as extractor:
        return scrape_county_with_extractor(extractor, state_fips, county_fips)


def scrape_county_with_extractor(
    extractor: WageExtractor,
    state_fips: str,
    county_fips: str
) -> ScrapeResult:
    """Scrape a county using an existing extractor session."""
    full_fips = state_fips + county_fips

    try:
        data = extractor.get_county_data(state_fips, county_fips)
        return ScrapeResult(
            fips_code=full_fips,
            success=True,
            wages_data=data["wages_data"],
            expenses_data=data["expenses_data"],
            page_updated_at=data["page_updated_at"],
        )
    except Exception as e:
        return ScrapeResult(
            fips_code=full_fips,
            success=False,
            error=str(e),
        )


def scrape_state_counties(
    state_fips: str,
    county_codes: list[str]
) -> Generator[ScrapeResult, None, None]:
    """Yield ScrapeResults for each county, using a single session."""
    with WageExtractor() as extractor:
        for county_fips in county_codes:
            yield scrape_county_with_extractor(extractor, state_fips, county_fips)

# --- Census lookups ---


def get_states() -> list[dict]:
    """Get all US states."""
    with CensusExtractor() as extractor:
        return extractor.get_states()


def get_all_counties() -> list[dict]:
    """Get all counties for all target states (from YAML)."""
    with CensusExtractor() as extractor:
        return extractor.get_counties()


def get_counties_for_state(state_fips: str) -> list[dict]:
    """Get all counties for a specific state (from FIPS)."""
    state_fips = state_fips.zfill(2)
    with CensusExtractor() as extractor:
        all_counties = extractor.get_counties()
        return [c for c in all_counties if c["state_fips"] == state_fips]


def get_county_codes_for_state(state_fips: str) -> list[str]:
    """Get county FIPS codes for a state."""
    counties = get_counties_for_state(state_fips)
    return [c["county_fips"] for c in counties]


def get_county_codes() -> list[str]:
    """Get county FIPS codes for all states."""
    with CensusExtractor() as extractor:
        return extractor.get_county_codes()
