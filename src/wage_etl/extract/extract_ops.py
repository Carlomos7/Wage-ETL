"""
Extraction operations and result types.
"""
from dataclasses import dataclass
from typing import Optional, Generator
from datetime import datetime
from wage_etl.extract.census_api import CensusExtractor
from wage_etl.extract.wage_scraper import WageExtractor


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


def scrape_county(
    extractor: WageExtractor,
    state_fips: str,
    county_fips: str,
) -> ScrapeResult:
    """Scrape a single county with an existing extractor."""
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
    extractor: WageExtractor,
    state_fips: str,
    county_codes: list[str],
) -> Generator[ScrapeResult, None, None]:
    """Yield ScrapeResults for each county, using the given extractor."""
    for county_fips in county_codes:
        yield scrape_county_with_extractor(extractor, state_fips, county_fips)

# --- Census lookups ---


def get_states(extractor: CensusExtractor) -> list[dict]:
    """Get all US states."""
    return extractor.get_states()


def get_all_counties(extractor: CensusExtractor) -> list[dict]:
    """Get all counties for the extractor's target states."""
    return extractor.get_counties()


def get_counties_for_state(extractor: CensusExtractor, state_fips: str) -> list[dict]:
    """Get all counties for a specific state (from FIPS)."""
    state_fips = state_fips.zfill(2)
    return [c for c in extractor.get_counties() if c["state_fips"] == state_fips]


def get_county_codes_for_state(extractor: CensusExtractor, state_fips: str) -> list[str]:
    """Get county FIPS codes for a state."""
    counties = get_counties_for_state(extractor, state_fips)
    return [c["county_fips"] for c in counties]


def get_county_codes(extractor: CensusExtractor) -> list[str]:
    """Get county FIPS codes for the extractor's target states."""
    return extractor.get_county_codes()
