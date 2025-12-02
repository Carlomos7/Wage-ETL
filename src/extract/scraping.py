'''
Web scraping for the ETL pipeline.
Extracts data from web sources and returns raw structured data.
'''
from config.logging import get_logger, format_log_with_metadata
from requests.exceptions import RequestException
from datetime import datetime
from dataclasses import dataclass
from typing import Optional

from src.extract.scrapers import WageScraper

logger = get_logger(module=__name__)


@dataclass
class ScrapeResult:
    '''
    Result of a single county scrape.
    '''
    fips_code: str
    success: bool
    wages_data: Optional[list[dict]] = None
    expenses_data: Optional[list[dict]] = None
    error: Optional[str] = None


def scrape_county_with_session(scraper: WageScraper, state_fips: str, county_fips: str) -> ScrapeResult:
    '''
    Internal function to scrape a county using an existing scraper session.

    Args:
        session: requests.Session instance to use
        state_fips: State FIPS code
        county_fips: County FIPS code
        
    Returns:
        ScrapeResult with extracted data (wages_data and expenses_data)
    '''
    current_year = datetime.now().year
    full_fips = state_fips + county_fips

    logger.debug(format_log_with_metadata(
        f'Starting scrape for county {full_fips}',
        current_year, state_fips, county_fips))

    # Fetch and scrape
    try:
        url = scraper.build_url(
            state_fips=state_fips, county_fips=county_fips)
        logger.debug(format_log_with_metadata(
            f'Fetching data from {url}',
            current_year, state_fips, county_fips))
        page = scraper.get_page(url)
        data = scraper.parse(page, county_fips=county_fips)

        logger.info(format_log_with_metadata(
            f"Successfully scraped county {full_fips}",
            current_year, state_fips, county_fips))
        
        return ScrapeResult(
            fips_code=full_fips,
            success=True,
            wages_data=data['wages_data'],
            expenses_data=data['expenses_data']
        )

    except RequestException as e:
        logger.error(format_log_with_metadata(
            f"Failed to fetch county {full_fips}: {e}",
            current_year, state_fips, county_fips))
        return ScrapeResult(fips_code=full_fips, success=False, error=str(e))

    except Exception as e:
        logger.error(format_log_with_metadata(
            f"Scrape failed: {e}",
            current_year, state_fips, county_fips))
        return ScrapeResult(fips_code=full_fips, success=False, error=str(e))


def scrape_county(state_fips: str, county_fips: str) -> ScrapeResult:
    '''
    Scrape a specific county from a specified state.
    Creates its own scraper instance.
    
    Returns:
        ScrapeResult with extracted data (wages_data and expenses_data)
    '''
    with WageScraper() as scraper:
        return scrape_county_with_session(scraper, state_fips, county_fips)
