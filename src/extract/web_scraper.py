'''
Web scraping orchestration for the ETL pipeline.
'''
from config.logging import get_logger
from config.settings import get_settings
from requests.exceptions import RequestException
import pandas as pd
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import Optional
import random
import time

from src.extract.scrapers import WageScraper
from src.extract.csv_storage import upsert_to_csv, CSVIndexCache
from src.extract.logging_utils import format_log_with_metadata

settings = get_settings()
logger = get_logger(module=__name__)


@dataclass
class ScrapeResult:
    '''
    Result of a single county scrape.
    '''
    fips_code: str
    success: bool
    error: Optional[str] = None


def _scrape_county_with_scraper(scraper: WageScraper, state_fips: str, county_fips: str, index_cache: Optional[CSVIndexCache] = None) -> ScrapeResult:
    '''
    Internal function to scrape a county using an existing scraper instance.

    Args:
        scraper: WageScraper instance to use
        state_fips: State FIPS code
        county_fips: County FIPS code
        index_cache: Optional cache to check for existing counties
    '''
    # Dynamically determine year from current date for logging and file paths
    current_year = datetime.now().year
    full_fips = state_fips + county_fips

    logger.debug(format_log_with_metadata(
        f'Starting scrape for county {full_fips}',
        current_year, state_fips, county_fips))

    output_path = settings.raw_dir / str(current_year)
    output_path.mkdir(parents=True, exist_ok=True)

    wages_filename = output_path / Path(f'wage_rates_{state_fips}.csv')
    expenses_filename = output_path / \
        Path(f'expense_breakdown_{state_fips}.csv')

    # Check if county data already exists in both files using cache
    if index_cache:
        wages_has_county = index_cache.has_county(wages_filename, county_fips)
        expenses_has_county = index_cache.has_county(
            expenses_filename, county_fips)
    else:
        # Fallback to reading files if no cache provided
        county_fips_str = str(county_fips).zfill(3)
        if wages_filename.exists() and expenses_filename.exists():
            wages_df_check = pd.read_csv(
                wages_filename, dtype={'county_fips': str})
            expenses_df_check = pd.read_csv(
                expenses_filename, dtype={'county_fips': str})

            wages_has_county = 'county_fips' in wages_df_check.columns and \
                county_fips_str in wages_df_check['county_fips'].values
            expenses_has_county = 'county_fips' in expenses_df_check.columns and \
                county_fips_str in expenses_df_check['county_fips'].values
        else:
            wages_has_county = False
            expenses_has_county = False

    if wages_has_county and expenses_has_county:
        logger.debug(format_log_with_metadata(
            f"County {full_fips} already exists in both files. Skipping.",
            current_year, state_fips, county_fips))
        return ScrapeResult(fips_code=full_fips, success=True)

    # Fetch and scrape
    try:
        url = scraper.build_url(
            state_fips=state_fips, county_fips=county_fips)
        logger.debug(format_log_with_metadata(
            f'Fetching data from {url}',
            current_year, state_fips, county_fips))
        page = scraper.get_page(url)
        data = scraper.parse(page, county_fips=county_fips)

        upsert_to_csv(data['wages_df'], wages_filename,
                      county_fips, index_cache, current_year, state_fips)
        upsert_to_csv(data['expenses_df'], expenses_filename,
                      county_fips, index_cache, current_year, state_fips)

        logger.info(format_log_with_metadata(
            f"Successfully scraped and saved county {full_fips}",
            current_year, state_fips, county_fips))
        return ScrapeResult(fips_code=full_fips, success=True)

    except RequestException as e:
        logger.error(format_log_with_metadata(
            f"Failed to fetch county {full_fips}: {e}",
            current_year, state_fips, county_fips))
        return ScrapeResult(fips_code=full_fips, success=False, error=str(e))

    except Exception as e:
        logger.error(format_log_with_metadata(
            f"Unexpected error scraping county {full_fips}: {e}",
            current_year, state_fips, county_fips))
        return ScrapeResult(fips_code=full_fips, success=False, error=str(e))


def scrape_county(state_fips: str, county_fips: str) -> ScrapeResult:
    '''
    Scrape a specific county from a specified state.
    Creates its own scraper instance.
    '''
    with WageScraper() as scraper:
        return _scrape_county_with_scraper(scraper, state_fips, county_fips)


def scrape_all_counties(state_fips: str, county_codes: list[str]) -> list[ScrapeResult]:
    '''
    Scrape all counties for a specified state.
    Uses a lightweight index cache to avoid repeated CSV reads.
    '''
    current_year = datetime.now().year
    logger.info(
        f'[year={current_year}][state={state_fips}] Starting scrape of {len(county_codes)} counties')
    scrape_config = settings.scraping
    results: list[ScrapeResult] = []

    # Create index cache to avoid repeated CSV reads
    index_cache = CSVIndexCache()

    # Reuse a single scraper session across all counties
    with WageScraper() as scraper:
        for county_fips in county_codes:
            result = _scrape_county_with_scraper(
                scraper, state_fips, county_fips, index_cache)
            results.append(result)

            delay = random.uniform(
                scrape_config.min_delay_seconds, scrape_config.max_delay_seconds)
            time.sleep(delay)

    # Summary
    successful = sum(1 for r in results if r.success)
    success_rate = successful / len(results) if results else 0

    logger.info(
        f'[year={current_year}][state={state_fips}] Scraping complete: {successful}/{len(results)} succeeded ({success_rate:.1%})')

    if success_rate < scrape_config.min_success_rate:
        logger.warning(
            f'[year={current_year}][state={state_fips}] Success rate {success_rate:.1%} below threshold {scrape_config.min_success_rate:.0%}')
        for r in [r for r in results if not r.success][:5]:
            # Extract county from full FIPS
            county_fips_from_result = r.fips_code[-3:]
            logger.warning(format_log_with_metadata(
                f"Failed: {r.error}",
                current_year, state_fips, county_fips_from_result))

    return results
