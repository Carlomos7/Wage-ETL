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


def upsert_to_csv(df: pd.DataFrame, filename: Path, county_fips: str) -> None:
    '''
    Upsert the dataframe to the csv file
    '''
    logger.debug(f'Upserting dataframe to {filename}')
    county_fips = str(county_fips).zfill(3)

    if filename.exists():
        df_master = pd.read_csv(filename, dtype={'county_fips': str})
        df_master['county_fips'] = df_master['county_fips'].str.zfill(3)
        # Remove old rows for the county
        if 'county_fips' in df_master.columns:
            df_master = df_master[df_master['county_fips'] != county_fips]
    else:
        df_master = pd.DataFrame()

    # Append new data
    df_master = pd.concat([df_master, df], ignore_index=True)
    df_master.to_csv(filename, index=False)


def scrape_county(state_fips: str, county_fips: str) -> ScrapeResult:
    '''
    Scrape a specific county from a specified state.
    '''
    full_fips = state_fips + county_fips
    logger.debug(f'Scraping county {full_fips}')

    current_year = datetime.now().year
    output_path = settings.raw_dir / str(current_year)
    output_path.mkdir(parents=True, exist_ok=True)

    wages_filename = output_path / Path(f'wage_rates_{state_fips}.csv')
    expenses_filename = output_path / \
        Path(f'expense_breakdown_{state_fips}.csv')

    # Check if county data already exists in both files
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

        if wages_has_county and expenses_has_county:
            logger.debug(f"County {full_fips} already exists. Skipping.")
            return ScrapeResult(fips_code=full_fips, success=True)

    # Fetch and scrape
    try:
        scraper = WageScraper()
        url = scraper.build_url(state_fips=state_fips, county_fips=county_fips)
        page = scraper.get_page(url)
        data = scraper.parse(page, county_fips=county_fips)

        upsert_to_csv(data['wages_df'], wages_filename, county_fips)
        upsert_to_csv(data['expenses_df'], expenses_filename, county_fips)

        logger.info(f"Successfully scraped county {full_fips}")
        return ScrapeResult(fips_code=full_fips, success=True)

    except RequestException as e:
        logger.error(f"Failed to fetch county {full_fips}: {e}")
        return ScrapeResult(fips_code=full_fips, success=False, error=str(e))

    except Exception as e:
        logger.error(f"Unexpected error scraping county {full_fips}: {e}")
        return ScrapeResult(fips_code=full_fips, success=False, error=str(e))


def scrape_all_counties(state_fips: str, county_codes: list[str]) -> list[ScrapeResult]:
    '''
    Scrape all counties for a specified state.
    '''
    logger.info(
        f'Scraping {len(county_codes)} counties for state {state_fips}')
    scrape_config = settings.scraping
    results: list[ScrapeResult] = []

    for county_fips in county_codes:
        logger.debug(f"Processing county FIPS: {county_fips}")
        result = scrape_county(state_fips, county_fips)
        results.append(result)

        delay = random.uniform(
            scrape_config.min_delay_seconds, scrape_config.max_delay_seconds)
        time.sleep(delay)

    # Summary
    successful = sum(1 for r in results if r.success)
    success_rate = successful / len(results) if results else 0

    logger.info(
        f"Scraping complete: {successful}/{len(results)} succeeded ({success_rate:.1%})")

    if success_rate < scrape_config.min_success_rate:
        logger.warning(
            f"Success rate {success_rate:.1%} below threshold {scrape_config.min_success_rate:.0%}"
        )
        for r in [r for r in results if not r.success][:5]:
            logger.warning(f"  Failed: {r.fips_code} - {r.error}")

    return results
