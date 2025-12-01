from config.logging import get_logger
from config.settings import get_settings
import requests
from requests.exceptions import RequestException, HTTPError, Timeout, ConnectionError
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import Optional
import random
import time

HEADERS = {
    'User-Agent': 'MIT-WageETL/1.0 (Educational Project)',
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'en-US,en;q=0.9',
}

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


def fetch_with_retry(url: str) -> requests.Response:
    '''
    Fetch URL with exponential backoff retry logic.
    '''
    scrape_config = settings.scraping
    max_retries = scrape_config.max_retries
    timeout = scrape_config.timeout_seconds

    for attempt in range(max_retries):
        try:
            logger.debug(
                f'Attempt {attempt + 1} of {max_retries} to fetch {url}')
            response = requests.get(url, timeout=timeout, headers=HEADERS)
            response.raise_for_status()
            logger.info(
                f'Successfully fetched data (Status: {response.status_code})')
            return response
        except (Timeout, ConnectionError) as e:
            wait_time = 2 ** attempt
            logger.warning(
                f'Request timed out or connection error. Retrying in {wait_time} seconds...')
            if attempt < max_retries - 1:
                time.sleep(wait_time)
        except HTTPError as e:
            status_code = e.response.status_code
            match status_code:
                case 404:
                    logger.error(f'Resource not found: {url}')
                    raise
                case 429:
                    wait_time = 2 ** (attempt + 2)
                    logger.error(f'Rate limit exceeded: {url}')
                    if attempt < max_retries - 1:
                        time.sleep(wait_time)
                case _ if status_code >= 500:
                    wait_time = 2 ** (attempt + 2)
                    logger.error(f'Server error {status_code}: {url}')
                    if attempt < max_retries - 1:
                        time.sleep(wait_time)
                case _:
                    logger.error(f'HTTP error {status_code}: {url}')
                    raise
        except RequestException as e:
            logger.error(f'Request failed for {url}: {e}')
            raise
    raise RequestException(
        f"Failed to fetch {url} after {max_retries} attempts")


def get_page(url) -> requests.Response:
    '''
    Fetch the page from the URL
    '''
    logger.info(f'Fetching data from {url}')
    response = fetch_with_retry(url)
    return response


def scrape_tables(page: requests.Response) -> list[BeautifulSoup]:
    '''
    Scrape the tables from the page
    '''
    logger.debug(f'Scraping tables from {page}')
    soup = BeautifulSoup(page.content, "html.parser")
    return soup.find_all('table', class_='results_table')


def extract_table_headers(table: BeautifulSoup) -> list[str]:
    '''
    Extract the headers from the table
    '''
    logger.debug(f'Extracting headers from {table}')
    theads = table.find_all('thead')
    first_row = theads[0].find('tr')
    adult_configs = []
    for th in first_row.find_all('th'):
        colspan = int(th.get('colspan', 1))
        text = th.get_text(strip=True)
        if text:
            adult_configs.append((text, colspan))

    second_row = theads[1].find('tr')
    child_counts = []

    for cell in second_row.find_all(['td', 'th']):
        child_text = cell.get_text(strip=True)
        child_counts.append(child_text)

    headers = []
    if child_counts[0]:
        headers.append(child_counts[0])
    else:
        headers.append('Category')

    col_index = 1
    for adult_text, colspan in adult_configs:
        for _ in range(colspan):
            if col_index < len(child_counts):
                headers.append(f'{adult_text} - {child_counts[col_index]}')
                col_index += 1

    return headers


def extract_table_rows(table: BeautifulSoup) -> list[list[str]]:
    '''
    Extract the rows from the table
    '''
    logger.debug(f'Extracting rows from {table}')
    tbody = table.find('tbody')
    rows = []
    for tr in tbody.find_all('tr'):
        row_data = [cell.get_text(strip=True) for cell in tr.find_all('td')]
        rows.append(row_data)
    return rows


def table_to_df(rows: list[list[str]], headers: list[str], county_fips: str) -> pd.DataFrame:
    '''
    Convert the rows and headers to a pandas dataframe
    '''
    logger.debug(f'Converting rows and headers to dataframe')
    df = pd.DataFrame(rows, columns=headers)
    df['county_fips'] = str(county_fips).zfill(3)
    return df


def process_table(table: BeautifulSoup, county_fips: str) -> pd.DataFrame:
    '''
    Process the table
    '''
    logger.debug(f'Processing table {table}')
    headers = extract_table_headers(table)
    rows = extract_table_rows(table)
    df = table_to_df(rows, headers, county_fips)
    return df


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
        base_url = settings.scraping.base_url
        url = f"{base_url}/counties/{full_fips}"
        page = get_page(url)
        tables = scrape_tables(page)

        if len(tables) < 2:
            error_msg = f"Expected at least 2 tables, found {len(tables)}"
            logger.error(error_msg)
            return ScrapeResult(fips_code=full_fips, success=False, error=error_msg)

        wages_df = process_table(tables[0], county_fips)
        expenses_df = process_table(tables[1], county_fips)

        upsert_to_csv(wages_df, wages_filename, county_fips)
        upsert_to_csv(expenses_df, expenses_filename, county_fips)

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
    logger.info(f'Scraping {len(county_codes)} counties for state {state_fips}')
    scrape_config = settings.scraping
    results: list[ScrapeResult] = []

    for county_fips in county_codes:
        logger.debug(f"Processing county FIPS: {county_fips}")
        result = scrape_county(state_fips, county_fips)
        results.append(result)
        
        delay = random.uniform(scrape_config.min_delay_seconds, scrape_config.max_delay_seconds)
        time.sleep(delay)

    # Summary
    successful = sum(1 for r in results if r.success)
    failed = len(results) - successful
    success_rate = successful / len(results) if results else 0

    logger.info(f"Scraping complete: {successful}/{len(results)} succeeded ({success_rate:.1%})")

    if success_rate < scrape_config.min_success_rate:
        logger.warning(
            f"Success rate {success_rate:.1%} below threshold {scrape_config.min_success_rate:.0%}"
        )
        for r in [r for r in results if not r.success][:5]:
            logger.warning(f"  Failed: {r.fips_code} - {r.error}")

    return results
