from config.logging import get_logger
from config.settings import get_settings
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
from datetime import datetime
import random

HEADERS = {
    'User-Agent': 'MIT-WageETL/1.0 (Educational Project)',
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'en-US,en;q=0.9',
}

settings = get_settings()
logger = get_logger(module=__name__)


# TODO: Add timeout to yaml config


def get_page(url, timeout=30) -> requests.Response:
    '''
    Fetch the page from the URL
    '''
    logger.info(f'Fetching data from {url}')
    try:
        response = requests.get(url, timeout=timeout, headers=HEADERS)
        response.raise_for_status()
        logger.info(
            f'Succesfully fetched data (Status: {response.status_code})')
        return response
    except requests.exceptions.RequestException as e:
        logger.error(f'Request failed: {e}')


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


def scrape_county(state_fips: str, county_fips: str) -> None:
    '''
    Scrape a specific county from a specified state
    '''
    logger.debug(f'Scraping county {county_fips}')
    current_year = datetime.now().year
    output_path = settings.raw_dir / str(current_year)
    output_path.mkdir(parents=True, exist_ok=True)

    wages_filename = output_path / Path(f'wage_rates_{state_fips}.csv')
    expenses_filename = output_path / \
        Path(f'expense_breakdown_{state_fips}.csv')
    
    # Check if county data already exists in both files
    county_fips_str = str(county_fips).zfill(3)
    if wages_filename.exists() and expenses_filename.exists():
        wages_df_check = pd.read_csv(wages_filename, dtype={'county_fips': str})
        expenses_df_check = pd.read_csv(expenses_filename, dtype={'county_fips': str})
        
        wages_has_county = 'county_fips' in wages_df_check.columns and \
            county_fips_str in wages_df_check['county_fips'].values
        expenses_has_county = 'county_fips' in expenses_df_check.columns and \
            county_fips_str in expenses_df_check['county_fips'].values
        
        if wages_has_county and expenses_has_county:
            logger.debug(f"County {county_fips} already exists in both CSV files. Skipping scrape.")
            return
    
    # Only fetch and scrape if county doesn't exist
    base_url = settings.scraping.base_url
    url = f"{base_url}/counties/{state_fips + county_fips}"
    page = get_page(url)
    tables = scrape_tables(page)
    if len(tables) < 2:
        logger.error("Expected at least 2 tables!")
        return
    
    wages_df = process_table(tables[0], county_fips)
    expenses_df = process_table(tables[1], county_fips)

    logger.debug(f"Upserting county {county_fips} into {wages_filename}")
    upsert_to_csv(wages_df, wages_filename, county_fips)
    logger.debug(f"Upserting county {county_fips} into {expenses_filename}")
    upsert_to_csv(expenses_df, expenses_filename, county_fips)


def scrape_all_counties(state_fips: str, county_codes: list[str]) -> None:
    '''
    Scrape all counties for a specified state
    '''
    logger.debug(f'Scraping all counties')
    for county_fips in county_codes:
        logger.debug(f"Processing county FIPS: {county_fips}")
        scrape_county(state_fips, county_fips)
