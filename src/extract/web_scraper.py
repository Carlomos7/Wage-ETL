from config.logging import get_logger
from config.settings import get_settings
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
from datetime import datetime

settings = get_settings()
logger = get_logger(module=__name__)


# TODO: Add timeout to yaml config


def get_page(url, timeout=30) -> requests.Response:
    '''
    Fetch the page from the URL
    '''
    logger.info(f'Fetching data from {url}')
    try:
        response = requests.get(url, timeout=timeout)
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