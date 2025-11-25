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
