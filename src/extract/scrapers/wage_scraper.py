'''
Scraper for the MIT Living Wage Calculator.
'''
import requests
from bs4 import BeautifulSoup
from src.extract.scrapers.base import WebScraperBase


class WageScraper(WebScraperBase):
    '''
    Scraper for the MIT Living Wage Calculator.
    '''

    def build_url(self, state_fips: str, county_fips) -> str:
        '''
        Build URL for a specific county.
        '''
        base_url = self.settings.scraping.base_url
        full_fips = state_fips + county_fips
        return f"{base_url}/counties/{full_fips}"

    def parse(self, response: requests.Response, county_fips: str) -> dict:
        '''
        Parse the MIT Living Wage page and extract wage/expense tables.

        Returns:
            dict with 'wages_data' and 'expenses_data' (lists of row dicts)
        '''
        tables = self._scrape_tables(response)

        if len(tables) < 2:
            raise ValueError(
                f"Expected at least 2 tables, found {len(tables)}")

        return {
            "wages_data": self._extract_table(tables[0], county_fips),
            "expenses_data": self._extract_table(tables[1], county_fips),
        }

    def _scrape_tables(self, page: requests.Response) -> list[BeautifulSoup]:
        '''
        Scrape the tables from the page.
        '''
        self.logger.debug('Scraping tables from page')
        soup = BeautifulSoup(page.content, "html.parser")
        return soup.find_all('table', class_='results_table')

    def _extract_table_headers(self, table: BeautifulSoup) -> list[str]:
        '''
        Extract the headers from the table.
        '''
        self.logger.debug('Extracting headers from table')
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

    def _extract_table_rows(self, table: BeautifulSoup) -> list[list[str]]:
        '''
        Extract the rows from the table.
        '''
        self.logger.debug('Extracting rows from table')
        return [[cell.get_text(strip=True) for cell in tr.find_all('td')]
                for tr in table.find('tbody').find_all('tr')]

    def _extract_table(self, table: BeautifulSoup, county_fips: str) -> list[dict]:
        """
        Extract a table (headers + rows) into a list of row dicts.

        Each dict represents a single row:
            {header1: value1, header2: value2, ..., "county_fips": county_fips}

        Logs a warning if any row length does not match headers.
        """
        self.logger.debug('Extracting table into list of row dicts')
        headers = self._extract_table_headers(table)
        rows = self._extract_table_rows(table)
        county_fips = str(county_fips).zfill(3)

        extracted = []
        for row in rows:
            if len(row) != len(headers):
                self.logger.warning(
                    f"Row/header length mismatch detected in county {county_fips}: "
                    f"row has {len(row)} columns, headers have {len(headers)}"
                )
                if len(row) < len(headers):
                    row += [None] * (len(headers) - len(row))
                else:
                    row = row[:len(headers)]
            row_dict = dict(zip(headers, row))
            row_dict["county_fips"] = county_fips
            extracted.append(row_dict)

        return extracted
