"""
Wage Calculator extractor.
"""
from bs4 import BeautifulSoup

from config import get_settings
from config.logging import get_logger
from src.extract.cache import ResponseCache
from src.extract.http import HttpClient

logger = get_logger(module=__name__)


class WageExtractor:
    """
    Extracts wage and expense data from the Wage Calculator.

    Uses HttpClient for HTTP operations with caching and retry logic.
    """

    def __init__(self, use_cache: bool = True):
        settings = get_settings()
        scraping_config = settings.scraping

        cache = None
        if use_cache:
            cache_dir = settings.cache_dir / "wage"
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache = ResponseCache(cache_dir=cache_dir, ttl_days=scraping_config.cache_ttl_days)
            cache.clear_expired()

        self._client = HttpClient(
            base_url=scraping_config.base_url,
            timeout=scraping_config.timeout_seconds,
            max_retries=scraping_config.max_retries,
            ssl_verify=scraping_config.ssl_verify,
            proxies=scraping_config.proxies,
            cache=cache,
        )

    def get_county_data(self, state_fips: str, county_fips: str) -> dict:
        """
        Fetch and parse wage/expense data for a county.

        Args:
            state_fips: State FIPS code
            county_fips: County FIPS code

        Returns:
            Dict with 'wages_data' and 'expenses_data' (lists of row dicts)
        """
        state_fips = str(state_fips).zfill(2)
        county_fips = str(county_fips).zfill(3)
        full_fips = state_fips + county_fips
        endpoint = f"counties/{full_fips}"

        content = self._client.get(endpoint=endpoint)
        return self._parse_page(content, county_fips)

    def _parse_page(self, content: bytes, county_fips: str) -> dict:
        """Parse HTML page and extract wage/expense tables."""
        soup = BeautifulSoup(content, "html.parser")
        tables = soup.find_all("table", class_="results_table")

        if len(tables) < 2:
            raise ValueError(
                f"Expected at least 2 tables, found {len(tables)}")

        return {
            "wages_data": self._extract_table(tables[0], county_fips),
            "expenses_data": self._extract_table(tables[1], county_fips),
        }

    def _extract_table(self, table: BeautifulSoup, county_fips: str) -> list[dict]:
        """Extract table into list of row dicts."""
        headers = self._extract_headers(table)
        rows = self._extract_rows(table)
        county_fips = str(county_fips).zfill(3)

        extracted = []
        for row in rows:
            # Handle row/header length mismatch
            if len(row) != len(headers):
                logger.warning(
                    f"Row/header mismatch in county {county_fips}: "
                    f"{len(row)} values, {len(headers)} headers"
                )
                if len(row) < len(headers):
                    row += [None] * (len(headers) - len(row))
                else:
                    row = row[:len(headers)]

            row_dict = dict(zip(headers, row))
            row_dict["county_fips"] = county_fips
            extracted.append(row_dict)

        return extracted

    def _extract_headers(self, table: BeautifulSoup) -> list[str]:
        """Extract column headers from table."""
        theads = table.find_all("thead")

        if len(theads) < 2:
            raise ValueError("Unexpected table header format")

        # First row: adult configurations with colspan
        first_row = theads[0].find("tr")
        adult_configs = []
        for th in first_row.find_all("th"):
            colspan = int(th.get("colspan", 1))
            text = th.get_text(strip=True)
            if text:
                adult_configs.append((text, colspan))

        # Second row: child counts
        second_row = theads[1].find("tr")
        child_counts = [
            cell.get_text(strip=True)
            for cell in second_row.find_all(["td", "th"])
        ]

        # Build combined headers
        headers = []
        if child_counts[0]:
            headers.append(child_counts[0])
        else:
            headers.append("Category")

        col_index = 1
        for adult_text, colspan in adult_configs:
            for _ in range(colspan):
                if col_index < len(child_counts):
                    headers.append(f"{adult_text} - {child_counts[col_index]}")
                    col_index += 1

        return headers

    def _extract_rows(self, table: BeautifulSoup) -> list[list[str]]:
        """Extract data rows from table body."""
        tbody = table.find("tbody")
        if tbody is None:
            return []
        return [
            [cell.get_text(strip=True) for cell in tr.find_all("td")]
            for tr in tbody.find_all("tr")
        ]

    @property
    def request_count(self) -> int:
        """Total network requests made."""
        return self._client.request_count

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._client.__exit__(exc_type, exc_val, exc_tb)
