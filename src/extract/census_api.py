"""
Census API extractor for county and state data.
"""
from typing import Any

from config import get_settings
from src.extract.cache import ResponseCache
from src.extract.http import HttpClient


class CensusExtractor:
    """
    Extracts county and state data from the Census Bureau API.

    Uses HttpClient for HTTP operations with caching and retry logic.
    """

    def __init__(self, use_cache: bool = True):
        settings = get_settings()
        api_config = settings.api

        cache = None
        if use_cache:
            cache_dir = settings.cache_dir / "census"
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache = ResponseCache(cache_dir=cache_dir,
                                  ttl_days=api_config.cache_ttl_days)
            cache.clear_expired()

        self._client = HttpClient(
            base_url=api_config.base_url,
            timeout=api_config.timeout_seconds,
            max_retries=api_config.max_retries,
            ssl_verify=api_config.ssl_verify,
            proxies=api_config.proxies,
            cache=cache,
        )

    def _get(self, params: dict[str, Any]) -> list[list[str]]:
        """Fetch from Census API and parse JSON response."""
        import json
        content = self._client.get(endpoint="", params=params)
        return json.loads(content)

    def _parse_counties(self, data: list[list[str]]) -> list[dict[str, str]]:
        """Parse Census county response into list of dicts."""
        counties = []
        for row in data[1:]:  # Skip header row
            county_name = row[0].split(",")[0].strip()
            state_fips = row[1].zfill(2)
            county_fips = row[2].zfill(3)
            counties.append({
                "county_name": county_name,
                "state_fips": state_fips,
                "county_fips": county_fips,
                "full_fips": state_fips + county_fips,
            })
        return counties

    def _parse_states(self, data: list[list[str]]) -> list[dict[str, str]]:
        """Parse Census state response into list of dicts."""
        return [
            {"state_name": row[0], "state_fips": row[1].zfill(2)}
            for row in data[1:]  # Skip header row
        ]

    def get_counties(self, state_fips: str) -> list[dict[str, str]]:
        """Get all counties for a given state."""
        params = {
            "get": "NAME",
            "for": "county:*",
            "in": f"state:{state_fips}",
        }
        data = self._get(params)
        return self._parse_counties(data)

    def get_states(self) -> list[dict[str, str]]:
        """Get all US states."""
        params = {"get": "NAME", "for": "state:*"}
        data = self._get(params)
        states = self._parse_states(data)

        # Add state abbreviations from config
        settings = get_settings()
        fips_map = settings.state_config.fips_map
        for state in states:
            state["state_abbr"] = next(
                (abbr for abbr, fips in fips_map.items()
                 if fips == state["state_fips"]),
                None,
            )
        return states

    def get_county_codes(self, state_fips: str) -> list[str]:
        """Get just county FIPS codes for a given state."""
        counties = self.get_counties(state_fips)
        return [c["county_fips"] for c in counties]

    def get_all_counties(self) -> list[dict[str, str]]:
        """Get all counties for all states nationwide."""
        params = {
            "get": "NAME",
            "for": "county:*",
            "in": "state:*",
        }
        data = self._get(params)
        return self._parse_counties(data)

    @property
    def request_count(self) -> int:
        """Total network requests made."""
        return self._client.request_count

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._client.__exit__(exc_type, exc_val, exc_tb)
