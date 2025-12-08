"""
Census API extractor for county and state data.
"""
from typing import Any
import json
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
        self._settings = settings
        self._api_config = settings.api
        self._pipeline = settings.pipeline
        self._state_fips_map = settings.state_config.fips_map

        # Resolve states into a normalized list of FIPS codes
        self.state_fips_list = self._resolve_state_fips_list()

        cache = None
        if use_cache:
            cache_dir = settings.cache_dir / "census"
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache = ResponseCache(cache_dir=cache_dir,
                                  ttl_days=self._api_config.cache_ttl_days)
            cache.clear_expired()

        # Initialize the HTTP client for Census API
        self._client = HttpClient(
            base_url=self._api_config.base_url,
            timeout=self._api_config.timeout_seconds,
            max_retries=self._api_config.max_retries,
            ssl_verify=self._api_config.ssl_verify,
            proxies=self._api_config.proxies,
            cache=cache,
        )

    # Helpers

    def _resolve_state_fips_list(self) -> list[str]:
        """
        Convert target state abbreviations or "*" into FIPS codes.
        """
        target_states = self._pipeline.target_states

        # ALL STATES
        if target_states == ["*"]:
            return list(self._state_fips_map.values())

        # Specific list of abbreviations to FIPS
        resolved = []
        for abbr in target_states:
            abbr = abbr.upper()
            if abbr not in self._state_fips_map:
                raise ValueError(f"Invalid state abbreviation: {abbr}")
            resolved.append(self._state_fips_map[abbr])

        return resolved

    def _build_for_parameter(self, geography: str, codes: list[str] | None = None) -> str:
        """
        Build the 'for' parameter for Census API requests.

        Args:
            geography: Geographic level (e.g., 'county', 'state')
            codes: List of codes or ['*'] for all. If None, uses config county.

        Returns:
            Formatted 'for' parameter string (e.g., 'county:*' or 'county:001,003')
        """
        if codes is None:
            codes = self._api_config.county

        if len(codes) == 1 and codes[0] == "*":
            return f"{geography}:*"
        else:
            return f"{geography}:{','.join(codes)}"

    def _base_params(self, geography: str) -> dict:
        """
        Build the base parameters for a Census API request.
        """
        return {
            "get": ",".join(self._api_config.variables),
            "for": self._build_for_parameter(geography),
        }

    def _county_params_for_state(self, state_fips: str) -> dict:
        """
        Build the parameters for a Census API request for counties in a specific state.
        """
        return {
            "get": ",".join(self._api_config.variables),
            "for": self._build_for_parameter("county"),
            "in": f"state:{state_fips}",
        }

    def _get(self, params: dict[str, Any]) -> list[list[str]]:
        """Fetch from Census API and parse JSON response."""
        endpoint = self._api_config.dataset
        content = self._client.get(endpoint=endpoint, params=params)
        return json.loads(content)

    # Parsing

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

    # Public API

    def get_counties(self) -> list[dict[str, str]]:
        """Get all counties for the target states."""
        params_base = self._base_params("county")

        results = []

        if len(self.state_fips_list) == len(self._state_fips_map):
            params = {**params_base, "in": "state:*"}
            data = self._get(params)
            return sorted(self._parse_counties(data), key=lambda c: c["full_fips"])

        for state_fips in self.state_fips_list:
            params = self._county_params_for_state(state_fips)
            data = self._get(params)
            results.extend(self._parse_counties(data))

        return sorted(results, key=lambda c: c["full_fips"])

    def get_states(self) -> list[dict[str, str]]:
        """Get all US states."""
        variables_str = ",".join(self._api_config.variables)
        params = {"get": variables_str, "for": "state:*"}

        data = self._get(params)
        states = self._parse_states(data)

        # Add abbreviations
        reverse_map = {fips: abbr for abbr,
                       fips in self._state_fips_map.items()}
        for s in states:
            s["state_abbr"] = reverse_map.get(s["state_fips"])
        return states

    def get_county_codes(self) -> list[str]:
        """Get county-only codes for all selected states."""
        return [c["county_fips"] for c in self.get_counties()]

    @property
    def request_count(self) -> int:
        """Total network requests made."""
        return self._client.request_count

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._client.__exit__(exc_type, exc_val, exc_tb)
