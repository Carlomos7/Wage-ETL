from config import get_settings
from config.logging import get_logger
import requests

settings = get_settings()
logger = get_logger(module=__name__)


def get_response(api_url: str, timeout: int = 30) -> requests.Response:
    '''
    Get the response from the API URL
    '''
    try:
        response = requests.get(api_url, timeout=timeout)
        response.raise_for_status()
        logger.info(
            f'Successfully fetched data (Status: {response.status_code})')
        return response
    except requests.exceptions.RequestException as e:
        logger.error(f'Census API request failed: {e}')
        raise


def build_county_api_url(base_url: str, state_fips: str) -> str:
    '''
    Build the API URL for the given state
    '''
    return f'{base_url}?get=NAME&for=county:*&in=state:{state_fips}'


def get_counties(base_url: str, state_fips: str) -> list[dict]:
    '''
    Get county information for the given state.

    Returns:
        List of dicts with state_fips, county_fips, full_fips, county_name
    '''
    api_url = build_county_api_url(base_url, state_fips)
    response = get_response(api_url)
    data = response.json()

    counties = []
    for row in data[1:]:  # Skip header row
        county_name = row[0].split(',')[0].strip()
        state_fips_padded = state_fips.zfill(2)
        county_fips = row[2].zfill(3)
        full_fips = state_fips_padded + county_fips  # Combine: "34001"

        counties.append({
            'state_fips': state_fips_padded,
            'county_fips': county_fips,
            'full_fips': full_fips,  # Add this
            'county_name': county_name
        })

    logger.info(f'Retrieved {len(counties)} counties for state {state_fips}')
    return counties


def build_state_api_url(base_url: str) -> str:
    '''
    Build the API URL to get all states
    '''
    return f'{base_url}?get=NAME&for=state:*'


def get_states(base_url: str) -> list[dict]:
    '''
    Get all US states from Census API.

    Returns:
        List of dicts with state_fips, state_name
    '''
    api_url = build_state_api_url(base_url)
    response = get_response(api_url)
    data = response.json()

    # Get state abbreviation mapping from settings
    fips_map = settings.state_config.fips_map

    states = []
    for row in data[1:]:  # Skip header
        state_fips = row[1].zfill(2)
        state_name = row[0]
        state_abbr = fips_map.get(state_fips, None)
        # TODO: Get populations from the API
        states.append({
            'state_fips': state_fips,
            'state_name': state_name,
            'state_abbr': state_abbr
        })

    logger.info(f'Retrieved {len(states)} states')
    return states


def get_county_codes(base_url: str, state_fips: str) -> list[str]:
    """Get just the county FIPS codes for a given state."""
    counties = get_counties(base_url, state_fips)
    return [county['county_fips'] for county in counties]


def get_all_counties(base_url: str) -> list[dict]:
    """Get all counties for all states (nationwide)."""
    api_url = f'{base_url}?get=NAME&for=county:*&in=state:*'
    response = get_response(api_url)
    data = response.json()

    counties = []
    for row in data[1:]:
        county_name = row[0].split(',')[0].strip()
        state_fips = row[1].zfill(2)
        county_fips = row[2].zfill(3)
        full_fips = state_fips + county_fips

        counties.append({
            'state_fips': state_fips,
            'county_fips': county_fips,
            'full_fips': full_fips,
            'county_name': county_name
        })

    logger.info(f'Retrieved {len(counties)} counties nationwide')
    return counties
