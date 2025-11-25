from config import get_settings
from config.logging import setup_logging, get_logger
import requests
from src.extract.web_scraper import scrape_all_counties

def main():
    setup_logging()
    logger = get_logger(module=__name__)
    logger.info("Starting the application")
    settings = get_settings()
    logger.info(f"Settings: {settings.model_dump_json(indent=4)}")
    state_fips = settings.target_state.state_fips
    base_api_url = settings.api.base_url

    # Get county codes from the Census API
    api_url = f'{base_api_url}?get=NAME&for=county:*&in=state:{state_fips}'
    response = requests.get(api_url)
    data = response.json()

    # Extract and zero-fill county codes
    county_codes = [row[2].zfill(3) for row in data[1:]]

    scrape_all_counties(state_fips, county_codes)
if __name__ == "__main__":
    main()
