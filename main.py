from config import get_settings
from config.logging import setup_logging, get_logger
from src.extract import scrape_all_counties,scrape_county, get_county_codes

def main():
    setup_logging()
    logger = get_logger(module=__name__)
    logger.info("Starting the application")
    settings = get_settings()
    logger.info(f"Settings: {settings.model_dump_json(indent=4)}")
    target_state = settings.target_state.state_abbr
    state_fips = settings.state_config.fips_map.get(target_state)
    if state_fips is None:
        logger.error(f"State FIPS not found for target state: {target_state}")
        return
    logger.info(f"Target state: {target_state}, State FIPS: {state_fips}")
    county_codes = get_county_codes(settings.api.base_url, state_fips)
    scrape_all_counties(state_fips, county_codes)
if __name__ == "__main__":
    main()
