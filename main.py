from config import get_settings
from config.logging import setup_logging, get_logger, format_log_with_metadata
from datetime import datetime
import random
import time

from src.extract import scrape_county, scrape_county_with_session, get_county_codes
from src.extract.scrapers import WageScraper
from src.transform import CSVIndexCache, transform_and_save

def main():
    setup_logging()
    logger = get_logger(module=__name__)
    logger.info("Starting the application")
    settings = get_settings()
    logger.info(f"Settings: {settings.model_dump_json(indent=4)}")
    
    # Get target state
    target_state = settings.target_state.state_abbr
    state_fips = settings.state_config.fips_map.get(target_state)
    if state_fips is None:
        logger.error(f"State FIPS not found for target state: {target_state}")
        return
    logger.info(f"Target state: {target_state}, State FIPS: {state_fips}")
    
    # Get county codes
    county_codes = get_county_codes(settings.api.base_url, state_fips)
    
    # Orchestrate ETL pipeline
    current_year = datetime.now().year
    logger.info(
        f'[year={current_year}][state={state_fips}] Starting ETL for {len(county_codes)} counties')
    
    scrape_config = settings.scraping
    results = []
    
    # Create index cache for transform layer
    index_cache = CSVIndexCache()
    
    # Reuse a single scraper session across all counties
    with WageScraper() as scraper:
        for county_fips in county_codes:
            # Extract: Scrape the data
            scrape_result = scrape_county_with_session(scraper, state_fips, county_fips)
            
            # Transform: Save the data if extraction was successful
            if scrape_result.success and scrape_result.wages_data and scrape_result.expenses_data:
                saved = transform_and_save(
                    scrape_result.wages_data,
                    scrape_result.expenses_data,
                    state_fips,
                    county_fips,
                    index_cache,
                    current_year
                )
                if saved:
                    logger.info(format_log_with_metadata(
                        f"Successfully processed county {scrape_result.fips_code}",
                        current_year, state_fips, county_fips))
            
            results.append(scrape_result)
            
            # Rate limiting
            delay = random.uniform(
                scrape_config.min_delay_seconds, scrape_config.max_delay_seconds)
            time.sleep(delay)
    
    # Summary
    successful = sum(1 for r in results if r.success)
    success_rate = successful / len(results) if results else 0
    
    logger.info(
        f'[year={current_year}][state={state_fips}] ETL complete: {successful}/{len(results)} succeeded ({success_rate:.1%})')
    
    if success_rate < scrape_config.min_success_rate:
        logger.warning(
            f'[year={current_year}][state={state_fips}] Success rate {success_rate:.1%} below threshold {scrape_config.min_success_rate:.0%}')
        for r in [r for r in results if not r.success][:5]:
            county_fips_from_result = r.fips_code[-3:]
            logger.warning(format_log_with_metadata(
                f"Failed: {r.error}",
                current_year, state_fips, county_fips_from_result))

if __name__ == "__main__":
    main()
