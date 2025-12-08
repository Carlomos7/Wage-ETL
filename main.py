import random
import time
from datetime import datetime

from config import get_settings
from config.logging import setup_logging, get_logger, format_log_with_metadata
from src.extract import scrape_state_counties, get_county_codes
from src.transform import CSVIndexCache, transform_and_save


def main():
    setup_logging()
    logger = get_logger(module=__name__)
    logger.info("Starting the application")
    settings = get_settings()
    logger.info(f"Settings: {settings.model_dump_json(indent=4)}")

    # Get target state
    target_state = settings.pipeline.target_states[0]
    state_fips = settings.state_config.fips_map.get(target_state)
    if not state_fips:
        logger.error(f"State FIPS not found for: {target_state}")
        return

    # Get county codes
    county_codes = get_county_codes()
    current_year = datetime.now().year
    logger.info(f"County codes: {county_codes}")

    logger.info(
        f"[year={current_year}][state={state_fips}] "
        f"Starting ETL for {len(county_codes)} counties in {target_state}"
    )

    # Rate limiting config
    min_delay = settings.scraping.min_delay_seconds
    max_delay = settings.scraping.max_delay_seconds

    # Transform layer setup
    index_cache = CSVIndexCache()

    # Extract and transform
    results = []
    for result in scrape_state_counties(state_fips, county_codes):
        county_fips = result.fips_code[-3:]

        if result.success:
            saved = transform_and_save(
                result.wages_data,
                result.expenses_data,
                state_fips,
                county_fips,
                index_cache,
                current_year,
            )
            if saved:
                logger.info(format_log_with_metadata(
                    f"Processed {result.fips_code}",
                    current_year, state_fips, county_fips
                ))
        else:
            logger.warning(format_log_with_metadata(
                f"Failed: {result.error}",
                current_year, state_fips, county_fips
            ))

        results.append(result)
        time.sleep(random.uniform(min_delay, max_delay))

    # Summary
    successful = sum(1 for r in results if r.success)
    total = len(results)
    success_rate = successful / total if total else 0

    logger.info(
        f"[year={current_year}][state={state_fips}] "
        f"ETL complete: {successful}/{total} ({success_rate:.1%})"
    )

    if success_rate < settings.pipeline.min_success_rate:
        logger.warning(
            f"Success rate {success_rate:.1%} below threshold "
            f"{settings.pipeline.min_success_rate:.0%}"
        )


if __name__ == "__main__":
    main()
