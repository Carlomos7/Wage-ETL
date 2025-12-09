import random
import time
from datetime import datetime

import pandas as pd

from config import get_settings
from config.logging import setup_logging, get_logger, format_log_with_metadata
from src.extract import scrape_state_counties, get_county_codes
from src.transform import (
    table_to_dataframe,
    validate_wide_format_input,
    normalize_wages,
    normalize_expenses,
)
from src.transform.csv_utils import get_output_paths, save_dataframe_to_csv


def main():
    setup_logging()
    logger = get_logger(module=__name__)
    logger.info("Starting the application")
    settings = get_settings()

    # Get target state
    target_state = settings.pipeline.target_states[0]
    state_fips = settings.state_config.fips_map.get(target_state)
    if not state_fips:
        logger.error(f"State FIPS not found for: {target_state}")
        return

    # Get county codes
    county_codes = get_county_codes()
    current_year = datetime.now().year

    logger.info(
        f"[year={current_year}][state={state_fips}] "
        f"Starting ETL for {len(county_codes)} counties in {target_state}"
    )

    # Rate limiting config
    min_delay = settings.scraping.min_delay_seconds
    max_delay = settings.scraping.max_delay_seconds

    # Accumulate all normalized data
    all_wages: list[pd.DataFrame] = []
    all_expenses: list[pd.DataFrame] = []

    # Extract and transform
    results = []
    for result in scrape_state_counties(state_fips, county_codes):
        county_fips = result.fips_code[-3:]

        if result.success:
            try:
                # Convert to DataFrames
                wages_df = table_to_dataframe(result.wages_data)
                expenses_df = table_to_dataframe(result.expenses_data)

                # Pre-transform validation
                is_valid, errors = validate_wide_format_input(wages_df, 'wages')
                if not is_valid:
                    logger.warning(format_log_with_metadata(
                        f"Invalid wages data: {errors}",
                        current_year, state_fips, county_fips
                    ))
                    results.append(result)
                    continue

                is_valid, errors = validate_wide_format_input(expenses_df, 'expenses')
                if not is_valid:
                    logger.warning(format_log_with_metadata(
                        f"Invalid expenses data: {errors}",
                        current_year, state_fips, county_fips
                    ))
                    results.append(result)
                    continue

                # Transform
                wages_normalized = normalize_wages(wages_df, county_fips)
                expenses_normalized = normalize_expenses(expenses_df, county_fips)

                # Accumulate
                all_wages.append(wages_normalized)
                all_expenses.append(expenses_normalized)

                logger.info(format_log_with_metadata(
                    f"Processed {result.fips_code}",
                    current_year, state_fips, county_fips
                ))

            except ValueError as e:
                logger.error(format_log_with_metadata(
                    f"Transform failed: {e}",
                    current_year, state_fips, county_fips
                ))
        else:
            logger.warning(format_log_with_metadata(
                f"Failed: {result.error}",
                current_year, state_fips, county_fips
            ))

        results.append(result)
        time.sleep(random.uniform(min_delay, max_delay))

    # Save accumulated data (one file per table)
    wages_path, expenses_path = get_output_paths(state_fips, current_year)

    if all_wages:
        wages_combined = pd.concat(all_wages, ignore_index=True)
        save_dataframe_to_csv(wages_combined, wages_path)
        logger.info(f"Saved {len(wages_combined)} wage records to {wages_path}")

    if all_expenses:
        expenses_combined = pd.concat(all_expenses, ignore_index=True)
        save_dataframe_to_csv(expenses_combined, expenses_path)
        logger.info(f"Saved {len(expenses_combined)} expense records to {expenses_path}")

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