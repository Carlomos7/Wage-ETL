"""
ETL Pipeline - extract -> transform -> load.
"""
import random
import time
from datetime import datetime

import pandas as pd

from config import get_settings
from config.logging import setup_logging, get_logger, format_log_with_metadata

from src.extract import get_county_codes, scrape_state_counties

from src.transform import (
    normalize_expenses,
    normalize_wages,
    table_to_dataframe,
    validate_wide_format_input,
)

from src.load import (
    bulk_upsert_expenses,
    bulk_upsert_wages,
    end_run,
    load_rejects,
    start_run,
    test_connection,
)


def main():
    setup_logging()
    logger = get_logger(module=__name__)
    logger.info("Starting ETL pipeline")
    settings = get_settings()

    if not test_connection():
        logger.error("Database connection failed")
        return

    # Setup
    target_state = settings.pipeline.target_states[0]
    state_fips = settings.state_config.fips_map.get(target_state)
    if not state_fips:
        logger.error(f"Unknown state: {target_state}")
        return

    county_codes = get_county_codes()
    current_year = datetime.now().year
    logger.info(f"Processing {len(county_codes)} counties in {target_state}")

    # Start run
    run_id = start_run(state_fips)

    # Accumulators
    all_wages = []
    all_expenses = []
    wage_rejects = []
    expense_rejects = []
    counties_processed = 0

    try:
        # Extract + Transform
        for result in scrape_state_counties(state_fips, county_codes):
            county_fips = result.fips_code[-3:]
            counties_processed += 1

            if result.success:
                try:
                    wages_df = table_to_dataframe(result.wages_data)
                    expenses_df = table_to_dataframe(result.expenses_data)

                    # Validate
                    valid, errors = validate_wide_format_input(wages_df)
                    if not valid:
                        wage_rejects.append({"raw_data": result.wages_data, "rejection_reason": str(errors)})
                        continue

                    valid, errors = validate_wide_format_input(expenses_df)
                    if not valid:
                        expense_rejects.append({"raw_data": result.expenses_data, "rejection_reason": str(errors)})
                        continue

                    # Transform
                    all_wages.append(normalize_wages(wages_df, county_fips))
                    all_expenses.append(normalize_expenses(expenses_df, county_fips))

                    logger.info(format_log_with_metadata(f"OK", current_year, state_fips, county_fips))

                except Exception as e:
                    logger.error(format_log_with_metadata(f"Transform error: {e}", current_year, state_fips, county_fips))
                    wage_rejects.append({"raw_data": result.wages_data, "rejection_reason": str(e)})
            else:
                logger.warning(format_log_with_metadata(f"Scrape failed: {result.error}", current_year, state_fips, county_fips))

            time.sleep(random.uniform(settings.scraping.min_delay_seconds, settings.scraping.max_delay_seconds))

        # Load
        wages_loaded = 0
        expenses_loaded = 0

        if all_wages:
            wages_loaded = bulk_upsert_wages(pd.concat(all_wages, ignore_index=True), run_id)

        if all_expenses:
            expenses_loaded = bulk_upsert_expenses(pd.concat(all_expenses, ignore_index=True), run_id)

        wages_rejected = load_rejects(wage_rejects, run_id, "stg_wages_rejects") if wage_rejects else 0
        expenses_rejected = load_rejects(expense_rejects, run_id, "stg_expenses_rejects") if expense_rejects else 0

        # Determine status
        total_loaded = wages_loaded + expenses_loaded
        total_rejected = wages_rejected + expenses_rejected

        if total_loaded == 0:
            status = "FAILED"
        elif total_rejected > 0:
            status = "PARTIAL"
        else:
            status = "SUCCESS"

        end_run(run_id, status, counties_processed, wages_loaded, wages_rejected, expenses_loaded, expenses_rejected)

        logger.info(f"ETL complete: {total_loaded} loaded, {total_rejected} rejected")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        end_run(run_id, "FAILED", counties_processed, error=str(e))
        raise


if __name__ == "__main__":
    main()