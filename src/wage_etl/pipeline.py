"""
ETL pipeline wiring. Settings and clients are built once and passed in.
"""
import random
import time
from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from wage_etl.config.logging import format_log_with_metadata, get_logger
from wage_etl.config.models import STATE_FIPS_PATH, HttpClientConfig, StateCodes
from wage_etl.config.settings import Settings, get_settings
from wage_etl.extract.cache import ResponseCache
from wage_etl.extract.census_api import CensusExtractor
from wage_etl.extract.extract_ops import get_county_codes_for_state, scrape_state_counties
from wage_etl.extract.http import HttpClient
from wage_etl.extract.wage_scraper import WageExtractor
from wage_etl.load.db import Database
from wage_etl.load.run_tracker import end_run, start_run
from wage_etl.load.staging import bulk_upsert_expenses, bulk_upsert_wages, load_rejects
from wage_etl.transform import (
    normalize_expenses,
    normalize_wages,
    table_to_dataframe,
    validate_wide_format_input,
)

logger = get_logger(module=__name__)


@dataclass
class RunResult:
    """Outcome of processing one state."""

    state: str
    status: str
    counties_processed: int = 0
    wages_loaded: int = 0
    wages_rejected: int = 0
    expenses_loaded: int = 0
    expenses_rejected: int = 0


@dataclass
class Pipeline:
    """Extractors and database built from one Settings object."""

    settings: Settings
    census: CensusExtractor
    wages: WageExtractor
    db: Database
    fips_map: dict[str, str]

    def run_state(self, target_state: str) -> RunResult:
        """Process a single state through the ETL pipeline."""
        state_fips = self.fips_map.get(target_state)
        if not state_fips:
            logger.error(f"Unknown state: {target_state}")
            return RunResult(state=target_state, status="FAILED")

        county_codes = get_county_codes_for_state(self.census, state_fips)
        current_year = datetime.now().year
        logger.info(
            f"Processing {len(county_codes)} counties in {target_state} (FIPS: {state_fips})"
        )

        run_id = start_run(self.db, state_fips)

        all_wages = []
        all_expenses = []
        wage_rejects = []
        expense_rejects = []
        counties_processed = 0

        try:
            for result in scrape_state_counties(self.wages, state_fips, county_codes):
                county_fips = result.fips_code[-3:]
                counties_processed += 1

                if result.success:
                    try:
                        wages_df = table_to_dataframe(result.wages_data)
                        expenses_df = table_to_dataframe(result.expenses_data)

                        valid, errors = validate_wide_format_input(wages_df)
                        if not valid:
                            wage_rejects.append(
                                {"raw_data": result.wages_data, "rejection_reason": str(errors)}
                            )
                            continue

                        valid, errors = validate_wide_format_input(expenses_df)
                        if not valid:
                            expense_rejects.append(
                                {
                                    "raw_data": result.expenses_data,
                                    "rejection_reason": str(errors),
                                }
                            )
                            continue

                        page_updated_at = (
                            result.page_updated_at.date() if result.page_updated_at else None
                        )
                        if page_updated_at is None:
                            logger.warning(
                                format_log_with_metadata(
                                    "No page_updated_at date available, using current date",
                                    current_year,
                                    state_fips,
                                    county_fips,
                                )
                            )
                            page_updated_at = datetime.now().date()

                        all_wages.append(
                            normalize_wages(wages_df, state_fips, county_fips, page_updated_at)
                        )
                        all_expenses.append(
                            normalize_expenses(
                                expenses_df, state_fips, county_fips, page_updated_at
                            )
                        )

                        logger.info(
                            format_log_with_metadata("OK", current_year, state_fips, county_fips)
                        )

                    except Exception as exc:
                        logger.error(
                            format_log_with_metadata(
                                f"Transform error: {exc}", current_year, state_fips, county_fips
                            )
                        )
                        wage_rejects.append(
                            {"raw_data": result.wages_data, "rejection_reason": str(exc)}
                        )
                else:
                    logger.warning(
                        format_log_with_metadata(
                            f"Scrape failed: {result.error}",
                            current_year,
                            state_fips,
                            county_fips,
                        )
                    )

                time.sleep(
                    random.uniform(
                        self.settings.scraping.min_delay_seconds,
                        self.settings.scraping.max_delay_seconds,
                    )
                )

            wages_loaded = 0
            expenses_loaded = 0

            if all_wages:
                wages_loaded = bulk_upsert_wages(
                    self.db, pd.concat(all_wages, ignore_index=True), run_id
                )

            if all_expenses:
                expenses_loaded = bulk_upsert_expenses(
                    self.db, pd.concat(all_expenses, ignore_index=True), run_id
                )

            wages_rejected = (
                load_rejects(self.db, wage_rejects, run_id, "stg_wages_rejects")
                if wage_rejects
                else 0
            )
            expenses_rejected = (
                load_rejects(self.db, expense_rejects, run_id, "stg_expenses_rejects")
                if expense_rejects
                else 0
            )

            total_loaded = wages_loaded + expenses_loaded
            total_rejected = wages_rejected + expenses_rejected

            if total_loaded == 0:
                status = "FAILED"
            elif total_rejected > 0:
                status = "PARTIAL"
            else:
                status = "SUCCESS"

            end_run(
                self.db,
                run_id,
                status,
                counties_processed,
                wages_loaded,
                wages_rejected,
                expenses_loaded,
                expenses_rejected,
            )

            logger.info(
                f"ETL complete for {target_state}: {total_loaded} loaded, {total_rejected} rejected"
            )
            return RunResult(
                state=target_state,
                status=status,
                counties_processed=counties_processed,
                wages_loaded=wages_loaded,
                wages_rejected=wages_rejected,
                expenses_loaded=expenses_loaded,
                expenses_rejected=expenses_rejected,
            )

        except Exception as exc:
            logger.error(f"Pipeline failed for {target_state}: {exc}")
            end_run(self.db, run_id, "FAILED", counties_processed, error=str(exc))
            raise


def _http_client(config: HttpClientConfig, cache_dir) -> HttpClient:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache = ResponseCache(cache_dir, ttl_days=config.cache_ttl_days)
    cache.clear_expired()
    return HttpClient(
        base_url=str(config.base_url),
        timeout=config.timeout_seconds,
        max_retries=config.max_retries,
        ssl_verify=config.ssl_verify,
        proxies=config.proxies,
        cache=cache,
    )


def build_pipeline(settings: Settings | None = None) -> Pipeline:
    """Build the extractors and database from settings."""
    settings = settings or get_settings()
    state_codes = StateCodes.from_json(STATE_FIPS_PATH)
    return Pipeline(
        settings=settings,
        census=CensusExtractor(
            _http_client(settings.api, settings.paths.cache_dir / "census"),
            settings.api,
            state_codes.fips_map,
            settings.pipeline.target_states,
        ),
        wages=WageExtractor(
            _http_client(settings.scraping, settings.paths.cache_dir / "wage"),
            settings.scraping,
        ),
        db=Database(settings.db),
        fips_map=state_codes.fips_map,
    )
