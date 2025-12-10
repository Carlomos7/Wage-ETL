"""
ETL run tracking.
"""
from datetime import datetime
from typing import Optional

from config.logging import get_logger
from src.load.db import get_cursor

logger = get_logger(module=__name__)


def start_run(state_fips: str) -> int:
    """
    Start an ETL run.

    Returns:
        run_id for tracking
    """
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_runs (run_start_timestamp, run_status, state_fips, scrape_date)
            VALUES (%s, 'RUNNING', %s, %s)
            RETURNING run_id
            """,
            (datetime.now(), state_fips, datetime.now().date()),
        )
        run_id = cur.fetchone()[0]

    logger.info(f"Started ETL run {run_id} for state {state_fips}")
    return run_id


def end_run(
    run_id: int,
    status: str,
    counties: int = 0,
    wages_loaded: int = 0,
    wages_rejected: int = 0,
    expenses_loaded: int = 0,
    expenses_rejected: int = 0,
    error: Optional[str] = None,
) -> None:
    """
    Complete an ETL run with final stats.

    Args:
        run_id: Run ID from start_run()
        status: 'SUCCESS', 'FAILED', or 'PARTIAL'
        counties: Number of counties processed
        wages_loaded: Wage records loaded
        wages_rejected: Wage records rejected
        expenses_loaded: Expense records loaded
        expenses_rejected: Expense records rejected
        error: Error message if failed
    """
    with get_cursor() as cur:
        cur.execute(
            """
            UPDATE etl_runs SET
                run_end_timestamp = %s,
                run_status = %s,
                counties_processed = %s,
                wages_loaded = %s,
                wages_rejected = %s,
                expenses_loaded = %s,
                expenses_rejected = %s,
                error_message = %s
            WHERE run_id = %s
            """,
            (datetime.now(), status, counties, wages_loaded, wages_rejected,
             expenses_loaded, expenses_rejected, error, run_id),
        )

    logger.info(f"Ended ETL run {run_id}: {status}")


def get_latest_run(state_fips: Optional[str] = None) -> Optional[dict]:
    """Get the most recent ETL run."""
    with get_cursor(dict_cursor=True) as cur:
        if state_fips:
            cur.execute(
                """
                SELECT * FROM etl_runs 
                WHERE state_fips = %s 
                ORDER BY run_start_timestamp DESC LIMIT 1
                """,
                (state_fips,),
            )
        else:
            cur.execute(
                "SELECT * FROM etl_runs ORDER BY run_start_timestamp DESC LIMIT 1")

        row = cur.fetchone()
        return dict(row) if row else None
