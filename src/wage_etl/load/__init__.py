"""
Load layer - database operations for ETL pipeline.
"""
from wage_etl.load.db import Database
from wage_etl.load.run_tracker import start_run, end_run, get_latest_run
from wage_etl.load.staging import (
    bulk_upsert_wages,
    bulk_upsert_expenses,
    load_rejects,
    get_staging_counts,
    truncate_staging,
)

__all__ = [
    # Connection
    "Database",
    # Run tracking
    "start_run",
    "end_run",
    "get_latest_run",
    # Staging operations
    "bulk_upsert_wages",
    "bulk_upsert_expenses",
    "load_rejects",
    "get_staging_counts",
    "truncate_staging",
]
