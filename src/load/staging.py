"""
Staging table operations.
"""
import json
from io import StringIO

import pandas as pd

from config.logging import get_logger
from src.load.db import get_connection, get_cursor
from src.load.bulk_ops import (
    copy_to_temp,
    WAGES_COLUMNS,
    WAGES_COLUMN_DEFS,
    EXPENSES_COLUMNS,
    EXPENSES_COLUMN_DEFS,
)

logger = get_logger(module=__name__)

ALLOWED_REJECT_TABLES = frozenset(
    {"stg_wages_rejects", "stg_expenses_rejects"})


def bulk_upsert_wages(df: pd.DataFrame, run_id: int) -> int:
    """
    Bulk upsert wages: COPY to temp → INSERT ON CONFLICT.

    Returns:
        Number of rows affected
    """
    if df.empty:
        return 0

    df = df.copy()
    df["run_id"] = run_id

    # Validate required columns exist
    missing_cols = set(WAGES_COLUMNS) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns for wages: {missing_cols}")

    # Enforce column order
    df = df[WAGES_COLUMNS]

    with get_connection() as conn:
        copy_to_temp(conn, df, "tmp_wages", WAGES_COLUMNS, WAGES_COLUMN_DEFS)

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO stg_wages (run_id, county_fips, adults, working_adults, children, wage_type, hourly_wage)
                SELECT run_id, county_fips, adults, working_adults, children, wage_type, hourly_wage
                FROM tmp_wages
                ON CONFLICT (county_fips, adults, working_adults, children, wage_type)
                DO UPDATE SET
                    run_id = EXCLUDED.run_id,
                    hourly_wage = EXCLUDED.hourly_wage,
                    load_timestamp = CURRENT_TIMESTAMP
            """)
            count = cur.rowcount

    logger.info(f"Upserted {count} wage records")
    return count


def bulk_upsert_expenses(df: pd.DataFrame, run_id: int) -> int:
    """
    Bulk upsert expenses: COPY to temp → INSERT ON CONFLICT.

    Returns:
        Number of rows affected
    """
    if df.empty:
        return 0

    df = df.copy()
    df["run_id"] = run_id

    # Validate required columns exist
    missing_cols = set(EXPENSES_COLUMNS) - set(df.columns)
    if missing_cols:
        raise ValueError(
            f"Missing required columns for expenses: {missing_cols}")

    # Enforce column order
    df = df[EXPENSES_COLUMNS]

    with get_connection() as conn:
        copy_to_temp(conn, df, "tmp_expenses",
                     EXPENSES_COLUMNS, EXPENSES_COLUMN_DEFS)

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO stg_expenses (run_id, county_fips, adults, working_adults, children, expense_category, annual_amount)
                SELECT run_id, county_fips, adults, working_adults, children, expense_category, annual_amount
                FROM tmp_expenses
                ON CONFLICT (county_fips, adults, working_adults, children, expense_category)
                DO UPDATE SET
                    run_id = EXCLUDED.run_id,
                    annual_amount = EXCLUDED.annual_amount,
                    load_timestamp = CURRENT_TIMESTAMP
            """)
            count = cur.rowcount

    logger.info(f"Upserted {count} expense records")
    return count


def load_rejects(records: list[dict], run_id: int, table: str) -> int:
    """
    Load rejected records to reject table using batch COPY.

    Args:
        records: List of dicts with 'raw_data' and 'rejection_reason' keys
        run_id: ETL run ID
        table: Target table (must be in ALLOWED_REJECT_TABLES)

    Returns:
        Number of records loaded

    Raises:
        ValueError: If table name is not in whitelist
    """
    if not records:
        return 0

    # SQL injection protection - whitelist validation
    if table not in ALLOWED_REJECT_TABLES:
        raise ValueError(
            f"Invalid reject table: {table}. Must be one of {ALLOWED_REJECT_TABLES}")

    # Build DataFrame for batch COPY
    rows = []
    for record in records:
        raw_data = record.get("raw_data", record)
        reason = record.get("rejection_reason", "Unknown")
        rows.append({
            "run_id": run_id,
            "raw_data": json.dumps(raw_data),
            "rejection_reason": str(reason)[:1000],  # Truncate long reasons
        })

    df = pd.DataFrame(rows)

    # Use COPY for batch insert
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.copy_expert(
                f"COPY {table} (run_id, raw_data, rejection_reason) FROM STDIN WITH CSV",
                buffer
            )
            count = cur.rowcount

    logger.debug(f"Loaded {count} rejects to {table}")
    return count


def get_staging_counts() -> dict[str, int]:
    """Get row counts for staging tables."""
    counts = {}
    tables = ["stg_wages", "stg_expenses",
              "stg_wages_rejects", "stg_expenses_rejects"]

    with get_cursor() as cur:
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cur.fetchone()[0]

    return counts


def truncate_staging() -> None:
    """Truncate all staging tables."""
    tables = ["stg_wages", "stg_expenses",
              "stg_wages_rejects", "stg_expenses_rejects"]

    with get_cursor() as cur:
        for table in tables:
            cur.execute(f"TRUNCATE TABLE {table} CASCADE")

    logger.info("Truncated staging tables")
