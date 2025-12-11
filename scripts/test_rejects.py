"""
Test reject table functionality.

Run with: uv run python scripts/test_rejects.py
"""
import random
from datetime import date
from pydantic import ValidationError

from config.logging import setup_logging, get_logger
from src.transform.models import WageRecord, ExpenseRecord
from src.load.db import get_cursor
from src.load.run_tracker import start_run, end_run
from src.load.staging import load_rejects


TODAY = str(date.today())
VALID_WAGE_TYPES = ["living", "poverty", "minimum"]
VALID_EXPENSE_CATEGORIES = ["food", "childcare", "housing", "transportation", "healthcare", "other", "civic", "internet_mobile"]


def generate_invalid_wage() -> dict:
    """Generate a wage record with one random validation error."""
    base = {
        "county_fips": f"{random.randint(10000, 99999)}",
        "page_updated_at": TODAY,
        "adults": random.choice([1, 2]),
        "working_adults": 1,
        "children": random.randint(0, 3),
        "wage_type": random.choice(VALID_WAGE_TYPES),
        "hourly_wage": round(random.uniform(10.0, 50.0), 2),
    }
    
    # Inject one random error
    error_type = random.choice(["adults", "working_adults", "children", "wage_type", "hourly_wage"])
    
    if error_type == "adults":
        base["adults"] = random.choice([0, 3, 5, -1])
    elif error_type == "working_adults":
        base["adults"] = 1
        base["working_adults"] = random.randint(2, 5)
    elif error_type == "children":
        base["children"] = random.choice([4, 5, 10, -1])
    elif error_type == "wage_type":
        base["wage_type"] = random.choice(["invalid", "bad", "wrong", ""])
    elif error_type == "hourly_wage":
        base["hourly_wage"] = random.uniform(-100.0, -0.01)
    
    return base


def generate_invalid_expense() -> dict:
    """Generate an expense record with one random validation error."""
    base = {
        "county_fips": f"{random.randint(10000, 99999)}",
        "page_updated_at": TODAY,
        "adults": random.choice([1, 2]),
        "working_adults": 1,
        "children": random.randint(0, 3),
        "expense_category": random.choice(VALID_EXPENSE_CATEGORIES),
        "annual_amount": round(random.uniform(1000.0, 50000.0), 2),
    }
    
    error_type = random.choice(["adults", "children", "expense_category", "annual_amount"])
    
    if error_type == "adults":
        base["adults"] = random.choice([0, 3, 5, -1])
    elif error_type == "children":
        base["children"] = random.choice([4, 5, 10, -1])
    elif error_type == "expense_category":
        base["expense_category"] = random.choice(["invalid", "bad", "wrong", ""])
    elif error_type == "annual_amount":
        base["annual_amount"] = random.uniform(-10000.0, -0.01)
    
    return base


def collect_rejects(records: list[dict], model_class) -> list[dict]:
    """Validate records, return those that fail as reject dicts."""
    rejects = []
    for record in records:
        try:
            model_class(**record)
        except ValidationError as e:
            rejects.append({"raw_data": record, "rejection_reason": str(e)})
    return rejects


def get_reject_count(table: str, run_id: int) -> int:
    """Get count of rejects for a specific run."""
    with get_cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (run_id,))
        return cur.fetchone()[0]


def main():
    setup_logging()
    logger = get_logger(module=__name__)
    
    num_wages = random.randint(5, 15)
    num_expenses = random.randint(3, 10)
    
    logger.info(f"Generating {num_wages} invalid wages, {num_expenses} invalid expenses")
    
    invalid_wages = [generate_invalid_wage() for _ in range(num_wages)]
    invalid_expenses = [generate_invalid_expense() for _ in range(num_expenses)]
    
    run_id = start_run("00")
    
    wage_rejects = collect_rejects(invalid_wages, WageRecord)
    expense_rejects = collect_rejects(invalid_expenses, ExpenseRecord)
    
    # All should be rejected
    assert len(wage_rejects) == num_wages, f"Expected {num_wages} wage rejects, got {len(wage_rejects)}"
    assert len(expense_rejects) == num_expenses, f"Expected {num_expenses} expense rejects, got {len(expense_rejects)}"
    
    wages_loaded = load_rejects(wage_rejects, run_id, "stg_wages_rejects")
    expenses_loaded = load_rejects(expense_rejects, run_id, "stg_expenses_rejects")
    
    assert get_reject_count("stg_wages_rejects", run_id) == num_wages
    assert get_reject_count("stg_expenses_rejects", run_id) == num_expenses
    
    end_run(run_id, "PARTIAL", 0, 0, wages_loaded, 0, expenses_loaded)
    
    logger.info(f"PASS: Loaded {wages_loaded} wage rejects, {expenses_loaded} expense rejects (run_id={run_id})")


if __name__ == "__main__":
    main()
