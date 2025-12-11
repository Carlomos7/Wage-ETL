# Load Layer

Loads transformed data into PostgreSQL staging tables with run tracking and reject handling.

```mint
src/load/
├── __init__.py
├── db.py              # Connection management
├── bulk_ops.py        # COPY operations, column definitions
├── run_tracker.py     # ETL run tracking
└── staging.py         # Staging table operations
```

```mermaid
flowchart TB
    input["<b>Long Format DataFrame</b>
    from transform layer"]

    input --> copy
    
    subgraph run["<b>Run Tracking</b> · start_run() → end_run()"]
        copy["<b>COPY to Temp</b>
        ───────────
        DataFrame → CSV buffer
        buffer → temp table"]

        copy --> upsert

        upsert["<b>Upsert to Staging</b>
        ───────────
        INSERT ... ON CONFLICT
        update if exists"]
    end

    input -.-> rejects

    rejects["<b>Reject Handling</b>
    ───────────
    validation failures
    stored with reason"]

    upsert --> staging
    rejects -.-> reject_tables

    staging["<b>stg_wages</b>
    <b>stg_expenses</b>"]

    reject_tables["<b>stg_wages_rejects</b>
    <b>stg_expenses_rejects</b>"]

    classDef dataStyle fill:#3b82f6,stroke:#1d4ed8,color:#fff
    classDef loadStyle fill:#22c55e,stroke:#15803d,color:#fff
    classDef rejectStyle fill:#a855f7,stroke:#7c3aed,color:#fff
    classDef outputStyle fill:#f59e0b,stroke:#d97706,color:#fff

    class input dataStyle
    class copy,upsert loadStyle
    class rejects rejectStyle
    class staging,reject_tables outputStyle
```

## Usage

```python
from src.load import (
    start_run, end_run,
    bulk_upsert_wages, bulk_upsert_expenses,
    load_rejects
)

# Start tracking
run_id = start_run(state_fips="34")

# Load data
wages_count = bulk_upsert_wages(wages_df, run_id)
expenses_count = bulk_upsert_expenses(expenses_df, run_id)

# Handle rejects
load_rejects(reject_records, run_id, "stg_wages_rejects")

# Finish with stats
end_run(
    run_id,
    status="SUCCESS",
    wages_loaded=wages_count,
    expenses_loaded=expenses_count
)
```

## Data Flow

**Input** (long format from transform layer):

```python
{
    "county_fips": "34001",
    "page_updated_at": date(2024, 1, 15),
    "adults": 1,
    "working_adults": 1,
    "children": 0,
    "wage_type": "living",
    "hourly_wage": 18.71
}
```

**Output** (staging tables):

- `stg_wages` - wage records with `run_id` and `load_timestamp`
- `stg_expenses` - expense records with `run_id` and `load_timestamp`
- `stg_wages_rejects` - failed records with `raw_data` and `rejection_reason`
- `stg_expenses_rejects` - failed records with `raw_data` and `rejection_reason`
- `etl_runs` - run metadata (status, counts, timestamps)

## Design Decisions

- **COPY to temp, then upsert** - Postgres `COPY` is much faster than row-by-row inserts using a `for` loop. Load into a temp table first, then `INSERT ... ON CONFLICT` to handle duplicates.

- **Staging tables with rejects** - Bad records go to reject tables instead of failing the whole load. Each reject stores the raw data and why it failed, so you can fix and reload.

- **Run tracking** - Every load gets a `run_id`. Makes it easy to see what loaded when, roll back a bad run, or debug issues.

- **Context managers for connections** - `get_connection()` handles commit/rollback automatically. No forgotten commits or leaked connections.

- **Whitelist for table names** - `load_rejects()` uses a [`frozenset`](https://www.w3schools.com/python/ref_func_frozenset.asp) of allowed table names. It's a set that can't be modified after creation, so the whitelist stays locked. Prevents SQL injection when the table name comes from a variable.
