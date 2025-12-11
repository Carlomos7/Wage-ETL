# sql/

Database migrations managed by [Flyway](https://flywaydb.org/).

```mint
sql/
└── migrations/
    ├── V001__CREATE_TABLE_etl_runs.sql
    ├── V002__CREATE_TABLE_stg_wages.sql
    ├── V003__CREATE_TABLE_stg_expenses.sql
    ├── V004__CREATE_TABLE_stg_wages_rejects.sql
    ├── V005__CREATE_TABLE_stg_expenses_rejects.sql
    ├── V006__CREATE_INDEXES.sql
    ├── V007__CREATE_TABLE_dim_county.sql
    ├── V008__ALTER_county_fips_and_add_year.sql
    └── V009__UPDATE_county_indexes.sql
```

## Tables

| Table | What it stores |
|-------|----------------|
| `etl_runs` | Each pipeline run - status, row counts, timestamps |
| `stg_wages` | Wage records by county and family config |
| `stg_expenses` | Expense records by county and family config |
| `stg_wages_rejects` | Failed wage records with raw data and why they failed |
| `stg_expenses_rejects` | Failed expense records with raw data and why they failed |
| `dim_county` | County lookup - FIPS codes and names |

## Key Details

- **Unique constraints** - Staging tables use `(county_fips, page_updated_at, adults, working_adults, children, ...)` so upserts work with `ON CONFLICT`
- **Rejects store JSONB** - Raw data saved exactly as received for debugging
- **Run tracking** - Every record has a `run_id` so you can trace what loaded when
- **Indexes on county + date** - Speeds up the most common queries

## Flyway Migrations

Numbered scripts instead of manual DDL. They show the history of changes, run automatically on fresh databases, and keep everything in sync.

New files, follow the naming convention:

- `V{number}__{description}.sql`
- Double underscore between version and description. Flyway runs them in order.

## Running Migrations

```bash
docker-compose up flyway
```

## Querying the Data

After running the pipeline, you can query the staging tables to inspect the loaded data.

### Check Run Status

```sql
SELECT 
    run_id,
    run_status,
    run_start_timestamp,
    run_end_timestamp,
    state_fips,
    counties_processed,
    wages_loaded,
    wages_rejected,
    expenses_loaded,
    expenses_rejected
FROM etl_runs
ORDER BY run_start_timestamp DESC
LIMIT 5;
```

### View Sample Wage Data

```sql
SELECT 
    county_fips, 
    adults, 
    working_adults, 
    children, 
    wage_type, 
    hourly_wage
FROM stg_wages
LIMIT 10;
```

### View Sample Expense Data

```sql
SELECT * FROM stg_expenses LIMIT 10;
```

### Check for Rejected Records

```sql
-- Wage rejects
SELECT COUNT(*) as reject_count, rejection_reason
FROM stg_wages_rejects
GROUP BY rejection_reason;

-- Expense rejects
SELECT COUNT(*) as reject_count, rejection_reason
FROM stg_expenses_rejects
GROUP BY rejection_reason;
```

### Monitoring Pipeline Execution

Query staging data to monitor pipeline execution:

```sql
-- Sample wage data
SELECT * FROM stg_wages LIMIT 10;

-- Sample expense data
SELECT * FROM stg_expenses LIMIT 10;

-- Rejected records
SELECT * FROM stg_wages_rejects;
SELECT * FROM stg_expenses_rejects;
```
