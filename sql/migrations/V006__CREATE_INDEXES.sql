CREATE INDEX IF NOT EXISTS idx_wages_county ON stg_wages(county_fips);
CREATE INDEX IF NOT EXISTS idx_expenses_county ON stg_expenses(county_fips);
CREATE INDEX IF NOT EXISTS idx_etl_runs_status ON etl_runs(run_status);