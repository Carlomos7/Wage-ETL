DROP INDEX IF EXISTS idx_wages_county;
DROP INDEX IF EXISTS idx_expenses_county;

CREATE INDEX idx_wages_county_date ON stg_wages(county_fips, page_updated_at);
CREATE INDEX idx_expenses_county_date ON stg_expenses(county_fips, page_updated_at);