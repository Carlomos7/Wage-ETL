ALTER TABLE stg_wages 
    ALTER COLUMN county_fips TYPE CHAR(5),
    ADD COLUMN page_updated_at DATE NOT NULL;

ALTER TABLE stg_expenses 
    ALTER COLUMN county_fips TYPE CHAR(5),
    ADD COLUMN page_updated_at DATE NOT NULL;

ALTER TABLE stg_wages 
    DROP CONSTRAINT unique_wage_record,
    ADD CONSTRAINT unique_wage_record UNIQUE (
        county_fips, page_updated_at, adults, working_adults, children, wage_type
    );

ALTER TABLE stg_expenses 
    DROP CONSTRAINT unique_expense_record,
    ADD CONSTRAINT unique_expense_record UNIQUE (
        county_fips, page_updated_at, adults, working_adults, children, expense_category
    );