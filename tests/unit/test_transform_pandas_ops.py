"""
Tests for transform pandas operations.
"""
import pytest
import pandas as pd
from pydantic import ValidationError
from src.transform.pandas_ops import (
    table_to_dataframe,
    clean_currency_columns,
    add_family_config_columns,
    normalize_category_column,
    dataframe_to_models,
    normalize_wages,
    normalize_expenses,
)
from src.transform.models import WageRecord, ExpenseRecord


class TestTableToDataframe:
    """Tests for table_to_dataframe function."""

    def test_empty_list(self):
        """Test that empty list returns empty DataFrame."""
        result = table_to_dataframe([])
        assert result.empty
        assert isinstance(result, pd.DataFrame)

    def test_basic_conversion(self):
        """Test basic conversion of list of dicts to DataFrame."""
        data = [
            {"county_fips": "001", "name": "County A"},
            {"county_fips": "002", "name": "County B"},
        ]
        result = table_to_dataframe(data)
        assert len(result) == 2
        assert "county_fips" in result.columns
        assert result["county_fips"].iloc[0] == "001"

    def test_county_fips_zero_padding(self):
        """Test that county_fips is zero-padded."""
        data = [
            {"county_fips": "1", "name": "County A"},
            {"county_fips": "12", "name": "County B"},
        ]
        result = table_to_dataframe(data)
        assert result["county_fips"].iloc[0] == "001"
        assert result["county_fips"].iloc[1] == "012"

    def test_missing_county_fips_column(self):
        """Test handling when county_fips column is missing."""
        data = [{"name": "County A"}]
        result = table_to_dataframe(data)
        assert "county_fips" not in result.columns


class TestCleanCurrencyColumns:
    """Tests for clean_currency_columns function."""

    def test_remove_dollar_sign(self):
        """Test removal of dollar signs."""
        df = pd.DataFrame({"amount": ["$100", "$200", "$300"]})
        result = clean_currency_columns(df, ["amount"])
        assert result["amount"].iloc[0] == 100.0
        assert result["amount"].iloc[1] == 200.0

    def test_remove_commas(self):
        """Test removal of commas."""
        df = pd.DataFrame({"amount": ["1,000", "2,500", "10,000"]})
        result = clean_currency_columns(df, ["amount"])
        assert result["amount"].iloc[0] == 1000.0
        assert result["amount"].iloc[1] == 2500.0

    def test_combined_formatting(self):
        """Test removal of both dollar signs and commas."""
        df = pd.DataFrame({"amount": ["$1,000", "$2,500.50", "$10,000.99"]})
        result = clean_currency_columns(df, ["amount"])
        assert result["amount"].iloc[0] == 1000.0
        assert result["amount"].iloc[1] == 2500.50
        assert result["amount"].iloc[2] == 10000.99

    def test_multiple_columns(self):
        """Test cleaning multiple columns."""
        df = pd.DataFrame({
            "wage": ["$20", "$25"],
            "expense": ["$1,000", "$2,000"]
        })
        result = clean_currency_columns(df, ["wage", "expense"])
        assert result["wage"].iloc[0] == 20.0
        assert result["expense"].iloc[0] == 1000.0

    def test_invalid_values_coerced_to_zero(self):
        """Test that invalid values are coerced to zero."""
        df = pd.DataFrame({"amount": ["$100", "invalid", "$200", None]})
        result = clean_currency_columns(df, ["amount"])
        assert result["amount"].iloc[0] == 100.0
        assert result["amount"].iloc[1] == 0.0
        assert result["amount"].iloc[2] == 200.0
        assert result["amount"].iloc[3] == 0.0

    def test_does_not_modify_original(self):
        """Test that original DataFrame is not modified."""
        df = pd.DataFrame({"amount": ["$100", "$200"]})
        original = df.copy()
        clean_currency_columns(df, ["amount"])
        assert df.equals(original)


class TestAddFamilyConfigColumns:
    """Tests for add_family_config_columns function."""

    def test_valid_family_config(self):
        """Test adding family config columns from valid config."""
        df = pd.DataFrame({"family": ["1 adult", "2 adults (1 working) 1 child"]})
        result = add_family_config_columns(df, "family")
        assert result["adults"].iloc[0] == 1
        assert result["working_adults"].iloc[0] == 1
        assert result["children"].iloc[0] == 0
        assert result["adults"].iloc[1] == 2
        assert result["working_adults"].iloc[1] == 1
        assert result["children"].iloc[1] == 1

    def test_invalid_family_config(self):
        """Test handling of invalid family config."""
        df = pd.DataFrame({"family": ["invalid config", "1 adult"]})
        result = add_family_config_columns(df, "family")
        assert pd.isna(result["adults"].iloc[0])
        assert result["adults"].iloc[1] == 1

    def test_case_insensitive(self):
        """Test that family config lookup is case-insensitive."""
        df = pd.DataFrame({"family": ["1 ADULT", "2 ADULTS"]})
        result = add_family_config_columns(df, "family")
        assert result["adults"].iloc[0] == 1
        assert result["adults"].iloc[1] == 2


class TestNormalizeCategoryColumn:
    """Tests for normalize_category_column function."""

    def test_wage_category_normalization(self):
        """Test normalization of wage categories."""
        df = pd.DataFrame({"category": ["living wage", "poverty wage", "minimum wage"]})
        result = normalize_category_column(df, "category", "wage_type")
        assert result["wage_type"].iloc[0] == "living"
        assert result["wage_type"].iloc[1] == "poverty"
        assert result["wage_type"].iloc[2] == "minimum"

    def test_expense_category_normalization(self):
        """Test normalization of expense categories."""
        df = pd.DataFrame({"category": ["food", "child care", "medical care"]})
        result = normalize_category_column(df, "category", "expense_category")
        assert result["expense_category"].iloc[0] == "food"
        assert result["expense_category"].iloc[1] == "childcare"
        assert result["expense_category"].iloc[2] == "healthcare"

    def test_unknown_category_fallback(self):
        """Test that unknown categories fallback to slugified version."""
        df = pd.DataFrame({"category": ["unknown category"]})
        result = normalize_category_column(df, "category", "target")
        assert result["target"].iloc[0] == "unknown_category"


class TestDataframeToModels:
    """Tests for dataframe_to_models function."""

    def test_valid_wage_records(self):
        """Test conversion of valid wage records."""
        df = pd.DataFrame({
            "county_fips": ["001", "002"],
            "adults": [1, 2],
            "working_adults": [1, 2],
            "children": [0, 1],
            "wage_type": ["living", "poverty"],
            "hourly_wage": [20.0, 15.0]
        })
        models, errors = dataframe_to_models(df, WageRecord)
        assert len(models) == 2
        assert len(errors) == 0
        assert models[0].county_fips == "001"
        assert models[0].wage_type == "living"

    def test_valid_expense_records(self):
        """Test conversion of valid expense records."""
        df = pd.DataFrame({
            "county_fips": ["001", "002"],
            "adults": [1, 2],
            "working_adults": [1, 2],
            "children": [0, 1],
            "expense_category": ["food", "housing"],
            "annual_amount": [5000.0, 12000.0]
        })
        models, errors = dataframe_to_models(df, ExpenseRecord)
        assert len(models) == 2
        assert len(errors) == 0

    def test_validation_errors_captured(self):
        """Test that validation errors are captured."""
        df = pd.DataFrame({
            "county_fips": ["001", "invalid"],
            "adults": [1, 3],  # Invalid: adults must be 1 or 2
            "working_adults": [1, 1],
            "children": [0, 0],
            "wage_type": ["living", "living"],
            "hourly_wage": [20.0, 15.0]
        })
        models, errors = dataframe_to_models(df, WageRecord)
        assert len(models) == 1  # Only first row is valid
        assert len(errors) == 1
        assert errors[0]["row_index"] == 1


class TestNormalizeWages:
    """Tests for normalize_wages function."""

    def test_empty_dataframe(self):
        """Test that empty DataFrame returns empty DataFrame."""
        df = pd.DataFrame()
        result = normalize_wages(df, "001")
        assert result.empty

    def test_basic_normalization(self):
        """Test basic wage normalization."""
        df = pd.DataFrame({
            "Category": ["living wage", "poverty wage"],
            "1 adult": ["$20.00", "$15.00"],
            "2 adults": ["$25.00", "$18.00"]
        })
        result = normalize_wages(df, "001", validate=False)
        assert "county_fips" in result.columns
        assert "wage_type" in result.columns
        assert "hourly_wage" in result.columns
        assert "adults" in result.columns
        assert "working_adults" in result.columns
        assert "children" in result.columns
        assert len(result) == 4  # 2 categories * 2 family configs

    def test_county_fips_zero_padding(self):
        """Test that county_fips is zero-padded."""
        df = pd.DataFrame({
            "Category": ["living wage"],
            "1 adult": ["$20.00"]
        })
        result = normalize_wages(df, "1", validate=False)
        assert result["county_fips"].iloc[0] == "001"

    def test_validation_enabled(self):
        """Test that validation works when enabled."""
        df = pd.DataFrame({
            "Category": ["living wage"],
            "1 adult": ["$20.00"]
        })
        result = normalize_wages(df, "001", validate=True)
        assert len(result) == 1
        assert result["wage_type"].iloc[0] == "living"


class TestNormalizeExpenses:
    """Tests for normalize_expenses function."""

    def test_empty_dataframe(self):
        """Test that empty DataFrame returns empty DataFrame."""
        df = pd.DataFrame()
        result = normalize_expenses(df, "001")
        assert result.empty

    def test_basic_normalization(self):
        """Test basic expense normalization."""
        df = pd.DataFrame({
            "Category": ["food", "housing"],
            "1 adult": ["$5,000", "$12,000"],
            "2 adults": ["$8,000", "$18,000"]
        })
        result = normalize_expenses(df, "001", validate=False)
        assert "county_fips" in result.columns
        assert "expense_category" in result.columns
        assert "annual_amount" in result.columns
        assert "adults" in result.columns
        assert len(result) == 4  # 2 categories * 2 family configs

    def test_county_fips_zero_padding(self):
        """Test that county_fips is zero-padded."""
        df = pd.DataFrame({
            "Category": ["food"],
            "1 adult": ["$5,000"]
        })
        result = normalize_expenses(df, "1", validate=False)
        assert result["county_fips"].iloc[0] == "001"

    def test_validation_enabled(self):
        """Test that validation works when enabled."""
        df = pd.DataFrame({
            "Category": ["food"],
            "1 adult": ["$5,000"]
        })
        result = normalize_expenses(df, "001", validate=True)
        assert len(result) == 1
        assert result["expense_category"].iloc[0] == "food"

