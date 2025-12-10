"""
Tests for transform validation functions.
"""
import pytest
import pandas as pd
from src.transform.validation import (
    validate_wide_format_input,
    validate_wages,
    validate_expenses,
)


class TestValidateWideFormatInput:
    """Tests for validate_wide_format_input function."""

    def test_valid_dataframe(self):
        """Test validation of valid DataFrame."""
        df = pd.DataFrame({
            "Category": ["living wage", "poverty wage"],
            "1 adult": [20.0, 15.0],
            "2 adults": [25.0, 18.0]
        })
        is_valid, errors = validate_wide_format_input(df)
        assert is_valid is True
        assert len(errors) == 0

    def test_empty_dataframe(self):
        """Test that empty DataFrame fails validation."""
        df = pd.DataFrame()
        is_valid, errors = validate_wide_format_input(df)
        assert is_valid is False
        assert "DataFrame is empty" in errors

    def test_missing_category_column(self):
        """Test that missing category column fails validation."""
        df = pd.DataFrame({
            "1 adult": [20.0],
            "2 adults": [25.0]
        })
        is_valid, errors = validate_wide_format_input(df)
        assert is_valid is False
        assert any("category" in error.lower() for error in errors)

    def test_case_insensitive_category_check(self):
        """Test that category column check is case-insensitive."""
        df = pd.DataFrame({
            "category": ["living wage"],
            "1 adult": [20.0]
        })
        is_valid, errors = validate_wide_format_input(df)
        assert is_valid is True

    def test_high_null_ratio(self):
        """Test that high null ratio fails validation."""
        df = pd.DataFrame({
            "Category": ["living wage"] * 10,
            "1 adult": [None] * 10,
            "2 adults": [None] * 10
        })
        is_valid, errors = validate_wide_format_input(df)
        assert is_valid is False
        assert any("null" in error.lower() for error in errors)

    def test_low_null_ratio_passes(self):
        """Test that low null ratio passes validation."""
        df = pd.DataFrame({
            "Category": ["living wage"] * 10,
            "1 adult": [20.0] * 9 + [None],  # 1 null out of 10
            "2 adults": [25.0] * 10
        })
        is_valid, errors = validate_wide_format_input(df)
        assert is_valid is True


class TestValidateWages:
    """Tests for validate_wages function."""

    def test_valid_wages(self):
        """Test validation of valid wages DataFrame."""
        df = pd.DataFrame({
            "county_fips": ["001", "001"],
            "adults": [1, 2],
            "working_adults": [1, 2],
            "children": [0, 1],
            "wage_type": ["living", "poverty"],
            "hourly_wage": [20.0, 15.0]
        })
        is_valid, errors = validate_wages(df, "001")
        assert is_valid is True
        assert len(errors) == 0

    def test_missing_county_fips_column(self):
        """Test that missing county_fips column fails validation."""
        df = pd.DataFrame({
            "adults": [1],
            "working_adults": [1],
            "children": [0],
            "wage_type": ["living"],
            "hourly_wage": [20.0]
        })
        is_valid, errors = validate_wages(df, "001")
        assert is_valid is False
        assert any("county_fips" in str(error).lower() for error in errors)

    def test_inconsistent_county_fips(self):
        """Test that inconsistent county_fips values fail validation."""
        df = pd.DataFrame({
            "county_fips": ["001", "002"],  # Different from input
            "adults": [1, 1],
            "working_adults": [1, 1],
            "children": [0, 0],
            "wage_type": ["living", "living"],
            "hourly_wage": [20.0, 20.0]
        })
        is_valid, errors = validate_wages(df, "001")
        assert is_valid is False
        # Check for inconsistent error in any error dict
        error_str = str(errors).lower()
        assert "inconsistent" in error_str or any(
            error.get("msg", "").lower().find("inconsistent") >= 0 
            for error in errors 
            if isinstance(error, dict)
        )

    def test_invalid_model_data(self):
        """Test that invalid model data fails validation."""
        df = pd.DataFrame({
            "county_fips": ["001", "001"],
            "adults": [1, 3],  # Invalid: adults must be 1 or 2
            "working_adults": [1, 1],
            "children": [0, 0],
            "wage_type": ["living", "living"],
            "hourly_wage": [20.0, 20.0]
        })
        is_valid, errors = validate_wages(df, "001")
        assert is_valid is False
        assert len(errors) > 0

    def test_county_fips_zero_padding(self):
        """Test that county_fips is zero-padded during validation."""
        df = pd.DataFrame({
            "county_fips": ["1", "1"],
            "adults": [1, 1],
            "working_adults": [1, 1],
            "children": [0, 0],
            "wage_type": ["living", "living"],
            "hourly_wage": [20.0, 20.0]
        })
        is_valid, errors = validate_wages(df, "1")
        assert is_valid is True


class TestValidateExpenses:
    """Tests for validate_expenses function."""

    def test_valid_expenses(self):
        """Test validation of valid expenses DataFrame."""
        df = pd.DataFrame({
            "county_fips": ["001", "001"],
            "adults": [1, 2],
            "working_adults": [1, 2],
            "children": [0, 1],
            "expense_category": ["food", "housing"],
            "annual_amount": [5000.0, 12000.0]
        })
        is_valid, errors = validate_expenses(df, "001")
        assert is_valid is True
        assert len(errors) == 0

    def test_missing_county_fips_column(self):
        """Test that missing county_fips column fails validation."""
        df = pd.DataFrame({
            "adults": [1],
            "working_adults": [1],
            "children": [0],
            "expense_category": ["food"],
            "annual_amount": [5000.0]
        })
        is_valid, errors = validate_expenses(df, "001")
        assert is_valid is False
        assert any("county_fips" in str(error).lower() for error in errors)

    def test_inconsistent_county_fips(self):
        """Test that inconsistent county_fips values fail validation."""
        df = pd.DataFrame({
            "county_fips": ["001", "002"],  # Different from input
            "adults": [1, 1],
            "working_adults": [1, 1],
            "children": [0, 0],
            "expense_category": ["food", "food"],
            "annual_amount": [5000.0, 5000.0]
        })
        is_valid, errors = validate_expenses(df, "001")
        assert is_valid is False
        # Check for inconsistent error in any error dict
        error_str = str(errors).lower()
        assert "inconsistent" in error_str or any(
            error.get("msg", "").lower().find("inconsistent") >= 0 
            for error in errors 
            if isinstance(error, dict)
        )

    def test_invalid_model_data(self):
        """Test that invalid model data fails validation."""
        df = pd.DataFrame({
            "county_fips": ["001", "001"],
            "adults": [1, 3],  # Invalid: adults must be 1 or 2
            "working_adults": [1, 1],
            "children": [0, 0],
            "expense_category": ["food", "food"],
            "annual_amount": [5000.0, 5000.0]
        })
        is_valid, errors = validate_expenses(df, "001")
        assert is_valid is False
        assert len(errors) > 0

    def test_county_fips_zero_padding(self):
        """Test that county_fips is zero-padded during validation."""
        df = pd.DataFrame({
            "county_fips": ["1", "1"],
            "adults": [1, 1],
            "working_adults": [1, 1],
            "children": [0, 0],
            "expense_category": ["food", "food"],
            "annual_amount": [5000.0, 5000.0]
        })
        is_valid, errors = validate_expenses(df, "1")
        assert is_valid is True

