"""
Tests for transform models.
"""
import pytest
from datetime import date
from pydantic import ValidationError
from src.transform.models import BaseRecord, WageRecord, ExpenseRecord


class TestBaseRecord:
    """Tests for BaseRecord model."""

    def test_valid_base_record(self):
        """Test BaseRecord with valid data."""
        record = BaseRecord(
            county_fips="01001",
            page_updated_at=date(2024, 1, 15),
            adults=2,
            working_adults=2,
            children=1
        )
        assert record.county_fips == "01001"
        assert record.adults == 2
        assert record.working_adults == 2
        assert record.children == 1

    def test_county_fips_zero_padding(self):
        """Test that county_fips is zero-padded to 5 digits."""
        record = BaseRecord(
            county_fips="101",
            page_updated_at=date(2024, 1, 15),
            adults=1,
            working_adults=1,
            children=0
        )
        assert record.county_fips == "00101"

    def test_county_fips_invalid_non_digit(self):
        """Test that non-digit county_fips raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            BaseRecord(
                county_fips="abc",
                page_updated_at=date(2024, 1, 15),
                adults=1,
                working_adults=1,
                children=0
            )
        assert "county_fips must be a 5-digit string" in str(exc_info.value)

    def test_county_fips_invalid_length(self):
        """Test that county_fips with wrong length raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            BaseRecord(
                county_fips="123456",
                page_updated_at=date(2024, 1, 15),
                adults=1,
                working_adults=1,
                children=0
            )
        assert "county_fips must be a 5-digit string" in str(exc_info.value)

    def test_adults_invalid_value(self):
        """Test that invalid adults value raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            BaseRecord(
                county_fips="01001",
                page_updated_at=date(2024, 1, 15),
                adults=3,
                working_adults=1,
                children=0
            )
        assert "adults must be 1 or 2" in str(exc_info.value)

    def test_working_adults_less_than_one(self):
        """Test that working_adults < 1 raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            BaseRecord(
                county_fips="01001",
                page_updated_at=date(2024, 1, 15),
                adults=1,
                working_adults=0,
                children=0
            )
        assert "working_adults must be at least 1" in str(exc_info.value)

    def test_working_adults_exceeds_adults(self):
        """Test that working_adults > adults raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            BaseRecord(
                county_fips="01001",
                page_updated_at=date(2024, 1, 15),
                adults=1,
                working_adults=2,
                children=0
            )
        assert "working_adults cannot exceed adults" in str(exc_info.value)

    def test_children_invalid_negative(self):
        """Test that negative children raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            BaseRecord(
                county_fips="01001",
                page_updated_at=date(2024, 1, 15),
                adults=1,
                working_adults=1,
                children=-1
            )
        assert "children must be between 0 and 3" in str(exc_info.value)

    def test_children_invalid_too_many(self):
        """Test that children > 3 raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            BaseRecord(
                county_fips="01001",
                page_updated_at=date(2024, 1, 15),
                adults=1,
                working_adults=1,
                children=4
            )
        assert "children must be between 0 and 3" in str(exc_info.value)


class TestWageRecord:
    """Tests for WageRecord model."""

    def test_valid_wage_record(self):
        """Test WageRecord with valid data."""
        record = WageRecord(
            county_fips="01001",
            page_updated_at=date(2024, 1, 15),
            adults=2,
            working_adults=2,
            children=1,
            wage_type="living",
            hourly_wage=25.50
        )
        assert record.wage_type == "living"
        assert record.hourly_wage == 25.50

    def test_wage_record_invalid_type(self):
        """Test that invalid wage_type raises ValidationError."""
        with pytest.raises(ValidationError):
            WageRecord(
                county_fips="01001",
                page_updated_at=date(2024, 1, 15),
                adults=1,
                working_adults=1,
                children=0,
                wage_type="invalid",
                hourly_wage=20.0
            )

    def test_wage_record_negative_wage(self):
        """Test that negative hourly_wage raises ValidationError."""
        with pytest.raises(ValidationError):
            WageRecord(
                county_fips="01001",
                page_updated_at=date(2024, 1, 15),
                adults=1,
                working_adults=1,
                children=0,
                wage_type="living",
                hourly_wage=-10.0
            )

    def test_wage_record_zero_wage(self):
        """Test that zero hourly_wage is valid."""
        record = WageRecord(
            county_fips="01001",
            page_updated_at=date(2024, 1, 15),
            adults=1,
            working_adults=1,
            children=0,
            wage_type="minimum",
            hourly_wage=0.0
        )
        assert record.hourly_wage == 0.0


class TestExpenseRecord:
    """Tests for ExpenseRecord model."""

    def test_valid_expense_record(self):
        """Test ExpenseRecord with valid data."""
        record = ExpenseRecord(
            county_fips="01001",
            page_updated_at=date(2024, 1, 15),
            adults=2,
            working_adults=2,
            children=1,
            expense_category="food",
            annual_amount=5000.00
        )
        assert record.expense_category == "food"
        assert record.annual_amount == 5000.00

    def test_expense_record_invalid_category(self):
        """Test that invalid expense_category raises ValidationError."""
        with pytest.raises(ValidationError):
            ExpenseRecord(
                county_fips="01001",
                page_updated_at=date(2024, 1, 15),
                adults=1,
                working_adults=1,
                children=0,
                expense_category="invalid",
                annual_amount=1000.0
            )

    def test_expense_record_negative_amount(self):
        """Test that negative annual_amount raises ValidationError."""
        with pytest.raises(ValidationError):
            ExpenseRecord(
                county_fips="01001",
                page_updated_at=date(2024, 1, 15),
                adults=1,
                working_adults=1,
                children=0,
                expense_category="housing",
                annual_amount=-1000.0
            )

    def test_expense_record_zero_amount(self):
        """Test that zero annual_amount is valid."""
        record = ExpenseRecord(
            county_fips="01001",
            page_updated_at=date(2024, 1, 15),
            adults=1,
            working_adults=1,
            children=0,
            expense_category="other",
            annual_amount=0.0
        )
        assert record.annual_amount == 0.0

