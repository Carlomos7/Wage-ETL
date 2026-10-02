"""
Transform models.
"""
from typing import Literal
from pydantic import BaseModel, Field, ValidationInfo, field_validator
from datetime import date, datetime


class BaseRecord(BaseModel):
    county_fips: str
    page_updated_at: date
    adults: int
    working_adults: int
    children: int

    @field_validator("county_fips")
    @classmethod
    def validate_county_fips(cls, v: str) -> str:
        """
        Validate county FIPS code as a zero-padded 5-character string (state + county).
        """
        v = str(v).zfill(5)
        if len(v) != 5 or not v.isdigit():
            raise ValueError(
                "county_fips must be a 5-digit string (state + county FIPS)")
        return v

    @field_validator("adults")
    @classmethod
    def validate_adults(cls, v: int) -> int:
        if v not in (1, 2):
            raise ValueError("adults must be 1 or 2")
        return v

    @field_validator("working_adults")
    @classmethod
    def validate_working_adults(cls, v: int, info: ValidationInfo) -> int:
        adults = info.data.get("adults")
        if v < 1:
            raise ValueError("working_adults must be at least 1")
        if adults is not None and v > adults:
            raise ValueError("working_adults cannot exceed adults")
        return v

    @field_validator("children")
    @classmethod
    def validate_children(cls, v: int) -> int:
        if v < 0 or v > 3:
            raise ValueError("children must be between 0 and 3")
        return v


class WageRecord(BaseRecord):
    wage_type: Literal["living", "poverty", "minimum"]
    hourly_wage: float = Field(ge=0)

    @field_validator("hourly_wage")
    @classmethod
    def validate_hourly_wage(cls, v: float) -> float:
        if v < 0:
            raise ValueError("hourly_wage must be non-negative")
        return v


class ExpenseRecord(BaseRecord):
    expense_category: Literal[
        "food",
        "childcare",
        "housing",
        "transportation",
        "healthcare",
        "other",
        "civic",
        "internet_mobile",
        "required_after_tax",
        "annual_taxes",
        "required_before_tax",
    ]
    annual_amount: float = Field(ge=0)

    @field_validator("annual_amount")
    @classmethod
    def validate_annual_amount(cls, v: float) -> float:
        if v < 0:
            raise ValueError("annual_amount must be non-negative")
        return v
