"""
Tests for transform normalizers.
"""
import pytest
from src.transform.normalizers import (
    normalize_header_for_lookup,
    get_family_config_metadata,
    normalize_category_key,
    lookup_category_value,
)


class TestNormalizeHeaderForLookup:
    """Tests for normalize_header_for_lookup function."""

    def test_basic_normalization(self):
        """Test basic header normalization."""
        assert normalize_header_for_lookup("1 adult") == "1 adult"
        assert normalize_header_for_lookup("2 adults") == "2 adults"

    def test_case_insensitive(self):
        """Test that normalization is case-insensitive."""
        assert normalize_header_for_lookup("1 ADULT") == "1 adult"
        assert normalize_header_for_lookup("2 ADULTS") == "2 adults"

    def test_remove_separator(self):
        """Test removal of separator between adult config and child count."""
        assert normalize_header_for_lookup("1 adult - 1 child") == "1 adult 1 child"

    def test_normalize_parentheses_spacing(self):
        """Test normalization of spacing around parentheses."""
        assert normalize_header_for_lookup("2 adults(1 working)") == "2 adults (1 working)"
        assert normalize_header_for_lookup("2 adults (1 working)") == "2 adults (1 working)"

    def test_remove_both_working_variant(self):
        """Test removal of '(both working)' variant."""
        assert normalize_header_for_lookup("2 adults (both working)") == "2 adults"

    def test_remove_zero_children(self):
        """Test removal of explicit zero-child cases."""
        assert normalize_header_for_lookup("1 adult 0 children") == "1 adult"
        assert normalize_header_for_lookup("2 adults 0 child") == "2 adults"

    def test_collapse_multiple_spaces(self):
        """Test that multiple spaces are collapsed."""
        assert normalize_header_for_lookup("1   adult   1   child") == "1 adult 1 child"

    def test_complex_example(self):
        """Test complex real-world example."""
        result = normalize_header_for_lookup("2 ADULTS (BOTH WORKING) - 2 CHILDREN")
        assert result == "2 adults 2 children"


class TestGetFamilyConfigMetadata:
    """Tests for get_family_config_metadata function."""

    def test_valid_family_configs(self):
        """Test lookup of valid family configurations."""
        assert get_family_config_metadata("1 adult") == {"adults": 1, "working_adults": 1, "children": 0}
        assert get_family_config_metadata("1 adult 1 child") == {"adults": 1, "working_adults": 1, "children": 1}
        assert get_family_config_metadata("2 adults (1 working)") == {"adults": 2, "working_adults": 1, "children": 0}
        assert get_family_config_metadata("2 adults") == {"adults": 2, "working_adults": 2, "children": 0}
        assert get_family_config_metadata("2 adults 3 children") == {"adults": 2, "working_adults": 2, "children": 3}

    def test_invalid_family_config(self):
        """Test that invalid family config returns None."""
        assert get_family_config_metadata("invalid config") is None
        assert get_family_config_metadata("3 adults") is None

    def test_normalized_header_lookup(self):
        """Test that normalization happens before lookup."""
        # These should normalize to valid keys
        assert get_family_config_metadata("1 ADULT") == {"adults": 1, "working_adults": 1, "children": 0}
        assert get_family_config_metadata("2 adults(1 working)") == {"adults": 2, "working_adults": 1, "children": 0}


class TestNormalizeCategoryKey:
    """Tests for normalize_category_key function."""

    def test_basic_normalization(self):
        """Test basic category key normalization."""
        assert normalize_category_key("Food") == "food"
        assert normalize_category_key("CHILD CARE") == "child care"

    def test_strip_whitespace(self):
        """Test that whitespace is stripped."""
        assert normalize_category_key("  food  ") == "food"
        assert normalize_category_key("\tchildcare\n") == "childcare"

    def test_clean_punctuation(self):
        """Test that punctuation is cleaned to spaces."""
        assert normalize_category_key("internet & mobile") == "internet mobile"
        assert normalize_category_key("health-care") == "health care"

    def test_multiple_spaces_collapsed(self):
        """Test that multiple spaces are collapsed."""
        assert normalize_category_key("child   care") == "child care"

    def test_none_input(self):
        """Test that None input is handled."""
        assert normalize_category_key(None) == "none"

    def test_numeric_input(self):
        """Test that numeric input is converted to string."""
        assert normalize_category_key(123) == "123"


class TestLookupCategoryValue:
    """Tests for lookup_category_value function."""

    def test_wage_categories(self):
        """Test lookup of wage categories."""
        assert lookup_category_value("living wage") == "living"
        assert lookup_category_value("poverty wage") == "poverty"
        assert lookup_category_value("minimum wage") == "minimum"

    def test_expense_categories(self):
        """Test lookup of expense categories."""
        assert lookup_category_value("food") == "food"
        assert lookup_category_value("child care") == "childcare"
        assert lookup_category_value("childcare") == "childcare"
        assert lookup_category_value("housing") == "housing"
        assert lookup_category_value("medical") == "healthcare"
        assert lookup_category_value("medical care") == "healthcare"
        assert lookup_category_value("health care") == "healthcare"

    def test_derived_income_categories(self):
        """Test lookup of derived income categories."""
        assert lookup_category_value("required annual income after taxes") == "required_after_tax"
        assert lookup_category_value("annual taxes") == "annual_taxes"
        assert lookup_category_value("required annual income before taxes") == "required_before_tax"

    def test_case_insensitive_lookup(self):
        """Test that lookup is case-insensitive."""
        assert lookup_category_value("FOOD") == "food"
        assert lookup_category_value("Living Wage") == "living"

    def test_unknown_category_fallback(self):
        """Test that unknown categories fallback to slugified key."""
        assert lookup_category_value("unknown category") == "unknown_category"
        assert lookup_category_value("some new category") == "some_new_category"

    def test_punctuation_handling(self):
        """Test that punctuation in category names is handled."""
        assert lookup_category_value("internet & mobile") == "internet_mobile"
        assert lookup_category_value("internet_mobile") == "internet_mobile"

