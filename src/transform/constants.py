"""
Constants for transform operations.
"""

import re

FAMILY_CONFIG_MAP = {
    "1 Adult": {"adults": 1, "working_adults": 1, "children": 0},
    "1 Adult 1 Child": {"adults": 1, "working_adults": 1, "children": 1},
    "1 Adult 2 Children": {"adults": 1, "working_adults": 1, "children": 2},
    "1 Adult 3 Children": {"adults": 1, "working_adults": 1, "children": 3},
    "2 Adults (1 Working)": {"adults": 2, "working_adults": 1, "children": 0},
    "2 Adults (1 Working) 1 Child": {"adults": 2, "working_adults": 1, "children": 1},
    "2 Adults (1 Working) 2 Children": {"adults": 2, "working_adults": 1, "children": 2},
    "2 Adults (1 Working) 3 Children": {"adults": 2, "working_adults": 1, "children": 3},
    "2 Adults": {"adults": 2, "working_adults": 2, "children": 0},
    "2 Adults 1 Child": {"adults": 2, "working_adults": 2, "children": 1},
    "2 Adults 2 Children": {"adults": 2, "working_adults": 2, "children": 2},
    "2 Adults 3 Children": {"adults": 2, "working_adults": 2, "children": 3},
}


def normalize_header_for_lookup(header: str) -> str:
    """
    Normalize a header string to match FAMILY_CONFIG_MAP keys.
    """

    normalized = header.replace(" - ", " ")
    normalized = " ".join(normalized.split())

    # Canonicalize working adult formats
    normalized = normalized.replace("(Both Working)", "").strip()

    # Remove explicit zero-child cases
    normalized = normalized.replace(
        " 0 Children", "").replace(" 0 Child", "").strip()

    return normalized


def get_family_config_metadata(header: str) -> dict[str, int] | None:
    """
    Look up family configuration metadata for a header.

    Args:
        header: Header string from DataFrame column

    Returns:
        Dict with keys: adults, working_adults, children. Returns None if not found.
    """
    normalized = normalize_header_for_lookup(header)
    return FAMILY_CONFIG_MAP.get(normalized)

CATEGORY_MAP = {
    # Wage categories
    "living wage": "living",
    "poverty wage": "poverty",
    "minimum wage": "minimum",

    # Expense categories
    "food": "food",
    "child care": "childcare",
    "childcare": "childcare",
    "housing": "housing",
    "transportation": "transportation",
    "medical": "healthcare",
    "medical care": "healthcare",
    "health care": "healthcare",
    "other": "other",
    "civic": "civic",
    "internet & mobile": "internet_mobile",
    "internet_mobile": "internet_mobile",

    # Derived income categories
    "required annual income after taxes": "required_after_tax",
    "annual taxes": "annual_taxes",
    "required annual income before taxes": "required_before_tax",
}

def normalize_category_key(text: str) -> str:
    """
    Normalize raw category text into a lookup key.
    """
    raw = str(text).strip().lower()

    # Clean multiple spaces / punctuation to a single space
    cleaned = re.sub(r"[^\w]+", " ", raw).strip()

    return cleaned


def lookup_category_value(key: str) -> str:
    """
    Return the canonical category value if known, otherwise fallback to slugified key.
    """
    normalized = normalize_category_key(key)

    if normalized in CATEGORY_MAP:
        return CATEGORY_MAP[normalized]

    # Fallback slug
    return normalized.replace(" ", "_")