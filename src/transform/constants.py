"""
Constants for transform operations.
"""

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
